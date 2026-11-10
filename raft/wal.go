package raft

// The write-ahead log (WAL) is the file a node writes to BEFORE it answers any
// message. It is append-only: we never edit old bytes, we only add records to
// the end. To recover after a crash we replay the records from the top.
//
// Three kinds of record:
//
//	hard state  the node's currentTerm and votedFor
//	entry       one ledger line (index, term, command)
//	truncate    "forget this ledger line and everything after it"
//	            (a follower writes this when the leader overrides its log)
//
// Each record on disk is a frame: [4-byte length][4-byte CRC][payload].
// The CRC lets us notice a half-written record after a crash.

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
)

const walFileName = "raft.wal"

type recKind byte

const (
	recHardState recKind = 1
	recEntry     recKind = 2
	recTruncate  recKind = 3
)

type record struct {
	kind     recKind
	index    int    // recEntry, recTruncate
	term     int    // recHardState (currentTerm), recEntry (the entry's term)
	votedFor int    // recHardState only; -1 means "nobody"
	command  []byte // recEntry only
}

func (r record) encode() []byte {
	var p []byte
	switch r.kind {
	case recHardState:
		p = make([]byte, 17)
		binary.BigEndian.PutUint64(p[1:], uint64(r.term))
		binary.BigEndian.PutUint64(p[9:], uint64(int64(r.votedFor)))
	case recEntry:
		p = make([]byte, 17+len(r.command))
		binary.BigEndian.PutUint64(p[1:], uint64(r.index))
		binary.BigEndian.PutUint64(p[9:], uint64(r.term))
		copy(p[17:], r.command)
	case recTruncate:
		p = make([]byte, 9)
		binary.BigEndian.PutUint64(p[1:], uint64(r.index))
	default:
		panic("raft: unknown record kind")
	}
	p[0] = byte(r.kind)
	frame := make([]byte, 8+len(p))
	binary.BigEndian.PutUint32(frame[0:], uint32(len(p)))
	binary.BigEndian.PutUint32(frame[4:], crc32.ChecksumIEEE(p))
	copy(frame[8:], p)
	return frame
}

// persistedState is what we rebuild from the WAL on startup.
// log[0] is a placeholder entry (term 0) so real entries start at index 1,
// matching the Raft paper's 1-based numbering.
type persistedState struct {
	term     int
	votedFor int
	log      []Entry
}

func (s *persistedState) apply(p []byte) error {
	if len(p) == 0 {
		return errors.New("empty record")
	}
	switch recKind(p[0]) {
	case recHardState:
		if len(p) != 17 {
			return fmt.Errorf("hard-state record has %d bytes, want 17", len(p))
		}
		s.term = int(binary.BigEndian.Uint64(p[1:]))
		s.votedFor = int(int64(binary.BigEndian.Uint64(p[9:])))
	case recEntry:
		if len(p) < 17 {
			return fmt.Errorf("entry record too short (%d bytes)", len(p))
		}
		index := int(binary.BigEndian.Uint64(p[1:]))
		term := int(binary.BigEndian.Uint64(p[9:]))
		if index != len(s.log) {
			return fmt.Errorf("entry index %d does not follow log length %d", index, len(s.log))
		}
		s.log = append(s.log, Entry{Term: term, Command: append([]byte(nil), p[17:]...)})
	case recTruncate:
		if len(p) != 9 {
			return fmt.Errorf("truncate record has %d bytes, want 9", len(p))
		}
		index := int(binary.BigEndian.Uint64(p[1:]))
		if index < 1 || index > len(s.log) {
			return fmt.Errorf("truncate index %d out of range (log length %d)", index, len(s.log))
		}
		s.log = s.log[:index]
	default:
		return fmt.Errorf("unknown record kind %d", p[0])
	}
	return nil
}

// ErrCorruptWAL means damage in the middle of the file, which is different from
// the harmless half-written record a crash leaves at the very end.
var ErrCorruptWAL = errors.New("raft: WAL is corrupt")

// replay applies every complete, valid record in data to st and returns how many
// bytes were valid. A damaged FINAL record is treated as a crash mid-write and
// ignored; damage anywhere earlier is an error, because silently dropping
// records from the middle could make a node forget something it promised.
func replay(data []byte, st *persistedState) (int, error) {
	off := 0
	for off+8 <= len(data) {
		n := int(binary.BigEndian.Uint32(data[off:]))
		end := off + 8 + n
		if n == 0 || end > len(data) {
			break // length runs past the end of the file: a torn final write
		}
		payload := data[off+8 : end]
		if crc32.ChecksumIEEE(payload) != binary.BigEndian.Uint32(data[off+4:]) {
			if end == len(data) {
				break // bad CRC on the very last record: torn final write
			}
			return 0, fmt.Errorf("%w: checksum mismatch at byte %d", ErrCorruptWAL, off)
		}
		if err := st.apply(payload); err != nil {
			return 0, fmt.Errorf("%w: record at byte %d: %v", ErrCorruptWAL, off, err)
		}
		off = end
	}
	return off, nil
}

type wal struct {
	f     *os.File
	fsync bool
}

// openWAL opens (or creates) the WAL in dir and returns the state it contains.
func openWAL(dir string, fsync bool) (*wal, *persistedState, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, nil, err
	}
	path := filepath.Join(dir, walFileName)
	_, statErr := os.Stat(path)
	created := errors.Is(statErr, os.ErrNotExist)

	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0o644)
	if err != nil {
		return nil, nil, err
	}
	data, err := io.ReadAll(f)
	if err != nil {
		f.Close()
		return nil, nil, err
	}
	st := &persistedState{votedFor: -1, log: []Entry{{}}}
	valid, err := replay(data, st)
	if err != nil {
		f.Close()
		return nil, nil, err
	}
	if valid < len(data) { // cut off the torn tail so new records start cleanly
		if err := f.Truncate(int64(valid)); err != nil {
			f.Close()
			return nil, nil, err
		}
		if fsync {
			if err := f.Sync(); err != nil {
				f.Close()
				return nil, nil, err
			}
		}
	}
	if created && fsync {
		syncDir(dir) // make the new file's existence durable too
	}
	return &wal{f: f, fsync: fsync}, st, nil
}

// commit writes the records and, unless fsync is disabled (tests only), forces
// them onto the physical disk before returning. Only after commit returns may
// the node reply to anyone.
func (w *wal) commit(recs []record) error {
	var buf []byte
	for _, r := range recs {
		buf = append(buf, r.encode()...)
	}
	if _, err := w.f.Write(buf); err != nil {
		return err
	}
	if w.fsync {
		return w.f.Sync()
	}
	return nil
}

func (w *wal) close() error { return w.f.Close() }

func syncDir(dir string) {
	if d, err := os.Open(dir); err == nil {
		d.Sync()
		d.Close()
	}
}