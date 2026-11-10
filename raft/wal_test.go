package raft

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func openTestWAL(t *testing.T, dir string) (*wal, *persistedState) {
	t.Helper()
	w, st, err := openWAL(dir, false)
	if err != nil {
		t.Fatalf("openWAL: %v", err)
	}
	return w, st
}

func TestWALFreshStartIsEmpty(t *testing.T) {
	w, st := openTestWAL(t, t.TempDir())
	defer w.close()
	if st.term != 0 || st.votedFor != -1 || len(st.log) != 1 {
		t.Fatalf("fresh state = %+v; want term 0, votedFor -1, log of just the placeholder", st)
	}
}

func TestWALRoundTrip(t *testing.T) {
	dir := t.TempDir()
	w, _ := openTestWAL(t, dir)
	err := w.commit([]record{
		{kind: recHardState, term: 3, votedFor: 2},
		{kind: recEntry, index: 1, term: 1, command: []byte("a")},
		{kind: recEntry, index: 2, term: 1, command: []byte("b")},
		{kind: recEntry, index: 3, term: 2, command: []byte("c")},
		{kind: recTruncate, index: 2}, // forget "b" and "c"
		{kind: recEntry, index: 2, term: 3, command: []byte("B")},
		{kind: recHardState, term: 4, votedFor: -1},
	})
	if err != nil {
		t.Fatal(err)
	}
	w.close()

	w, st := openTestWAL(t, dir)
	defer w.close()
	if st.term != 4 || st.votedFor != -1 {
		t.Fatalf("hard state = (%d, %d); want (4, -1)", st.term, st.votedFor)
	}
	if len(st.log) != 3 || string(st.log[1].Command) != "a" || st.log[2].Term != 3 || string(st.log[2].Command) != "B" {
		t.Fatalf("log = %+v; want [placeholder, a(t1), B(t3)]", st.log)
	}
}

func TestWALTornTailIsDiscarded(t *testing.T) {
	dir := t.TempDir()
	w, _ := openTestWAL(t, dir)
	w.commit([]record{{kind: recHardState, term: 1, votedFor: 0}, {kind: recEntry, index: 1, term: 1, command: []byte("keep")}})
	w.close()

	path := filepath.Join(dir, walFileName)
	good, _ := os.ReadFile(path)

	// Simulate a crash in the middle of writing a third record: only some of its bytes landed.
	partial := record{kind: recEntry, index: 2, term: 1, command: []byte("half-written")}.encode()
	for cut := 1; cut < len(partial); cut++ {
		os.WriteFile(path, append(append([]byte(nil), good...), partial[:cut]...), 0o644)
		w, st := openTestWAL(t, dir)
		if len(st.log) != 2 || string(st.log[1].Command) != "keep" {
			t.Fatalf("cut=%d: log = %+v; want only the complete record", cut, st.log)
		}
		// The file must have been trimmed so that new writes land cleanly.
		if err := w.commit([]record{{kind: recEntry, index: 2, term: 1, command: []byte("next")}}); err != nil {
			t.Fatal(err)
		}
		w.close()
		w, st = openTestWAL(t, dir)
		if len(st.log) != 3 || string(st.log[2].Command) != "next" {
			t.Fatalf("cut=%d: after new write log = %+v", cut, st.log)
		}
		w.close()
	}
}

func TestWALCorruptionInTheMiddleIsAnError(t *testing.T) {
	dir := t.TempDir()
	w, _ := openTestWAL(t, dir)
	w.commit([]record{
		{kind: recEntry, index: 1, term: 1, command: []byte("first")},
		{kind: recEntry, index: 2, term: 1, command: []byte("second")},
		{kind: recEntry, index: 3, term: 1, command: []byte("third")},
	})
	w.close()

	path := filepath.Join(dir, walFileName)
	data, _ := os.ReadFile(path)
	data[8+17+1] ^= 0xFF // flip a byte inside the first record's command
	os.WriteFile(path, data, 0o644)

	_, _, err := openWAL(dir, false)
	if !errors.Is(err, ErrCorruptWAL) {
		t.Fatalf("openWAL err = %v; want ErrCorruptWAL", err)
	}
}