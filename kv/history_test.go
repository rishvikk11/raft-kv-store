package kv

// The referee. After a messy run, was every answer a client was given allowed?
//
// The workload is built so this can be checked simply: each key has exactly ONE
// writer, which writes 1, 2, 3, ... in order, waiting for each to be
// acknowledged before the next. So for any read of that key we know exactly
// which answers are allowed:
//
//	too old:     a write that was ACKNOWLEDGED before the read began must be visible.
//	from the future: a value whose write had not even STARTED before the read ended is impossible.
//	going backwards: if one read finished before another began, the later one cannot see an older value.
//
// Reading a key that was never written returns 0.

import (
	"fmt"
	"sort"
	"testing"
	"time"
)

type writeRec struct {
	v           int
	invoke, ack time.Time // when the client first sent it, and when it was told "done"
}

type readRec struct {
	v            int // 0 = key absent
	invoke, resp time.Time
}

func checkHistory(writes map[string][]writeRec, reads map[string][]readRec) []string {
	var bad []string
	add := func(format string, a ...any) {
		if len(bad) < 10 {
			bad = append(bad, fmt.Sprintf(format, a...))
		}
	}
	for key, rs := range reads {
		ws := append([]writeRec(nil), writes[key]...)
		sort.Slice(ws, func(i, j int) bool { return ws[i].v < ws[j].v })

		// Reads ordered by when they finished, with the biggest value seen so far.
		byResp := append([]readRec(nil), rs...)
		sort.Slice(byResp, func(i, j int) bool { return byResp[i].resp.Before(byResp[j].resp) })
		prefixMax := make([]int, len(byResp))
		for i, r := range byResp {
			prefixMax[i] = r.v
			if i > 0 && prefixMax[i-1] > r.v {
				prefixMax[i] = prefixMax[i-1]
			}
		}

		for _, r := range rs {
			acked := sort.Search(len(ws), func(i int) bool { return !ws[i].ack.Before(r.invoke) })
			started := sort.Search(len(ws), func(i int) bool { return !ws[i].invoke.Before(r.resp) })
			low, high := 0, 0
			if acked > 0 {
				low = ws[acked-1].v
			}
			if started > 0 {
				high = ws[started-1].v
			}
			switch {
			case r.v < low:
				add("key %q: read returned %d, but write %d had already been acknowledged (stale read)", key, r.v, low)
			case r.v > high:
				add("key %q: read returned %d, but only writes up to %d had started (value from the future)", key, r.v, high)
			}
			n := sort.Search(len(byResp), func(i int) bool { return !byResp[i].resp.Before(r.invoke) })
			if n > 0 && prefixMax[n-1] > r.v {
				add("key %q: read returned %d after an earlier, finished read had returned %d (went backwards)", key, r.v, prefixMax[n-1])
			}
		}
	}
	return bad
}

// ---- the referee must itself be tested: it has to say YES to good histories and NO to bad ones ----

var t0 = time.Unix(1000, 0)

func at(ms int) time.Time { return t0.Add(time.Duration(ms) * time.Millisecond) }

// write 1 runs 0-10ms, write 2 runs 20-30ms, write 3 starts at 40ms and is still in flight.
func sampleWrites() map[string][]writeRec {
	return map[string][]writeRec{"k": {
		{v: 1, invoke: at(0), ack: at(10)},
		{v: 2, invoke: at(20), ack: at(30)},
		{v: 3, invoke: at(40), ack: at(1000)},
	}}
}

func TestRefereeAcceptsAGoodHistory(t *testing.T) {
	reads := map[string][]readRec{"k": {
		{v: 0, invoke: at(1), resp: at(2)},   // during write 1: old (absent) or new are both fine
		{v: 1, invoke: at(5), resp: at(6)},   // during write 1
		{v: 1, invoke: at(12), resp: at(13)}, // after write 1 was acknowledged
		{v: 2, invoke: at(35), resp: at(36)}, // after write 2
		{v: 2, invoke: at(50), resp: at(60)}, // write 3 in flight: 2 is fine
		{v: 3, invoke: at(55), resp: at(65)}, // ...and so is 3
	}}
	if bad := checkHistory(sampleWrites(), reads); len(bad) > 0 {
		t.Fatalf("a valid history was rejected: %v", bad)
	}
}

func TestRefereeCatchesAStaleRead(t *testing.T) {
	reads := map[string][]readRec{"k": {{v: 1, invoke: at(35), resp: at(36)}}} // write 2 was acknowledged at 30ms
	if bad := checkHistory(sampleWrites(), reads); len(bad) == 0 {
		t.Fatal("a stale read was not caught")
	}
}

func TestRefereeCatchesAnAbsentValueAfterAnAck(t *testing.T) {
	reads := map[string][]readRec{"k": {{v: 0, invoke: at(15), resp: at(16)}}} // write 1 acknowledged at 10ms
	if bad := checkHistory(sampleWrites(), reads); len(bad) == 0 {
		t.Fatal("reading 'absent' after an acknowledged write was not caught")
	}
}

func TestRefereeCatchesAValueFromTheFuture(t *testing.T) {
	reads := map[string][]readRec{"k": {{v: 3, invoke: at(21), resp: at(22)}}} // write 3 starts at 40ms
	if bad := checkHistory(sampleWrites(), reads); len(bad) == 0 {
		t.Fatal("a value from the future was not caught")
	}
}

func TestRefereeCatchesGoingBackwards(t *testing.T) {
	// Both reads happen while write 3 is in flight, so 2 and 3 are each allowed on their own.
	// But 3 was seen, that read FINISHED, and then a later read saw 2: time went backwards.
	reads := map[string][]readRec{"k": {
		{v: 3, invoke: at(50), resp: at(51)},
		{v: 2, invoke: at(52), resp: at(53)},
	}}
	if bad := checkHistory(sampleWrites(), reads); len(bad) == 0 {
		t.Fatal("a new-then-old inversion was not caught")
	}
}