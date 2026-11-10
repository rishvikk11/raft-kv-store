package kv

// Retried requests. A client whose reply was lost cannot tell whether its write
// happened, so it must be able to send the SAME request again safely.

import (
	"net/http"
	"strconv"
	"testing"
)

func id(client string, seq int) map[string]string {
	return map[string]string{"Client-Id": client, "Request-Id": strconv.Itoa(seq)}
}

func mustCode(t *testing.T, got, want int, what string) {
	t.Helper()
	if got != want {
		t.Fatalf("%s = %d; want %d", what, got, want)
	}
}

func mustGet(t *testing.T, lc *liveCluster, via int, key, want string) {
	t.Helper()
	if code, body := lc.do(via, "GET", key, ""); code != 200 || body != want {
		t.Fatalf("GET %s = %d %q; want 200 %q", key, code, body, want)
	}
}

// The classic bug. A's write succeeded but its reply was lost. B then wrote a
// newer value. A's retry must NOT drag the key back to A's old value.
func TestRetriedWriteIsNotAppliedAgain(t *testing.T) {
	lc := startCluster(t, 3)
	lc.leader()

	code, _ := lc.doHdr(0, "PUT", "x", "from-A", id("A", 1))
	mustCode(t, code, http.StatusNoContent, "A's first PUT")
	code, _ = lc.doHdr(1, "PUT", "x", "from-B", id("B", 1))
	mustCode(t, code, http.StatusNoContent, "B's PUT")

	code, _ = lc.doHdr(2, "PUT", "x", "from-A", id("A", 1)) // A's retry, same request number
	mustCode(t, code, http.StatusNoContent, "A's retry (should look like success)")
	mustGet(t, lc, 0, "x", "from-B")

	// For contrast: with no request ID, the same repeat IS applied again.
	code, _ = lc.do(0, "PUT", "x", "from-A")
	mustCode(t, code, http.StatusNoContent, "PUT without request ID")
	mustGet(t, lc, 0, "x", "from-A")
}

// A retried DELETE must repeat the original answer, not "not found".
func TestRetriedDeleteRepeatsTheOriginalAnswer(t *testing.T) {
	lc := startCluster(t, 3)
	lc.leader()
	lc.do(0, "PUT", "k", "v")

	code, _ := lc.doHdr(0, "DELETE", "k", "", id("A", 1))
	mustCode(t, code, http.StatusNoContent, "first DELETE")
	code, _ = lc.doHdr(1, "DELETE", "k", "", id("A", 1))
	mustCode(t, code, http.StatusNoContent, "retried DELETE")

	code, _ = lc.doHdr(2, "DELETE", "k", "", id("B", 1)) // a different client, genuinely nothing to delete
	mustCode(t, code, http.StatusNotFound, "another client's DELETE")
}

func TestOldRequestNumberIsRejected(t *testing.T) {
	lc := startCluster(t, 3)
	lc.leader()
	code, _ := lc.doHdr(0, "PUT", "a", "1", id("A", 5))
	mustCode(t, code, http.StatusNoContent, "PUT #5")
	code, _ = lc.doHdr(0, "PUT", "a", "old", id("A", 3))
	mustCode(t, code, http.StatusConflict, "PUT #3 after #5")
	mustGet(t, lc, 0, "a", "1")
}

func TestBadRequestIDHeaders(t *testing.T) {
	lc := startCluster(t, 3)
	lc.leader()
	for name, hdr := range map[string]map[string]string{
		"only Client-Id":  {"Client-Id": "A"},
		"only Request-Id": {"Request-Id": "1"},
		"non-numeric":     {"Client-Id": "A", "Request-Id": "abc"},
		"zero":            {"Client-Id": "A", "Request-Id": "0"},
		"negative":        {"Client-Id": "A", "Request-Id": "-4"},
	} {
		code, _ := lc.doHdr(0, "PUT", "k", "v", hdr)
		mustCode(t, code, http.StatusBadRequest, name)
	}
}

// The memory of "A's request 1 is done" lives in the replicated state, so a
// NEW leader knows it too.
func TestRetryAfterLeaderCrashIsStillRecognised(t *testing.T) {
	lc := startCluster(t, 3)
	leader := lc.leader()
	lc.doHdr(leader, "PUT", "x", "from-A", id("A", 1))
	lc.doHdr(leader, "PUT", "x", "from-B", id("B", 1))

	lc.stop(leader)
	survivor := (leader + 1) % 3
	code, _ := lc.doHdr(survivor, "PUT", "x", "from-A", id("A", 1)) // A retries against the new leader
	mustCode(t, code, http.StatusNoContent, "retry after the leader died")
	mustGet(t, lc, survivor, "x", "from-B")
}

// ...and it is rebuilt from disk after every node restarts.
func TestRetryAfterFullRestartIsStillRecognised(t *testing.T) {
	lc := startCluster(t, 3)
	lc.leader()
	lc.doHdr(0, "PUT", "x", "from-A", id("A", 1))
	lc.doHdr(0, "PUT", "x", "from-B", id("B", 1))

	for i := 0; i < 3; i++ {
		lc.stop(i)
	}
	for i := 0; i < 3; i++ {
		lc.restart(i)
	}
	lc.leader()
	code, _ := lc.doHdr(0, "PUT", "x", "from-A", id("A", 1))
	mustCode(t, code, http.StatusNoContent, "retry after full restart")
	mustGet(t, lc, 0, "x", "from-B")
}