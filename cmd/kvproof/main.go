// Command kvproof proves, with Docker, that the cluster's saved state survives a
// crash. It runs the experiment itself and prints PASS or FAIL for each claim:
//
//  1. start 3 containers, each with its own named volume
//  2. keep writing to the cluster the whole time
//  3. docker kill the LEADER in the middle of the traffic (an abrupt crash)
//  4. check the cluster keeps accepting writes without it
//  5. start the same container again: it must come back with the ledger it had
//     saved in its volume, then catch up with the others
//  6. delete the container completely and create a new one on the same volume:
//     the state must still be there (the container is disposable, the volume is not)
//  7. check every write that was ever acknowledged is still in the cluster
//
// Usage (from the repo root, with Docker running):  go run ./cmd/kvproof
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"
)

type nodeStatus struct {
	RecoveredEntries int    `json:"recoveredEntries"` // what the process read from its own disk at startup
	RecoveredTerm    int    `json:"recoveredTerm"`
	ID               int    `json:"id"`
	Term             int    `json:"term"`
	Role             string `json:"role"`
	LeaderID         int    `json:"leaderId"`
	CommitIndex      int    `json:"commitIndex"`
	LastApplied      int    `json:"lastApplied"`
	LastLogIndex     int    `json:"lastLogIndex"`
}

type proof struct {
	compose string
	kctx    string // kubectl context (Kubernetes target)
	kns     string // kubectl namespace (Kubernetes target)
	urls    []string
	client  *http.Client // follows redirects to the leader

	mu     sync.Mutex
	acked  []acked
	stop   chan struct{}
	wg     sync.WaitGroup
	failed []string
	passed int
}

type acked struct{ key, value string }

func say(format string, a ...any) { fmt.Printf(format+"\n", a...) }

func (p *proof) step(n int, title, why string) {
	say("\n=== Step %d: %s", n, title)
	if why != "" {
		say("    why: %s", why)
	}
}

func (p *proof) check(ok bool, what string, detail ...any) {
	d := ""
	if len(detail) > 0 {
		d = "  (" + fmt.Sprintf(detail[0].(string), detail[1:]...) + ")"
	}
	if ok {
		p.passed++
		say("    PASS  %s%s", what, d)
	} else {
		p.failed = append(p.failed, what)
		say("    FAIL  %s%s", what, d)
	}
}

// docker runs `docker compose -f FILE args...` and shows its output, minus the
// numbered build-step lines, which are very long and not interesting here.
func (p *proof) docker(args ...string) error {
	say("    $ docker compose %s", strings.Join(args, " "))
	cmd := exec.Command("docker", append([]string{"compose", "-f", p.compose}, args...)...)
	out, err := cmd.CombinedOutput()
	for _, line := range strings.Split(strings.TrimRight(string(out), "\n"), "\n") {
		if line != "" && !strings.HasPrefix(line, "#") {
			say("      | %s", line)
		}
	}
	return err
}

func (p *proof) dockerPlain(args ...string) {
	say("    $ docker %s", strings.Join(args, " "))
	out, _ := exec.Command("docker", args...).CombinedOutput()
	for _, line := range strings.Split(strings.TrimRight(string(out), "\n"), "\n") {
		if line != "" {
			say("      | %s", line)
		}
	}
}

func (p *proof) status(i int) (nodeStatus, error) {
	var st nodeStatus
	c := &http.Client{Timeout: time.Second}
	resp, err := c.Get(p.urls[i] + "/raft/status")
	if err != nil {
		return st, err
	}
	defer resp.Body.Close()
	return st, json.NewDecoder(resp.Body).Decode(&st)
}

func (p *proof) leader() (int, nodeStatus, bool) {
	best, bestTerm := -1, -1
	var bs nodeStatus
	for i := range p.urls {
		if st, err := p.status(i); err == nil && st.Role == "leader" && st.Term > bestTerm {
			best, bestTerm, bs = i, st.Term, st
		}
	}
	return best, bs, best >= 0
}

func waitUntil(d time.Duration, cond func() bool) bool {
	for end := time.Now().Add(d); time.Now().Before(end); time.Sleep(50 * time.Millisecond) {
		if cond() {
			return true
		}
	}
	return false
}

// firstStatus polls a node as fast as it can and returns its very FIRST answer.
// For a node that was just restarted this is the state it came back with,
// before it has had time to hear from anyone.
func (p *proof) firstStatus(i int, d time.Duration) (nodeStatus, bool) {
	var st nodeStatus
	ok := false
	for end := time.Now().Add(d); time.Now().Before(end); time.Sleep(10 * time.Millisecond) {
		if s, err := p.status(i); err == nil {
			return s, true
		}
	}
	return st, ok
}

func (p *proof) ackedCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.acked)
}

// leaderWithCommits waits until some node is leader AND has committed at least
// one entry, then returns its status. If no leader can be found it says so
// instead of returning zeros: comparing against zero would make a check pass
// without proving anything.
func (p *proof) leaderWithCommits(d time.Duration) (nodeStatus, bool) {
	var st nodeStatus
	ok := waitUntil(d, func() bool {
		_, s, found := p.leader()
		if found && s.CommitIndex > 0 {
			st = s
			return true
		}
		return false
	})
	return st, ok
}

// writer writes key/value pairs one at a time, retrying each (with the SAME
// request number, so a retry can never apply twice) until the cluster says done.
func (p *proof) writer(id int, runID string) {
	defer p.wg.Done()
	for n := 1; ; n++ {
		select {
		case <-p.stop:
			return
		default:
		}
		key, val := fmt.Sprintf("proof-w%d-%d", id, n), fmt.Sprintf("value-%d-%d", id, n)
		done := false
		for try, deadline := 0, time.Now().Add(90*time.Second); time.Now().Before(deadline); try++ {
			// A different node on every attempt, so a dead node can never trap a writer.
			req, _ := http.NewRequest("PUT", p.urls[(id+n+try)%len(p.urls)]+"/kv/"+key, strings.NewReader(val))
			req.Header.Set("Client-Id", fmt.Sprintf("proof-%s-%d", runID, id))
			req.Header.Set("Request-Id", strconv.Itoa(n))
			if resp, err := p.client.Do(req); err == nil {
				io.Copy(io.Discard, resp.Body)
				resp.Body.Close()
				if resp.StatusCode == http.StatusNoContent {
					done = true
					break
				}
			}
			time.Sleep(25 * time.Millisecond)
		}
		if !done {
			return
		}
		p.mu.Lock()
		p.acked = append(p.acked, acked{key, val})
		p.mu.Unlock()
	}
}

func (p *proof) converged() bool {
	var first nodeStatus
	for i := range p.urls {
		st, err := p.status(i)
		if err != nil || st.LastApplied != st.CommitIndex || st.CommitIndex == 0 {
			return false
		}
		if i == 0 {
			first = st
		} else if st.CommitIndex != first.CommitIndex || st.LastLogIndex != first.LastLogIndex {
			return false
		}
	}
	return true
}

// verifyAll stops the writers and checks the promise: everything the cluster
// ever acknowledged is still there, and all three nodes agree.
func (p *proof) verifyAll(stepNo int) {
	p.step(stepNo, "stop writing and check nothing acknowledged was lost", "this is the promise: if the cluster said a write was saved, it must still be there")
	close(p.stop)
	p.wg.Wait()
	same := waitUntil(60*time.Second, p.converged)
	p.check(same, "all three nodes ended with identical ledgers, fully applied")
	p.mu.Lock()
	all := append([]acked(nil), p.acked...)
	p.mu.Unlock()
	missing, wrong := 0, 0
	for _, a := range all {
		var got string
		found := false
		for try := 0; try < 40 && !found; try++ {
			resp, err := p.client.Get(p.urls[try%len(p.urls)] + "/kv/" + a.key)
			if err != nil {
				time.Sleep(50 * time.Millisecond)
				continue
			}
			b, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				got, found = string(b), true
			} else if resp.StatusCode == http.StatusNotFound {
				break
			} else {
				time.Sleep(50 * time.Millisecond)
			}
		}
		switch {
		case !found:
			missing++
		case got != a.value:
			wrong++
		}
	}
	p.check(missing == 0 && wrong == 0, "every acknowledged write is still in the cluster",
		"%d writes checked, %d missing, %d with the wrong value", len(all), missing, wrong)

}

// runDocker is the Docker Compose experiment (see the package comment).
func runDocker(composeFile string, urls []string, victimFlag string, keep, fresh bool, writers int, downFor time.Duration) {
	if _, err := exec.LookPath("docker"); err != nil {
		fmt.Fprintln(os.Stderr, "docker was not found. Install Docker Desktop and make sure it is running.")
		os.Exit(2)
	}
	p := &proof{
		compose: composeFile, urls: urls, stop: make(chan struct{}),
		client: &http.Client{Timeout: 1500 * time.Millisecond},
	}
	runID := strconv.FormatInt(time.Now().UnixNano(), 36)

	// ---- Step 1 ----
	p.step(1, "start 3 containers, each with its own volume", "each node's saved state (term, vote, ledger) will live in a volume, not in the container")
	if fresh {
		p.docker("down", "-v", "--remove-orphans")
	}
	if err := p.docker("up", "-d", "--build"); err != nil {
		say("    docker compose up failed: %v", err)
		say("    (run `docker compose logs` to see why)")
		os.Exit(1)
	}
	up := waitUntil(60*time.Second, func() bool {
		_, _, ok := p.leader()
		if !ok {
			return false
		}
		for i := range p.urls {
			if _, err := p.status(i); err != nil {
				return false
			}
		}
		return true
	})
	p.check(up, "all three nodes are reachable and the cluster elected a leader")
	if !up {
		say("\nThe cluster did not come up. Try: docker compose logs")
		os.Exit(1)
	}
	p.dockerPlain("volume", "ls", "--filter", "name=raftkv")

	// ---- Step 2 ----
	p.step(2, "start continuous writing", fmt.Sprintf("%d clients write one value after another; a write only counts once the cluster says it is saved", writers))
	for w := 0; w < writers; w++ {
		p.wg.Add(1)
		go p.writer(w, runID)
	}
	flowing := waitUntil(30*time.Second, func() bool { return p.ackedCount() >= 50 })
	p.check(flowing, "writes are flowing before any failure", "%d acknowledged so far", p.ackedCount())

	// ---- Step 3 ----
	leaderID, _, _ := p.leader()
	victim := leaderID
	if victimFlag != "leader" {
		v, err := strconv.Atoi(victimFlag)
		if err != nil || v < 0 || v >= len(p.urls) {
			say("bad -victim %q", victimFlag)
			os.Exit(2)
		}
		victim = v
	}
	svc := fmt.Sprintf("kv%d", victim)
	before, err := p.status(victim)
	p.step(3, fmt.Sprintf("docker kill %s (node %d, role: %s) in the middle of the traffic", svc, victim, before.Role), "SIGKILL: the process gets no chance to tidy up, like a crash")
	if err != nil {
		say("    could not read the victim's status before the kill: %v", err)
		os.Exit(1)
	}
	say("    just before the kill: node %d had %d ledger entries saved (term %d)", victim, before.LastLogIndex, before.Term)
	if err := p.docker("kill", svc); err != nil {
		say("    kill failed: %v", err)
		os.Exit(1)
	}
	// The kill command takes a moment, and writes keep flowing until the process
	// really dies. So the clock starts only once the node is confirmed down.
	down := waitUntil(10*time.Second, func() bool { _, err := p.status(victim); return err != nil })
	deathAt := time.Now()
	ackedAtDeath := p.ackedCount()
	p.check(down, fmt.Sprintf("%s is really down", svc))

	// ---- Step 4 ----
	p.step(4, "the other two nodes keep the cluster working while it stays down", "two of three is still a majority: a new leader must be elected and writes must continue")
	var newLeaderID, newTerm int
	elected := waitUntil(30*time.Second, func() bool {
		id, st, ok := p.leader()
		if ok && id != victim && st.Term > before.Term {
			newLeaderID, newTerm = id, st.Term
			return true
		}
		return false
	})
	failover := time.Since(deathAt)
	if elected {
		p.check(true, "a new leader was elected without the killed node",
			"node %d, term %d, found %v after the node was confirmed down", newLeaderID, newTerm, failover.Round(time.Millisecond))
	} else {
		p.check(false, "a new leader was elected without the killed node", "none appeared within 30s")
	}
	holdUntil := deathAt.Add(downFor)
	enough := waitUntil(30*time.Second, func() bool {
		return time.Now().After(holdUntil) && p.ackedCount() >= ackedAtDeath+50
	})
	ackedBeforeStart := p.ackedCount()
	downTime := time.Since(deathAt)
	acksWhileDown := ackedBeforeStart - ackedAtDeath
	p.check(enough, "writes kept being acknowledged while the node was down",
		"%d writes acknowledged during the %v it was down (needs at least 50)", acksWhileDown, downTime.Round(10*time.Millisecond))

	// ---- Step 5 ----
	p.step(5, fmt.Sprintf("start %s again (same container, same volume)", svc), "the program starts from nothing, so anything it remembers must have come from the volume")
	if err := p.docker("start", svc); err != nil {
		say("    start failed: %v", err)
		os.Exit(1)
	}
	startAt := time.Now()
	after, ok := p.firstStatus(victim, 30*time.Second)
	say("    on startup it read %d ledger entries and term %d from its own volume", after.RecoveredEntries, after.RecoveredTerm)
	p.check(ok && after.RecoveredEntries >= before.LastLogIndex && after.RecoveredEntries > 0,
		"it read back the ledger it had saved before the crash, from its volume",
		"had %d entries just before the kill, read %d from disk; a node with an empty volume reads 0", before.LastLogIndex, after.RecoveredEntries)
	p.check(ok && after.RecoveredTerm >= before.Term, "it remembered its term from disk (it did not go backwards)", "was %d, read %d", before.Term, after.RecoveredTerm)
	ls, haveLeader := p.leaderWithCommits(20 * time.Second)
	p.check(haveLeader && ls.CommitIndex > after.RecoveredEntries,
		"the rest of the cluster had moved ahead of what the node had saved",
		"the cluster had committed %d entries; the node's volume held %d, so catching up is real work", ls.CommitIndex, after.RecoveredEntries)
	caught := haveLeader && waitUntil(60*time.Second, func() bool {
		st, err := p.status(victim)
		return err == nil && st.CommitIndex >= ls.CommitIndex && st.LastApplied == st.CommitIndex
	})
	grown, _ := p.status(victim)
	p.check(caught, "it caught up with the rest of the cluster",
		"took %v; its ledger grew from %d entries (read from disk) to %d", time.Since(startAt).Round(10*time.Millisecond), after.RecoveredEntries, grown.LastLogIndex)

	// ---- Step 6 ----
	before2, _ := p.status(victim)
	p.step(6, fmt.Sprintf("delete the %s container completely, then create a brand-new one", svc), "if the saved state lived inside the container it would be gone now; it lives in the volume")
	say("    before: node %d has %d ledger entries (it had %d when it started up, so this includes entries it received after restarting)", victim, before2.LastLogIndex, after.RecoveredEntries)
	p.docker("rm", "-sf", svc)
	p.docker("ps", "-a")
	p.dockerPlain("volume", "ls", "--filter", "name=raftkv")
	say("    (the container is gone; its volume is still listed above)")
	if err := p.docker("up", "-d", "--no-deps", svc); err != nil {
		say("    up failed: %v", err)
		os.Exit(1)
	}
	after2, ok2 := p.firstStatus(victim, 30*time.Second)
	say("    the new container read %d ledger entries and term %d from the volume", after2.RecoveredEntries, after2.RecoveredTerm)
	p.check(ok2 && after2.RecoveredEntries >= before2.LastLogIndex && after2.RecoveredEntries > after.RecoveredEntries,
		"a brand-new container read the same ledger from the volume, including what was saved after the first restart",
		"had %d, read %d (the first restart read %d)", before2.LastLogIndex, after2.RecoveredEntries, after.RecoveredEntries)
	ls2, haveLeader2 := p.leaderWithCommits(20 * time.Second)
	caught2 := haveLeader2 && waitUntil(60*time.Second, func() bool {
		st, err := p.status(victim)
		return err == nil && st.CommitIndex >= ls2.CommitIndex && st.LastApplied == st.CommitIndex
	})
	p.check(caught2, "and it caught up again", "cluster had committed %d entries", ls2.CommitIndex)

	p.verifyAll(7)

	if len(p.failed) > 0 {
		say("\nsomething failed, so the cluster is being LEFT RUNNING for inspection. Recent logs:")
		p.docker("logs", "--tail", "25")
		say("clean up with: docker compose down -v")
	} else if !keep {
		say("\ncleaning up: docker compose down (the volumes are kept; add -v to delete them)")
		p.docker("down")
	}
	say("\nwhat was measured on this run: %d writes acknowledged in total; %d of them while the node was down; a new leader was found %v after the kill", p.ackedCount(), acksWhileDown, failover.Round(time.Millisecond))
	say("\n==== %d checks passed, %d failed ====", p.passed, len(p.failed))
	if len(p.failed) > 0 {
		for _, f := range p.failed {
			say("  FAILED: %s", f)
		}
		os.Exit(1)
	}
	say("Saved state survived an abrupt kill and a full container replacement.")
}

func main() {
	target := flag.String("target", "docker", "docker (Docker Compose) or k8s (Kubernetes in kind)")
	compose := flag.String("compose", "docker-compose.yml", "docker target: path to the compose file")
	nodes := flag.String("nodes", "", "URLs of the three nodes as seen from this machine (default depends on -target)")
	victimFlag := flag.String("victim", "", "which node to kill: leader, or a node number (default: the leader for docker and for k8s restart, a follower for k8s disk-loss)")
	keep := flag.Bool("keep", false, "docker target: leave the cluster running when finished")
	fresh := flag.Bool("fresh", true, "docker target: begin by deleting old containers AND volumes (docker compose down -v)")
	writers := flag.Int("writers", 4, "number of concurrent writers")
	downFor := flag.Duration("down-for", 3*time.Second, "docker target: keep the killed node down at least this long, with traffic still flowing")
	hold := flag.Duration("hold", 4*time.Second, "k8s target: keep the replacement pod from starting for this long (the node is cordoned), so only two nodes serve traffic; 0 turns this off")
	scenario := flag.String("scenario", "restart", "k8s target: restart (pod deleted, its disk kept) or disk-loss (pod AND its disk deleted)")
	kctx := flag.String("context", "kind-raftkv", "k8s target: kubectl context")
	namespace := flag.String("namespace", "raftkv", "k8s target: namespace")
	serviceURL := flag.String("service-url", "http://localhost:30080", "k8s target: address of the client Service as seen from this machine")
	flag.Parse()

	switch *target {
	case "docker":
		n := *nodes
		if n == "" {
			n = "http://localhost:8081,http://localhost:8082,http://localhost:8083"
		}
		v := *victimFlag
		if v == "" {
			v = "leader"
		}
		runDocker(*compose, strings.Split(n, ","), v, *keep, *fresh, *writers, *downFor)
	case "k8s":
		n := *nodes
		if n == "" {
			n = "http://localhost:30081,http://localhost:30082,http://localhost:30083"
		}
		runK8s(k8sOptions{
			scenario: *scenario, context: *kctx, namespace: *namespace, serviceURL: *serviceURL,
			urls: strings.Split(n, ","), victim: *victimFlag, writers: *writers, hold: *hold,
		})
	default:
		fmt.Fprintf(os.Stderr, "unknown -target %q (use docker or k8s)\n", *target)
		os.Exit(2)
	}
}