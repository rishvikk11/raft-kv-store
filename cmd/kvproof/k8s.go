package main

// The Kubernetes target of kvproof. It deletes a pod while clients are writing
// and checks who did what:
//
//	Kubernetes  noticed the pod was gone, created a new one with the SAME name,
//	            and plugged the SAME disk (PersistentVolumeClaim) into it.
//	Raft        elected a new leader, let the returning node read its own saved
//	            ledger, and caught it up. No acknowledged write was lost.
//
// Scenario "restart":   delete the pod (the disk is kept).
// Scenario "disk-loss": delete the pod AND its disk. The pod comes back with the
//                       same name but an empty disk: a different Raft member
//                       wearing the old name. Raft cannot safely resurrect it.

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"
)

type k8sOptions struct {
	scenario, context, namespace, serviceURL string
	urls                                     []string
	victim                                   string
	writers                                  int
	hold                                     time.Duration
}

// nodeNames lists the cluster's machines (one, in kind).
func (p *proof) nodeNames() []string {
	out, err := p.kubectl("get", "nodes", "-o", "jsonpath={.items[*].metadata.name}")
	if err != nil {
		return nil
	}
	return strings.Fields(out)
}

// setCordon stops (true) or allows (false) Kubernetes to start NEW pods on the
// machines. Pods already running are not touched.
func (p *proof) setCordon(nodes []string, on bool) error {
	verb := "uncordon"
	if on {
		verb = "cordon"
	}
	for _, n := range nodes {
		if out, err := p.kubectl(verb, n); err != nil {
			return fmt.Errorf("kubectl %s %s: %v: %s", verb, n, err, out)
		}
	}
	return nil
}

// kubectl runs a kubectl command and returns its trimmed output.
func (p *proof) kubectl(args ...string) (string, error) {
	full := append([]string{"--context", p.kctx, "-n", p.kns}, args...)
	out, err := exec.Command("kubectl", full...).CombinedOutput()
	return strings.TrimSpace(string(out)), err
}

// showKubectl runs a kubectl command and prints it with its output.
func (p *proof) showKubectl(args ...string) error {
	say("    $ kubectl %s", strings.Join(args, " "))
	out, err := p.kubectl(args...)
	for _, line := range strings.Split(out, "\n") {
		if line != "" {
			say("      | %s", line)
		}
	}
	return err
}

func (p *proof) uid(kind, name string) string {
	out, err := p.kubectl("get", kind, name, "-o", "jsonpath={.metadata.uid}")
	if err != nil {
		return ""
	}
	return out
}

func (p *proof) podReady(name string) bool {
	out, err := p.kubectl("get", "pod", name, "-o", `jsonpath={.status.conditions[?(@.type=="Ready")].status}`)
	return err == nil && out == "True"
}

func short(uid string) string {
	if len(uid) > 8 {
		return uid[:8]
	}
	return uid
}

// serviceCheck sends requests through the client Service, which picks any pod.
// Whichever pod answers, followers must pass the request on to the leader.
func (p *proof) serviceCheck(serviceURL, runID string) {
	good := 0
	for i := 0; i < 20; i++ {
		key, val := fmt.Sprintf("proof-svc-%d", i), fmt.Sprintf("via-service-%d", i)
		stored := false
		for try := 0; try < 40 && !stored; try++ {
			req, _ := http.NewRequest("PUT", serviceURL+"/kv/"+key, strings.NewReader(val))
			req.Header.Set("Client-Id", "proof-svc-"+runID)
			req.Header.Set("Request-Id", strconv.Itoa(i+1))
			if resp, err := p.client.Do(req); err == nil {
				io.Copy(io.Discard, resp.Body)
				resp.Body.Close()
				stored = resp.StatusCode == http.StatusNoContent
			}
			if !stored {
				time.Sleep(50 * time.Millisecond)
			}
		}
		for try := 0; try < 40 && stored; try++ {
			resp, err := p.client.Get(serviceURL + "/kv/" + key)
			if err == nil {
				b, _ := io.ReadAll(resp.Body)
				resp.Body.Close()
				if resp.StatusCode == http.StatusOK {
					if string(b) == val {
						good++
					}
					break
				}
			}
			time.Sleep(50 * time.Millisecond)
		}
	}
	p.check(good == 20, "the client Service stored and served 20 values, whichever pod it picked", "%d of 20 round trips correct", good)
}

func runK8s(o k8sOptions) {
	if _, err := exec.LookPath("kubectl"); err != nil {
		fmt.Fprintln(os.Stderr, "kubectl was not found. Install it (brew install kubectl) and run: bash scripts/k8s.sh up")
		os.Exit(2)
	}
	if o.scenario != "restart" && o.scenario != "disk-loss" {
		fmt.Fprintf(os.Stderr, "unknown -scenario %q (use restart or disk-loss)\n", o.scenario)
		os.Exit(2)
	}
	p := &proof{
		kctx: o.context, kns: o.namespace, urls: o.urls, stop: make(chan struct{}),
		client: &http.Client{Timeout: 1500 * time.Millisecond},
	}
	nodes := p.nodeNames()
	p.setCordon(nodes, false) // a previous run that was interrupted could have left the machine cordoned
	runID := strconv.FormatInt(time.Now().UnixNano(), 36)
	diskLoss := o.scenario == "disk-loss"

	// ---- Step 1 ----
	p.step(1, "check the cluster Kubernetes is running", "three pods (kv-0, kv-1, kv-2), each with its own disk (a PersistentVolumeClaim)")
	if ready, err := p.kubectl("get", "statefulset", "kv", "-o", "jsonpath={.status.readyReplicas}"); err != nil || ready != "3" {
		say("    the StatefulSet is not up and ready (kubectl says %q, %v).", ready, err)
		say("    Start it with: bash scripts/k8s.sh up")
		os.Exit(1)
	}
	p.showKubectl("get", "pods,pvc", "-o", "wide")
	up := waitUntil(60*time.Second, func() bool {
		if _, _, ok := p.leader(); !ok {
			return false
		}
		for i := range p.urls {
			if _, err := p.status(i); err != nil {
				return false
			}
		}
		return true
	})
	p.check(up, "all three nodes answer and the cluster has a leader")
	if !up {
		say("\n    Could not reach the nodes at %v. Is the kind cluster up? (bash scripts/k8s.sh status)", p.urls)
		os.Exit(1)
	}

	// ---- Step 2 ----
	p.step(2, "start continuous writing", fmt.Sprintf("%d clients write one value after another; a write only counts once the cluster says it is saved", o.writers))
	for w := 0; w < o.writers; w++ {
		p.wg.Add(1)
		go p.writer(w, runID)
	}
	flowing := waitUntil(30*time.Second, func() bool { return p.ackedCount() >= 50 })
	p.check(flowing, "writes are flowing before any failure", "%d acknowledged so far", p.ackedCount())

	// ---- Step 3 ----
	leaderID, _, _ := p.leader()
	victim := leaderID
	if o.victim != "" && o.victim != "leader" {
		v, err := strconv.Atoi(o.victim)
		if err != nil || v < 0 || v >= len(p.urls) {
			say("bad -victim %q", o.victim)
			os.Exit(2)
		}
		victim = v
	} else if diskLoss {
		victim = (leaderID + 1) % len(p.urls) // lose a follower's disk; the leader keeps working
	}
	podName, claim := fmt.Sprintf("kv-%d", victim), fmt.Sprintf("data-kv-%d", victim)
	oldPod, oldDisk := p.uid("pod", podName), p.uid("pvc", claim)
	before, err := p.status(victim)
	if oldPod == "" || oldDisk == "" || err != nil {
		say("could not read the identity of %s (pod %q, disk %q): %v", podName, oldPod, oldDisk, err)
		os.Exit(1)
	}
	what := "delete the pod"
	if diskLoss {
		what = "delete the pod AND its disk"
	}
	p.step(3, fmt.Sprintf("%s: %s (Raft node %d, role: %s)", what, podName, victim, before.Role),
		"Kubernetes is responsible for noticing and recreating it; Raft is responsible for the data")
	say("    its identity: pod %s (uid %s...), disk %s (uid %s...), %d ledger entries saved, term %d",
		podName, short(oldPod), claim, short(oldDisk), before.LastLogIndex, before.Term)

	// To keep the replacement pod from starting, the machine is cordoned first. Kubernetes
	// still creates the pod, but cannot place it anywhere, so it waits (Pending). That
	// holds the cluster at two nodes for a while: Raft must serve traffic without the third.
	held := false
	if o.hold > 0 && len(nodes) > 0 {
		if err := p.setCordon(nodes, true); err != nil {
			say("    could not cordon the node, so the replacement pod will not be held back: %v", err)
		} else {
			held = true
			say("    holding the replacement back: the node is cordoned for about %v (Kubernetes can create the pod but not start it)", o.hold)
		}
	}
	deletedAt := time.Now()
	if diskLoss {
		p.showKubectl("delete", "pvc", claim, "--wait=false")
	}
	if err := p.showKubectl("delete", "pod", podName, "--grace-period=0", "--force", "--wait=false"); err != nil {
		say("    delete failed: %v", err)
		p.setCordon(nodes, false)
		os.Exit(1)
	}
	down := waitUntil(15*time.Second, func() bool { _, err := p.status(victim); return err != nil })
	deathAt := time.Now()
	ackedAtDeath := p.ackedCount()
	p.check(down, fmt.Sprintf("%s is really gone", podName))

	// ---- Step 4 ----
	p.step(4, "the other two nodes keep the cluster working, and Kubernetes replaces the pod", "Raft needs 2 of 3 nodes; Kubernetes wants 3 replicas, so it creates a new pod")
	if before.Role == "leader" {
		var newLeader, newTerm int
		elected := waitUntil(30*time.Second, func() bool {
			id, st, ok := p.leader()
			if ok && id != victim && st.Term > before.Term {
				newLeader, newTerm = id, st.Term
				return true
			}
			return false
		})
		if elected {
			p.check(true, "Raft elected a new leader without it", "node %d, term %d, found %v after the pod was confirmed gone", newLeader, newTerm, time.Since(deathAt).Round(time.Millisecond))
		} else {
			p.check(false, "Raft elected a new leader without it", "none appeared within 30s")
		}
	}
	// How long were clients refused? (The first acknowledged write after the pod died.)
	resumed := waitUntil(30*time.Second, func() bool { return p.ackedCount() > ackedAtDeath })
	refusedFor := time.Since(deathAt)
	p.check(resumed, "the cluster accepted writes again", "the first write was acknowledged %v after the pod was confirmed gone (while a new leader was elected, writes are refused)", refusedFor.Round(time.Millisecond))

	var newPod string
	recreated := waitUntil(120*time.Second, func() bool {
		newPod = p.uid("pod", podName)
		return newPod != "" && newPod != oldPod
	})
	recreatedAfter := time.Since(deletedAt)
	p.check(recreated, "Kubernetes created a NEW pod with the same name", "%s: uid %s... became %s..., %v after the delete",
		podName, short(oldPod), short(newPod), recreatedAfter.Round(10*time.Millisecond))

	acksWhileGone := 0
	if held {
		serving := waitUntil(60*time.Second, func() bool {
			return time.Since(deathAt) >= o.hold && p.ackedCount() >= ackedAtDeath+50
		})
		acksWhileGone = p.ackedCount() - ackedAtDeath
		podState, _ := p.kubectl("get", "pod", podName, "-o", "jsonpath={.status.phase}")
		p.check(serving && !p.podReady(podName), "writes kept being acknowledged while the replacement pod was held back",
			"%d writes acknowledged in %v with only two nodes up; the new pod's phase was %q (needs at least 50 writes)", acksWhileGone, time.Since(deathAt).Round(10*time.Millisecond), podState)
		if err := p.setCordon(nodes, false); err != nil {
			say("    WARNING: could not uncordon the node: %v   (run: kubectl uncordon %s)", err, strings.Join(nodes, " "))
		} else {
			say("    node uncordoned: Kubernetes may now start the replacement pod")
		}
	}
	readyStart := deletedAt
	if held {
		readyStart = time.Now()
	}
	ready := recreated && waitUntil(120*time.Second, func() bool { return p.podReady(podName) })
	readyAfter := time.Since(deletedAt)
	if held {
		p.check(ready, "the new pod became Ready once Kubernetes was allowed to start it", "%v after the node was uncordoned", time.Since(readyStart).Round(10*time.Millisecond))
	} else {
		p.check(ready, "the new pod became Ready", "%v after the delete", readyAfter.Round(10*time.Millisecond))
		acksWhileGone = p.ackedCount() - ackedAtDeath
	}
	p.showKubectl("get", "pods,pvc", "-o", "wide")

	// ---- Step 5 ----
	newDisk := p.uid("pvc", claim)
	after, ok := p.firstStatus(victim, 90*time.Second)
	say("    on startup the node read %d ledger entries and term %d from its disk", after.RecoveredEntries, after.RecoveredTerm)
	if !diskLoss {
		p.step(5, "the same disk, the same Raft member", "Kubernetes re-attached the old disk, so the process found its own saved state")
		p.check(newDisk == oldDisk, "the new pod got the SAME disk, not a new one", "%s: uid %s... before, %s... after", claim, short(oldDisk), short(newDisk))
		p.check(ok && after.RecoveredEntries >= before.LastLogIndex && after.RecoveredEntries > 0,
			"it read back the ledger it had saved before the pod was deleted",
			"had %d entries at the last status read, read %d from disk; a blank disk reads 0", before.LastLogIndex, after.RecoveredEntries)
		p.check(ok && after.RecoveredTerm >= before.Term, "it remembered its term from disk", "was %d, read %d", before.Term, after.RecoveredTerm)
	} else {
		p.step(5, "same name, NEW empty disk: a different Raft member wearing the old name", "the data was on the deleted disk; Raft's promises (votes, entries) went with it")
		p.check(newDisk != "" && newDisk != oldDisk, "Kubernetes created a NEW, empty disk for the new pod", "%s uid %s... became %s...", claim, short(oldDisk), short(newDisk))
		p.check(ok && after.RecoveredEntries == 0 && after.RecoveredTerm == 0,
			"the node came back BLANK: it remembers nothing it had promised",
			"read %d entries and term %d from disk; before it had %d entries and term %d", after.RecoveredEntries, after.RecoveredTerm, before.LastLogIndex, before.Term)
		say("    NOTE: the cluster will now refill this node and it will LOOK healthy. It is not safe:")
		say("    it may have acknowledged entries or cast votes that it has now forgotten. If another")
		say("    node were lost next, a committed write could vanish. The raft test")
		say("    TestNodeThatLosesItsDiskCanLoseAcknowledgedWrites reproduces exactly that. The correct")
		say("    fix is to REPLACE the member through a membership change, which this project does not implement.")
	}
	ls, haveLeader := p.leaderWithCommits(30 * time.Second)
	if !diskLoss {
		gap := 1
		if held {
			gap = 20 // two nodes served traffic for seconds, so the gap must be substantial
		}
		p.check(haveLeader && ls.CommitIndex >= after.RecoveredEntries+gap, "the rest of the cluster had moved well ahead of what the disk held",
			"the cluster had committed %d entries; the disk held %d: a gap of %d (needs at least %d), so catching up is real work", ls.CommitIndex, after.RecoveredEntries, ls.CommitIndex-after.RecoveredEntries, gap)
	}
	caught := haveLeader && waitUntil(120*time.Second, func() bool {
		st, err := p.status(victim)
		return err == nil && st.CommitIndex >= ls.CommitIndex && st.LastApplied == st.CommitIndex
	})
	rejoinedAfter := time.Since(deletedAt)
	grown, _ := p.status(victim)
	p.check(caught, "it caught up with the rest of the cluster", "%v after the delete; its ledger went from %d entries to %d", rejoinedAfter.Round(10*time.Millisecond), after.RecoveredEntries, grown.LastLogIndex)

	// ---- Step 6 ----
	p.step(6, "the client Service", "clients use ONE address; Kubernetes sends each request to some pod; followers pass it on to the leader")
	p.serviceCheck(o.serviceURL, runID)

	// ---- Step 7 ----
	p.verifyAll(7)

	if len(p.failed) > 0 {
		say("\nsomething failed, so the cluster is being LEFT RUNNING for inspection:")
		p.showKubectl("get", "pods,pvc", "-o", "wide")
		for _, n := range []string{podName} {
			p.showKubectl("logs", n, "--tail=25")
		}
		say("clean up with: bash scripts/k8s.sh down")
	}
	say("\nwhat was measured on this run (scenario %q, deleted %s, replacement held back %v): clients were refused for %v after the delete; new pod object created %v after the delete; %d writes acknowledged while only two nodes were up; node caught up %v after the delete; %d writes acknowledged in total",
		o.scenario, podName, map[bool]time.Duration{true: o.hold, false: 0}[held], refusedFor.Round(time.Millisecond), recreatedAfter.Round(10*time.Millisecond), acksWhileGone, rejoinedAfter.Round(10*time.Millisecond), p.ackedCount())
	say("\n==== %d checks passed, %d failed ====", p.passed, len(p.failed))
	if len(p.failed) > 0 {
		for _, f := range p.failed {
			say("  FAILED: %s", f)
		}
		os.Exit(1)
	}
	if diskLoss {
		say("A pod and its disk were deleted. The cluster kept every acknowledged write, but the returning node is a blank stranger, not the member that left.")
	} else {
		say("Kubernetes recreated the pod and re-attached its disk; Raft rejoined it with its own saved state. Nothing acknowledged was lost.")
	}
}