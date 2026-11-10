// Command kvbench measures the throughput and latency of a running cluster.
//
// It sends load to the cluster's leader from many concurrent clients, records
// how long every request took, and afterwards reads back what it wrote to make
// sure the numbers come from a cluster that actually worked.
//
//	kvbench -nodes http://127.0.0.1:9001,http://127.0.0.1:9002,http://127.0.0.1:9003
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type bench struct {
	urls          []string
	client        *http.Client
	leader        atomic.Value // string: base URL of the current leader
	valueSize     int
	keysPerClient int
	useIDs        bool
	readRatio     float64
}

type nodeStatus struct {
	ID          int    `json:"id"`
	Term        int    `json:"term"`
	Role        string `json:"role"`
	CommitIndex int    `json:"commitIndex"`
}

func (b *bench) status(url string) (nodeStatus, error) {
	var st nodeStatus
	resp, err := b.client.Get(url + "/raft/status")
	if err != nil {
		return st, err
	}
	defer resp.Body.Close()
	return st, json.NewDecoder(resp.Body).Decode(&st)
}

// findLeader asks every node who the leader is and remembers the answer.
func (b *bench) findLeader() (nodeStatus, bool) {
	for _, u := range b.urls {
		if st, err := b.status(u); err == nil && st.Role == "leader" {
			b.leader.Store(u)
			return st, true
		}
	}
	return nodeStatus{}, false
}

func (b *bench) waitForLeader(d time.Duration) nodeStatus {
	for end := time.Now().Add(d); time.Now().Before(end); time.Sleep(100 * time.Millisecond) {
		if st, ok := b.findLeader(); ok {
			return st
		}
	}
	fmt.Fprintln(os.Stderr, "no leader found: are all the nodes running?")
	os.Exit(1)
	return nodeStatus{}
}

func (b *bench) leaderURL() string { return b.leader.Load().(string) }

type keyState struct {
	value   string
	tainted bool // a write to this key failed, so we cannot say what it should hold
}

type workerResult struct {
	lat  []time.Duration
	errs int
	keys map[string]*keyState
}

type runResult struct {
	workload   string
	clients    int
	ops        int
	errs       int
	window     time.Duration
	lat        []time.Duration
	termBefore int
	termAfter  int
	verified   int
	skipped    int
	mismatched int
}

func (b *bench) padded(prefix string) string {
	if len(prefix) >= b.valueSize {
		return prefix
	}
	return prefix + strings.Repeat("x", b.valueSize-len(prefix))
}

func (b *bench) put(key, value, clientID string, seq int) error {
	req, _ := http.NewRequest("PUT", b.leaderURL()+"/kv/"+key, strings.NewReader(value))
	if b.useIDs {
		req.Header.Set("Client-Id", clientID)
		req.Header.Set("Request-Id", strconv.Itoa(seq))
	}
	resp, err := b.client.Do(req)
	if err != nil {
		return err
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		return fmt.Errorf("PUT %s: status %d", key, resp.StatusCode)
	}
	return nil
}

func (b *bench) get(key string) (string, error) {
	resp, err := b.client.Get(b.leaderURL() + "/kv/" + key)
	if err != nil {
		return "", err
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("GET %s: status %d", key, resp.StatusCode)
	}
	return string(body), nil
}

func keyName(client, j int) string { return fmt.Sprintf("bench-c%d-k%d", client, j) }

// run measures one workload at one concurrency level.
func (b *bench) run(workload string, clients int, warmup, measure time.Duration) runResult {
	nonce := time.Now().UnixNano() // a fresh Client-Id every run, so request numbers can restart at 1
	before, _ := b.findLeader()

	results := make([]workerResult, clients)
	var ready, done sync.WaitGroup
	start := make(chan struct{})
	var measureStart, measureEnd time.Time
	var startOnce sync.Once

	for c := 0; c < clients; c++ {
		ready.Add(1)
		done.Add(1)
		go func(c int) {
			defer done.Done()
			rng := rand.New(rand.NewSource(nonce + int64(c)))
			res := &results[c]
			res.keys = map[string]*keyState{}
			clientID := fmt.Sprintf("bench-%d-%d", nonce, c)
			seq := 0

			// Preload this client's own keys (not measured), so reads have something to read.
			for j := 0; j < b.keysPerClient; j++ {
				seq++
				k := keyName(c, j)
				v := b.padded(fmt.Sprintf("init-%d-%d-", c, j))
				for try := 0; ; try++ {
					if err := b.put(k, v, clientID, seq); err == nil {
						break
					} else if try > 200 {
						fmt.Fprintf(os.Stderr, "preload failed: %v\n", err)
						os.Exit(1)
					}
					time.Sleep(20 * time.Millisecond)
				}
				res.keys[k] = &keyState{value: v}
			}
			ready.Done()
			<-start

			for n := 0; ; n++ {
				now := time.Now()
				if now.After(measureEnd) {
					return
				}
				isRead := workload == "read" || (workload == "mixed" && rng.Float64() < b.readRatio)
				t0 := time.Now()
				var err error
				var key string
				var val string
				if isRead {
					key = keyName(rng.Intn(clients), rng.Intn(b.keysPerClient))
					_, err = b.get(key)
				} else {
					key = keyName(c, rng.Intn(b.keysPerClient))
					seq++
					val = b.padded(fmt.Sprintf("w-%d-%d-%d-", c, seq, n))
					err = b.put(key, val, clientID, seq)
				}
				d := time.Since(t0)
				if !isRead {
					if err != nil {
						res.keys[key].tainted = true
					} else {
						res.keys[key].value = val
					}
				}
				if t0.Before(measureStart) { // warm-up: do the work, don't count it
					continue
				}
				if err != nil {
					res.errs++
				} else {
					res.lat = append(res.lat, d)
				}
			}
		}(c)
	}

	ready.Wait()
	startOnce.Do(func() {
		measureStart = time.Now().Add(warmup)
		measureEnd = measureStart.Add(measure)
		close(start)
	})
	done.Wait()

	after, _ := b.findLeader()
	rr := runResult{workload: workload, clients: clients, window: measure, termBefore: before.Term, termAfter: after.Term}
	for _, r := range results {
		rr.lat = append(rr.lat, r.lat...)
		rr.errs += r.errs
	}
	rr.ops = len(rr.lat)

	// Did the cluster really do what it said? Read back what each client last wrote.
	if workload != "read" {
		var vwg sync.WaitGroup
		var mu sync.Mutex
		sem := make(chan struct{}, 16)
		for c := range results {
			for k, st := range results[c].keys {
				vwg.Add(1)
				sem <- struct{}{}
				go func(k string, st *keyState) {
					defer vwg.Done()
					defer func() { <-sem }()
					if st.tainted {
						mu.Lock()
						rr.skipped++
						mu.Unlock()
						return
					}
					var got string
					var err error
					for try := 0; try < 20; try++ {
						if got, err = b.get(k); err == nil {
							break
						}
						time.Sleep(50 * time.Millisecond)
					}
					mu.Lock()
					if err == nil && got == st.value {
						rr.verified++
					} else {
						rr.mismatched++
					}
					mu.Unlock()
				}(k, st)
			}
		}
		vwg.Wait()
	}
	return rr
}

func pct(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	i := int(float64(len(sorted))*p+0.999999) - 1
	if i < 0 {
		i = 0
	}
	if i >= len(sorted) {
		i = len(sorted) - 1
	}
	return sorted[i]
}

func ms(d time.Duration) string { return fmt.Sprintf("%.2f", float64(d.Microseconds())/1000) }

func main() {
	nodes := flag.String("nodes", "", "comma-separated base URLs of ALL cluster nodes (required)")
	duration := flag.Duration("duration", 10*time.Second, "measured time per run")
	warmup := flag.Duration("warmup", 2*time.Second, "warm-up time per run (not counted)")
	conc := flag.String("concurrency", "1,8,32,128", "comma-separated numbers of concurrent clients; one run each")
	workloads := flag.String("workloads", "write,read", "comma-separated: write, read, mixed")
	readRatio := flag.Float64("read-ratio", 0.9, "fraction of reads in the mixed workload")
	valueSize := flag.Int("value-size", 100, "bytes per value")
	keysPerClient := flag.Int("keys-per-client", 50, "keys each client owns")
	ids := flag.Bool("ids", true, "send Client-Id/Request-Id on writes (the exactly-once path)")
	out := flag.String("out", "", "append one CSV line per run to this file")
	label := flag.String("label", "", "free text saved with each CSV line, e.g. 3-node-durable")
	flag.Parse()

	if *nodes == "" {
		fmt.Fprintln(os.Stderr, "-nodes is required")
		os.Exit(2)
	}
	b := &bench{
		urls: strings.Split(*nodes, ","), valueSize: *valueSize, keysPerClient: *keysPerClient,
		useIDs: *ids, readRatio: *readRatio,
		client: &http.Client{Timeout: 5 * time.Second, Transport: &http.Transport{MaxIdleConns: 1024, MaxIdleConnsPerHost: 1024}},
	}
	st := b.waitForLeader(20 * time.Second)
	// Keep following the leader in the background, in case it changes mid-run.
	go func() {
		for range time.Tick(500 * time.Millisecond) {
			b.findLeader()
		}
	}()

	fmt.Printf("kvbench  go=%s  %s/%s  cpus=%d\n", runtime.Version(), runtime.GOOS, runtime.GOARCH, runtime.NumCPU())
	fmt.Printf("cluster  nodes=%d  leader=node %d (term %d)  value=%dB  request-ids=%v  warmup=%v  measured=%v\n\n",
		len(b.urls), st.ID, st.Term, *valueSize, *ids, *warmup, *duration)
	fmt.Printf("%-9s %7s %10s %9s %9s %9s %9s %7s  %s\n", "workload", "clients", "ops/sec", "p50 ms", "p95 ms", "p99 ms", "max ms", "errors", "checks")

	var csv *os.File
	if *out != "" {
		var err error
		if csv, err = os.OpenFile(*out, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		defer csv.Close()
	}

	bad := false
	for _, wl := range strings.Split(*workloads, ",") {
		for _, cs := range strings.Split(*conc, ",") {
			n, err := strconv.Atoi(strings.TrimSpace(cs))
			if err != nil || n < 1 {
				fmt.Fprintf(os.Stderr, "bad -concurrency value %q\n", cs)
				os.Exit(2)
			}
			rr := b.run(strings.TrimSpace(wl), n, *warmup, *duration)
			sort.Slice(rr.lat, func(i, j int) bool { return rr.lat[i] < rr.lat[j] })
			opsPerSec := float64(rr.ops) / rr.window.Seconds()
			max := time.Duration(0)
			if len(rr.lat) > 0 {
				max = rr.lat[len(rr.lat)-1]
			}
			checks := "ok"
			switch {
			case rr.mismatched > 0:
				checks = fmt.Sprintf("WRONG DATA: %d keys differ", rr.mismatched)
				bad = true
			case rr.termBefore != rr.termAfter:
				checks = fmt.Sprintf("leader changed (term %d to %d): run includes a failover", rr.termBefore, rr.termAfter)
			case rr.workload != "read":
				checks = fmt.Sprintf("ok (%d keys read back correctly", rr.verified)
				if rr.skipped > 0 {
					checks += fmt.Sprintf(", %d skipped after errors", rr.skipped)
				}
				checks += ")"
			}
			fmt.Printf("%-9s %7d %10.0f %9s %9s %9s %9s %7d  %s\n", rr.workload, rr.clients, opsPerSec,
				ms(pct(rr.lat, .50)), ms(pct(rr.lat, .95)), ms(pct(rr.lat, .99)), ms(max), rr.errs, checks)
			if csv != nil {
				fmt.Fprintf(csv, "%s,%s,%d,%d,%d,%v,%.0f,%s,%s,%s,%s,%d,%v\n", time.Now().Format(time.RFC3339), *label, len(b.urls),
					*valueSize, rr.clients, *ids, opsPerSec, ms(pct(rr.lat, .50)), ms(pct(rr.lat, .95)), ms(pct(rr.lat, .99)), ms(max), rr.errs, rr.termBefore != rr.termAfter)
			}
		}
	}
	if bad {
		os.Exit(1)
	}
}