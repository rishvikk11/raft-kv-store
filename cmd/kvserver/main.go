// Command kvserver runs the key-value store over HTTP, either as a single
// standalone server or as one node of a Raft cluster.
//
// Standalone:  kvserver -addr :8080
// Cluster:     kvserver -id 0 -data ./data/node0 \
//
//	-peers http://localhost:8081,http://localhost:8082,http://localhost:8083
//
// -peers lists every node's base URL in node-ID order; each node serves both
// client requests and Raft messages on its own URL.
//
// -client-urls is optional. When nodes reach each other by names that only exist
// inside a private network (for example Docker's kv1:8080), clients outside that
// network cannot follow a redirect to such a name. -client-urls gives, in the
// same order, the addresses clients should be redirected to instead.
package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/rishvikk11/raft-kv-store/kv"
	"github.com/rishvikk11/raft-kv-store/raft"
)

func main() {
	addr := flag.String("addr", "", "listen address (cluster default: taken from this node's -peers URL; standalone default :8080)")
	id := flag.Int("id", 0, "this node's index in -peers")
	peers := flag.String("peers", "", "comma-separated base URLs of all nodes in ID order; empty runs standalone without Raft")
	clientURLs := flag.String("client-urls", "", "comma-separated URLs clients use to reach each node, in ID order, used for redirects (default: same as -peers)")
	data := flag.String("data", "", "directory for this node's Raft state (required with -peers)")
	noFsync := flag.Bool("unsafe-no-fsync", false, "BENCHMARKING ONLY: do not force writes to disk; a crash can lose acknowledged data")
	flag.Parse()

	if *peers == "" {
		listen := *addr
		if listen == "" {
			listen = ":8080"
		}
		srv := &http.Server{Addr: listen, Handler: kv.NewHandler(kv.NewStore()), ReadHeaderTimeout: 5 * time.Second}
		log.Printf("kvserver (standalone, no replication) listening on %s", listen)
		log.Fatal(srv.ListenAndServe())
	}

	urls := strings.Split(*peers, ",")
	if *id < 0 || *id >= len(urls) {
		log.Fatalf("-id %d is out of range for %d peers", *id, len(urls))
	}
	if *data == "" {
		log.Fatal("-data is required in cluster mode")
	}
	listen := *addr
	if listen == "" {
		u, err := url.Parse(urls[*id])
		if err != nil || u.Host == "" {
			log.Fatalf("cannot work out a listen address from %q; pass -addr", urls[*id])
		}
		listen = u.Host
	}

	redirectTo := urls
	if *clientURLs != "" {
		redirectTo = strings.Split(*clientURLs, ",")
		if len(redirectTo) != len(urls) {
			log.Fatalf("-client-urls has %d entries but -peers has %d", len(redirectTo), len(urls))
		}
	}

	rep, err := kv.NewReplicated(raft.Config{
		ID: *id, ClusterSize: len(urls), Transport: raft.NewHTTPTransport(urls),
		DataDir: *data, Logf: log.Printf, DisableFsync: *noFsync,
	}, redirectTo)
	if *noFsync {
		log.Printf("WARNING: -unsafe-no-fsync is on: acknowledged writes are NOT guaranteed to survive a crash")
	}
	if err != nil {
		log.Fatal(err)
	}
	srv := &http.Server{Addr: listen, Handler: rep.Handler(), ReadHeaderTimeout: 5 * time.Second}
	rep.Start()
	go func() {
		log.Printf("node %d of %d listening on %s, state in %s", *id, len(urls), listen, *data)
		if err := srv.ListenAndServe(); err != http.ErrServerClosed {
			log.Fatal(err)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop
	log.Printf("shutting down")
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	srv.Shutdown(ctx)
	rep.Stop()
}