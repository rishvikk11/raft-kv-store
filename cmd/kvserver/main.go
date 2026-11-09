// Command kvserver runs a single-node key-value store over HTTP.
package main

import (
	"flag"
	"log"
	"net/http"
	"time"

	"github.com/rishvikk11/raft-kv-store/kv"
)

func main() {
	addr := flag.String("addr", ":8080", "address to listen on")
	flag.Parse()

	srv := &http.Server{
		Addr:              *addr,
		Handler:           kv.NewHandler(kv.NewStore()),
		ReadHeaderTimeout: 5 * time.Second,
	}
	log.Printf("kvserver listening on %s", *addr)
	log.Fatal(srv.ListenAndServe())
}