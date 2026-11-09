package kv

import (
	"errors"
	"io"
	"net/http"
)

// maxValueBytes caps a single value so one client can't exhaust memory.
const maxValueBytes = 1 << 20 // 1 MiB

// NewHandler exposes a Store over HTTP:
//
//	PUT    /kv/{key}   body is the value   -> 204
//	GET    /kv/{key}                       -> 200 + value, or 404
//	DELETE /kv/{key}                       -> 204, or 404 if absent
func NewHandler(s *Store) http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("PUT /kv/{key}", func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxValueBytes))
		if err != nil {
			var tooBig *http.MaxBytesError
			if errors.As(err, &tooBig) {
				http.Error(w, "value too large", http.StatusRequestEntityTooLarge)
			} else {
				http.Error(w, "bad request body", http.StatusBadRequest)
			}
			return
		}
		s.Put(r.PathValue("key"), string(body))
		w.WriteHeader(http.StatusNoContent)
	})

	mux.HandleFunc("GET /kv/{key}", func(w http.ResponseWriter, r *http.Request) {
		v, ok := s.Get(r.PathValue("key"))
		if !ok {
			http.Error(w, "key not found", http.StatusNotFound)
			return
		}
		io.WriteString(w, v)
	})

	mux.HandleFunc("DELETE /kv/{key}", func(w http.ResponseWriter, r *http.Request) {
		if !s.Delete(r.PathValue("key")) {
			http.Error(w, "key not found", http.StatusNotFound)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})

	return mux
}