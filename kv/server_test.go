package kv

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

// do sends one request to the handler and returns status code and body.
func do(t *testing.T, h http.Handler, method, path, body string) (int, string) {
	t.Helper()
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec.Code, rec.Body.String()
}

func TestHTTPPutGetDelete(t *testing.T) {
	h := NewHandler(NewStore())

	if code, _ := do(t, h, "PUT", "/kv/alice", "42"); code != http.StatusNoContent {
		t.Fatalf("PUT = %d; want 204", code)
	}
	if code, body := do(t, h, "GET", "/kv/alice", ""); code != 200 || body != "42" {
		t.Fatalf("GET = %d %q; want 200 \"42\"", code, body)
	}
	if code, _ := do(t, h, "DELETE", "/kv/alice", ""); code != http.StatusNoContent {
		t.Fatalf("DELETE = %d; want 204", code)
	}
	if code, _ := do(t, h, "GET", "/kv/alice", ""); code != http.StatusNotFound {
		t.Fatalf("GET after DELETE = %d; want 404", code)
	}
}

func TestHTTPMissingKey(t *testing.T) {
	h := NewHandler(NewStore())
	if code, _ := do(t, h, "GET", "/kv/ghost", ""); code != http.StatusNotFound {
		t.Fatalf("GET missing = %d; want 404", code)
	}
	if code, _ := do(t, h, "DELETE", "/kv/ghost", ""); code != http.StatusNotFound {
		t.Fatalf("DELETE missing = %d; want 404", code)
	}
}

// An empty value is a real value: GET must return 200, not 404.
func TestHTTPEmptyValue(t *testing.T) {
	h := NewHandler(NewStore())
	do(t, h, "PUT", "/kv/k", "")
	if code, body := do(t, h, "GET", "/kv/k", ""); code != 200 || body != "" {
		t.Fatalf("GET empty = %d %q; want 200 \"\"", code, body)
	}
}

func TestHTTPEncodedKey(t *testing.T) {
	h := NewHandler(NewStore())
	do(t, h, "PUT", "/kv/hello%20world", "v")
	if code, body := do(t, h, "GET", "/kv/hello%20world", ""); code != 200 || body != "v" {
		t.Fatalf("GET encoded = %d %q; want 200 \"v\"", code, body)
	}
}

func TestHTTPWrongMethodAndPath(t *testing.T) {
	h := NewHandler(NewStore())
	if code, _ := do(t, h, "POST", "/kv/k", "x"); code != http.StatusMethodNotAllowed {
		t.Fatalf("POST = %d; want 405", code)
	}
	if code, _ := do(t, h, "GET", "/kv/", ""); code != http.StatusNotFound {
		t.Fatalf("GET with empty key = %d; want 404", code)
	}
	if code, _ := do(t, h, "GET", "/other", ""); code != http.StatusNotFound {
		t.Fatalf("GET /other = %d; want 404", code)
	}
}

func TestHTTPValueTooLarge(t *testing.T) {
	h := NewHandler(NewStore())
	big := strings.Repeat("x", maxValueBytes+1)
	if code, _ := do(t, h, "PUT", "/kv/big", big); code != http.StatusRequestEntityTooLarge {
		t.Fatalf("PUT oversize = %d; want 413", code)
	}
	if code, _ := do(t, h, "GET", "/kv/big", ""); code != http.StatusNotFound {
		t.Fatalf("oversize value was stored (GET = %d)", code)
	}
}

// Real TCP, real concurrent clients, under -race.
func TestHTTPConcurrentClients(t *testing.T) {
	srv := httptest.NewServer(NewHandler(NewStore()))
	defer srv.Close()

	const clients, perClient = 8, 50
	var wg sync.WaitGroup
	errs := make(chan error, clients)
	for c := 0; c < clients; c++ {
		wg.Add(1)
		go func(c int) {
			defer wg.Done()
			for i := 0; i < perClient; i++ {
				url := fmt.Sprintf("%s/kv/c%d-k%d", srv.URL, c, i)
				val := fmt.Sprintf("v%d", i)
				req, _ := http.NewRequest("PUT", url, strings.NewReader(val))
				resp, err := http.DefaultClient.Do(req)
				if err != nil {
					errs <- err
					return
				}
				resp.Body.Close()
				resp, err = http.Get(url)
				if err != nil {
					errs <- err
					return
				}
				got, _ := io.ReadAll(resp.Body)
				resp.Body.Close()
				if resp.StatusCode != 200 || string(got) != val {
					errs <- fmt.Errorf("GET %s = %d %q; want 200 %q", url, resp.StatusCode, got, val)
					return
				}
			}
		}(c)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
}