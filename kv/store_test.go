package kv

import (
	"fmt"
	"sync"
	"testing"
)

func TestGetMissingKey(t *testing.T) {
	s := NewStore()
	if v, ok := s.Get("nope"); ok || v != "" {
		t.Fatalf("Get(nope) = %q, %v; want \"\", false", v, ok)
	}
}

func TestPutThenGet(t *testing.T) {
	s := NewStore()
	s.Put("color", "blue")
	if v, ok := s.Get("color"); !ok || v != "blue" {
		t.Fatalf("Get(color) = %q, %v; want blue, true", v, ok)
	}
}

func TestPutOverwrites(t *testing.T) {
	s := NewStore()
	s.Put("k", "old")
	s.Put("k", "new")
	if v, _ := s.Get("k"); v != "new" {
		t.Fatalf("Get(k) = %q; want new", v)
	}
}

// An empty value is a real value; it must not look like "missing".
func TestEmptyValueIsNotMissing(t *testing.T) {
	s := NewStore()
	s.Put("k", "")
	if v, ok := s.Get("k"); !ok || v != "" {
		t.Fatalf("Get(k) = %q, %v; want \"\", true", v, ok)
	}
}

func TestDelete(t *testing.T) {
	s := NewStore()
	s.Put("k", "v")
	if !s.Delete("k") {
		t.Fatal("Delete of existing key reported existed=false")
	}
	if _, ok := s.Get("k"); ok {
		t.Fatal("key still present after Delete")
	}
	if s.Delete("k") {
		t.Fatal("Delete of already-deleted key reported existed=true")
	}
}

func TestDeleteMissingKey(t *testing.T) {
	s := NewStore()
	if s.Delete("ghost") {
		t.Fatal("Delete of never-existing key reported existed=true")
	}
}

// Many goroutines each write their own keys while others read and delete.
// Under `go test -race` this fails loudly if the locking is wrong.
func TestConcurrentAccess(t *testing.T) {
	s := NewStore()
	const workers, perWorker = 16, 500

	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < perWorker; i++ {
				key := fmt.Sprintf("w%d-k%d", w, i)
				s.Put(key, "v")
				s.Get(key)
				// Also touch a key every worker fights over.
				s.Put("shared", fmt.Sprint(w))
				s.Get("shared")
				if i%2 == 0 {
					s.Delete(key)
				}
			}
		}(w)
	}
	wg.Wait()

	// Each worker deleted its even-numbered keys and kept its odd ones.
	for w := 0; w < workers; w++ {
		for i := 0; i < perWorker; i++ {
			_, ok := s.Get(fmt.Sprintf("w%d-k%d", w, i))
			if want := i%2 == 1; ok != want {
				t.Fatalf("w%d-k%d present=%v; want %v", w, i, ok, want)
			}
		}
	}
	if _, ok := s.Get("shared"); !ok {
		t.Fatal("shared key missing")
	}
}
