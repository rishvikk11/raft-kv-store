// Package kv is the state machine: a map from string keys to string values.
// It knows nothing about networks, disks, or Raft. Later stages put those
// around it, and this file should barely change.
package kv

import "sync"

// Store is safe for use by many goroutines at once.
type Store struct {
	mu   sync.RWMutex // many readers at once, or one writer
	data map[string]string
}

func NewStore() *Store {
	return &Store{data: make(map[string]string)}
}

// Get returns the value for key. ok is false if the key does not exist,
// which is different from the key existing with an empty value.
func (s *Store) Get(key string) (value string, ok bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	value, ok = s.data[key]
	return value, ok
}

// Put stores value under key, replacing any previous value.
func (s *Store) Put(key, value string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[key] = value
}

// Delete removes key. It reports whether the key existed.
func (s *Store) Delete(key string) (existed bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, existed = s.data[key]
	delete(s.data, key)
	return existed
}
