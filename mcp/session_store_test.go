// Copyright 2025 The Go MCP SDK Authors. All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

package mcp

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
)

// memSessionStore is an in-memory SessionStore for use in tests.
type memSessionStore struct {
	mu      sync.Mutex
	entries map[string]*ServerSessionState
	// storeCalls and deleteCalls track how many times each method was called,
	// to allow tests to assert on store behaviour.
	storeCalls  int
	deleteCalls int
}

func newMemSessionStore() *memSessionStore {
	return &memSessionStore{entries: make(map[string]*ServerSessionState)}
}

func (s *memSessionStore) Load(_ context.Context, sessionID string) (*ServerSessionState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	state, ok := s.entries[sessionID]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrSessionNotFound, sessionID)
	}
	return state, nil
}

func (s *memSessionStore) Store(_ context.Context, sessionID string, state *ServerSessionState) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.entries[sessionID] = state
	s.storeCalls++
	return nil
}

func (s *memSessionStore) Delete(_ context.Context, sessionID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.entries, sessionID)
	s.deleteCalls++
	return nil
}

// TestSessionStore verifies the SessionStore integration in StreamableHTTPHandler.
// It covers four behaviours:
//  1. State is persisted to the store after a successful initialize handshake.
//  2. A session missing from the in-memory map is transparently recovered from the store.
//  3. State is deleted from the store when the session is closed.
//  4. A session missing from both the in-memory map and the store still returns 404.
func TestSessionStore(t *testing.T) {
	ctx := context.Background()

	t.Run("persists state after initialization", func(t *testing.T) {
		store := newMemSessionStore()
		server := NewServer(testImpl, nil)
		handler := NewStreamableHTTPHandler(
			func(_ *http.Request) *Server { return server },
			&StreamableHTTPOptions{SessionStore: store},
		)
		httpServer := httptest.NewServer(mustNotPanic(t, handler))
		defer httpServer.Close()

		client := NewClient(testImpl, nil)
		session, err := client.Connect(ctx, &StreamableClientTransport{Endpoint: httpServer.URL}, nil)
		if err != nil {
			t.Fatalf("Connect failed: %v", err)
		}
		defer session.Close()

		sessionID := session.ID()
		if sessionID == "" {
			t.Fatal("expected non-empty session ID")
		}

		// After initialization the store should have exactly one entry.
		store.mu.Lock()
		storeLen := len(store.entries)
		storeCalls := store.storeCalls
		store.mu.Unlock()

		if storeLen != 1 {
			t.Errorf("store has %d entries, want 1", storeLen)
		}
		if storeCalls == 0 {
			t.Error("Store was never called after initialization")
		}

		state, err := store.Load(ctx, sessionID)
		if err != nil {
			t.Fatalf("store.Load failed: %v", err)
		}
		if state.InitializeParams == nil {
			t.Error("persisted state has nil InitializeParams")
		}
		if state.InitializedParams == nil {
			t.Error("persisted state has nil InitializedParams")
		}
	})

	t.Run("recovers session from store on in-memory miss", func(t *testing.T) {
		store := newMemSessionStore()
		server := NewServer(testImpl, nil)
		handler := NewStreamableHTTPHandler(
			func(_ *http.Request) *Server { return server },
			&StreamableHTTPOptions{SessionStore: store},
		)
		httpServer := httptest.NewServer(mustNotPanic(t, handler))
		defer httpServer.Close()

		// Connect and fully initialize.
		client := NewClient(testImpl, nil)
		session, err := client.Connect(ctx, &StreamableClientTransport{Endpoint: httpServer.URL}, nil)
		if err != nil {
			t.Fatalf("Connect failed: %v", err)
		}
		defer session.Close()

		// Verify a normal tool call works first.
		if _, err := session.ListTools(ctx, nil); err != nil {
			t.Fatalf("ListTools failed before eviction: %v", err)
		}

		// Simulate the session being lost from the in-memory map
		// (e.g. a request routed to a different instance).
		handler.mu.Lock()
		for id := range handler.sessions {
			delete(handler.sessions, id)
		}
		handler.mu.Unlock()

		// The next call should be transparently recovered from the store,
		// not return 404 / ErrSessionMissing.
		if _, err := session.ListTools(ctx, nil); err != nil {
			if errors.Is(err, ErrSessionMissing) {
				t.Fatal("session was not recovered from store: got ErrSessionMissing")
			}
			t.Fatalf("ListTools failed after recovery: %v", err)
		}
	})

	t.Run("deletes state from store when session is closed", func(t *testing.T) {
		store := newMemSessionStore()
		server := NewServer(testImpl, nil)

		deleted := make(chan string, 1)
		handler := NewStreamableHTTPHandler(
			func(_ *http.Request) *Server { return server },
			&StreamableHTTPOptions{SessionStore: store},
		)
		handler.onTransportDeletion = func(sessionID string) {
			deleted <- sessionID
		}
		httpServer := httptest.NewServer(mustNotPanic(t, handler))
		defer httpServer.Close()

		client := NewClient(testImpl, nil)
		session, err := client.Connect(ctx, &StreamableClientTransport{Endpoint: httpServer.URL}, nil)
		if err != nil {
			t.Fatalf("Connect failed: %v", err)
		}

		sessionID := session.ID()

		// Verify it was stored.
		if _, err := store.Load(ctx, sessionID); err != nil {
			t.Fatalf("session not in store before close: %v", err)
		}

		// Close the session.
		session.Close()

		select {
		case <-deleted:
		case <-ctx.Done():
			t.Fatal("timed out waiting for session deletion")
		}

		// Verify it was removed from the store.
		store.mu.Lock()
		deleteCalls := store.deleteCalls
		store.mu.Unlock()

		if deleteCalls == 0 {
			t.Error("Delete was never called on store after session close")
		}

		_, err = store.Load(ctx, sessionID)
		if err == nil {
			t.Error("session still in store after close, expected it to be deleted")
		}
		if !errors.Is(err, ErrSessionNotFound) {
			t.Errorf("expected ErrSessionNotFound, got: %v", err)
		}
	})

	t.Run("returns 404 when session missing from both memory and store", func(t *testing.T) {
		store := newMemSessionStore()
		server := NewServer(testImpl, nil)
		handler := NewStreamableHTTPHandler(
			func(_ *http.Request) *Server { return server },
			&StreamableHTTPOptions{SessionStore: store},
		)
		httpServer := httptest.NewServer(mustNotPanic(t, handler))
		defer httpServer.Close()

		client := NewClient(testImpl, nil)
		session, err := client.Connect(ctx, &StreamableClientTransport{Endpoint: httpServer.URL}, nil)
		if err != nil {
			t.Fatalf("Connect failed: %v", err)
		}
		defer session.Close()

		// Remove from both in-memory map and store.
		handler.mu.Lock()
		for id := range handler.sessions {
			delete(handler.sessions, id)
		}
		handler.mu.Unlock()
		store.mu.Lock()
		store.entries = make(map[string]*ServerSessionState)
		store.mu.Unlock()

		_, err = session.ListTools(ctx, nil)
		if err == nil {
			t.Fatal("expected error when session missing from both memory and store, got nil")
		}
		if !errors.Is(err, ErrSessionMissing) {
			t.Errorf("expected ErrSessionMissing, got: %v", err)
		}
	})
}
