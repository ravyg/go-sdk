// Copyright 2025 The Go MCP SDK Authors. All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

package mcp

import (
	"context"
	"errors"
)

// ErrSessionNotFound is returned by [SessionStore.Load] when the session does
// not exist in the store.
var ErrSessionNotFound = errors.New("session not found")

// SessionStore defines the interface for persistent, shared session storage.
//
// In a default single-instance deployment, MCP sessions are kept in memory
// inside [StreamableHTTPHandler]. This works fine for one server, but breaks
// in a multi-instance deployment behind a load balancer: a request routed to
// a different instance than the one that created the session will return 404,
// forcing the client to reinitialise and lose all session context.
//
// Implementing SessionStore and setting it on [StreamableHTTPOptions] allows
// session state to be shared across instances. When a session ID arrives that
// is not in the local in-memory map, the handler will attempt to recover it
// from the store before falling back to a 404.
//
// The handler calls the store at three points:
//   - [SessionStore.Store] after the client completes the initialize handshake.
//   - [SessionStore.Load] when a session ID is presented that is not in memory.
//   - [SessionStore.Delete] when a session is closed (DELETE request, timeout, or server close).
//
// Implementations must be safe for concurrent use.
//
// Example: a Redis-backed store
//
//	type RedisSessionStore struct {
//	    client *redis.Client
//	    ttl    time.Duration
//	}
//
//	func (s *RedisSessionStore) Load(ctx context.Context, id string) (*mcp.ServerSessionState, error) {
//	    data, err := s.client.Get(ctx, id).Bytes()
//	    if errors.Is(err, redis.Nil) {
//	        return nil, fmt.Errorf("%w: %s", mcp.ErrSessionNotFound, id)
//	    }
//	    if err != nil {
//	        return nil, err
//	    }
//	    var state mcp.ServerSessionState
//	    if err := json.Unmarshal(data, &state); err != nil {
//	        return nil, err
//	    }
//	    return &state, nil
//	}
//
//	func (s *RedisSessionStore) Store(ctx context.Context, id string, state *mcp.ServerSessionState) error {
//	    data, err := json.Marshal(state)
//	    if err != nil {
//	        return err
//	    }
//	    return s.client.Set(ctx, id, data, s.ttl).Err()
//	}
//
//	func (s *RedisSessionStore) Delete(ctx context.Context, id string) error {
//	    return s.client.Del(ctx, id).Err()
//	}
type SessionStore interface {
	// Load retrieves the session state for the given session ID.
	//
	// If the session does not exist, Load must return an error wrapping
	// [ErrSessionNotFound]. Any other error is treated as a transient failure
	// and results in a 404 being returned to the client.
	Load(ctx context.Context, sessionID string) (*ServerSessionState, error)

	// Store saves the session state for the given session ID.
	//
	// Store is called after the client completes the initialize handshake.
	// Errors are logged but do not affect the in-progress request.
	Store(ctx context.Context, sessionID string, state *ServerSessionState) error

	// Delete removes the session state for the given session ID.
	//
	// Delete is called when a session is closed. Errors are logged but
	// do not affect the close operation.
	Delete(ctx context.Context, sessionID string) error
}
