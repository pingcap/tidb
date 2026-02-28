// Copyright 2025 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package workload

import (
	"context"
	"encoding/json"
	"sync"
)

type StateStore interface {
	Reset(ctx context.Context) error
	Put(ctx context.Context, caseName string, state json.RawMessage) error
	PutMany(ctx context.Context, states map[string]json.RawMessage) error
	GetAll(ctx context.Context) (map[string]json.RawMessage, error)
}

type MemoryStore struct {
	mu     sync.RWMutex
	states map[string]json.RawMessage
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		states: make(map[string]json.RawMessage),
	}
}

func (s *MemoryStore) Reset(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.states = make(map[string]json.RawMessage)
	return nil
}

func (s *MemoryStore) Put(ctx context.Context, caseName string, state json.RawMessage) error {
	s.mu.Lock()
	s.states[caseName] = cloneRaw(state)
	s.mu.Unlock()
	return nil
}

func (s *MemoryStore) PutMany(ctx context.Context, states map[string]json.RawMessage) error {
	if len(states) == 0 {
		return nil
	}
	s.mu.Lock()
	for caseName, state := range states {
		s.states[caseName] = cloneRaw(state)
	}
	s.mu.Unlock()
	return nil
}

func (s *MemoryStore) GetAll(ctx context.Context) (map[string]json.RawMessage, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make(map[string]json.RawMessage, len(s.states))
	for caseName, state := range s.states {
		out[caseName] = cloneRaw(state)
	}
	return out, nil
}

func cloneRaw(state json.RawMessage) json.RawMessage {
	if len(state) == 0 {
		return nil
	}
	out := make([]byte, len(state))
	copy(out, state)
	return out
}
