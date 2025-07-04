// Copyright 2025 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//go:build !codes

package syssession

// NewSessionForTest creates a new session for test.
func NewSessionForTest(sctx SessionContext) (*Session, error) {
	se := &Session{}
	var err error
	se.internal, err = newInternalSession(sctx, se)
	if err != nil {
		return nil, err
	}
	return se, err
}

// InternalSctxForTest returns the internal session context for test
func (s *Session) InternalSctxForTest() SessionContext {
	s.internal.mu.Lock()
	defer s.internal.mu.Unlock()
	return s.internal.sctx
}

// ResetSctxForTest is only used for testing to mock some methods of internal context
func (s *Session) ResetSctxForTest(fn func(SessionContext) SessionContext) error {
	s.internal.mu.Lock()
	defer s.internal.mu.Unlock()
	if err := s.internal.checkOwnerWithoutLock(s, "ResetSctxForTest"); err != nil {
		return err
	}
	s.internal.sctx = fn(s.internal.sctx)
	return nil
}

// IsInternalClosed returns whether the internal session closed
// It is only used in testing
func (s *Session) IsInternalClosed() bool {
	return s.internal.IsClosed()
}

// Size returns the size of the session pool
func (p *AdvancedSessionPool) Size() int {
	return len(p.pool)
}

// IsAvoidReuse returns whether the internal session avoids re-use
// It is only used in testing
func (s *Session) IsAvoidReuse() bool {
	return s.internal.IsAvoidReuse()
}
