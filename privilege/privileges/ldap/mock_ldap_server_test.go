// Copyright 2023 PingCAP, Inc.
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

package ldap

import (
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

// MockLDAPServer is a mock LDAP server to help testing the LDAP authentication
type MockLDAPServer struct {
	t  *testing.T
	wg sync.WaitGroup

	listener net.Listener
	closed   atomic.Bool

	connections chan net.Conn
}

// NewMockLDAPServer creates the MockLDAPServer
func NewMockLDAPServer(t *testing.T) *MockLDAPServer {
	return &MockLDAPServer{
		t:           t,
		connections: make(chan net.Conn),
	}
}

// Listen listens on the specific port
func (s *MockLDAPServer) Listen(port int) {
	l, err := net.Listen("tcp", fmt.Sprintf("localhost:%d", port))
	require.NoError(s.t, err)

	s.wg.Add(1)
	go func() {
		for {
			conn, err := l.Accept()
			if err != nil {
				if s.closed.Load() {
					break
				}

				require.NoError(s.t, err)
			}
			go func() {
				s.connections <- conn
			}()
		}
		s.wg.Done()
	}()

	s.listener = l
	return
}

// Close closes the listener
func (s *MockLDAPServer) Close() {
	s.closed.Store(true)
	err := s.listener.Close()
	require.NoError(s.t, err)

	s.wg.Wait()
}

// GetConnections returns all live connections
func (s *MockLDAPServer) GetConnections() chan net.Conn {
	return s.connections
}
