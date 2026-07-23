// Copyright 2026 PingCAP, Inc.
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

package server

import (
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/util/sqlkiller"
	"github.com/stretchr/testify/require"
)

type livenessTestConn struct {
	alive atomic.Bool
}

func newLivenessTestConn() *livenessTestConn {
	conn := &livenessTestConn{}
	conn.alive.Store(true)
	return conn
}

func (c *livenessTestConn) Read([]byte) (int, error) {
	if c.alive.Load() {
		return 0, livenessTimeoutError{}
	}
	return 0, io.EOF
}

func (*livenessTestConn) Write(p []byte) (int, error) { return len(p), nil }
func (c *livenessTestConn) Close() error {
	c.alive.Store(false)
	return nil
}
func (*livenessTestConn) LocalAddr() net.Addr              { return livenessTestAddr{} }
func (*livenessTestConn) RemoteAddr() net.Addr             { return livenessTestAddr{} }
func (*livenessTestConn) SetDeadline(time.Time) error      { return nil }
func (*livenessTestConn) SetReadDeadline(time.Time) error  { return nil }
func (*livenessTestConn) SetWriteDeadline(time.Time) error { return nil }

type livenessTestAddr struct{}

func (livenessTestAddr) Network() string { return "test" }
func (livenessTestAddr) String() string  { return "test" }

type livenessTimeoutError struct{}

func (livenessTimeoutError) Error() string   { return "timeout" }
func (livenessTimeoutError) Timeout() bool   { return true }
func (livenessTimeoutError) Temporary() bool { return true }

func TestSetConnectionAliveChecker(t *testing.T) {
	var killer sqlkiller.SQLKiller
	cc := &clientConn{}

	cc.setConnectionAliveChecker(&killer)
	require.True(t, (*killer.IsConnectionAlive.Load())())

	conn := newLivenessTestConn()
	cc.setConn(conn)
	cc.setConnectionAliveChecker(&killer)
	require.True(t, (*killer.IsConnectionAlive.Load())())

	require.NoError(t, conn.Close())
	require.False(t, (*killer.IsConnectionAlive.Load())())

	cc.setConn(newLivenessTestConn())
	cc.setConnectionAliveChecker(&killer)
	require.True(t, (*killer.IsConnectionAlive.Load())())
}

func BenchmarkSetConnectionAliveChecker(b *testing.B) {
	b.Run("connected", func(b *testing.B) {
		cc := &clientConn{}
		cc.setConn(newLivenessTestConn())
		var killer sqlkiller.SQLKiller

		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			cc.setConnectionAliveChecker(&killer)
			killer.IsConnectionAlive.Store(nil)
		}
	})

	b.Run("nil-connection", func(b *testing.B) {
		cc := &clientConn{}
		var killer sqlkiller.SQLKiller

		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			cc.setConnectionAliveChecker(&killer)
			killer.IsConnectionAlive.Store(nil)
		}
	})
}

func BenchmarkSetConnectionAliveCheckerParallel(b *testing.B) {
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		cc := &clientConn{}
		var killer sqlkiller.SQLKiller
		for pb.Next() {
			cc.setConnectionAliveChecker(&killer)
			killer.IsConnectionAlive.Store(nil)
		}
	})
}
