// Copyright 2017 PingCAP, Inc.
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
	"crypto/tls"
	"maps"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/parser/terror"
	servererr "github.com/pingcap/tidb/pkg/server/err"
	"github.com/pingcap/tidb/pkg/session/sessmgr"
	"github.com/pingcap/tidb/pkg/session/txninfo"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/sqlkiller"
	"go.uber.org/zap"
)

func (s *Server) checkConnectionCount() error {
	// When the value of Instance.MaxConnections is 0, the number of connections is unlimited.
	if int(s.cfg.Instance.MaxConnections) == 0 {
		return nil
	}

	conns := s.ConnectionCount()

	if conns >= int(s.cfg.Instance.MaxConnections) {
		logutil.BgLogger().Error("too many connections",
			zap.Uint32("max connections", s.cfg.Instance.MaxConnections), zap.Error(servererr.ErrConCount))
		return servererr.ErrConCount
	}
	return nil
}

// ShowProcessList implements the SessionManager interface.
func (s *Server) ShowProcessList() map[uint64]*sessmgr.ProcessInfo {
	rs := make(map[uint64]*sessmgr.ProcessInfo)
	maps.Copy(rs, s.GetUserProcessList())
	if s.dom != nil {
		maps.Copy(rs, s.dom.SysProcTracker().GetSysProcessList())
	}
	return rs
}

// GetUserProcessList returns all process info that are created by user.
func (s *Server) GetUserProcessList() map[uint64]*sessmgr.ProcessInfo {
	s.rwlock.RLock()
	defer s.rwlock.RUnlock()
	rs := make(map[uint64]*sessmgr.ProcessInfo)
	for _, client := range s.clients {
		if pi := client.ctx.ShowProcess(); pi != nil {
			rs[pi.ID] = pi
		}
	}
	return rs
}

// GetClientCapabilityList returns all client capability.
func (s *Server) GetClientCapabilityList() map[uint64]uint32 {
	s.rwlock.RLock()
	defer s.rwlock.RUnlock()
	rs := make(map[uint64]uint32)
	for id, client := range s.clients {
		if client.ctx.Session != nil {
			rs[id] = client.capability
		}
	}
	return rs
}

// ShowTxnList shows all txn info for displaying in `TIDB_TRX`.
// Internal sessions are not taken into consideration.
func (s *Server) ShowTxnList() []*txninfo.TxnInfo {
	s.rwlock.RLock()
	defer s.rwlock.RUnlock()
	rs := make([]*txninfo.TxnInfo, 0, len(s.clients))
	for _, client := range s.clients {
		if client.ctx.Session != nil {
			info := client.ctx.Session.TxnInfo()
			if info != nil && info.ProcessInfo != nil {
				rs = append(rs, info)
			}
		}
	}
	return rs
}

// UpdateProcessCPUTime updates specific process's tidb CPU time when the process is still running
// It implements ProcessCPUTimeUpdater interface
func (s *Server) UpdateProcessCPUTime(connID uint64, sqlID uint64, cpuTime time.Duration) {
	s.rwlock.RLock()
	conn, ok := s.clients[connID]
	s.rwlock.RUnlock()
	if !ok {
		return
	}
	vars := conn.ctx.GetSessionVars()
	if vars != nil {
		vars.SQLCPUUsages.MergeTidbCPUTime(sqlID, cpuTime)
	}
}

// GetProcessInfo implements the SessionManager interface.
func (s *Server) GetProcessInfo(id uint64) (*sessmgr.ProcessInfo, bool) {
	s.rwlock.RLock()
	conn, ok := s.clients[id]
	s.rwlock.RUnlock()
	if !ok {
		if s.dom != nil {
			if pinfo, ok2 := s.dom.SysProcTracker().GetSysProcessList()[id]; ok2 {
				return pinfo, true
			}
		}
		return &sessmgr.ProcessInfo{}, false
	}
	return conn.ctx.ShowProcess(), ok
}

// GetConAttrs returns the connection attributes
func (s *Server) GetConAttrs(user *auth.UserIdentity) map[uint64]map[string]string {
	s.rwlock.RLock()
	defer s.rwlock.RUnlock()
	rs := make(map[uint64]map[string]string)
	for _, client := range s.clients {
		if user != nil {
			if user.Username != client.user {
				continue
			}
			if user.Hostname != client.peerHost {
				continue
			}
		}
		if pi := client.ctx.ShowProcess(); pi != nil {
			rs[pi.ID] = client.attrs
		}
	}
	return rs
}

// Kill implements the SessionManager interface.
func (s *Server) Kill(connectionID uint64, query bool, maxExecutionTime bool, runaway bool) {
	logutil.BgLogger().Info("kill", zap.Uint64("conn", connectionID),
		zap.Bool("query", query), zap.Bool("maxExecutionTime", maxExecutionTime), zap.Bool("runawayExceed", runaway))
	metrics.ServerEventCounter.WithLabelValues(metrics.EventKill).Inc()

	s.rwlock.RLock()
	defer s.rwlock.RUnlock()
	conn, ok := s.clients[connectionID]
	if !ok && s.dom != nil {
		s.dom.SysProcTracker().KillSysProcess(connectionID)
		return
	}

	if !query {
		// Mark the client connection status as WaitShutdown, when clientConn.Run detect
		// this, it will end the dispatch loop and exit.
		conn.setStatus(connStatusWaitShutdown)
		if conn.bufReadConn != nil {
			// When attempting to 'kill connection' and TiDB is stuck in the network stack while writing packets,
			// we can quickly exit the network stack and terminate the SQL execution by setting WriteDeadline.
			if err := conn.bufReadConn.SetWriteDeadline(time.Now()); err != nil {
				logutil.BgLogger().Warn("error setting write deadline for kill.", zap.Error(err))
			}
			if err := conn.bufReadConn.SetReadDeadline(time.Now()); err != nil {
				logutil.BgLogger().Warn("error setting read deadline for kill.", zap.Error(err))
			}
		}
	}
	killQuery(conn, maxExecutionTime, runaway)
}

// UpdateTLSConfig implements the SessionManager interface.
func (s *Server) UpdateTLSConfig(cfg *tls.Config) {
	atomic.StorePointer(&s.tlsConfig, unsafe.Pointer(cfg))
}

// GetTLSConfig implements the SessionManager interface.
func (s *Server) GetTLSConfig() *tls.Config {
	return (*tls.Config)(atomic.LoadPointer(&s.tlsConfig))
}

func killQuery(conn *clientConn, maxExecutionTime, runaway bool) {
	sessVars := conn.ctx.GetSessionVars()
	if runaway {
		sessVars.SQLKiller.SendKillSignal(sqlkiller.RunawayQueryExceeded)
	} else if maxExecutionTime {
		sessVars.SQLKiller.SendKillSignal(sqlkiller.MaxExecTimeExceeded)
	} else {
		sessVars.SQLKiller.SendKillSignal(sqlkiller.QueryInterrupted)
	}
	conn.mu.RLock()
	cancelFunc := conn.mu.cancelFunc
	conn.mu.RUnlock()

	if cancelFunc != nil {
		cancelFunc()
	}
	sessVars.SQLKiller.FinishResultSet()
}

// KillSysProcesses kill sys processes such as auto analyze.
func (s *Server) KillSysProcesses() {
	if s.dom == nil {
		return
	}
	sysProcTracker := s.dom.SysProcTracker()
	for connID := range sysProcTracker.GetSysProcessList() {
		sysProcTracker.KillSysProcess(connID)
	}
}

// KillAllConnections implements the SessionManager interface.
// KillAllConnections kills all connections.
func (s *Server) KillAllConnections() {
	logutil.BgLogger().Info("kill all connections.", zap.String("category", "server"))

	s.rwlock.RLock()
	defer s.rwlock.RUnlock()
	for _, conn := range s.clients {
		conn.setStatus(connStatusShutdown)
		if err := conn.closeWithoutLock(); err != nil {
			terror.Log(err)
		}
		if conn.bufReadConn != nil {
			if err := conn.bufReadConn.SetReadDeadline(time.Now()); err != nil {
				logutil.BgLogger().Warn("error setting read deadline for kill.", zap.Error(err))
			}
		}
		killQuery(conn, false, false)
	}

	s.KillSysProcesses()
}

// DrainClients drain all connections in drainWait.
// After drainWait duration, we kill all connections still not quit explicitly and wait for cancelWait.
func (s *Server) DrainClients(drainWait time.Duration, cancelWait time.Duration) {
	logger := logutil.BgLogger()
	logger.Info("start drain clients")

	conns := make(map[uint64]*clientConn)

	s.rwlock.Lock()
	maps.Copy(conns, s.clients)
	s.rwlock.Unlock()

	allDone := make(chan struct{})
	quitWaitingForConns := make(chan struct{})
	defer close(quitWaitingForConns)
	go func() {
		defer close(allDone)
		for _, conn := range conns {
			// Wait for the connections with explicit transaction or an executing auto-commit query.
			if conn.getStatus() == connStatusReading && !conn.getCtx().GetSessionVars().InTxn() {
				// The waitgroup is not protected by the `quitWaitingForConns`. However, the implementation
				// of `client-go` will guarantee this `Wait` will return at least after killing the
				// connections. We also wait for a similar `WaitGroup` on the store after killing the connections.
				//
				// Therefore, it'll not cause goroutine leak. Even if, it's not a big issue when the TiDB is
				// going to shutdown.
				//
				// It should be waited for connections in all status, even if it's not in transactions and is reading
				// from the client. Because it may run background commit goroutines at any time.
				conn.getCtx().Session.GetCommitWaitGroup().Wait()

				continue
			}
			select {
			case <-conn.quit:
			case <-quitWaitingForConns:
				return
			}

			// Wait for the commit wait group after waiting for the `conn.quit` channel to make sure the foreground
			// process has finished to avoid the situation that after waiting for the wait group, the transaction starts
			// a new background goroutine and increase the wait group.
			conn.getCtx().Session.GetCommitWaitGroup().Wait()
		}
	}()

	select {
	case <-allDone:
		logger.Info("all sessions quit in drain wait time")
	case <-time.After(drainWait):
		logger.Info("timeout waiting all sessions quit")
	}

	s.KillAllConnections()

	select {
	case <-allDone:
	case <-time.After(cancelWait):
		logger.Warn("some sessions do not quit in cancel wait time")
	}
}
