// Copyright 2015 PingCAP, Inc.
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
	"fmt"
	"sync"
	"time"

	"github.com/pingcap/tidb/pkg/infoschema/issyncer/mdldef"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/session"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	statsutil "github.com/pingcap/tidb/pkg/statistics/handle/util"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/timeutil"
	tlsutil "github.com/pingcap/tidb/pkg/util/tls"
	"go.uber.org/zap"
)

func (cc *clientConn) connectInfo() *variable.ConnectionInfo {
	connType := variable.ConnTypeSocket
	sslVersion := ""
	if cc.isUnixSocket {
		connType = variable.ConnTypeUnixSocket
	} else if cc.tlsConn != nil {
		connType = variable.ConnTypeTLS
		sslVersionNum := cc.tlsConn.ConnectionState().Version
		switch sslVersionNum {
		case tls.VersionTLS12:
			sslVersion = "TLSv1.2"
		case tls.VersionTLS13:
			sslVersion = "TLSv1.3"
		default:
			sslVersion = fmt.Sprintf("Unknown TLS version: %d", sslVersionNum)
		}
	}
	connInfo := &variable.ConnectionInfo{
		ConnectionID:      cc.connectionID,
		ConnectionType:    connType,
		Host:              cc.peerHost,
		ClientIP:          cc.peerHost,
		ClientPort:        cc.peerPort,
		ServerID:          1,
		ServerIP:          cc.serverHost,
		ServerPort:        int(cc.server.cfg.Port),
		User:              cc.user,
		ServerOSLoginUser: osUser,
		OSVersion:         osVersion,
		ServerVersion:     mysql.TiDBReleaseVersion,
		SSLVersion:        sslVersion,
		PID:               serverPID,
		DB:                cc.dbname,
		AuthMethod:        cc.authPlugin,
		Attributes:        cc.attrs,
	}
	return connInfo
}


// ServerID implements SessionManager interface.
func (s *Server) ServerID() uint64 {
	return s.dom.ServerID()
}

// StoreInternalSession implements SessionManager interface.
// @param addr	The address of a session.session struct variable
func (s *Server) StoreInternalSession(se any) {
	s.sessionMapMutex.Lock()
	s.internalSessions[se] = struct{}{}
	metrics.InternalSessions.Set(float64(len(s.internalSessions)))
	s.sessionMapMutex.Unlock()
}

// ContainsInternalSession implements SessionManager interface.
func (s *Server) ContainsInternalSession(se any) bool {
	s.sessionMapMutex.Lock()
	defer s.sessionMapMutex.Unlock()
	_, ok := s.internalSessions[se]
	return ok
}

// InternalSessionCount implements sessionmgr.InfoSchemaCoordinator interface.
func (s *Server) InternalSessionCount() int {
	s.sessionMapMutex.Lock()
	defer s.sessionMapMutex.Unlock()
	return len(s.internalSessions)
}

// DeleteInternalSession implements SessionManager interface.
// @param addr	The address of a session.session struct variable
func (s *Server) DeleteInternalSession(se any) {
	s.sessionMapMutex.Lock()
	delete(s.internalSessions, se)
	metrics.InternalSessions.Set(float64(len(s.internalSessions)))
	s.sessionMapMutex.Unlock()
}

// GetInternalSessionStartTSList implements SessionManager interface.
func (s *Server) GetInternalSessionStartTSList() []uint64 {
	s.sessionMapMutex.Lock()
	defer s.sessionMapMutex.Unlock()
	tsList := make([]uint64, 0, len(s.internalSessions))
	for se := range s.internalSessions {
		if ts, processInfoID := session.GetStartTSFromSession(se); ts != 0 {
			if statsutil.GlobalAutoAnalyzeProcessList.Contains(processInfoID) {
				continue
			}
			tsList = append(tsList, ts)
		}
	}
	return tsList
}

// InternalSessionExists is used for test
func (s *Server) InternalSessionExists(se any) bool {
	s.sessionMapMutex.Lock()
	_, ok := s.internalSessions[se]
	s.sessionMapMutex.Unlock()
	return ok
}

// setSysTimeZoneOnce is used for parallel run tests. When several servers are running,
// only the first will actually do setSystemTimeZoneVariable, thus we can avoid data race.
var setSysTimeZoneOnce = &sync.Once{}

func setSystemTimeZoneVariable() {
	setSysTimeZoneOnce.Do(func() {
		tz, err := timeutil.GetSystemTZ()
		if err != nil {
			logutil.BgLogger().Error(
				"Error getting SystemTZ, use default value instead",
				zap.Error(err),
				zap.String("default system_time_zone", variable.GetSysVar("system_time_zone").Value))
			return
		}
		variable.SetSysVar("system_time_zone", tz)
	})
}

// CheckOldRunningTxn implements SessionManager interface.
func (s *Server) CheckOldRunningTxn(jobs map[int64]*mdldef.JobMDL) {
	s.rwlock.RLock()
	defer s.rwlock.RUnlock()

	printLog := false
	if time.Since(s.printMDLLogTime) > 10*time.Second {
		printLog = true
		s.printMDLLogTime = time.Now()
	}
	for _, client := range s.clients {
		se := client.ctx.Session
		if se != nil {
			variable.RemoveLockDDLJobs(se.GetSessionVars(), jobs, printLog)
		}
	}
}

// KillNonFlashbackClusterConn implements SessionManager interface.
func (s *Server) KillNonFlashbackClusterConn() {
	s.rwlock.RLock()
	connIDs := make([]uint64, 0, len(s.clients))
	for _, client := range s.clients {
		if client.ctx.Session != nil {
			processInfo := client.ctx.Session.ShowProcess()
			ddl, ok := processInfo.StmtCtx.GetPlan().(*core.DDL)
			if !ok {
				connIDs = append(connIDs, client.connectionID)
				continue
			}
			_, ok = ddl.Statement.(*ast.FlashBackToTimestampStmt)
			if !ok {
				connIDs = append(connIDs, client.connectionID)
				continue
			}
		}
	}
	s.rwlock.RUnlock()
	for _, id := range connIDs {
		s.Kill(id, false, false, false)
	}
}

// GetStatusVars is getting the per process status variables from the server
func (s *Server) GetStatusVars() map[uint64]map[string]string {
	s.rwlock.RLock()
	defer s.rwlock.RUnlock()
	rs := make(map[uint64]map[string]string)
	for _, client := range s.clients {
		if pi := client.ctx.ShowProcess(); pi != nil {
			if client.tlsConn != nil {
				connState := client.tlsConn.ConnectionState()
				rs[pi.ID] = map[string]string{
					"Ssl_cipher":  tlsutil.CipherSuiteName(connState.CipherSuite),
					"Ssl_version": tlsutil.VersionName(connState.Version),
				}
			}
		}
	}
	return rs
}

// Health returns if the server is healthy (begin to shut down)
func (s *Server) Health() bool {
	return s.health.Load()
}
