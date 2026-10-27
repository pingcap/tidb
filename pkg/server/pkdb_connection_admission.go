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

import servererr "github.com/pingcap/tidb/pkg/server/err"

func (s *Server) checkReservedAdminConnection(conn *clientConn) error {
	maxConnections := uint64(s.cfg.Instance.MaxConnections)
	if maxConnections == 0 {
		return nil
	}

	// shortcut: only registered connections are counted; atomic admission is needed for strict concurrent limits.
	currentConnections := uint64(s.ConnectionCount())
	if currentConnections < maxConnections {
		return nil
	}
	if currentConnections < maxConnections+uint64(s.cfg.Instance.ReservedAdminConnections) && conn.isAdminConnection() {
		return nil
	}
	return servererr.ErrConCount
}

func (cc *clientConn) isAdminConnection() bool {
	ctx := cc.getCtx()
	if ctx == nil {
		return false
	}
	if user := ctx.GetSessionVars().User; user != nil && user.AuthUsername == "root" {
		return true
	}
	return cc.skipInitConnect()
}
