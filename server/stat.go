// Copyright 2018 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package server

import (
	"time"

	"github.com/pingcap/tidb/sessionctx/variable"
)

var (
	uptime = "Uptime"
)

// GetScope gets the status variables scope.
func (s *Server) GetScope(status string) variable.ScopeFlag {
	// Now server status variables scope are all default scope.
	return variable.DefaultStatusVarScopeFlag
}

// Stats returns the server statistics.
func (s *Server) Stats(vars *variable.SessionVars) (map[string]interface{}, error) {
	m := make(map[string]interface{})
	m[uptime] = int(time.Since(s.startTime).Seconds())
	return m, nil
}
