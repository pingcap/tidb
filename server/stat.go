// Copyright 2021 PingCAP, Inc.
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
	"crypto/x509"

	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

var (
	serverNotAfter  = "Ssl_server_not_after"
	serverNotBefore = "Ssl_server_not_before"
)

var defaultStatus = map[string]*variable.StatusVal{
	serverNotAfter:  {Scope: variable.ScopeGlobal | variable.ScopeSession, Value: ""},
	serverNotBefore: {Scope: variable.ScopeGlobal | variable.ScopeSession, Value: ""},
}

// GetScope gets the status variables scope.
func (s *Server) GetScope(status string) variable.ScopeFlag {
	return variable.DefaultStatusVarScopeFlag
}

// Stats returns the server statistics.
func (s *Server) Stats(vars *variable.SessionVars) (map[string]interface{}, error) {
	m := make(map[string]interface{}, len(defaultStatus))

	for name, v := range defaultStatus {
		m[name] = v.Value
	}

	tlsConfig := s.getTLSConfig()
	if tlsConfig != nil {
		if len(tlsConfig.Certificates) == 1 {
			pc, err := x509.ParseCertificate(tlsConfig.Certificates[0].Certificate[0])
			if err != nil {
				logutil.BgLogger().Error("Failed to parse TLS certficates to get server status", zap.Error(err))
			} else {
				m[serverNotAfter] = pc.NotAfter.Format("Jan _2 15:04:05 2006 MST")
				m[serverNotBefore] = pc.NotBefore.Format("Jan _2 15:04:05 2006 MST")
			}
		}
	}
	return m, nil
}
