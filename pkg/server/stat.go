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
	"time"

	"github.com/emmansun/gmsm/smx509"
	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/server/handler/tikvhandler"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

var (
	serverNotAfter  = "Ssl_server_not_after"
	serverNotBefore = "Ssl_server_not_before"
	upTime          = "Uptime"
	tlcpEncNotAfter  = "tlcp_server_enc_not_after"
	tlcpEncNotBefore = "tlcp_server_enc_not_before"
	tlcpSigNotAfter  = "tlcp_server_sign_not_after"
	tlcpSigNotBefore = "tlcp_server_sign_not_before"
)

var defaultStatus = map[string]*variable.StatusVal{
	serverNotAfter:  {Scope: variable.ScopeGlobal | variable.ScopeSession, Value: ""},
	serverNotBefore: {Scope: variable.ScopeGlobal | variable.ScopeSession, Value: ""},
	upTime:          {Scope: variable.ScopeGlobal, Value: 0},
	tlcpEncNotAfter:  {Scope: variable.ScopeGlobal | variable.ScopeSession, Value: ""},
	tlcpEncNotBefore: {Scope: variable.ScopeGlobal | variable.ScopeSession, Value: ""},
	tlcpSigNotAfter:  {Scope: variable.ScopeGlobal | variable.ScopeSession, Value: ""},
	tlcpSigNotBefore: {Scope: variable.ScopeGlobal | variable.ScopeSession, Value: ""},
}

// GetScope gets the Status variables scope.
func (*Server) GetScope(_ string) variable.ScopeFlag {
	return variable.DefaultStatusVarScopeFlag
}

// Stats returns the server statistics.
func (s *Server) Stats(_ *variable.SessionVars) (map[string]any, error) {
	m := make(map[string]any, len(defaultStatus))

	for name, v := range defaultStatus {
		m[name] = v.Value
	}

	tlsConfig := s.GetTLSConfig()
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

	tlcpConfig := s.getTLCPConfig()
	if tlcpConfig != nil {
		if len(tlcpConfig.Certificates) == 2 {
			sig, err := smx509.ParseCertificate(tlcpConfig.Certificates[0].Certificate[0])
			if err != nil {
				logutil.BgLogger().Error("Failed to parse TLCP signature certficate to get server status", zap.Error(err))
			} else {
				m[tlcpSigNotAfter] = sig.NotAfter.Format("Jan _2 15:04:05 2006 MST")
				m[tlcpSigNotBefore] = sig.NotBefore.Format("Jan _2 15:04:05 2006 MST")
			}

			enc, err := smx509.ParseCertificate(tlcpConfig.Certificates[1].Certificate[0])
			if err != nil {
				logutil.BgLogger().Error("Failed to parse TLCP encryption certficate to get server status", zap.Error(err))
			} else {
				m[tlcpEncNotAfter] = enc.NotAfter.Format("Jan _2 15:04:05 2006 MST")
				m[tlcpEncNotBefore] = enc.NotBefore.Format("Jan _2 15:04:05 2006 MST")
			}
		} else {
			logutil.BgLogger().Error("TLCP use different certficates to signature and encryption")
		}
	}

	var err error
	info := tikvhandler.ServerInfo{}
	info.ServerInfo, err = infosync.GetServerInfo()
	if err != nil {
		logutil.BgLogger().Error("Failed to get ServerInfo for uptime status", zap.Error(err))
	} else {
		m[upTime] = int64(time.Since(time.Unix(info.ServerInfo.StartTimestamp, 0)).Seconds())
	}

	return m, nil
}
