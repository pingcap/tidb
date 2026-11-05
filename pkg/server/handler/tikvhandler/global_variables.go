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

package tikvhandler

import (
	"context"
	"net/http"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/server/handler"
	"github.com/pingcap/tidb/pkg/session"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

// GlobalVariablesHandler serves global variables with sensitive values masked.
type GlobalVariablesHandler struct {
	*handler.TikvHandlerTool
}

// NewGlobalVariablesHandler creates a GlobalVariablesHandler.
func NewGlobalVariablesHandler(tool *handler.TikvHandlerTool) *GlobalVariablesHandler {
	return &GlobalVariablesHandler{tool}
}

// ServeHTTP implements the HTTPHandler interface.
func (h GlobalVariablesHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	if req.Method != http.MethodGet {
		handler.WriteErrorWithCode(w, http.StatusMethodNotAllowed, errors.New("method not allowed"))
		return
	}

	// Only getters that propagate ctx honor this timeout and client cancellation.
	// GC getters currently read mysql.tidb through GetTiDBTableValue with
	// context.TODO(), so storage stalls can keep this handler and its session
	// alive beyond the deadline.
	ctx, cancel := context.WithTimeout(req.Context(), requestDefaultTimeout)
	defer cancel()
	s, err := session.CreateSession(h.Store)
	if err != nil {
		logutil.BgLogger().Error("unable to create session for global variables", zap.Error(err))
		handler.WriteErrorWithCode(w, http.StatusInternalServerError, errors.New("unable to read global variables"))
		return
	}
	defer s.Close()

	sysVars := variable.GetSysVars()
	values := make(map[string]string, len(sysVars))
	for _, sv := range sysVars {
		if sv.Scope == vardef.ScopeSession || sv.IsNoop && !vardef.EnableNoopVariables.Load() {
			continue
		}
		// tidb_cloud_storage_uri's getter already applies ast.RedactURL, preserving
		// non-secret URI details without needing additional redaction here.
		value, err := sv.GetGlobalFromHook(ctx, s.GetSessionVars())
		if err != nil {
			logutil.BgLogger().Error("unable to read global variable", zap.String("name", sv.Name), zap.Error(err))
			handler.WriteErrorWithCode(w, http.StatusInternalServerError, errors.New("unable to read global variables"))
			return
		}
		// Getters do not uniformly hide secrets: some return configuration or SQL
		// text unchanged, and embedding API keys may retain a suffix. Fully mask
		// non-empty sensitive values here, even if a getter already masks them
		// (e.g. LDAP passwords), while preserving empty values as unset.
		if sv.IsSensitive && value != "" {
			value = vardef.MaskPwd
		}
		values[sv.Name] = value
	}
	handler.WriteData(w, values)
}
