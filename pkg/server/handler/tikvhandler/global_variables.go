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
	"github.com/pingcap/tidb/pkg/util/sem/compat"
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

	ctx, cancel := context.WithTimeout(req.Context(), requestDefaultTimeout)
	defer cancel()
	if ctx.Err() != nil {
		handler.WriteErrorWithCode(w, http.StatusInternalServerError, errors.New("unable to read global variables"))
		return
	}
	s, err := session.CreateSession(h.Store)
	if err != nil {
		handler.WriteErrorWithCode(w, http.StatusInternalServerError, errors.New("unable to read global variables"))
		return
	}
	defer s.Close()

	sysVars := variable.GetSysVars()
	values := make(map[string]string, len(sysVars))
	for _, sv := range sysVars {
		if ctx.Err() != nil {
			handler.WriteErrorWithCode(w, http.StatusInternalServerError, errors.New("unable to read global variables"))
			return
		}
		if sv.Scope == vardef.ScopeSession || sv.IsNoop && !vardef.EnableNoopVariables.Load() {
			continue
		}
		// The internal session must not bypass SEM visibility for this HTTP API.
		if compat.IsInvisibleSysVar(sv.Name) {
			continue
		}
		if sv.IsSensitive {
			values[sv.Name] = "******"
			continue
		}
		value, err := sv.GetGlobalFromHook(ctx, s.GetSessionVars())
		if err != nil {
			handler.WriteErrorWithCode(w, http.StatusInternalServerError, errors.New("unable to read global variables"))
			return
		}
		values[sv.Name] = value
	}
	if ctx.Err() != nil {
		handler.WriteErrorWithCode(w, http.StatusInternalServerError, errors.New("unable to read global variables"))
		return
	}
	handler.WriteData(w, values)
}
