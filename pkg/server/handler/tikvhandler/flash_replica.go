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
	"net/http"
	"strconv"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/config"
	infoschemacontext "github.com/pingcap/tidb/pkg/infoschema/context"
	"github.com/pingcap/tidb/pkg/server/handler"
	"github.com/pingcap/tidb/pkg/session"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
)

// FlashReplicaSummary is a best-effort live TiFlash replica count for cluster
// operators. It does not include per-table identity or drop/truncate leftovers.
// Callers that set `tidb_columnar_storage_enabled=OFF` must not treat can_disable
// as a distributed lock: coordinate the database users so workloads do not add
// replicas during the check/operate window, and verify again afterward.
type FlashReplicaSummary struct {
	Keyspace                   string `json:"keyspace"`
	KeyspaceID                 uint32 `json:"keyspace_id"`
	TiDBColumnarStorageEnabled string `json:"tidb_columnar_storage_enabled"`
	ColumnarStoreType          string `json:"columnar_store_type"`
	CanDisable                 bool   `json:"can_disable"`
	TableCount                 int    `json:"table_count"`
	// synced schema before counting
	Reloaded bool `json:"reloaded"`
}

// FlashReplicaSummaryHandler serves GET /tiflash/replica.
type FlashReplicaSummaryHandler struct {
	*handler.TikvHandlerTool
}

// NewFlashReplicaSummaryHandler creates a FlashReplicaSummaryHandler.
func NewFlashReplicaSummaryHandler(tool *handler.TikvHandlerTool) *FlashReplicaSummaryHandler {
	return &FlashReplicaSummaryHandler{tool}
}

// ServeHTTP implements the HTTPHandler interface.
func (h FlashReplicaSummaryHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodGet {
		handler.WriteErrorWithCode(w, http.StatusMethodNotAllowed, errors.New("method not allowed"))
		return
	}

	doReload, err := parseFlashReplicaReloadQuery(req)
	if err != nil {
		handler.WriteErrorWithCode(w, http.StatusBadRequest, err)
		return
	}

	dom, err := session.GetDomain(h.Store)
	if err != nil {
		handler.WriteErrorWithCode(w, http.StatusInternalServerError, err)
		return
	}

	// Default (?reload omitted or false): return a cheap in-memory tiflash
	// replica summary snapshot of InfoSchema without Domain.Reload.
	//
	// Optional (?reload=true): Domain.Reload reduces lease-lag
	// under-counts on non-owner nodes (stale InfoSchema missing a recent
	// SET TIFLASH REPLICA 1 can inflate can_disable), but still is not a
	// linearizable cluster lock and serializes with other schema sync /
	// Reload callers.
	if doReload {
		if err := dom.Reload(); err != nil {
			handler.WriteErrorWithCode(w, http.StatusInternalServerError, err)
			return
		}
	}
	is := dom.InfoSchema()
	if is == nil {
		handler.WriteErrorWithCode(w, http.StatusInternalServerError, errors.New("schema is unavailable"))
		return
	}

	tableCount := 0
	for _, db := range is.ListTablesWithSpecialAttribute(infoschemacontext.TiFlashAttribute) {
		tableCount += len(db.TableInfos)
	}

	enabledVal, err := dom.GetGlobalVar(vardef.TiDBColumnarStorageEnabled)
	if err != nil {
		handler.WriteErrorWithCode(w, http.StatusInternalServerError, err)
		return
	}

	handler.WriteData(w, &FlashReplicaSummary{
		Keyspace:                   h.Store.GetKeyspace(),
		KeyspaceID:                 uint32(h.Store.GetCodec().GetKeyspaceID()),
		TiDBColumnarStorageEnabled: variable.BoolToOnOff(variable.TiDBOptOn(enabledVal)),
		ColumnarStoreType:          config.GetGlobalConfig().CSE.ColumnarStoreType,
		CanDisable:                 tableCount == 0,
		TableCount:                 tableCount,
		Reloaded:                   doReload,
	})
}

// parseFlashReplicaReloadQuery returns whether to Reload InfoSchema.
// Missing reload defaults to false. Explicit values use strconv.ParseBool.
func parseFlashReplicaReloadQuery(req *http.Request) (bool, error) {
	raw := req.URL.Query().Get("reload")
	if raw == "" {
		return false, nil
	}
	doReload, err := strconv.ParseBool(raw)
	if err != nil {
		return false, errors.Errorf("invalid reload query value %q, expect true/false/1/0", raw)
	}
	return doReload, nil
}
