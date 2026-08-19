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

	"github.com/pingcap/errors"
	infoschemacontext "github.com/pingcap/tidb/pkg/infoschema/context"
	"github.com/pingcap/tidb/pkg/server/handler"
	"github.com/pingcap/tidb/pkg/session"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
)

// FlashReplicaSummary is the live TiFlash replica count for cluster operators.
// It does not include per-table identity or drop/truncate leftovers.
type FlashReplicaSummary struct {
	Keyspace                   string `json:"keyspace"`
	KeyspaceID                 uint32 `json:"keyspace_id"`
	TiDBColumnarStorageEnabled string `json:"tidb_columnar_storage_enabled"`
	CanDisable                 bool   `json:"can_disable"`
	TableCount                 int    `json:"table_count"`
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

	dom, err := session.GetDomain(h.Store)
	if err != nil {
		handler.WriteErrorWithCode(w, http.StatusInternalServerError, err)
		return
	}
	// Reload from the latest committed schema version before counting.
	// Non-owner nodes learn schema versions through etcd watch and may lag by a
	// lease; a stale snapshot can under-count live replicas and report a false
	// can_disable. Fail closed on Reload error instead of using that snapshot.
	if err := dom.Reload(); err != nil {
		handler.WriteErrorWithCode(w, http.StatusInternalServerError, err)
		return
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
		CanDisable:                 tableCount == 0,
		TableCount:                 tableCount,
	})
}
