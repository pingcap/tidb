// Copyright 2019 PingCAP, Inc.
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

package bindinfo

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/sessionstates"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/hack"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

// SessionHandle is used to handle all session sql bind operations.
type SessionHandle struct {
	ch *bindCache
}

// NewSessionBindHandle creates a new SessionBindHandle.
func NewSessionBindHandle() *SessionHandle {
	sessionHandle := &SessionHandle{}
	sessionHandle.ch = newBindCache()
	return sessionHandle
}

// appendBindRecord adds the BindRecord to the cache, all the stale bindMetas are
// removed from the cache after this operation.
func (h *SessionHandle) appendBindRecord(hash string, meta *BindRecord) {
	oldRecord := h.ch.GetBindRecord(hash, meta.OriginalSQL, meta.Db)
	err := h.ch.SetBindRecord(hash, meta)
	if err != nil {
		logutil.BgLogger().Warn("[sql-bind] SessionHandle.appendBindRecord", zap.Error(err))
	}
	updateMetrics(metrics.ScopeSession, oldRecord, meta, false)
}

// CreateBindRecord creates a BindRecord to the cache.
// It replaces all the exists bindings for the same normalized SQL.
func (h *SessionHandle) CreateBindRecord(sctx sessionctx.Context, record *BindRecord) (err error) {
	err = record.prepareHints(sctx)
	if err != nil {
		return err
	}
	record.Db = strings.ToLower(record.Db)
	now := types.NewTime(types.FromGoTime(time.Now().In(sctx.GetSessionVars().StmtCtx.TimeZone)), mysql.TypeTimestamp, 3)
	for i := range record.Bindings {
		record.Bindings[i].CreateTime = now
		record.Bindings[i].UpdateTime = now
	}

	// update the BindMeta to the cache.
	h.appendBindRecord(parser.DigestNormalized(record.OriginalSQL).String(), record)
	return nil
}

// DropBindRecord drops a BindRecord in the cache.
func (h *SessionHandle) DropBindRecord(originalSQL, db string, binding *Binding) error {
	db = strings.ToLower(db)
	hash := parser.DigestNormalized(originalSQL).String()
	oldRecord := h.GetBindRecord(hash, originalSQL, db)
	var newRecord *BindRecord
	record := &BindRecord{OriginalSQL: originalSQL, Db: db}
	if binding != nil {
		record.Bindings = append(record.Bindings, *binding)
	}
	if oldRecord != nil {
		newRecord = oldRecord.remove(record)
	} else {
		newRecord = record
	}
	err := h.ch.SetBindRecord(hash, newRecord)
	if err != nil {
		// Should never reach here, just return an error for safety
		return err
	}
	updateMetrics(metrics.ScopeSession, oldRecord, newRecord, false)
	return nil
}

// DropBindRecordByDigest drop BindRecord in the cache.
func (h *SessionHandle) DropBindRecordByDigest(sqlDigest string) error {
	oldRecord, err := h.GetBindRecordBySQLDigest(sqlDigest)
	if err != nil {
		return err
	}
	return h.DropBindRecord(oldRecord.OriginalSQL, strings.ToLower(oldRecord.Db), nil)
}

// GetBindRecord return the BindMeta of the (normdOrigSQL,db) if BindMeta exist.
func (h *SessionHandle) GetBindRecord(hash, normdOrigSQL, db string) *BindRecord {
	return h.ch.GetBindRecord(hash, normdOrigSQL, db)
}

// GetBindRecordBySQLDigest return all BindMeta corresponding to sqlDigest.
func (h *SessionHandle) GetBindRecordBySQLDigest(sqlDigest string) (*BindRecord, error) {
	return h.ch.GetBindRecordBySQLDigest(sqlDigest)
}

// GetAllBindRecord return all session bind info.
func (h *SessionHandle) GetAllBindRecord() (bindRecords []*BindRecord) {
	return h.ch.GetAllBindRecords()
}

// EncodeSessionStates implements SessionStatesHandler.EncodeSessionStates interface.
func (h *SessionHandle) EncodeSessionStates(ctx context.Context, sctx sessionctx.Context, sessionStates *sessionstates.SessionStates) error {
	bindRecords := h.ch.GetAllBindRecords()
	if len(bindRecords) == 0 {
		return nil
	}
	bytes, err := json.Marshal(bindRecords)
	if err != nil {
		return err
	}
	sessionStates.Bindings = string(hack.String(bytes))
	return nil
}

// DecodeSessionStates implements SessionStatesHandler.DecodeSessionStates interface.
func (h *SessionHandle) DecodeSessionStates(ctx context.Context, sctx sessionctx.Context, sessionStates *sessionstates.SessionStates) error {
	if len(sessionStates.Bindings) == 0 {
		return nil
	}
	var records []*BindRecord
	if err := json.Unmarshal(hack.Slice(sessionStates.Bindings), &records); err != nil {
		return err
	}
	for _, record := range records {
		// Restore hints and ID because hints are hard to encode.
		if err := record.prepareHints(sctx); err != nil {
			return err
		}
		h.appendBindRecord(parser.DigestNormalized(record.OriginalSQL).String(), record)
	}
	return nil
}

// Close closes the session handle.
func (h *SessionHandle) Close() {
	for _, bindRecord := range h.ch.GetAllBindRecords() {
		updateMetrics(metrics.ScopeSession, bindRecord, nil, false)
	}
}

// sessionBindInfoKeyType is a dummy type to avoid naming collision in context.
type sessionBindInfoKeyType int

// String defines a Stringer function for debugging and pretty printing.
func (k sessionBindInfoKeyType) String() string {
	return "session_bindinfo"
}

// SessionBindInfoKeyType is a variable key for store session bind info.
const SessionBindInfoKeyType sessionBindInfoKeyType = 0
