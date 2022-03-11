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
	"strings"
	"time"

	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types"
)

// SessionHandle is used to handle all session sql bind operations.
type SessionHandle struct {
	ch     *bindCache
	parser *parser.Parser
}

// NewSessionBindHandle creates a new SessionBindHandle.
func NewSessionBindHandle(parser *parser.Parser) *SessionHandle {
	sessionHandle := &SessionHandle{parser: parser}
	sessionHandle.ch = newBindCache()
	return sessionHandle
}

// appendBindRecord adds the BindRecord to the cache, all the stale bindMetas are
// removed from the cache after this operation.
func (h *SessionHandle) appendBindRecord(hash string, meta *BindRecord) {
	oldRecord := h.ch.GetBindRecord(hash, meta.OriginalSQL, meta.Db)
	h.ch.SetBindRecord(hash, meta)
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
	h.ch.SetBindRecord(hash, newRecord)
	updateMetrics(metrics.ScopeSession, oldRecord, newRecord, false)
	return nil
}

// GetBindRecord return the BindMeta of the (normdOrigSQL,db) if BindMeta exist.
func (h *SessionHandle) GetBindRecord(hash, normdOrigSQL, db string) *BindRecord {
	return h.ch.GetBindRecord(hash, normdOrigSQL, db)
}

// GetAllBindRecord return all session bind info.
func (h *SessionHandle) GetAllBindRecord() (bindRecords []*BindRecord) {
	return h.ch.GetAllBindRecords()
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
