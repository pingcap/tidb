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
// See the License for the specific language governing permissions and
// limitations under the License.

package bindinfo

import (
	"time"

	"github.com/pingcap/parser"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types"
)

// SessionHandle is used to handle all session sql bind operations.
type SessionHandle struct {
	ch     cache
	parser *parser.Parser
}

// NewSessionBindHandle creates a new SessionBindHandle.
func NewSessionBindHandle(parser *parser.Parser) *SessionHandle {
	sessionHandle := &SessionHandle{parser: parser}
	sessionHandle.ch = make(cache)
	return sessionHandle
}

// appendBindRecord adds the BindRecord to the cache, all the stale bindMetas are
// removed from the cache after this operation.
func (h *SessionHandle) appendBindRecord(meta *BindRecord) {
	oldRecord := h.ch.getBindRecord(meta)
	h.ch.setBindRecord(meta)
	updateMetrics(metrics.ScopeSession, oldRecord, meta, false)
}

// CreateBindRecord creates a BindRecord to the cache.
// It replaces all the exists bindings for the same normalized SQL.
func (h *SessionHandle) CreateBindRecord(sctx sessionctx.Context, record *BindRecord) (err error) {
	err = record.prepareHints(sctx)
	if err != nil {
		return err
	}
	now := types.NewTime(types.FromGoTime(time.Now().In(sctx.GetSessionVars().StmtCtx.TimeZone)), mysql.TypeTimestamp, 3)
	for i := range record.Bindings {
		record.Bindings[i].CreateTime = now
		record.Bindings[i].UpdateTime = now
	}

	// update the BindMeta to the cache.
	h.appendBindRecord(record)
	return nil
}

// DropBindRecord drops a BindRecord in the cache.
func (h *SessionHandle) DropBindRecord(sctx sessionctx.Context, record *BindRecord, binding *Binding) error {
	oldRecord := h.GetBindRecord(record)
	var newRecord *BindRecord
	if binding != nil {
		record.Bindings = append(record.Bindings, *binding)
	}
	err := record.prepareHints(nil)
	if err != nil {
		return err
	}
	if oldRecord != nil {
		newRecord = oldRecord.remove(record)
	} else {
		newRecord = record
	}
	h.ch.setBindRecord(newRecord)
	updateMetrics(metrics.ScopeSession, oldRecord, newRecord, false)
	return nil
}

// GetBindRecord return the BindMeta of the (normdOrigSQL,db) if BindMeta exist.
func (h *SessionHandle) GetBindRecord(record *BindRecord) *BindRecord {
	bindRecords := h.ch[record.StmtDigest]
	for _, bindRecord := range bindRecords {
		if bindRecord.Db == record.Db {
			return bindRecord
		}
	}
	return nil
}

// GetAllBindRecord return all session bind info.
func (h *SessionHandle) GetAllBindRecord() (bindRecords []*BindRecord) {
	for _, bindRecord := range h.ch {
		bindRecords = append(bindRecords, bindRecord...)
	}
	return bindRecords
}

// Close closes the session handle.
func (h *SessionHandle) Close() {
	for _, bindRecords := range h.ch {
		for _, bindRecord := range bindRecords {
			updateMetrics(metrics.ScopeSession, bindRecord, nil, false)
		}
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
