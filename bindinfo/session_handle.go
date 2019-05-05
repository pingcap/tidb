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

// appendBindMeta addes the BindMeta to the cache, all the stale bindMetas are
// removed from the cache after this operation.
func (h *SessionHandle) appendBindMeta(hash string, meta *BindMeta) {
	// Make sure there is only one goroutine writes the cache.
	h.ch.removeStaleBindMetas(hash, meta)
	h.ch[hash] = append(h.ch[hash], meta)
}

func (h *SessionHandle) newBindMeta(record *BindRecord) (hash string, meta *BindMeta, err error) {
	hash = parser.DigestHash(record.OriginalSQL)
	stmtNodes, _, err := h.parser.Parse(record.BindSQL, record.Charset, record.Collation)
	if err != nil {
		return "", nil, err
	}
	meta = &BindMeta{BindRecord: record, Ast: stmtNodes[0]}
	return hash, meta, nil
}

// AddBindRecord new a BindRecord with BindMeta, add it to the cache.
func (h *SessionHandle) AddBindRecord(record *BindRecord) error {
	record.CreateTime = types.Time{
		Time: types.FromGoTime(time.Now()),
		Type: mysql.TypeDatetime,
		Fsp:  3,
	}
	record.UpdateTime = record.CreateTime

	// update the BindMeta to the cache.
	hash, meta, err := h.newBindMeta(record)
	if err == nil {
		h.appendBindMeta(hash, meta)
	}
	return err
}

// DropBindRecord drops a BindRecord in the cache.
func (h *SessionHandle) DropBindRecord(record *BindRecord) {
	meta := &BindMeta{BindRecord: record}
	meta.Status = deleted
	hash := parser.DigestHash(record.OriginalSQL)
	h.ch.removeDeletedBindMeta(hash, meta)
	h.appendBindMeta(hash, meta)
}

// GetBindRecord return the BindMeta of the (normdOrigSQL,db) if BindMeta exist.
func (h *SessionHandle) GetBindRecord(normdOrigSQL, db string) *BindMeta {
	hash := parser.DigestHash(normdOrigSQL)
	bindRecords := h.ch[hash]
	if bindRecords != nil {
		for _, bindRecord := range bindRecords {
			if bindRecord.OriginalSQL == normdOrigSQL && bindRecord.Db == db {
				return bindRecord
			}
		}
	}
	return nil
}

// GetAllBindRecord return all session bind info.
func (h *SessionHandle) GetAllBindRecord() (bindRecords []*BindMeta) {
	for _, bindRecord := range h.ch {
		bindRecords = append(bindRecords, bindRecord...)
	}
	return bindRecords
}

// sessionBindInfoKeyType is a dummy type to avoid naming collision in context.
type sessionBindInfoKeyType int

// String defines a Stringer function for debugging and pretty printing.
func (k sessionBindInfoKeyType) String() string {
	return "session_bindinfo"
}

// SessionBindInfoKeyType is a variable key for store session bind info.
const SessionBindInfoKeyType sessionBindInfoKeyType = 0
