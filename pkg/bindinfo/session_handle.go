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

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/sessionstates"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/hack"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

// SessionBindingHandle is used to handle all session sql bind operations.
type SessionBindingHandle interface {
	// CreateSessionBinding creates a binding to the cache.
	CreateSessionBinding(sctx sessionctx.Context, record *BindRecord) (err error)

	// DropSessionBinding drops a binding by the sql digest.
	DropSessionBinding(sqlDigest string) error

	// MatchSessionBinding returns the matched binding for this statement.
	MatchSessionBinding(currentDB string, stmt ast.StmtNode) (*BindRecord, error)

	// GetAllSessionBindings return all bindings.
	GetAllSessionBindings() (bindRecords []*BindRecord)

	// Close closes the SessionBindingHandle.
	Close()

	sessionctx.SessionStatesHandler
}

// sessionBindingHandle is used to handle all session sql bind operations.
type sessionBindingHandle struct {
	ch *bindCache
}

// NewSessionBindingHandle creates a new SessionBindingHandle.
func NewSessionBindingHandle() SessionBindingHandle {
	sessionHandle := &sessionBindingHandle{}
	sessionHandle.ch = newBindCache()
	return sessionHandle
}

// appendSessionBinding adds the BindRecord to the cache, all the stale bindMetas are
// removed from the cache after this operation.
func (h *sessionBindingHandle) appendSessionBinding(sqlDigest string, meta *BindRecord) {
	oldRecord := h.ch.GetBinding(sqlDigest)
	err := h.ch.SetBinding(sqlDigest, meta)
	if err != nil {
		logutil.BgLogger().Warn("SessionHandle.appendBindRecord", zap.String("category", "sql-bind"), zap.Error(err))
	}
	updateMetrics(metrics.ScopeSession, oldRecord, meta, false)
}

// CreateSessionBinding creates a BindRecord to the cache.
// It replaces all the exists bindings for the same normalized SQL.
func (h *sessionBindingHandle) CreateSessionBinding(sctx sessionctx.Context, record *BindRecord) (err error) {
	err = record.prepareHints(sctx)
	if err != nil {
		return err
	}
	record.Db = strings.ToLower(record.Db)
	now := types.NewTime(types.FromGoTime(time.Now().In(sctx.GetSessionVars().StmtCtx.TimeZone())), mysql.TypeTimestamp, 3)
	for i := range record.Bindings {
		record.Bindings[i].CreateTime = now
		record.Bindings[i].UpdateTime = now
	}

	// update the BindMeta to the cache.
	h.appendSessionBinding(parser.DigestNormalized(record.OriginalSQL).String(), record)
	return nil
}

// DropSessionBinding drop BindRecord in the cache.
func (h *sessionBindingHandle) DropSessionBinding(sqlDigest string) error {
	if sqlDigest == "" {
		return errors.New("sql digest is empty")
	}
	h.ch.RemoveBinding(sqlDigest)
	return nil
}

// MatchSessionBinding returns the matched binding for this statement.
func (h *sessionBindingHandle) MatchSessionBinding(currentDB string, stmt ast.StmtNode) (*BindRecord, error) {
	if h.ch.Size() == 0 {
		return nil, nil
	}
	// TODO: support fuzzy matching.
	_, _, sqlDigest, err := normalizeStmt(stmt, currentDB)
	if err != nil {
		return nil, err
	}
	return h.ch.GetBinding(sqlDigest), nil
}

// GetAllSessionBindings return all session bind info.
func (h *sessionBindingHandle) GetAllSessionBindings() (bindRecords []*BindRecord) {
	return h.ch.GetAllBindings()
}

// EncodeSessionStates implements SessionStatesHandler.EncodeSessionStates interface.
func (h *sessionBindingHandle) EncodeSessionStates(_ context.Context, _ sessionctx.Context, sessionStates *sessionstates.SessionStates) error {
	bindRecords := h.ch.GetAllBindings()
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
func (h *sessionBindingHandle) DecodeSessionStates(_ context.Context, sctx sessionctx.Context, sessionStates *sessionstates.SessionStates) error {
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
		h.appendSessionBinding(parser.DigestNormalized(record.OriginalSQL).String(), record)
	}
	return nil
}

// Close closes the session handle.
func (h *sessionBindingHandle) Close() {
	for _, bindRecord := range h.ch.GetAllBindings() {
		updateMetrics(metrics.ScopeSession, bindRecord, nil, false)
	}
}

// sessionBindInfoKeyType is a dummy type to avoid naming collision in context.
type sessionBindInfoKeyType int

// String defines a Stringer function for debugging and pretty printing.
func (sessionBindInfoKeyType) String() string {
	return "session_bindinfo"
}

// SessionBindInfoKeyType is a variable key for store session bind info.
const SessionBindInfoKeyType sessionBindInfoKeyType = 0
