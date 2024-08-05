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
	"github.com/pingcap/tidb/pkg/bindinfo/internal/logutil"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/sessionstates"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/hack"
	"go.uber.org/zap"
)

// SessionBindingHandle is used to handle all session sql bind operations.
type SessionBindingHandle interface {
	// CreateSessionBinding creates a binding to the cache.
	CreateSessionBinding(sctx sessionctx.Context, binding Binding) (err error)

	// DropSessionBinding drops a binding by the sql digest.
	DropSessionBinding(sqlDigest string) error

	// MatchSessionBinding returns the matched binding for this statement.
	MatchSessionBinding(sctx sessionctx.Context, fuzzyDigest string, tableNames []*ast.TableName) (matchedBinding Binding, isMatched bool)

	// GetAllSessionBindings return all bindings.
	GetAllSessionBindings() (bindings Bindings)

	// Close closes the SessionBindingHandle.
	Close()

	sessionctx.SessionStatesHandler
}

// sessionBindingHandle is used to handle all session sql bind operations.
type sessionBindingHandle struct {
	ch FuzzyBindingCache
}

// NewSessionBindingHandle creates a new SessionBindingHandle.
func NewSessionBindingHandle() SessionBindingHandle {
	sessionHandle := &sessionBindingHandle{}
	sessionHandle.ch = newFuzzyBindingCache(nil)
	return sessionHandle
}

// appendSessionBinding adds the Bindings to the cache, all the stale bindMetas are
// removed from the cache after this operation.
func (h *sessionBindingHandle) appendSessionBinding(sqlDigest string, meta Bindings) {
	err := h.ch.SetBinding(sqlDigest, meta)
	if err != nil {
		logutil.BindLogger().Warn("SessionHandle.appendSessionBinding", zap.Error(err))
	}
}

// CreateSessionBinding creates a Bindings to the cache.
// It replaces all the exists bindings for the same normalized SQL.
func (h *sessionBindingHandle) CreateSessionBinding(sctx sessionctx.Context, binding Binding) (err error) {
	if err := prepareHints(sctx, &binding); err != nil {
		return err
	}
	binding.Db = strings.ToLower(binding.Db)
	now := types.NewTime(types.FromGoTime(time.Now().In(sctx.GetSessionVars().StmtCtx.TimeZone())), mysql.TypeTimestamp, 3)
	binding.CreateTime = now
	binding.UpdateTime = now

	// update the BindMeta to the cache.
	h.appendSessionBinding(parser.DigestNormalized(binding.OriginalSQL).String(), []Binding{binding})
	return nil
}

// DropSessionBinding drop Bindings in the cache.
func (h *sessionBindingHandle) DropSessionBinding(sqlDigest string) error {
	if sqlDigest == "" {
		return errors.New("sql digest is empty")
	}
	h.ch.RemoveBinding(sqlDigest)
	return nil
}

// MatchSessionBinding returns the matched binding for this statement.
func (h *sessionBindingHandle) MatchSessionBinding(sctx sessionctx.Context, fuzzyDigest string, tableNames []*ast.TableName) (matchedBinding Binding, isMatched bool) {
	matchedBinding, isMatched = h.ch.FuzzyMatchingBinding(sctx, fuzzyDigest, tableNames)
	return
}

// GetAllSessionBindings return all session bind info.
func (h *sessionBindingHandle) GetAllSessionBindings() (bindings Bindings) {
	return h.ch.GetAllBindings()
}

// EncodeSessionStates implements SessionStatesHandler.EncodeSessionStates interface.
func (h *sessionBindingHandle) EncodeSessionStates(_ context.Context, _ sessionctx.Context, sessionStates *sessionstates.SessionStates) error {
	bindings := h.ch.GetAllBindings()
	if len(bindings) == 0 {
		return nil
	}
	bytes, err := json.Marshal([]Binding(bindings))
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

	var m []map[string]any
	var err error
	bindingBytes := hack.Slice(sessionStates.Bindings)
	if err = json.Unmarshal(bindingBytes, &m); err != nil {
		return err
	}
	if len(m) == 0 {
		return nil
	}

	var records []Binding
	// Key "Bindings" only exists in old versions.
	if _, ok := m[0]["Bindings"]; ok {
		err = h.decodeOldStyleSessionStates(bindingBytes, &records)
	} else {
		err = json.Unmarshal(bindingBytes, &records)
	}
	if err != nil {
		return err
	}

	for _, record := range records {
		// Restore hints and ID because hints are hard to encode.
		if err = prepareHints(sctx, &record); err != nil {
			return err
		}
		h.appendSessionBinding(parser.DigestNormalized(record.OriginalSQL).String(), []Binding{record})
	}
	return nil
}

// Before v8.0.0, the data structure is different. We need to adapt to the old structure so that the sessions
// can be migrated from an old version to a new version.
func (*sessionBindingHandle) decodeOldStyleSessionStates(bindingBytes []byte, bindings *[]Binding) error {
	type bindRecord struct {
		OriginalSQL string
		Db          string
		Bindings    []Binding
	}
	var records []bindRecord
	if err := json.Unmarshal(bindingBytes, &records); err != nil {
		return err
	}
	*bindings = make([]Binding, 0, len(records))
	for _, record := range records {
		for _, binding := range record.Bindings {
			binding.OriginalSQL = record.OriginalSQL
			binding.Db = record.Db
			*bindings = append(*bindings, binding)
		}
	}
	return nil
}

// Close closes the session handle.
func (*sessionBindingHandle) Close() {}

// sessionBindInfoKeyType is a dummy type to avoid naming collision in context.
type sessionBindInfoKeyType int

// String defines a Stringer function for debugging and pretty printing.
func (sessionBindInfoKeyType) String() string {
	return "session_bindinfo"
}

// SessionBindInfoKeyType is a variable key for store session bind info.
const SessionBindInfoKeyType sessionBindInfoKeyType = 0
