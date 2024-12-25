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
	"sync"
	"time"

	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/sessionstates"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/hack"
)

// SessionBindingHandle is used to handle all session sql bind operations.
type SessionBindingHandle interface {
	// CreateSessionBinding creates a binding to the cache.
	CreateSessionBinding(sctx sessionctx.Context, bindings []*Binding) (err error)

	// DropSessionBinding drops a binding by the sql digest.
	DropSessionBinding(sqlDigests []string) error

	// MatchSessionBinding returns the matched binding for this statement.
	MatchSessionBinding(sctx sessionctx.Context, noDBDigest string, tableNames []*ast.TableName) (matchedBinding *Binding, isMatched bool)

	// GetAllSessionBindings return all bindings.
	GetAllSessionBindings() (bindings []*Binding)

	// Close closes the SessionBindingHandle.
	Close()

	sessionctx.SessionStatesHandler
}

// sessionBindingHandle is used to handle all session sql bind operations.
type sessionBindingHandle struct {
	mu       sync.RWMutex
	bindings map[string]*Binding // sqlDigest --> Binding
}

// NewSessionBindingHandle creates a new SessionBindingHandle.
func NewSessionBindingHandle() SessionBindingHandle {
	return &sessionBindingHandle{bindings: make(map[string]*Binding)}
}

// CreateSessionBinding creates a Bindings to the cache.
// It replaces all the exists bindings for the same normalized SQL.
func (h *sessionBindingHandle) CreateSessionBinding(sctx sessionctx.Context, bindings []*Binding) (err error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, binding := range bindings {
		if err := prepareHints(sctx, binding); err != nil {
			return err
		}
	}
	for _, binding := range bindings {
		binding.Db = strings.ToLower(binding.Db)
		now := types.NewTime(
			types.FromGoTime(time.Now().In(sctx.GetSessionVars().StmtCtx.TimeZone())),
			mysql.TypeTimestamp,
			3,
		)
		binding.CreateTime = now
		binding.UpdateTime = now

		// update the BindMeta to the cache.
		h.bindings[parser.DigestNormalized(binding.OriginalSQL).String()] = binding
	}
	return nil
}

// DropSessionBinding drop Bindings in the cache.
func (h *sessionBindingHandle) DropSessionBinding(sqlDigests []string) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, sqlDigest := range sqlDigests {
		delete(h.bindings, sqlDigest)
	}
	return nil
}

// MatchSessionBinding returns the matched binding for this statement.
func (h *sessionBindingHandle) MatchSessionBinding(sctx sessionctx.Context, noDBDigest string, tableNames []*ast.TableName) (matchedBinding *Binding, isMatched bool) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	// session bindings in most cases is only used for test, so there should be many session bindings, so match
	// them one by one is acceptable.
	possibleBindings := make([]*Binding, 0, 2)
	for _, binding := range h.bindings {
		bindingNoDBDigest, err := noDBDigestFromBinding(binding)
		if err != nil {
			continue
		}
		if noDBDigest != bindingNoDBDigest {
			continue
		}
		possibleBindings = append(possibleBindings, binding)
	}
	matchedBinding, isMatched = crossDBMatchBindings(sctx, tableNames, possibleBindings)
	return
}

// GetAllSessionBindings return all session bind info.
func (h *sessionBindingHandle) GetAllSessionBindings() (bindings []*Binding) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	for _, binding := range h.bindings {
		bindings = append(bindings, binding)
	}
	return
}

// EncodeSessionStates implements SessionStatesHandler.EncodeSessionStates interface.
func (h *sessionBindingHandle) EncodeSessionStates(_ context.Context, _ sessionctx.Context, sessionStates *sessionstates.SessionStates) error {
	bindings := h.GetAllSessionBindings()
	if len(bindings) == 0 {
		return nil
	}
	bytes, err := json.Marshal(bindings)
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

	var records []*Binding
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
		if err = prepareHints(sctx, record); err != nil {
			return err
		}
		h.bindings[parser.DigestNormalized(record.OriginalSQL).String()] = record
	}
	return nil
}

// Before v8.0.0, the data structure is different. We need to adapt to the old structure so that the sessions
// can be migrated from an old version to a new version.
func (*sessionBindingHandle) decodeOldStyleSessionStates(bindingBytes []byte, bindings *[]*Binding) error {
	type bindRecord struct {
		OriginalSQL string
		Db          string
		Bindings    []*Binding
	}
	var records []bindRecord
	if err := json.Unmarshal(bindingBytes, &records); err != nil {
		return err
	}
	*bindings = make([]*Binding, 0, len(records))
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
