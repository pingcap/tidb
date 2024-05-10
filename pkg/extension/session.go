// Copyright 2022 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package extension

import (
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/types"
)

// ConnEventInfo is the connection info for the event
type ConnEventInfo struct {
	*variable.ConnectionInfo
	SessionAlias string
	ActiveRoles  []*auth.RoleIdentity
	Error        error
}

// ConnEventTp is the type of the connection event
type ConnEventTp uint8

const (
	// ConnConnected means connection connected, but not handshake yet
	ConnConnected ConnEventTp = iota
	// ConnHandshakeAccepted means connection is accepted after handshake
	ConnHandshakeAccepted
	// ConnHandshakeRejected means connections is rejected after handshake
	ConnHandshakeRejected
	// ConnReset means the connection is reset
	ConnReset
	// ConnDisconnected means the connection is disconnected
	ConnDisconnected
)

// StmtEventTp is the type of the statement event
type StmtEventTp uint8

const (
	// StmtError means the stmt is failed
	StmtError StmtEventTp = iota
	// StmtSuccess means the stmt is successfully executed
	StmtSuccess
)

// StmtEventInfo is the information of stmt event
type StmtEventInfo interface {
	// User returns the user of the session
	User() *auth.UserIdentity
	// ActiveRoles returns the active roles of the user
	ActiveRoles() []*auth.RoleIdentity
	// CurrentDB returns the current database
	CurrentDB() string
	// ConnectionInfo returns the connection info of the current session
	ConnectionInfo() *variable.ConnectionInfo
	// SessionAlias returns the session alias value set by user
	SessionAlias() string
	// StmtNode returns the parsed ast of the statement
	// When parse error, this method will return a nil value
	StmtNode() ast.StmtNode
	// ExecuteStmtNode will return the `ast.ExecuteStmt` node when the current statement is EXECUTE,
	// otherwise a nil value will be returned
	ExecuteStmtNode() *ast.ExecuteStmt
	// ExecutePreparedStmt will return the prepared stmt node for the EXECUTE statement.
	// If the current statement is not EXECUTE or prepared statement is not found, a nil value will be returned
	ExecutePreparedStmt() ast.StmtNode
	// PreparedParams will return the params for the EXECUTE statement
	PreparedParams() []types.Datum
	// OriginalText will return the text of the statement.
	// Notice that for the EXECUTE statement, the prepared statement text will be used as the return value
	OriginalText() string
	// SQLDigest will return the normalized and redact text of the `OriginalText()`
	SQLDigest() (normalized string, digest *parser.Digest)
	// AffectedRows will return the affected rows of the current statement
	AffectedRows() uint64
	// RelatedTables will return the related tables of the current statement
	// For statements succeeding to build logical plan, it uses the `visitinfo` to get the related tables
	// For statements failing to build logical plan, it traverses the ast node to get the related tables
	RelatedTables() []stmtctx.TableEntry
	// GetError will return the error when the current statement is failed
	GetError() error
}

// SessionHandler is used to listen session events
type SessionHandler struct {
	OnConnectionEvent func(ConnEventTp, *ConnEventInfo)
	OnStmtEvent       func(StmtEventTp, StmtEventInfo)
}

func newSessionExtensions(es *Extensions) *SessionExtensions {
	connExtensions := &SessionExtensions{}
	for _, m := range es.Manifests() {
		if m.sessionHandlerFactory != nil {
			if handler := m.sessionHandlerFactory(); handler != nil {
				if fn := handler.OnConnectionEvent; fn != nil {
					connExtensions.connectionEventFuncs = append(connExtensions.connectionEventFuncs, fn)
				}
				if fn := handler.OnStmtEvent; fn != nil {
					connExtensions.stmtEventFuncs = append(connExtensions.stmtEventFuncs, fn)
				}
			}
		}
	}
	return connExtensions
}

// SessionExtensions is the extensions
type SessionExtensions struct {
	connectionEventFuncs []func(ConnEventTp, *ConnEventInfo)
	stmtEventFuncs       []func(StmtEventTp, StmtEventInfo)
}

// OnConnectionEvent will be called when a connection event happens
func (es *SessionExtensions) OnConnectionEvent(tp ConnEventTp, event *ConnEventInfo) {
	if es == nil {
		return
	}

	for _, fn := range es.connectionEventFuncs {
		fn(tp, event)
	}
}

// HasStmtEventListeners returns a bool that indicates if any stmt event listener exists
func (es *SessionExtensions) HasStmtEventListeners() bool {
	return es != nil && len(es.stmtEventFuncs) > 0
}

// OnStmtEvent will be called when a stmt event happens
func (es *SessionExtensions) OnStmtEvent(tp StmtEventTp, event StmtEventInfo) {
	if es == nil {
		return
	}

	for _, fn := range es.stmtEventFuncs {
		fn(tp, event)
	}
}
