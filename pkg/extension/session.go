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
	Info         string
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
	// RedactedText will return the normalized and redact text of the `OriginalText()`
	RedactedText() (normalized string)
	// AffectedRows will return the affected rows of the current statement
	AffectedRows() uint64
	// RelatedTables will return the related tables of the current statement
	// For statements succeeding to build logical plan, it uses the `visitinfo` to get the related tables
	// For statements failing to build logical plan, it traverses the ast node to get the related tables
	RelatedTables() []stmtctx.TableEntry
	// GetError will return the error when the current statement is failed
	GetError() error
}

// SecurityEventTp is the type of the SECURITY event
type SecurityEventTp uint8

const (
	// SecurityEvent means the general security event
	SecurityEvent SecurityEventTp = iota
)

// DataOpEventTp is the type of the data operation event
type DataOpEventTp uint8

const (
	// DataOpEvent means the general data operation event
	DataOpEvent DataOpEventTp = iota
)

// SecurityEventInfo is the information of SECURITY event
type SecurityEventInfo interface {
	// User returns the user name
	User() string
	// Host returns the user host
	Host() string
	// SecurityInfo returns the security reason/info
	SecurityInfo() string
	// OriginalText returns the original sql text (if possible)
	OriginalText() string
	// RedactedText returns the redacted sql text (if possible)
	RedactedText() string
}

// DataOpEventInfo is the information of data operation event
type DataOpEventInfo interface {
	// dumpling or lightning
	Component() string
	User() string
	Host() string
	Result() string
	TargetList() string
	InputDir() string
	OutputDir() string
	ConnectionInfo() *variable.ConnectionInfo
}

// SessionHandler is used to listen session events
type SessionHandler struct {
	OnConnectionEvent func(ConnEventTp, *ConnEventInfo)
	OnStmtEvent       func(StmtEventTp, StmtEventInfo)
	OnSecurityEvent   func(SecurityEventTp, SecurityEventInfo)
	OnDataOpEvent     func(DataOpEventTp, DataOpEventInfo)
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
				if fn := handler.OnSecurityEvent; fn != nil {
					connExtensions.securityEventFuncs = append(connExtensions.securityEventFuncs, fn)
				}
				if fn := handler.OnDataOpEvent; fn != nil {
					connExtensions.dataOpEventFuncs = append(connExtensions.dataOpEventFuncs, fn)
				}
			}
		}
		if m.authPlugins != nil {
			connExtensions.authPlugins = make(map[string]*AuthPlugin)
			for _, p := range m.authPlugins {
				connExtensions.authPlugins[p.Name] = p
			}
		}
	}
	return connExtensions
}

// SessionExtensions is the extensions
type SessionExtensions struct {
	connectionEventFuncs []func(ConnEventTp, *ConnEventInfo)
	stmtEventFuncs       []func(StmtEventTp, StmtEventInfo)
	securityEventFuncs   []func(SecurityEventTp, SecurityEventInfo)
	dataOpEventFuncs     []func(DataOpEventTp, DataOpEventInfo)

	authPlugins map[string]*AuthPlugin
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

// HasSecurityEventListeners returns a bool that indicates if any HA event listener exists
func (es *SessionExtensions) HasSecurityEventListeners() bool {
	return es != nil && len(es.securityEventFuncs) > 0
}

// HasDataOpEventListeners returns a bool that indicates if any data operation event listener exists
func (es *SessionExtensions) HasDataOpEventListeners() bool {
	return es != nil && len(es.dataOpEventFuncs) > 0
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

// OnSecurityEvent will be called when a SECURITY event happens
func (es *SessionExtensions) OnSecurityEvent(tp SecurityEventTp, info SecurityEventInfo) {
	if es == nil {
		return
	}

	for _, fn := range es.securityEventFuncs {
		fn(tp, info)
	}
}

// OnDataOpEvent will be called when a data operation event happens
func (es *SessionExtensions) OnDataOpEvent(tp DataOpEventTp, info DataOpEventInfo) {
	if es == nil {
		return
	}

	for _, fn := range es.dataOpEventFuncs {
		fn(tp, info)
	}
}

// GetAuthPlugin returns the required registered extension auth plugin and whether it exists.
func (es *SessionExtensions) GetAuthPlugin(name string) (*AuthPlugin, bool) {
	if es == nil {
		return nil, false
	}
	p, ok := es.authPlugins[name]
	return p, ok
}
