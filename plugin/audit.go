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

package plugin

import (
	"context"

	"github.com/pingcap/tidb/sessionctx/variable"
)

// GeneralEvent presents TiDB generate event.
type GeneralEvent byte

const (
	// Log presents log event.
	Log GeneralEvent = iota
	// Error presents error event.
	Error
	// Result presents result event.
	Result
	// Status presents status event.
	Status
)

// ConnectionEvent presents TiDB connection event.
type ConnectionEvent byte

const (
	// Connected presents new connection establish event(finish auth).
	Connected ConnectionEvent = iota
	// Disconnect presents disconnect event.
	Disconnect
	// ChangeUser presents change user.
	ChangeUser
	// PreAuth presents event before start auth.
	PreAuth
	// Reject presents event reject connection event.
	Reject
)

func (c ConnectionEvent) String() string {
	switch c {
	case Connected:
		return "Connected"
	case Disconnect:
		return "Disconnect"
	case ChangeUser:
		return "ChangeUser"
	case PreAuth:
		return "PreAuth"
	case Reject:
		return "Reject"
	}
	return ""
}

// ParseEvent presents events happen around parser.
type ParseEvent byte

const (
	// PreParse presents event before parse.
	PreParse ParseEvent = 1 + iota
	// PostParse presents event after parse.
	PostParse
)

// AuditManifest presents a sub-manifest that every audit plugin must provide.
type AuditManifest struct {
	Manifest
	// OnConnectionEvent will be called when TiDB receive or disconnect from client.
	// return error will ignore and close current connection.
	OnConnectionEvent func(ctx context.Context, event ConnectionEvent, info *variable.ConnectionInfo) error
	// OnGeneralEvent will be called during TiDB execution.
	OnGeneralEvent func(ctx context.Context, sctx *variable.SessionVars, event GeneralEvent, cmd string)
	// OnGlobalVariableEvent will be called when Change GlobalVariable.
	OnGlobalVariableEvent func(ctx context.Context, sctx *variable.SessionVars, varName, varValue string)
	// OnParseEvent will be called around parse logic.
	OnParseEvent func(ctx context.Context, sctx *variable.SessionVars, event ParseEvent) error
}

type (
	// RejectReasonCtxValue will be used in OnConnectionEvent to pass RejectReason to plugin.
	RejectReasonCtxValue struct{}
)

type execStartTimeCtxKeyType struct{}

// ExecStartTimeCtxKey indicates stmt start execution time.
var ExecStartTimeCtxKey = execStartTimeCtxKeyType{}
