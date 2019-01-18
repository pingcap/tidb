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
	"github.com/pingcap/parser/auth"
)

type GeneralEvent byte

const (
	Log GeneralEvent = iota
	Error
	Result
	Status
)

type ConnectionEvent byte

const (
	Connected ConnectionEvent = iota
	Disconnect
	ChangeUser
	PreAuth
)

type ParseEvent byte

const (
	PreParse ParseEvent = 1 + iota
	PostParse
)

type ServerEvent byte

const (
	Startup ParseEvent = 1 + iota
	Shut
)

type CommandEvent int

const (
	Start CommandEvent = 1 + iota
	End
)

type QueryEvent int

type TableAccessEvent int

const (
	Insert TableAccessEvent = 1 + iota
	Delete
	Update
	Read
)

// AuditManifest presents a sub-manifest that every audit plugin must provide.
type AuditManifest struct {
	Manifest
	OnGeneralEvent        func(ctx context.Context, sctx *variable.SessionVars, event GeneralEvent) error
	OnConnectionEvent     func(ctx context.Context, identity *auth.UserIdentity, event ConnectionEvent, errorCode uint16) error
	OnParseEvent          func(ctx context.Context, sctx *variable.SessionVars, event ParseEvent) error
	OnServerEvent         func(ctx context.Context, event ServerEvent) error
	OnCommandEvent        func(ctx context.Context, sctx *variable.SessionVars, event CommandEvent) error
	OnQueryEvent          func(ctx context.Context, sctx *variable.SessionVars, event QueryEvent) error
	OnTableAccessEvent    func(ctx context.Context, sctx *variable.SessionVars, event TableAccessEvent) error
	OnGlobalVariableEvent func(ctx context.Context, sctx *variable.SessionVars, varName, varValue string) error
}
