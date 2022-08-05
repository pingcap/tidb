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

package plugin

import (
	"context"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/sessionctx/variable"
)

// GeneralEvent presents TiDB generate event.
type GeneralEvent byte

const (
	// Starting represents a GeneralEvent that is about to start
	Starting GeneralEvent = iota
	// Completed represents a GeneralEvent that has completed
	Completed
	// Error represents a GeneralEvent that has error (and typically couldn't start)
	Error
	// GeneralEventCount is the count for the general events
	// The new events MUST be added before it
	GeneralEventCount
)

// GeneralEventFromString gets the `GeneralEvent` from the given string
func GeneralEventFromString(s string) (GeneralEvent, error) {
	upperStr := strings.ToUpper(s)
	for i := 0; i < int(GeneralEventCount); i++ {
		event := GeneralEvent(i)
		if event.String() == upperStr {
			return event, nil
		}
	}
	return 0, errors.Errorf("Invalid general event: %s", s)
}

// String returns the string for the `GeneralEvent`
func (e GeneralEvent) String() string {
	switch e {
	case Starting:
		return "STARTING"
	case Completed:
		return "COMPLETED"
	case Error:
		return "ERROR"
	}
	return ""
}

// ConnectionEvent presents TiDB connection event.
type ConnectionEvent byte

const (
	// Connected represents new connection establish event(finish auth).
	Connected ConnectionEvent = iota
	// Disconnect represents disconnect event.
	Disconnect
	// ChangeUser represents change user.
	ChangeUser
	// PreAuth represents event before start auth.
	PreAuth
	// Reject represents event reject connection event.
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
