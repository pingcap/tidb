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

import "github.com/pingcap/tidb/sessionctx/variable"

// ConnEventInfo is the connection info for the event
type ConnEventInfo variable.ConnectionInfo

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

// SessionHandler is used to listen session events
type SessionHandler struct {
	OnConnectionEvent func(ConnEventTp, *ConnEventInfo)
}

func newSessionExtensions(es *Extensions) *SessionExtensions {
	connExtensions := &SessionExtensions{}
	for _, m := range es.Manifests() {
		if m.sessionHandlerFactory != nil {
			if handler := m.sessionHandlerFactory(); handler != nil {
				if fn := handler.OnConnectionEvent; fn != nil {
					connExtensions.connectionEventFuncs = append(connExtensions.connectionEventFuncs, fn)
				}
			}
		}
	}
	return connExtensions
}

// SessionExtensions is the extensions
type SessionExtensions struct {
	connectionEventFuncs []func(ConnEventTp, *ConnEventInfo)
}

// OnConnectionEvent will be called when a connection event happens
func (es *SessionExtensions) OnConnectionEvent(tp ConnEventTp, info *variable.ConnectionInfo) {
	if es == nil {
		return
	}

	eventInfo := ConnEventInfo(*info)
	for _, fn := range es.connectionEventFuncs {
		fn(tp, &eventInfo)
	}
}
