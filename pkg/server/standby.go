// Copyright 2024 PingCAP, Inc.
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

package server

import (
	"net/http"
)

// StandbyController is the interface for standby module.
type StandbyController interface {
	// WaitForActivate starts to wait for the active signal before server starts.
	WaitForActivate()
	// EndStandby ends the standby mode. Controller implements this method to do the necessary cleanup.
	EndStandby(error)
	// Handler returns the handler for the standby server. It will be registered to the status server.
	Handler(svr *Server) (pathPrefix string, mux *http.ServeMux)

	// OnConnActive is called when a new connection is established or a connection successfully executes a command.
	OnConnActive()
	// OnServerCreated is called when the server is created.
	OnServerCreated(svr *Server)
}
