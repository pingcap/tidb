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

package handler

import (
	"context"
	"net/http"

	"github.com/pingcap/tidb/pkg/owner"
)

// AutoIDResignHandler is the http handler for ${TIDB_HOST:PORT}//autoid/leader/resign
type AutoIDResignHandler func() owner.Manager

// ServeHTTP implements http.Handler
func (h AutoIDResignHandler) ServeHTTP(w http.ResponseWriter, _ *http.Request) {
	om := h()
	if om.IsOwner() {
		err := om.ResignOwner(context.Background())
		if err != nil {
			WriteError(w, err)
			return
		}
		WriteData(w, "success!")
	} else {
		WriteData(w, "not owner")
	}
}

// AutoIDLeaderHandler is the http handler for ${TIDB_HOST:PORT}//autoid/leader
type AutoIDLeaderHandler func() owner.Manager

// ServeHTTP implements http.Handler
func (h AutoIDLeaderHandler) ServeHTTP(w http.ResponseWriter, _ *http.Request) {
	om := h()
	ddlOwnerID, err := om.GetOwnerID(context.Background())
	if err != nil {
		WriteError(w, err)
	} else {
		WriteData(w, ddlOwnerID)
	}
}
