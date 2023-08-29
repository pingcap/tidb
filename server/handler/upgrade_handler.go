// Copyright 2023 PingCAP, Inc.
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
	"net/http"

	"github.com/gorilla/mux"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

// ClusterUpgradeHandler is the handler for upgrading cluster.
type ClusterUpgradeHandler struct {
	store kv.Storage
}

// NewClusterUpgradeHandler creates a new ClusterUpgradeHandler.
func NewClusterUpgradeHandler(store kv.Storage) *ClusterUpgradeHandler {
	return &ClusterUpgradeHandler{store: store}
}

// ServeHTTP handles request of ddl server info.
func (h ClusterUpgradeHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodPost {
		WriteError(w, errors.Errorf("This API only support POST method"))
		return
	}

	var err error
	var hasDone bool
	params := mux.Vars(req)
	op := params[Operation]
	switch op {
	case "start":
		hasDone, err = h.StartUpgrade()
	case "finish":
		hasDone, err = h.FinishUpgrade()
	default:
		WriteError(w, errors.Errorf("wrong operation:%s", op))
		return
	}

	if err != nil {
		WriteError(w, err)
		return
	}
	if hasDone {
		switch op {
		case "start":
			WriteData(w, "It's a duplicated op and the cluster is already in upgrading state.")
		case "finish":
			WriteData(w, "It's a duplicated op and the cluster is already in normal state.")
		}
	} else {
		WriteData(w, "success!")
	}
	logutil.Logger(req.Context()).Info("upgrade op success",
		zap.String("category", "upgrading"), zap.String("op", req.FormValue("op")), zap.Bool("hasDone", hasDone))
}

// StartUpgrade is used to start the upgrade.
func (h ClusterUpgradeHandler) StartUpgrade() (hasDone bool, err error) {
	se, err := session.CreateSession(h.store)
	if err != nil {
		return false, err
	}
	defer se.Close()

	isUpgrading, err := session.IsUpgradingClusterState(se)
	if err != nil {
		return false, err
	}
	if isUpgrading {
		return true, nil
	}

	err = session.SyncUpgradeState(se)
	return false, err
}

// FinishUpgrade is used to finish the upgrade.
func (h ClusterUpgradeHandler) FinishUpgrade() (hasDone bool, err error) {
	se, err := session.CreateSession(h.store)
	if err != nil {
		return false, err
	}
	defer se.Close()

	isUpgrading, err := session.IsUpgradingClusterState(se)
	if err != nil {
		return false, err
	}
	if !isUpgrading {
		return true, nil
	}

	err = session.SyncNormalRunning(se)
	return false, err
}
