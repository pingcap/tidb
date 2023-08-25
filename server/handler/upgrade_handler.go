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
	"context"
	"net/http"
	"strings"
	"time"

	"github.com/gorilla/mux"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/domain/infosync"
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
		hasDone, err = h.startUpgrade()
	case "finish":
		hasDone, err = h.finishUpgrade()
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

func (h ClusterUpgradeHandler) startUpgrade() (hasDone bool, err error) {
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

func (h ClusterUpgradeHandler) finishUpgrade() (hasDone bool, err error) {
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

// clusterUpgradeInfo is used to report cluster upgrade info when do http request.
type clusterUpgradeInfo struct {
	ServersNum             int                          `json:"servers_num,omitempty"`
	OwnerID                string                       `json:"owner_id"`
	UpgradedPercent        int                          `json:"upgraded_percent"`
	IsAllUpgraded          bool                         `json:"is_all_server_version_consistent,omitempty"`
	AllServersDiffVersions []infosync.ServerVersionInfo `json:"all_servers_diff_versions,omitempty"`
}

func (h ClusterUpgradeHandler) showUpgrade(w http.ResponseWriter, req *http.Request) error {
	do, err := session.GetDomain(h.Store)
	if err != nil {
		logutil.BgLogger().Error("failed to get session domain", zap.Error(err))
		return errors.New("create session error")
	}
	ctx := context.Background()
	allServersInfo, err := infosync.GetAllServerInfo(ctx)
	if err != nil {
		logutil.BgLogger().Error("failed to get all server info", zap.Error(err))
		return errors.New("ddl server information not found")
	}
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	ownerID, err := do.DDL().OwnerManager().GetOwnerID(ctx)
	cancel()
	if err != nil {
		logutil.BgLogger().Error("failed to get owner id", zap.Error(err))
		return errors.New("ddl server information not found")
	}
	allVersionsMap := map[infosync.ServerVersionInfo]int{}
	allVersions := make([]infosync.ServerVersionInfo, 0, len(allServersInfo))
	for _, v := range allServersInfo {
		if s, ok := allVersionsMap[v.ServerVersionInfo]; ok {
			s++
			continue
		}
		allVersionsMap[v.ServerVersionInfo] = 1
		allVersions = append(allVersions, v.ServerVersionInfo)
	}
	maxVer := allVersions[0].Version
	maxInfo := allVersions[0]
	for k, _ := range allVersionsMap {
		if strings.Compare(maxVer, k.Version) < 0 {
			maxInfo = k
		}
	}
	upgradedNum := len(allServersInfo) - allVersionsMap[maxInfo]
	upgradedPercent := (upgradedNum / len(allServersInfo)) * 100
	upgradeInfo := clusterUpgradeInfo{
		ServersNum:             len(allServersInfo),
		OwnerID:                ownerID,
		AllServersDiffVersions: allVersions,
		UpgradedPercent:        upgradedPercent,
		IsAllUpgraded:          upgradedPercent == 100,
	}
	// if IsAllUpgraded is false, return the all tidb servers version.
	if !upgradeInfo.IsAllUpgraded {
		upgradeInfo.AllServersDiffVersions = allVersions
	}
	WriteData(w, upgradeInfo)
	return nil
}
