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
	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/session"
	"github.com/pingcap/tidb/pkg/util/logutil"
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
	case "show":
		err = h.showUpgrade(w)
	default:
		WriteError(w, errors.Errorf("wrong operation:%s", op))
		return
	}

	if err != nil {
		WriteError(w, err)
		logutil.Logger(req.Context()).Info("upgrade operation failed",
			zap.String("category", "upgrading"), zap.String("operation", op), zap.Bool("hasDone", hasDone))
		return
	}
	if hasDone {
		switch op {
		case "start":
			WriteData(w, "It's a duplicated operation and the cluster is already in upgrading state.")
		case "finish":
			WriteData(w, "It's a duplicated operation and the cluster is already in normal state.")
		}
	} else {
		WriteData(w, "success!")
	}
	logutil.Logger(req.Context()).Info("upgrade operation success",
		zap.String("category", "upgrading"), zap.String("operation", op), zap.Bool("hasDone", hasDone))
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

	err = session.SyncUpgradeState(se, 10*time.Second)
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

// SimpleServerInfo is some simple information such as version and address.
type SimpleServerInfo struct {
	infosync.ServerVersionInfo
	ID           string `json:"ddl_id"`
	IP           string `json:"ip"`
	Port         uint   `json:"listening_port"`
	JSONServerID uint64 `json:"server_id"`
}

// ClusterUpgradeInfo is used to report cluster upgrade info when do http request.
type ClusterUpgradeInfo struct {
	ServersNum          int                `json:"servers_num,omitempty"`
	OwnerID             string             `json:"owner_id"`
	UpgradedPercent     int                `json:"upgraded_percent"`
	IsAllUpgraded       bool               `json:"is_all_server_version_consistent,omitempty"`
	AllServersDiffInfos []SimpleServerInfo `json:"all_servers_diff_info,omitempty"`
}

func (h ClusterUpgradeHandler) showUpgrade(w http.ResponseWriter) error {
	se, err := session.CreateSession(h.store)
	if err != nil {
		return err
	}
	defer se.Close()

	// Check if we are upgrading by pausing user DDL(in other words by "/upgrade/start").
	isUpgrading, err := session.IsUpgradingClusterState(se)
	if err != nil {
		return err
	}
	if !isUpgrading {
		WriteData(w, "The cluster state is normal.")
		return nil
	}

	do, err := session.GetDomain(h.store)
	if err != nil {
		return err
	}
	ctx := context.Background()
	allServersInfo, err := infosync.GetAllServerInfo(ctx)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	ownerID, err := do.DDL().OwnerManager().GetOwnerID(ctx)
	cancel()
	if err != nil {
		return err
	}

	allVersionsMap := map[infosync.ServerVersionInfo]int{}
	allVersions := make([]infosync.ServerVersionInfo, 0, len(allServersInfo))
	for _, v := range allServersInfo {
		if _, ok := allVersionsMap[v.ServerVersionInfo]; ok {
			allVersionsMap[v.ServerVersionInfo]++
			continue
		}
		allVersionsMap[v.ServerVersionInfo] = 1
		allVersions = append(allVersions, v.ServerVersionInfo)
	}
	maxVerInfo := allVersions[0]
	for k := range allVersionsMap {
		if strings.Compare(maxVerInfo.Version, k.Version) < 0 {
			maxVerInfo = k
		}
	}
	upgradedPercent := (allVersionsMap[maxVerInfo] * 100) / len(allServersInfo)
	upgradeInfo := ClusterUpgradeInfo{
		ServersNum:      len(allServersInfo),
		OwnerID:         ownerID,
		UpgradedPercent: upgradedPercent,
		IsAllUpgraded:   upgradedPercent == 100,
	}
	// If IsAllUpgraded is false, return the all tidb servers version.
	if !upgradeInfo.IsAllUpgraded {
		allSimpleServerInfo := make([]SimpleServerInfo, 0, len(allServersInfo))
		for _, info := range allServersInfo {
			sInfo := SimpleServerInfo{
				ServerVersionInfo: info.ServerVersionInfo,
				ID:                info.ID,
				IP:                info.IP,
				Port:              info.Port,
				JSONServerID:      info.JSONServerID,
			}
			allSimpleServerInfo = append(allSimpleServerInfo, sInfo)
		}
		upgradeInfo.AllServersDiffInfos = allSimpleServerInfo
	}
	WriteData(w, upgradeInfo)
	return nil
}
