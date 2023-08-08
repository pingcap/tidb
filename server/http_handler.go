// Copyright 2018 PingCAP, Inc.
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

	"github.com/gorilla/mux"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/domain/infosync"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/server/handler"
	"github.com/pingcap/tidb/server/handler/optimizor"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/statistics/handle"
	"github.com/pingcap/tidb/store/helper"
)

// NewTikvHandlerTool checks and prepares for tikv handler.
// It would panic when any error happens.
func (s *Server) NewTikvHandlerTool() *handler.TikvHandlerTool {
	var tikvStore helper.Storage
	store, ok := s.driver.(*TiDBDriver)
	if !ok {
		panic("Invalid KvStore with illegal driver")
	}

	if tikvStore, ok = store.store.(helper.Storage); !ok {
		panic("Invalid KvStore with illegal store")
	}

	return handler.NewTikvHandlerTool(tikvStore)
}

func (s *Server) newOptimizeTraceHandler() *optimizor.OptimizeTraceHandler {
	cfg := config.GetGlobalConfig()
	var infoGetter *infosync.InfoSyncer
	if s.dom != nil && s.dom.InfoSyncer() != nil {
		infoGetter = s.dom.InfoSyncer()
	}
	return optimizor.NewOptimizeTraceHandler(infoGetter, cfg.AdvertiseAddress, cfg.Status.StatusPort)
}

func (s *Server) newPlanReplayerHandler() *optimizor.PlanReplayerHandler {
	cfg := config.GetGlobalConfig()
	var infoGetter *infosync.InfoSyncer
	if s.dom != nil && s.dom.InfoSyncer() != nil {
		infoGetter = s.dom.InfoSyncer()
	}
	var is infoschema.InfoSchema
	if s.dom != nil && s.dom.InfoSchema() != nil {
		is = s.dom.InfoSchema()
	}
	var statsHandle *handle.Handle
	if s.dom != nil && s.dom.StatsHandle() != nil {
		statsHandle = s.dom.StatsHandle()
	}
	return optimizor.NewPlanReplayerHandler(is, statsHandle, infoGetter, cfg.AdvertiseAddress, cfg.Status.StatusPort)
}

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
	// parse params
	params := mux.Vars(req)

	var err error
	var hasDone bool
	op := params[handler.Operation]
	switch op {
	case "start":
		hasDone, err = h.startUpgrade()
	case "finish":
		hasDone, err = h.finishUpgrade()
	default:
		handler.WriteError(w, errors.Errorf("wrong operation:%s", op))
		return
	}

	if err != nil {
		handler.WriteError(w, err)
		return
	}
	if hasDone {
		switch op {
		case "start":
			handler.WriteData(w, "Be upgrading.")
		case "finish":
			handler.WriteData(w, "Be normal.")
		}
	}
	handler.WriteData(w, "success!")
}

func (h ClusterUpgradeHandler) startUpgrade() (hasDone bool, err error) {
	se, err := session.CreateSession(h.store)
	if err != nil {
		return false, err
	}
	defer se.Close()

	isUpgrading, err := session.IsUpgrading(se)
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

	isUpgrading, err := session.IsUpgrading(se)
	if err != nil {
		return false, err
	}
	if !isUpgrading {
		return true, nil
	}

	err = session.SyncNormalRunning(se)
	return false, err
}
