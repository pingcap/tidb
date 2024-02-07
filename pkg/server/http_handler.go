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
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/server/handler"
	"github.com/pingcap/tidb/pkg/server/handler/optimizor"
	"github.com/pingcap/tidb/pkg/statistics/handle"
	"github.com/pingcap/tidb/pkg/store/helper"
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
