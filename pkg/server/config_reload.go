// Copyright 2026 PingCAP, Inc.
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
	"runtime"

	"github.com/pingcap/tidb/pkg/config"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/server/handler"
	"github.com/pingcap/tidb/pkg/statistics"
	tikvstore "github.com/tikv/client-go/v2/kv"
)

func handleConfigReload(w http.ResponseWriter, _ *http.Request) {
	result, err := config.ReloadGlobalConfigWithResult(config.GetGlobalConfig().ConfigFile, applyConfigReload)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	handler.WriteData(w, result)
}

func applyConfigReload(_, next *config.Config) {
	if next.Performance.MaxProcs > 0 {
		if runtime.GOMAXPROCS(0) != int(next.Performance.MaxProcs) {
			runtime.GOMAXPROCS(int(next.Performance.MaxProcs))
		}
	} else {
		runtime.SetDefaultGOMAXPROCS()
	}
	plannercore.AllowCartesianProduct.Store(next.Performance.CrossJoin)
	statistics.RatioOfPseudoEstimate.Store(next.Performance.PseudoEstimateRatio)
	tikvstore.StoreLimit.Store(next.TiKVClient.StoreLimit)
}
