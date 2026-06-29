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

package session

import (
	"context"
	"os"

	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/config/deploymode"
	"github.com/pingcap/tidb/pkg/extworkload"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/logutil"
	pd "github.com/tikv/pd/client"
	"go.uber.org/zap"
)

func abortGCV2(store kv.Storage) {
	if !deploymode.IsStarter() {
		return
	}
	cfg := config.GetGlobalConfig().ExternalWorkload
	if !cfg.Enable || cfg.Role != config.RoleGCV2Worker {
		return
	}
	meta := store.GetCodec().GetKeyspaceMeta()
	if !pd.IsKeyspaceUsingKeyspaceLevelGC(meta) {
		return
	}
	mgr, err := extworkload.NewManager(context.Background(), meta, cfg)
	if err != nil {
		logutil.BgLogger().Fatal("initialize external workload manager for GCV2 abort failed", zap.Error(err))
	}
	abortErr := mgr.AbortGCV2(context.Background())
	if err := mgr.Close(); err != nil {
		logutil.BgLogger().Warn("failed to close external workload manager after GCV2 abort", zap.Error(err))
	}
	if abortErr != nil {
		logutil.BgLogger().Fatal("abort GCV2 worker failed", zap.Error(abortErr))
	}
	logutil.BgLogger().Info("GCV2 worker aborted")
	if intest.InTest {
		return
	}
	os.Exit(0)
}
