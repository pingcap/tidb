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

package main

import (
	"context"
	"strconv"
	"time"

	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/config/deploymode"
	"github.com/pingcap/tidb/pkg/keyspace"
	"github.com/pingcap/tidb/pkg/util/logutil"
	tikvconfig "github.com/tikv/client-go/v2/config"
	pd "github.com/tikv/pd/client"
	"github.com/tikv/pd/client/opt"
	"github.com/tikv/pd/client/pkg/caller"
	"go.uber.org/zap"
)

// applyStarterKeyspaceMeta copies starter branch/restore labels from PD's
// keyspace meta into the global config. No-op outside starter mode.
func applyStarterKeyspaceMeta() {
	if !deploymode.IsStarter() {
		return
	}
	cfg := config.GetGlobalConfig()
	if keyspace.IsKeyspaceNameEmpty(cfg.KeyspaceName) || cfg.Store != config.StoreTypeTiKV {
		return
	}
	pdAddrs, _, _, err := tikvconfig.ParsePath("tikv://" + cfg.Path)
	if err != nil {
		logutil.BgLogger().Fatal("starter: parse pd path failed", zap.Error(err))
	}

	timeout := time.Duration(cfg.PDClient.PDServerTimeout) * time.Second
	pdCli, err := pd.NewClient(caller.Component("tidb-starter-meta"), pdAddrs, pd.SecurityOption{
		CAPath:   cfg.Security.ClusterSSLCA,
		CertPath: cfg.Security.ClusterSSLCert,
		KeyPath:  cfg.Security.ClusterSSLKey,
	}, opt.WithCustomTimeoutOption(timeout))
	if err != nil {
		logutil.BgLogger().Fatal("starter: pd client init failed", zap.Error(err))
	}
	defer pdCli.Close()

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	meta, err := pdCli.LoadKeyspace(ctx, cfg.KeyspaceName)
	if err != nil {
		logutil.BgLogger().Fatal("starter: load keyspace meta failed",
			zap.String("keyspace", cfg.KeyspaceName), zap.Error(err))
	}
	if meta == nil || meta.Config == nil {
		return
	}
	config.UpdateGlobal(func(c *config.Config) {
		if v, ok := meta.Config[keyspace.LabelIsBranch]; ok {
			c.Starter.IsBranch, _ = strconv.ParseBool(v)
		}
		if v, ok := meta.Config[keyspace.LabelIsBranchBootstrapped]; ok {
			c.Starter.IsBranchBootstrapped = v
		}
		if v, ok := meta.Config[keyspace.LabelIsBootstrappedForRestore]; ok {
			c.Starter.IsBootstrappedForRestore = v
		}
	})
}
