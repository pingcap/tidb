// Copyright 2021 PingCAP, Inc.
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

package globalconfigsync

import (
	"context"

	"github.com/pingcap/tidb/util/logutil"
	pd "github.com/tikv/pd/client"
	"go.uber.org/zap"
)

// GlobalConfigSyncer is used to sync pd global config.
type GlobalConfigSyncer struct {
	pd       pd.Client
	NotifyCh chan pd.GlobalConfigItem
}

func (s *GlobalConfigSyncer) StoreGlobalConfig(ctx context.Context, item pd.GlobalConfigItem) error {
	err := s.pd.StoreGlobalConfig(ctx, []pd.GlobalConfigItem{item})
	if err != nil {
		logutil.BgLogger().Error("store global config failed", zap.Error(err))
		return err
	}
	logutil.BgLogger().Info("store global config", zap.Any("Configs", item))
	return err
}

func (s *GlobalConfigSyncer) PushGlobalConfigItem(globalConfigItem pd.GlobalConfigItem) {
	s.NotifyCh <- globalConfigItem
}

func NewGlobalConfigSyncer(p pd.Client) *GlobalConfigSyncer {
	return &GlobalConfigSyncer{
		pd:       p,
		NotifyCh: make(chan pd.GlobalConfigItem, 100),
	}
}
