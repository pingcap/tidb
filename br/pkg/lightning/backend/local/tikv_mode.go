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

package local

import (
	"context"

	sstpb "github.com/pingcap/kvproto/pkg/import_sstpb"
	"github.com/pingcap/tidb/br/pkg/lightning/common"
	"github.com/pingcap/tidb/br/pkg/lightning/tikv"
	"go.uber.org/zap"
)

// TiKVModeSwitcher is used to switch TiKV nodes between Import and Normal mode.
type TiKVModeSwitcher struct {
	tls    *common.TLS
	pdAddr string
	logger *zap.Logger
}

// NewTiKVModeSwitcher creates a new TiKVModeSwitcher.
func NewTiKVModeSwitcher(tls *common.TLS, pdAddr string, logger *zap.Logger) *TiKVModeSwitcher {
	return &TiKVModeSwitcher{
		tls:    tls,
		pdAddr: pdAddr,
		logger: logger,
	}
}

// ToImportMode switches all TiKV nodes to Import mode.
func (rc *TiKVModeSwitcher) ToImportMode(ctx context.Context) {
	rc.switchTiKVMode(ctx, sstpb.SwitchMode_Import)
}

// ToNormalMode switches all TiKV nodes to Normal mode.
func (rc *TiKVModeSwitcher) ToNormalMode(ctx context.Context) {
	rc.switchTiKVMode(ctx, sstpb.SwitchMode_Normal)
}

func (rc *TiKVModeSwitcher) switchTiKVMode(ctx context.Context, mode sstpb.SwitchMode) {
	rc.logger.Info("switch tikv mode", zap.Stringer("mode", mode))

	// It is fine if we miss some stores which did not switch to Import mode,
	// since we're running it periodically, so we exclude disconnected stores.
	// But it is essentially all stores be switched back to Normal mode to allow
	// normal operation.
	var minState tikv.StoreState
	if mode == sstpb.SwitchMode_Import {
		minState = tikv.StoreStateOffline
	} else {
		minState = tikv.StoreStateDisconnected
	}
	tls := rc.tls.WithHost(rc.pdAddr)
	// we ignore switch mode failure since it is not fatal.
	// no need log the error, it is done in kv.SwitchMode already.
	_ = tikv.ForAllStores(
		ctx,
		tls,
		minState,
		func(c context.Context, store *tikv.Store) error {
			return tikv.SwitchMode(c, tls, store.Address, mode)
		},
	)
}
