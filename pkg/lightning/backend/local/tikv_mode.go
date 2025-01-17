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
	"crypto/tls"

	sstpb "github.com/pingcap/kvproto/pkg/import_sstpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/tidb/pkg/lightning/tikv"
	pdhttp "github.com/tikv/pd/client/http"
	"go.uber.org/zap"
)

// TiKVModeSwitcher is used to switch TiKV nodes between Import and Normal mode.
type TiKVModeSwitcher interface {
	// ToImportMode switches all TiKV nodes to Import mode.
	ToImportMode(ctx context.Context, ranges ...*sstpb.Range)
	// ToNormalMode switches all TiKV nodes to Normal mode.
	ToNormalMode(ctx context.Context, ranges ...*sstpb.Range)
}

// TiKVModeSwitcher is used to switch TiKV nodes between Import and Normal mode.
type switcher struct {
	tls       *tls.Config
	pdHTTPCli pdhttp.Client
	logger    *zap.Logger
}

// NewTiKVModeSwitcher creates a new TiKVModeSwitcher.
func NewTiKVModeSwitcher(tls *tls.Config, pdHTTPCli pdhttp.Client, logger *zap.Logger) TiKVModeSwitcher {
	return &switcher{
		tls:       tls,
		pdHTTPCli: pdHTTPCli,
		logger:    logger,
	}
}

func (rc *switcher) ToImportMode(ctx context.Context, ranges ...*sstpb.Range) {
	rc.switchTiKVMode(ctx, sstpb.SwitchMode_Import, ranges...)
}

func (rc *switcher) ToNormalMode(ctx context.Context, ranges ...*sstpb.Range) {
	rc.switchTiKVMode(ctx, sstpb.SwitchMode_Normal, ranges...)
}

func (rc *switcher) switchTiKVMode(ctx context.Context, mode sstpb.SwitchMode, ranges ...*sstpb.Range) {
	rc.logger.Info("switch tikv mode", zap.Stringer("mode", mode))

	// It is fine if we miss some stores which did not switch to Import mode,
	// since we're running it periodically, so we exclude disconnected stores.
	// But it is essentially all stores be switched back to Normal mode to allow
	// normal operation.
	// we ignore switch mode failure since it is not fatal.
	// no need log the error, it is done in kv.SwitchMode already.
	_ = tikv.ForAllStores(
		ctx,
		rc.pdHTTPCli,
		metapb.StoreState_Offline,
		func(c context.Context, store *pdhttp.MetaStore) error {
			return tikv.SwitchMode(c, rc.tls, store.Address, mode, ranges...)
		},
	)
}
