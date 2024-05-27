// Copyright 2024 PingCAP, Inc.
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

package snapclient

import (
	"context"
	"crypto/tls"
	"encoding/hex"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/conn"
	"github.com/pingcap/tidb/br/pkg/conn/util"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
	"github.com/pingcap/tidb/br/pkg/restore/split"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/util/codec"
	pd "github.com/tikv/pd/client"
	pdhttp "github.com/tikv/pd/client/http"
	"go.uber.org/zap"
)

// ContextManager is the struct to manage a TiKV 'context' for restore.
// Batcher will call Enter when any table should be restore on batch,
// so you can do some prepare work here(e.g. set placement rules for online restore).
type ContextManager interface {
	// Enter make some tables 'enter' this context(a.k.a., prepare for restore).
	Enter(ctx context.Context, tables []CreatedTable) error
	// Leave make some tables 'leave' this context(a.k.a., restore is done, do some post-works).
	Leave(ctx context.Context, tables []CreatedTable) error
	// Close closes the context manager, sometimes when the manager is 'killed' and should do some cleanup
	// it would be call.
	Close(ctx context.Context)
}

// NewBRContextManager makes a BR context manager, that is,
// set placement rules for online restore when enter(see <splitPrepareWork>),
// unset them when leave.
func NewBRContextManager(ctx context.Context, pdClient pd.Client, pdHTTPCli pdhttp.Client, tlsConf *tls.Config, isOnline bool) (ContextManager, error) {
	manager := &brContextManager{
		// toolClient reuse the split.SplitClient to do miscellaneous things. It doesn't
		// call split related functions so set the arguments to arbitrary values.
		toolClient: split.NewClient(pdClient, pdHTTPCli, tlsConf, maxSplitKeysOnce, 3),
		isOnline:   isOnline,

		hasTable: make(map[int64]CreatedTable),
	}

	err := manager.loadRestoreStores(ctx, pdClient)
	return manager, errors.Trace(err)
}

type brContextManager struct {
	toolClient    split.SplitClient
	restoreStores []uint64
	isOnline      bool

	// This 'set' of table ID allow us to handle each table just once.
	hasTable map[int64]CreatedTable
	mu       sync.Mutex
}

func (manager *brContextManager) Close(ctx context.Context) {
	tbls := make([]*model.TableInfo, 0, len(manager.hasTable))
	for _, tbl := range manager.hasTable {
		tbls = append(tbls, tbl.Table)
	}
	manager.splitPostWork(ctx, tbls)
}

func (manager *brContextManager) Enter(ctx context.Context, tables []CreatedTable) error {
	placementRuleTables := make([]*model.TableInfo, 0, len(tables))
	manager.mu.Lock()
	defer manager.mu.Unlock()

	for _, tbl := range tables {
		if _, ok := manager.hasTable[tbl.Table.ID]; !ok {
			placementRuleTables = append(placementRuleTables, tbl.Table)
		}
		manager.hasTable[tbl.Table.ID] = tbl
	}

	return manager.splitPrepareWork(ctx, placementRuleTables)
}

func (manager *brContextManager) Leave(ctx context.Context, tables []CreatedTable) error {
	manager.mu.Lock()
	defer manager.mu.Unlock()
	placementRuleTables := make([]*model.TableInfo, 0, len(tables))

	for _, table := range tables {
		placementRuleTables = append(placementRuleTables, table.Table)
	}

	manager.splitPostWork(ctx, placementRuleTables)
	log.Info("restore table done", zapTables(tables))
	for _, tbl := range placementRuleTables {
		delete(manager.hasTable, tbl.ID)
	}
	return nil
}

func (manager *brContextManager) splitPostWork(ctx context.Context, tables []*model.TableInfo) {
	err := manager.resetPlacementRules(ctx, tables)
	if err != nil {
		log.Warn("reset placement rules failed", zap.Error(err))
		return
	}
}

func (manager *brContextManager) splitPrepareWork(ctx context.Context, tables []*model.TableInfo) error {
	err := manager.setupPlacementRules(ctx, tables)
	if err != nil {
		log.Error("setup placement rules failed", zap.Error(err))
		return errors.Trace(err)
	}

	err = manager.waitPlacementSchedule(ctx, tables)
	if err != nil {
		log.Error("wait placement schedule failed", zap.Error(err))
		return errors.Trace(err)
	}
	return nil
}

const (
	restoreLabelKey   = "exclusive"
	restoreLabelValue = "restore"
)

// loadRestoreStores loads the stores used to restore data. This function is called only when is online.
func (manager *brContextManager) loadRestoreStores(ctx context.Context, pdClient util.StoreMeta) error {
	if !manager.isOnline {
		return nil
	}
	stores, err := conn.GetAllTiKVStoresWithRetry(ctx, pdClient, util.SkipTiFlash)
	if err != nil {
		return errors.Trace(err)
	}
	for _, s := range stores {
		if s.GetState() != metapb.StoreState_Up {
			continue
		}
		for _, l := range s.GetLabels() {
			if l.GetKey() == restoreLabelKey && l.GetValue() == restoreLabelValue {
				manager.restoreStores = append(manager.restoreStores, s.GetId())
				break
			}
		}
	}
	log.Info("load restore stores", zap.Uint64s("store-ids", manager.restoreStores))
	return nil
}

// SetupPlacementRules sets rules for the tables' regions.
func (manager *brContextManager) setupPlacementRules(ctx context.Context, tables []*model.TableInfo) error {
	if !manager.isOnline || len(manager.restoreStores) == 0 {
		return nil
	}
	log.Info("start setting placement rules")
	rule, err := manager.toolClient.GetPlacementRule(ctx, "pd", "default")
	if err != nil {
		return errors.Trace(err)
	}
	rule.Index = 100
	rule.Override = true
	rule.LabelConstraints = append(rule.LabelConstraints, pdhttp.LabelConstraint{
		Key:    restoreLabelKey,
		Op:     "in",
		Values: []string{restoreLabelValue},
	})
	for _, t := range tables {
		rule.ID = getRuleID(t.ID)
		rule.StartKeyHex = hex.EncodeToString(codec.EncodeBytes([]byte{}, tablecodec.EncodeTablePrefix(t.ID)))
		rule.EndKeyHex = hex.EncodeToString(codec.EncodeBytes([]byte{}, tablecodec.EncodeTablePrefix(t.ID+1)))
		err = manager.toolClient.SetPlacementRule(ctx, rule)
		if err != nil {
			return errors.Trace(err)
		}
	}
	log.Info("finish setting placement rules")
	return nil
}

func (manager *brContextManager) checkRegions(ctx context.Context, tables []*model.TableInfo) (bool, string, error) {
	for i, t := range tables {
		start := codec.EncodeBytes([]byte{}, tablecodec.EncodeTablePrefix(t.ID))
		end := codec.EncodeBytes([]byte{}, tablecodec.EncodeTablePrefix(t.ID+1))
		ok, regionProgress, err := manager.checkRange(ctx, start, end)
		if err != nil {
			return false, "", errors.Trace(err)
		}
		if !ok {
			return false, fmt.Sprintf("table %v/%v, %s", i, len(tables), regionProgress), nil
		}
	}
	return true, "", nil
}

func (manager *brContextManager) checkRange(ctx context.Context, start, end []byte) (bool, string, error) {
	regions, err := manager.toolClient.ScanRegions(ctx, start, end, -1)
	if err != nil {
		return false, "", errors.Trace(err)
	}
	for i, r := range regions {
	NEXT_PEER:
		for _, p := range r.Region.GetPeers() {
			for _, storeID := range manager.restoreStores {
				if p.GetStoreId() == storeID {
					continue NEXT_PEER
				}
			}
			return false, fmt.Sprintf("region %v/%v", i, len(regions)), nil
		}
	}
	return true, "", nil
}

// waitPlacementSchedule waits PD to move tables to restore stores.
func (manager *brContextManager) waitPlacementSchedule(ctx context.Context, tables []*model.TableInfo) error {
	if !manager.isOnline || len(manager.restoreStores) == 0 {
		return nil
	}
	log.Info("start waiting placement schedule")
	ticker := time.NewTicker(time.Second * 10)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			ok, progress, err := manager.checkRegions(ctx, tables)
			if err != nil {
				return errors.Trace(err)
			}
			if ok {
				log.Info("finish waiting placement schedule")
				return nil
			}
			log.Info("placement schedule progress: " + progress)
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func getRuleID(tableID int64) string {
	return "restore-t" + strconv.FormatInt(tableID, 10)
}

// resetPlacementRules removes placement rules for tables.
func (manager *brContextManager) resetPlacementRules(ctx context.Context, tables []*model.TableInfo) error {
	if !manager.isOnline || len(manager.restoreStores) == 0 {
		return nil
	}
	log.Info("start resetting placement rules")
	var failedTables []int64
	for _, t := range tables {
		err := manager.toolClient.DeletePlacementRule(ctx, "pd", getRuleID(t.ID))
		if err != nil {
			log.Info("failed to delete placement rule for table", zap.Int64("table-id", t.ID))
			failedTables = append(failedTables, t.ID)
		}
	}
	if len(failedTables) > 0 {
		return errors.Annotatef(berrors.ErrPDInvalidResponse, "failed to delete placement rules for tables %v", failedTables)
	}
	return nil
}
