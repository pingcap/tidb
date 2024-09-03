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
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/conn"
	"github.com/pingcap/tidb/br/pkg/conn/util"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
	"github.com/pingcap/tidb/br/pkg/restore/split"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/util/codec"
	pd "github.com/tikv/pd/client"
	pdhttp "github.com/tikv/pd/client/http"
	"go.uber.org/zap"
)

// PlacementRuleManager manages to set the placement rule of tables to label constraint key `exclusive`,
// and unset the rule.
type PlacementRuleManager interface {
	SetPlacementRule(ctx context.Context, tables []*CreatedTable) error
	ResetPlacementRules(ctx context.Context) error
}

const (
	restoreLabelKey   = "exclusive"
	restoreLabelValue = "restore"
)

// loadRestoreStores loads the stores used to restore data. This function is called only when is online.
func loadRestoreStores(ctx context.Context, pdClient util.StoreMeta) ([]uint64, error) {
	restoreStores := make([]uint64, 0)
	stores, err := conn.GetAllTiKVStoresWithRetry(ctx, pdClient, util.SkipTiFlash)
	if err != nil {
		return nil, errors.Trace(err)
	}
	for _, s := range stores {
		if s.GetState() != metapb.StoreState_Up {
			continue
		}
		for _, l := range s.GetLabels() {
			if l.GetKey() == restoreLabelKey && l.GetValue() == restoreLabelValue {
				restoreStores = append(restoreStores, s.GetId())
				break
			}
		}
	}
	log.Info("load restore stores", zap.Uint64s("store-ids", restoreStores))
	return restoreStores, nil
}

// NewPlacementRuleManager sets and unset placement rules for online restore.
func NewPlacementRuleManager(ctx context.Context, pdClient pd.Client, pdHTTPCli pdhttp.Client, tlsConf *tls.Config, isOnline bool) (PlacementRuleManager, error) {
	if !isOnline {
		return offlinePlacementRuleManager{}, nil
	}

	restoreStores, err := loadRestoreStores(ctx, pdClient)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if len(restoreStores) == 0 {
		log.Warn("The cluster has not any TiKV node with the specify label, so skip setting placement rules",
			zap.String("label-key", restoreLabelKey), zap.String("label-value", restoreLabelValue))
		return offlinePlacementRuleManager{}, nil
	}

	return &onlinePlacementRuleManager{
		// toolClient reuse the split.SplitClient to do miscellaneous things. It doesn't
		// call split related functions so set the arguments to arbitrary values.
		toolClient: split.NewClient(pdClient, pdHTTPCli, tlsConf, maxSplitKeysOnce, 3),

		restoreStores: restoreStores,
		restoreTables: make(map[int64]struct{}),
	}, nil
}

// An offline placement rule manager, which does nothing for placement rule.
type offlinePlacementRuleManager struct{}

// SetPlacementRule implements the interface `PlacementRuleManager`, it does nothing actually.
func (offlinePlacementRuleManager) SetPlacementRule(ctx context.Context, tables []*CreatedTable) error {
	return nil
}

// ResetPlacementRules implements the interface `PlacementRuleManager`, it does nothing actually.
func (offlinePlacementRuleManager) ResetPlacementRules(ctx context.Context) error {
	return nil
}

// An online placement rule manager, it sets the placement rule of tables to label constraint key `exclusive`,
// and unsets the rule.
type onlinePlacementRuleManager struct {
	toolClient split.SplitClient

	restoreStores []uint64
	restoreTables map[int64]struct{}
}

// SetPlacementRule sets the placement rule of tables to label constraint key `exclusive`,
func (manager *onlinePlacementRuleManager) SetPlacementRule(ctx context.Context, tables []*CreatedTable) error {
	for _, tbl := range tables {
		manager.restoreTables[tbl.Table.ID] = struct{}{}
		if tbl.Table.Partition != nil && tbl.Table.Partition.Definitions != nil {
			for _, def := range tbl.Table.Partition.Definitions {
				manager.restoreTables[def.ID] = struct{}{}
			}
		}
	}

	err := manager.setupPlacementRules(ctx)
	if err != nil {
		log.Error("setup placement rules failed", zap.Error(err))
		return errors.Trace(err)
	}

	err = manager.waitPlacementSchedule(ctx)
	if err != nil {
		log.Error("wait placement schedule failed", zap.Error(err))
		return errors.Trace(err)
	}
	return nil
}

// SetupPlacementRules sets rules for the tables' regions.
func (manager *onlinePlacementRuleManager) setupPlacementRules(ctx context.Context) error {
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
	for tableID := range manager.restoreTables {
		rule.ID = getRuleID(tableID)
		rule.StartKeyHex = hex.EncodeToString(codec.EncodeBytes([]byte{}, tablecodec.EncodeTablePrefix(tableID)))
		rule.EndKeyHex = hex.EncodeToString(codec.EncodeBytes([]byte{}, tablecodec.EncodeTablePrefix(tableID+1)))
		err = manager.toolClient.SetPlacementRule(ctx, rule)
		if err != nil {
			return errors.Trace(err)
		}
	}
	log.Info("finish setting placement rules")
	return nil
}

func (manager *onlinePlacementRuleManager) checkRegions(ctx context.Context) (bool, string, error) {
	progress := 0
	for tableID := range manager.restoreTables {
		start := codec.EncodeBytes([]byte{}, tablecodec.EncodeTablePrefix(tableID))
		end := codec.EncodeBytes([]byte{}, tablecodec.EncodeTablePrefix(tableID+1))
		ok, regionProgress, err := manager.checkRange(ctx, start, end)
		if err != nil {
			return false, "", errors.Trace(err)
		}
		if !ok {
			return false, fmt.Sprintf("table %v/%v, %s", progress, len(manager.restoreTables), regionProgress), nil
		}
		progress += 1
	}
	return true, "", nil
}

func (manager *onlinePlacementRuleManager) checkRange(ctx context.Context, start, end []byte) (bool, string, error) {
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
func (manager *onlinePlacementRuleManager) waitPlacementSchedule(ctx context.Context) error {
	log.Info("start waiting placement schedule")
	ticker := time.NewTicker(time.Second * 10)
	failpoint.Inject("wait-placement-schedule-quicker-ticker", func() {
		ticker.Stop()
		ticker = time.NewTicker(time.Millisecond * 500)
	})
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			ok, progress, err := manager.checkRegions(ctx)
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
func (manager *onlinePlacementRuleManager) ResetPlacementRules(ctx context.Context) error {
	log.Info("start resetting placement rules")
	var failedTables []int64
	for tableID := range manager.restoreTables {
		err := manager.toolClient.DeletePlacementRule(ctx, "pd", getRuleID(tableID))
		if err != nil {
			log.Info("failed to delete placement rule for table", zap.Int64("table-id", tableID))
			failedTables = append(failedTables, tableID)
		}
	}
	if len(failedTables) > 0 {
		return errors.Annotatef(berrors.ErrPDInvalidResponse, "failed to delete placement rules for tables %v", failedTables)
	}
	return nil
}
