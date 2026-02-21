// Copyright 2022 PingCAP, Inc.
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

package infosync

import (
	"context"
	"fmt"
	"sync"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/pkg/ddl/placement"
	"github.com/pingcap/tidb/pkg/store/helper"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/pd/client/clients/router"
	pd "github.com/tikv/pd/client/http"
	"go.uber.org/zap"
)

var (
	_ TiFlashReplicaManager = &TiFlashReplicaManagerCtx{}
	_ TiFlashReplicaManager = &mockTiFlashReplicaManagerCtx{}
)

// TiFlashReplicaManager manages placement settings and replica progress for TiFlash.
type TiFlashReplicaManager interface {
	// SetTiFlashGroupConfig sets the group index of the tiflash placement rule
	SetTiFlashGroupConfig(ctx context.Context) error
	// SetPlacementRule is a helper function to set placement rule.
	SetPlacementRule(ctx context.Context, rule *pd.Rule) error
	// SetPlacementRuleBatch is a helper function to set a batch of placement rules.
	SetPlacementRuleBatch(ctx context.Context, rules []*pd.Rule) error
	// GetPlacementRule is a helper function to get placement rule.
	GetPlacementRule(ctx context.Context, tableID int64) (*pd.Rule, error)
	// DeletePlacementRule is to delete placement rule for certain group.
	DeletePlacementRule(ctx context.Context, group string, ruleID string) error
	// GetGroupRules to get all placement rule in a certain group.
	GetGroupRules(ctx context.Context, group string) ([]*pd.Rule, error)
	// PostAccelerateScheduleBatch sends `regions/accelerate-schedule/batch` request.
	PostAccelerateScheduleBatch(ctx context.Context, tableIDs []int64) error
	// GetRegionCountFromPD is a helper function calling `/stats/region`.
	GetRegionCountFromPD(ctx context.Context, tableID int64, regionCount *int) error
	// GetStoresStat gets the TiKV store information by accessing PD's api.
	GetStoresStat(ctx context.Context) (*pd.StoresInfo, error)
	// CalculateTiFlashProgress calculates TiFlash replica progress
	CalculateTiFlashProgress(tableID int64, replicaCount uint64, TiFlashStores map[int64]pd.StoreInfo) (fullReplicaProgress float64, oneReplicaProgress float64, err error)
	// UpdateTiFlashProgressCache updates tiflashProgressCache
	UpdateTiFlashProgressCache(tableID int64, progress float64)
	// GetTiFlashProgressFromCache gets tiflash replica progress from tiflashProgressCache
	GetTiFlashProgressFromCache(tableID int64) (float64, bool)
	// DeleteTiFlashProgressFromCache delete tiflash replica progress from tiflashProgressCache
	DeleteTiFlashProgressFromCache(tableID int64)
	// CleanTiFlashProgressCache clean progress cache
	CleanTiFlashProgressCache()
	// SyncTiFlashTableSchema syncs the table's schema to TiFlash.
	SyncTiFlashTableSchema(tableID int64, storesStat []pd.StoreInfo) error
	// Close is to close TiFlashReplicaManager
	Close(ctx context.Context)
}

// TiFlashReplicaManagerCtx manages placement with pd and replica progress for TiFlash.
type TiFlashReplicaManagerCtx struct {
	pdHTTPCli            pd.Client
	sync.RWMutex         // protect tiflashProgressCache
	tiflashProgressCache map[int64]float64
	codec                tikv.Codec
}

// GetPlacementRule is a helper function to get placement rule by table id.
func (m *TiFlashReplicaManagerCtx) GetPlacementRule(ctx context.Context, tableID int64) (*pd.Rule, error) {
	ruleID := MakeRuleID(tableID)
	ruleID = encodeRuleID(m.codec, ruleID)
	return m.pdHTTPCli.GetPlacementRule(ctx, placement.TiFlashRuleGroupID, ruleID)
}

// Close is called to close TiFlashReplicaManagerCtx.
func (*TiFlashReplicaManagerCtx) Close(context.Context) {}

// getTiFlashPeerWithoutLagCount returns
// - the number of tiflash peers without lag
// - the number of regions that have at least 1 tiflash peer
func getTiFlashPeerWithoutLagCount(tiFlashStores map[int64]pd.StoreInfo, keyspaceID tikv.KeyspaceID, tableID int64) (flashPeerCount int, flashRegionCount int, errStoreAddr string, err error) {
	// For each storeIDs -> regionID, PD will not create two peer on the same store
	// The regionIDs that have at least 1 tiflash peer
	allRegionReplica := make(map[int64]int)
	for _, store := range tiFlashStores {
		regionReplica := make(map[int64]int)
		err := helper.CollectTiFlashStatus(store.Store.StatusAddress, keyspaceID, tableID, &regionReplica)
		failpoint.Inject("OneTiFlashStoreDown", func() {
			if store.Store.StateName == "Down" {
				err = errors.New("mock TiFlash down")
			}
		})
		if err != nil {
			// Just skip down or offline or tombstone stores, because PD will migrate regions from these stores.
			if store.Store.StateName == "Up" || store.Store.StateName == "Disconnected" {
				return 0, 0, store.Store.StatusAddress, err
			}
			continue
		}
		flashPeerCount += len(regionReplica)
		for r := range regionReplica {
			allRegionReplica[r] = 1
		}
	}
	return flashPeerCount, len(allRegionReplica), "", nil
}

// calculateTiFlashProgress calculates progress based on the region status from PD and TiFlash.
func calculateTiFlashProgress(keyspaceID tikv.KeyspaceID, tableID int64, replicaCount uint64, tiFlashStores map[int64]pd.StoreInfo) (fullReplicaProgress float64, oneReplicaProgress float64, err error) {
	var regionCount int
	if err := GetTiFlashRegionCountFromPD(context.Background(), tableID, &regionCount); err != nil {
		logutil.BgLogger().Warn("Fail to get regionCount from PD.",
			zap.Int64("tableID", tableID))
		return 0, 0, errors.Trace(err)
	}

	if regionCount == 0 {
		logutil.BgLogger().Warn("region count getting from PD is 0.",
			zap.Int64("tableID", tableID))
		return 0, 0, fmt.Errorf("region count getting from PD is 0")
	}

	tiflashPeerCount, tiflashRegionCount, errStoreAddr, err := getTiFlashPeerWithoutLagCount(tiFlashStores, keyspaceID, tableID)
	if err != nil {
		logutil.BgLogger().Warn("Fail to get peer count from TiFlash.",
			zap.Int64("tableID", tableID), zap.String("storeAddr", errStoreAddr), zap.Error(err))
		return 0, 0, errors.Trace(err)
	}
	// fullReplicaProgress range is [0, 1], 1 means all regions have tiflash peers with `replicaCount` replicas.
	fullReplicaProgress = float64(tiflashPeerCount) / float64(regionCount*int(replicaCount))
	// oneReplicaProgress range is [0, 1], 1 means all regions have tiflash peers with at least 1 replicas.
	oneReplicaProgress = float64(tiflashRegionCount) / float64(regionCount)
	if fullReplicaProgress > 1 {
		// when pd do balance between TiFlash stores, it may create more than `replicaCount` tiflash peers for some regions.
		logutil.BgLogger().Debug("TiFlash peer count > pd peer count, maybe doing balance.",
			zap.Int64("tableID", tableID), zap.Int("tiflashPeerCount", tiflashPeerCount), zap.Int("tiflashRegionCount", tiflashRegionCount), zap.Int("regionCount", regionCount), zap.Uint64("replicaCount", replicaCount))
		fullReplicaProgress = 1
	}
	if fullReplicaProgress < 1 {
		logutil.BgLogger().Debug("TiFlash replica progress < 1.",
			zap.Int64("tableID", tableID), zap.Int("tiflashPeerCount", tiflashPeerCount), zap.Int("tiflashRegionCount", tiflashRegionCount), zap.Int("regionCount", regionCount), zap.Uint64("replicaCount", replicaCount))
	}
	return fullReplicaProgress, oneReplicaProgress, nil
}

func encodeRule(c tikv.Codec, rule *pd.Rule) {
	rule.StartKey, rule.EndKey = c.EncodeRange(rule.StartKey, rule.EndKey)
	rule.ID = encodeRuleID(c, rule.ID)
}

// encodeRule encodes the rule ID by the following way:
//  1. if the codec is in API V1 then the rule ID is not encoded, should be like "table-<tableID>-r".
//  2. if the codec is in API V2 then the rule ID is encoded,
//     should be like "keyspace-<keyspaceID>-table-<tableID>-r".
func encodeRuleID(c tikv.Codec, ruleID string) string {
	if c.GetAPIVersion() == kvrpcpb.APIVersion_V2 {
		return fmt.Sprintf("keyspace-%v-%s", c.GetKeyspaceID(), ruleID)
	}
	return ruleID
}

// CalculateTiFlashProgress calculates TiFlash replica progress.
func (m *TiFlashReplicaManagerCtx) CalculateTiFlashProgress(tableID int64, replicaCount uint64, tiFlashStores map[int64]pd.StoreInfo) (fullReplicaProgress float64, oneReplicaProgress float64, err error) {
	return calculateTiFlashProgress(m.codec.GetKeyspaceID(), tableID, replicaCount, tiFlashStores)
}

// SyncTiFlashTableSchema syncs the table's schema to TiFlash.
func (m *TiFlashReplicaManagerCtx) SyncTiFlashTableSchema(tableID int64, tiFlashStores []pd.StoreInfo) error {
	for _, store := range tiFlashStores {
		err := helper.SyncTableSchemaToTiFlash(store.Store.StatusAddress, m.codec.GetKeyspaceID(), tableID)
		if err != nil {
			logutil.BgLogger().Error("Fail to sync peer schema to TiFlash",
				zap.Int64("storeID", store.Store.ID),
				zap.Int64("tableID", tableID))
			return err
		}
	}
	return nil
}

// UpdateTiFlashProgressCache updates tiflashProgressCache
func (m *TiFlashReplicaManagerCtx) UpdateTiFlashProgressCache(tableID int64, progress float64) {
	m.Lock()
	defer m.Unlock()
	m.tiflashProgressCache[tableID] = progress
}

// GetTiFlashProgressFromCache gets tiflash replica progress from tiflashProgressCache
func (m *TiFlashReplicaManagerCtx) GetTiFlashProgressFromCache(tableID int64) (float64, bool) {
	m.RLock()
	defer m.RUnlock()
	progress, ok := m.tiflashProgressCache[tableID]
	return progress, ok
}

// DeleteTiFlashProgressFromCache delete tiflash replica progress from tiflashProgressCache
func (m *TiFlashReplicaManagerCtx) DeleteTiFlashProgressFromCache(tableID int64) {
	m.Lock()
	defer m.Unlock()
	delete(m.tiflashProgressCache, tableID)
}

// CleanTiFlashProgressCache clean progress cache
func (m *TiFlashReplicaManagerCtx) CleanTiFlashProgressCache() {
	m.Lock()
	defer m.Unlock()
	m.tiflashProgressCache = make(map[int64]float64)
}

// SetTiFlashGroupConfig sets the tiflash's rule group config
func (m *TiFlashReplicaManagerCtx) SetTiFlashGroupConfig(ctx context.Context) error {
	groupConfig, err := m.pdHTTPCli.GetPlacementRuleGroupByID(ctx, placement.TiFlashRuleGroupID)
	if err != nil {
		return errors.Trace(err)
	}
	if groupConfig != nil && groupConfig.Index == placement.RuleIndexTiFlash && !groupConfig.Override {
		return nil
	}
	groupConfig = &pd.RuleGroup{
		ID:       placement.TiFlashRuleGroupID,
		Index:    placement.RuleIndexTiFlash,
		Override: false,
	}
	return m.pdHTTPCli.SetPlacementRuleGroup(ctx, groupConfig)
}

// SetPlacementRule is a helper function to set placement rule.
func (m *TiFlashReplicaManagerCtx) SetPlacementRule(ctx context.Context, rule *pd.Rule) error {
	encodeRule(m.codec, rule)
	return m.doSetPlacementRule(ctx, rule)
}

func (m *TiFlashReplicaManagerCtx) doSetPlacementRule(ctx context.Context, rule *pd.Rule) error {
	if err := m.SetTiFlashGroupConfig(ctx); err != nil {
		return err
	}
	if rule.Count == 0 {
		return m.pdHTTPCli.DeletePlacementRule(ctx, rule.GroupID, rule.ID)
	}
	return m.pdHTTPCli.SetPlacementRule(ctx, rule)
}

// SetPlacementRuleBatch is a helper function to set a batch of placement rules.
func (m *TiFlashReplicaManagerCtx) SetPlacementRuleBatch(ctx context.Context, rules []*pd.Rule) error {
	r := make([]*pd.Rule, 0, len(rules))
	for _, rule := range rules {
		encodeRule(m.codec, rule)
		r = append(r, rule)
	}
	return m.doSetPlacementRuleBatch(ctx, r)
}

func (m *TiFlashReplicaManagerCtx) doSetPlacementRuleBatch(ctx context.Context, rules []*pd.Rule) error {
	if err := m.SetTiFlashGroupConfig(ctx); err != nil {
		return err
	}
	ruleOps := make([]*pd.RuleOp, 0, len(rules))
	for i, r := range rules {
		if r.Count == 0 {
			ruleOps = append(ruleOps, &pd.RuleOp{
				Rule:   rules[i],
				Action: pd.RuleOpDel,
			})
		} else {
			ruleOps = append(ruleOps, &pd.RuleOp{
				Rule:   rules[i],
				Action: pd.RuleOpAdd,
			})
		}
	}
	return m.pdHTTPCli.SetPlacementRuleInBatch(ctx, ruleOps)
}

// DeletePlacementRule is to delete placement rule for certain group.
func (m *TiFlashReplicaManagerCtx) DeletePlacementRule(ctx context.Context, group string, ruleID string) error {
	ruleID = encodeRuleID(m.codec, ruleID)
	return m.pdHTTPCli.DeletePlacementRule(ctx, group, ruleID)
}

// GetGroupRules to get all placement rule in a certain group.
func (m *TiFlashReplicaManagerCtx) GetGroupRules(ctx context.Context, group string) ([]*pd.Rule, error) {
	return m.pdHTTPCli.GetPlacementRulesByGroup(ctx, group)
}

// PostAccelerateScheduleBatch sends `regions/batch-accelerate-schedule` request.
func (m *TiFlashReplicaManagerCtx) PostAccelerateScheduleBatch(ctx context.Context, tableIDs []int64) error {
	if len(tableIDs) == 0 {
		return nil
	}
	input := make([]*router.KeyRange, 0, len(tableIDs))
	for _, tableID := range tableIDs {
		startKey := tablecodec.GenTableRecordPrefix(tableID)
		endKey := tablecodec.EncodeTablePrefix(tableID + 1)
		startKey, endKey = m.codec.EncodeRegionRange(startKey, endKey)
		input = append(input, pd.NewKeyRange(startKey, endKey))
	}
	return m.pdHTTPCli.AccelerateScheduleInBatch(ctx, input)
}

// GetRegionCountFromPD is a helper function calling `/stats/region`.
func (m *TiFlashReplicaManagerCtx) GetRegionCountFromPD(ctx context.Context, tableID int64, regionCount *int) error {
	startKey := tablecodec.GenTableRecordPrefix(tableID)
	endKey := tablecodec.EncodeTablePrefix(tableID + 1)
	startKey, endKey = m.codec.EncodeRegionRange(startKey, endKey)
	stats, err := m.pdHTTPCli.GetRegionStatusByKeyRange(ctx, pd.NewKeyRange(startKey, endKey), true)
	if err != nil {
		return err
	}
	*regionCount = stats.Count
	return nil
}

// GetStoresStat gets the TiKV store information by accessing PD's api.
func (m *TiFlashReplicaManagerCtx) GetStoresStat(ctx context.Context) (*pd.StoresInfo, error) {
	return m.pdHTTPCli.GetStores(ctx)
}

