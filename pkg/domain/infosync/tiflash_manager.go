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
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/mux"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/pkg/ddl/placement"
	"github.com/pingcap/tidb/pkg/store/helper"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/syncutil"
	"github.com/tikv/client-go/v2/tikv"
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
	CalculateTiFlashProgress(tableID int64, replicaCount uint64, TiFlashStores map[int64]pd.StoreInfo) (float64, error)
	// UpdateTiFlashProgressCache updates tiflashProgressCache
	UpdateTiFlashProgressCache(tableID int64, progress float64)
	// GetTiFlashProgressFromCache gets tiflash replica progress from tiflashProgressCache
	GetTiFlashProgressFromCache(tableID int64) (float64, bool)
	// DeleteTiFlashProgressFromCache delete tiflash replica progress from tiflashProgressCache
	DeleteTiFlashProgressFromCache(tableID int64)
	// CleanTiFlashProgressCache clean progress cache
	CleanTiFlashProgressCache()
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

// Close is called to close TiFlashReplicaManagerCtx.
func (*TiFlashReplicaManagerCtx) Close(context.Context) {}

func getTiFlashPeerWithoutLagCount(tiFlashStores map[int64]pd.StoreInfo, keyspaceID tikv.KeyspaceID, tableID int64) (int, error) {
	// storeIDs -> regionID, PD will not create two peer on the same store
	var flashPeerCount int
	for _, store := range tiFlashStores {
		regionReplica := make(map[int64]int)
		err := helper.CollectTiFlashStatus(store.Store.StatusAddress, keyspaceID, tableID, &regionReplica)
		failpoint.Inject("OneTiFlashStoreDown", func() {
			if store.Store.StateName == "Down" {
				err = errors.New("mock TiFlasah down")
			}
		})
		if err != nil {
			logutil.BgLogger().Error("Fail to get peer status from TiFlash.",
				zap.Int64("tableID", tableID))
			// Just skip down or offline or tomestone stores, because PD will migrate regions from these stores.
			if store.Store.StateName == "Up" || store.Store.StateName == "Disconnected" {
				return 0, err
			}
			continue
		}
		flashPeerCount += len(regionReplica)
	}
	return flashPeerCount, nil
}

// calculateTiFlashProgress calculates progress based on the region status from PD and TiFlash.
func calculateTiFlashProgress(keyspaceID tikv.KeyspaceID, tableID int64, replicaCount uint64, tiFlashStores map[int64]pd.StoreInfo) (float64, error) {
	var regionCount int
	if err := GetTiFlashRegionCountFromPD(context.Background(), tableID, &regionCount); err != nil {
		logutil.BgLogger().Error("Fail to get regionCount from PD.",
			zap.Int64("tableID", tableID))
		return 0, errors.Trace(err)
	}

	if regionCount == 0 {
		logutil.BgLogger().Warn("region count getting from PD is 0.",
			zap.Int64("tableID", tableID))
		return 0, fmt.Errorf("region count getting from PD is 0")
	}

	tiflashPeerCount, err := getTiFlashPeerWithoutLagCount(tiFlashStores, keyspaceID, tableID)
	if err != nil {
		logutil.BgLogger().Error("Fail to get peer count from TiFlash.",
			zap.Int64("tableID", tableID))
		return 0, errors.Trace(err)
	}
	progress := float64(tiflashPeerCount) / float64(regionCount*int(replicaCount))
	if progress > 1 { // when pd do balance
		logutil.BgLogger().Debug("TiFlash peer count > pd peer count, maybe doing balance.",
			zap.Int64("tableID", tableID), zap.Int("tiflashPeerCount", tiflashPeerCount), zap.Int("regionCount", regionCount), zap.Uint64("replicaCount", replicaCount))
		progress = 1
	}
	if progress < 1 {
		logutil.BgLogger().Debug("TiFlash replica progress < 1.",
			zap.Int64("tableID", tableID), zap.Int("tiflashPeerCount", tiflashPeerCount), zap.Int("regionCount", regionCount), zap.Uint64("replicaCount", replicaCount))
	}
	return progress, nil
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
func (m *TiFlashReplicaManagerCtx) CalculateTiFlashProgress(tableID int64, replicaCount uint64, tiFlashStores map[int64]pd.StoreInfo) (float64, error) {
	return calculateTiFlashProgress(m.codec.GetKeyspaceID(), tableID, replicaCount, tiFlashStores)
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
	input := make([]*pd.KeyRange, 0, len(tableIDs))
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

type mockTiFlashReplicaManagerCtx struct {
	sync.RWMutex
	// Set to nil if there is no need to set up a mock TiFlash server.
	// Otherwise use NewMockTiFlash to create one.
	tiflash              *MockTiFlash
	tiflashProgressCache map[int64]float64
}

func makeBaseRule() pd.Rule {
	return pd.Rule{
		GroupID:  placement.TiFlashRuleGroupID,
		ID:       "",
		Index:    placement.RuleIndexTiFlash,
		Override: false,
		Role:     pd.Learner,
		Count:    2,
		LabelConstraints: []pd.LabelConstraint{
			{
				Key:    "engine",
				Op:     pd.In,
				Values: []string{"tiflash"},
			},
		},
	}
}

// MakeNewRule creates a pd rule for TiFlash.
func MakeNewRule(id int64, count uint64, locationLabels []string) pd.Rule {
	ruleID := MakeRuleID(id)
	startKey := tablecodec.GenTableRecordPrefix(id)
	endKey := tablecodec.EncodeTablePrefix(id + 1)

	ruleNew := makeBaseRule()
	ruleNew.ID = ruleID
	ruleNew.StartKey = startKey
	ruleNew.EndKey = endKey
	ruleNew.Count = int(count)
	ruleNew.LocationLabels = locationLabels

	return ruleNew
}

// MakeRuleID creates a rule ID for TiFlash with given TableID.
// This interface is exported for the module who wants to manipulate the TiFlash rule.
// The rule ID is in the format of "table-<TableID>-r".
// NOTE: PLEASE DO NOT write the rule ID manually, use this interface instead.
func MakeRuleID(id int64) string {
	return fmt.Sprintf("table-%v-r", id)
}

type mockTiFlashTableInfo struct {
	Regions []int
	Accel   bool
}

func (m *mockTiFlashTableInfo) String() string {
	regionStr := ""
	for _, s := range m.Regions {
		regionStr = regionStr + strconv.Itoa(s) + "\n"
	}
	if regionStr == "" {
		regionStr = "\n"
	}
	return fmt.Sprintf("%v\n%v", len(m.Regions), regionStr)
}

// MockTiFlash mocks a TiFlash, with necessary Pd support.
type MockTiFlash struct {
	syncutil.Mutex
	groupIndex                  int
	StatusAddr                  string
	StatusServer                *httptest.Server
	SyncStatus                  map[int]mockTiFlashTableInfo
	StoreInfo                   map[uint64]pd.MetaStore
	GlobalTiFlashPlacementRules map[string]*pd.Rule
	PdEnabled                   bool
	TiflashDelay                time.Duration
	StartTime                   time.Time
	NotAvailable                bool
	NetworkError                bool
}

func (tiflash *MockTiFlash) setUpMockTiFlashHTTPServer() {
	tiflash.Lock()
	defer tiflash.Unlock()
	// mock TiFlash http server
	router := mux.NewRouter()
	server := httptest.NewServer(router)
	// mock store stats stat
	statusAddr := strings.TrimPrefix(server.URL, "http://")
	statusAddrVec := strings.Split(statusAddr, ":")
	statusPort, _ := strconv.Atoi(statusAddrVec[1])
	router.HandleFunc("/tiflash/sync-status/keyspace/{keyspaceid:\\d+}/table/{tableid:\\d+}", func(w http.ResponseWriter, req *http.Request) {
		tiflash.Lock()
		defer tiflash.Unlock()
		if tiflash.NetworkError {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		params := mux.Vars(req)
		tableID, err := strconv.Atoi(params["tableid"])
		if err != nil {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		table, ok := tiflash.SyncStatus[tableID]
		if tiflash.NotAvailable {
			// No region is available, so the table is not available.
			table.Regions = []int{}
		}
		if !ok {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("0\n\n"))
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(table.String()))
	})
	router.HandleFunc("/config", func(w http.ResponseWriter, _ *http.Request) {
		tiflash.Lock()
		defer tiflash.Unlock()
		s := fmt.Sprintf("{\n    \"engine-store\": {\n        \"http_port\": %v\n    }\n}", statusPort)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(s))
	})
	tiflash.StatusServer = server
	tiflash.StatusAddr = statusAddr
}

// NewMockTiFlash creates a MockTiFlash with a mocked TiFlash server.
func NewMockTiFlash() *MockTiFlash {
	tiflash := &MockTiFlash{
		StatusAddr:                  "",
		StatusServer:                nil,
		SyncStatus:                  make(map[int]mockTiFlashTableInfo),
		StoreInfo:                   make(map[uint64]pd.MetaStore),
		GlobalTiFlashPlacementRules: make(map[string]*pd.Rule),
		PdEnabled:                   true,
		TiflashDelay:                0,
		StartTime:                   time.Now(),
		NotAvailable:                false,
	}
	tiflash.setUpMockTiFlashHTTPServer()
	return tiflash
}

// HandleSetPlacementRule is mock function for SetTiFlashPlacementRule.
func (tiflash *MockTiFlash) HandleSetPlacementRule(rule *pd.Rule) error {
	tiflash.Lock()
	defer tiflash.Unlock()
	tiflash.groupIndex = placement.RuleIndexTiFlash
	if !tiflash.PdEnabled {
		logutil.BgLogger().Info("pd server is manually disabled, just quit")
		return nil
	}

	if rule.Count == 0 {
		delete(tiflash.GlobalTiFlashPlacementRules, rule.ID)
	} else {
		tiflash.GlobalTiFlashPlacementRules[rule.ID] = rule
	}
	// Pd shall schedule TiFlash, we can mock here
	tid := 0
	_, err := fmt.Sscanf(rule.ID, "table-%d-r", &tid)
	if err != nil {
		return errors.New("Can't parse rule")
	}
	// Set up mock TiFlash replica
	f := func() {
		if z, ok := tiflash.SyncStatus[tid]; ok {
			z.Regions = []int{1}
			tiflash.SyncStatus[tid] = z
		} else {
			tiflash.SyncStatus[tid] = mockTiFlashTableInfo{
				Regions: []int{1},
				Accel:   false,
			}
		}
	}
	if tiflash.TiflashDelay > 0 {
		go func() {
			time.Sleep(tiflash.TiflashDelay)
			logutil.BgLogger().Warn("TiFlash replica is available after delay", zap.Duration("duration", tiflash.TiflashDelay))
			f()
		}()
	} else {
		f()
	}
	return nil
}

// HandleSetPlacementRuleBatch is mock function for batch SetTiFlashPlacementRule.
func (tiflash *MockTiFlash) HandleSetPlacementRuleBatch(rules []*pd.Rule) error {
	for _, r := range rules {
		if err := tiflash.HandleSetPlacementRule(r); err != nil {
			return err
		}
	}
	return nil
}

// ResetSyncStatus is mock function for reset sync status.
func (tiflash *MockTiFlash) ResetSyncStatus(tableID int, canAvailable bool) {
	tiflash.Lock()
	defer tiflash.Unlock()
	if canAvailable {
		if z, ok := tiflash.SyncStatus[tableID]; ok {
			z.Regions = []int{1}
			tiflash.SyncStatus[tableID] = z
		} else {
			tiflash.SyncStatus[tableID] = mockTiFlashTableInfo{
				Regions: []int{1},
				Accel:   false,
			}
		}
	} else {
		delete(tiflash.SyncStatus, tableID)
	}
}

// HandleDeletePlacementRule is mock function for DeleteTiFlashPlacementRule.
func (tiflash *MockTiFlash) HandleDeletePlacementRule(_ string, ruleID string) {
	tiflash.Lock()
	defer tiflash.Unlock()
	delete(tiflash.GlobalTiFlashPlacementRules, ruleID)
}

// HandleGetGroupRules is mock function for GetTiFlashGroupRules.
func (tiflash *MockTiFlash) HandleGetGroupRules(_ string) ([]*pd.Rule, error) {
	tiflash.Lock()
	defer tiflash.Unlock()
	var result = make([]*pd.Rule, 0)
	for _, item := range tiflash.GlobalTiFlashPlacementRules {
		result = append(result, item)
	}
	return result, nil
}

// HandlePostAccelerateSchedule is mock function for PostAccelerateSchedule
func (tiflash *MockTiFlash) HandlePostAccelerateSchedule(endKey string) error {
	tiflash.Lock()
	defer tiflash.Unlock()
	tableID := helper.GetTiFlashTableIDFromEndKey(endKey)

	table, ok := tiflash.SyncStatus[int(tableID)]
	if ok {
		table.Accel = true
		tiflash.SyncStatus[int(tableID)] = table
	} else {
		tiflash.SyncStatus[int(tableID)] = mockTiFlashTableInfo{
			Regions: []int{},
			Accel:   true,
		}
	}
	return nil
}

// HandleGetPDRegionRecordStats is mock function for GetRegionCountFromPD.
// It currently always returns 1 Region for convenience.
func (*MockTiFlash) HandleGetPDRegionRecordStats(int64) pd.RegionStats {
	return pd.RegionStats{
		Count: 1,
	}
}

// AddStore is mock function for adding store info into MockTiFlash.
func (tiflash *MockTiFlash) AddStore(storeID uint64, address string) {
	tiflash.StoreInfo[storeID] = pd.MetaStore{
		ID:             int64(storeID),
		Address:        address,
		State:          0,
		StateName:      "Up",
		Version:        "4.0.0-alpha",
		StatusAddress:  tiflash.StatusAddr,
		GitHash:        "mock-tikv-githash",
		StartTimestamp: tiflash.StartTime.Unix(),
		Labels: []pd.StoreLabel{{
			Key:   "engine",
			Value: "tiflash",
		}},
	}
}

// HandleGetStoresStat is mock function for GetStoresStat.
// It returns address of our mocked TiFlash server.
func (tiflash *MockTiFlash) HandleGetStoresStat() *pd.StoresInfo {
	tiflash.Lock()
	defer tiflash.Unlock()
	if len(tiflash.StoreInfo) == 0 {
		// default Store
		return &pd.StoresInfo{
			Count: 1,
			Stores: []pd.StoreInfo{
				{
					Store: pd.MetaStore{
						ID:             1,
						Address:        "127.0.0.1:3930",
						State:          0,
						StateName:      "Up",
						Version:        "4.0.0-alpha",
						StatusAddress:  tiflash.StatusAddr,
						GitHash:        "mock-tikv-githash",
						StartTimestamp: tiflash.StartTime.Unix(),
						Labels: []pd.StoreLabel{{
							Key:   "engine",
							Value: "tiflash",
						}},
					},
				},
			},
		}
	}
	stores := make([]pd.StoreInfo, 0, len(tiflash.StoreInfo))
	for _, storeInfo := range tiflash.StoreInfo {
		stores = append(stores, pd.StoreInfo{Store: storeInfo, Status: pd.StoreStatus{}})
	}
	return &pd.StoresInfo{
		Count:  len(tiflash.StoreInfo),
		Stores: stores,
	}
}

// SetRuleGroupIndex sets the group index of tiflash
func (tiflash *MockTiFlash) SetRuleGroupIndex(groupIndex int) {
	tiflash.Lock()
	defer tiflash.Unlock()
	tiflash.groupIndex = groupIndex
}

// GetRuleGroupIndex gets the group index of tiflash
func (tiflash *MockTiFlash) GetRuleGroupIndex() int {
	tiflash.Lock()
	defer tiflash.Unlock()
	return tiflash.groupIndex
}

// Compare supposed rule, and we actually get from TableInfo
func isRuleMatch(rule pd.Rule, startKey []byte, endKey []byte, count int, labels []string) bool {
	// Compute startKey
	if !(bytes.Equal(rule.StartKey, startKey) && bytes.Equal(rule.EndKey, endKey)) {
		return false
	}
	ok := false
	for _, c := range rule.LabelConstraints {
		if c.Key == "engine" && len(c.Values) == 1 && c.Values[0] == "tiflash" && c.Op == pd.In {
			ok = true
			break
		}
	}
	if !ok {
		return false
	}

	if len(rule.LocationLabels) != len(labels) {
		return false
	}
	for i, lb := range labels {
		if lb != rule.LocationLabels[i] {
			return false
		}
	}
	if rule.Count != count {
		return false
	}
	if rule.Role != pd.Learner {
		return false
	}
	return true
}

// CheckPlacementRule find if a given rule precisely matches already set rules.
func (tiflash *MockTiFlash) CheckPlacementRule(rule pd.Rule) bool {
	tiflash.Lock()
	defer tiflash.Unlock()
	for _, r := range tiflash.GlobalTiFlashPlacementRules {
		if isRuleMatch(rule, r.StartKey, r.EndKey, r.Count, r.LocationLabels) {
			return true
		}
	}
	return false
}

// GetPlacementRule find a rule by name.
func (tiflash *MockTiFlash) GetPlacementRule(ruleName string) (*pd.Rule, bool) {
	tiflash.Lock()
	defer tiflash.Unlock()
	if r, ok := tiflash.GlobalTiFlashPlacementRules[ruleName]; ok {
		p := r
		return p, ok
	}
	return nil, false
}

// CleanPlacementRules cleans all placement rules.
func (tiflash *MockTiFlash) CleanPlacementRules() {
	tiflash.Lock()
	defer tiflash.Unlock()
	tiflash.GlobalTiFlashPlacementRules = make(map[string]*pd.Rule)
}

// PlacementRulesLen gets length of all currently set placement rules.
func (tiflash *MockTiFlash) PlacementRulesLen() int {
	tiflash.Lock()
	defer tiflash.Unlock()
	return len(tiflash.GlobalTiFlashPlacementRules)
}

// GetTableSyncStatus returns table sync status by given tableID.
func (tiflash *MockTiFlash) GetTableSyncStatus(tableID int) (*mockTiFlashTableInfo, bool) {
	tiflash.Lock()
	defer tiflash.Unlock()
	if r, ok := tiflash.SyncStatus[tableID]; ok {
		p := r
		return &p, ok
	}
	return nil, false
}

// PdSwitch controls if pd is enabled.
func (tiflash *MockTiFlash) PdSwitch(enabled bool) {
	tiflash.Lock()
	defer tiflash.Unlock()
	tiflash.PdEnabled = enabled
}

// SetNetworkError sets network error state.
func (tiflash *MockTiFlash) SetNetworkError(e bool) {
	tiflash.Lock()
	defer tiflash.Unlock()
	tiflash.NetworkError = e
}

// CalculateTiFlashProgress return truncated string to avoid float64 comparison.
func (*mockTiFlashReplicaManagerCtx) CalculateTiFlashProgress(tableID int64, replicaCount uint64, tiFlashStores map[int64]pd.StoreInfo) (float64, error) {
	return calculateTiFlashProgress(tikv.NullspaceID, tableID, replicaCount, tiFlashStores)
}

// UpdateTiFlashProgressCache updates tiflashProgressCache
func (m *mockTiFlashReplicaManagerCtx) UpdateTiFlashProgressCache(tableID int64, progress float64) {
	m.Lock()
	defer m.Unlock()
	m.tiflashProgressCache[tableID] = progress
}

// GetTiFlashProgressFromCache gets tiflash replica progress from tiflashProgressCache
func (m *mockTiFlashReplicaManagerCtx) GetTiFlashProgressFromCache(tableID int64) (float64, bool) {
	m.RLock()
	defer m.RUnlock()
	progress, ok := m.tiflashProgressCache[tableID]
	return progress, ok
}

// DeleteTiFlashProgressFromCache delete tiflash replica progress from tiflashProgressCache
func (m *mockTiFlashReplicaManagerCtx) DeleteTiFlashProgressFromCache(tableID int64) {
	m.Lock()
	defer m.Unlock()
	delete(m.tiflashProgressCache, tableID)
}

// CleanTiFlashProgressCache clean progress cache
func (m *mockTiFlashReplicaManagerCtx) CleanTiFlashProgressCache() {
	m.Lock()
	defer m.Unlock()
	m.tiflashProgressCache = make(map[int64]float64)
}

// SetMockTiFlash is set a mock TiFlash server.
func (m *mockTiFlashReplicaManagerCtx) SetMockTiFlash(tiflash *MockTiFlash) {
	m.Lock()
	defer m.Unlock()
	m.tiflash = tiflash
}

// SetTiFlashGroupConfig sets the tiflash's rule group config
func (m *mockTiFlashReplicaManagerCtx) SetTiFlashGroupConfig(_ context.Context) error {
	m.Lock()
	defer m.Unlock()
	if m.tiflash == nil {
		return nil
	}
	m.tiflash.SetRuleGroupIndex(placement.RuleIndexTiFlash)
	return nil
}

// SetPlacementRule is a helper function to set placement rule.
func (m *mockTiFlashReplicaManagerCtx) SetPlacementRule(_ context.Context, rule *pd.Rule) error {
	m.Lock()
	defer m.Unlock()
	if m.tiflash == nil {
		return nil
	}
	return m.tiflash.HandleSetPlacementRule(rule)
}

// SetPlacementRuleBatch is a helper function to set a batch of placement rules.
func (m *mockTiFlashReplicaManagerCtx) SetPlacementRuleBatch(_ context.Context, rules []*pd.Rule) error {
	m.Lock()
	defer m.Unlock()
	if m.tiflash == nil {
		return nil
	}
	return m.tiflash.HandleSetPlacementRuleBatch(rules)
}

// DeletePlacementRule is to delete placement rule for certain group.
func (m *mockTiFlashReplicaManagerCtx) DeletePlacementRule(_ context.Context, group string, ruleID string) error {
	m.Lock()
	defer m.Unlock()
	if m.tiflash == nil {
		return nil
	}
	logutil.BgLogger().Info("Remove TiFlash rule", zap.String("ruleID", ruleID))
	m.tiflash.HandleDeletePlacementRule(group, ruleID)
	return nil
}

// GetGroupRules to get all placement rule in a certain group.
func (m *mockTiFlashReplicaManagerCtx) GetGroupRules(_ context.Context, group string) ([]*pd.Rule, error) {
	m.Lock()
	defer m.Unlock()
	if m.tiflash == nil {
		return []*pd.Rule{}, nil
	}
	return m.tiflash.HandleGetGroupRules(group)
}

// PostAccelerateScheduleBatch sends `regions/batch-accelerate-schedule` request.
func (m *mockTiFlashReplicaManagerCtx) PostAccelerateScheduleBatch(_ context.Context, tableIDs []int64) error {
	m.Lock()
	defer m.Unlock()
	if m.tiflash == nil {
		return nil
	}
	for _, tableID := range tableIDs {
		endKey := tablecodec.EncodeTablePrefix(tableID + 1)
		endKey = codec.EncodeBytes([]byte{}, endKey)
		if err := m.tiflash.HandlePostAccelerateSchedule(hex.EncodeToString(endKey)); err != nil {
			return err
		}
	}
	return nil
}

// GetRegionCountFromPD is a helper function calling `/stats/region`.
func (m *mockTiFlashReplicaManagerCtx) GetRegionCountFromPD(_ context.Context, tableID int64, regionCount *int) error {
	m.Lock()
	defer m.Unlock()
	if m.tiflash == nil {
		return nil
	}
	stats := m.tiflash.HandleGetPDRegionRecordStats(tableID)
	*regionCount = stats.Count
	return nil
}

// GetStoresStat gets the TiKV store information by accessing PD's api.
func (m *mockTiFlashReplicaManagerCtx) GetStoresStat(_ context.Context) (*pd.StoresInfo, error) {
	m.Lock()
	defer m.Unlock()
	if m.tiflash == nil {
		return nil, &MockTiFlashError{"MockTiFlash is not accessible"}
	}
	return m.tiflash.HandleGetStoresStat(), nil
}

// Close is called to close mockTiFlashReplicaManager.
func (m *mockTiFlashReplicaManagerCtx) Close(_ context.Context) {
	m.Lock()
	defer m.Unlock()
	if m.tiflash == nil {
		return
	}
	if m.tiflash.StatusServer != nil {
		m.tiflash.StatusServer.Close()
	}
}

// MockTiFlashError represents MockTiFlash error
type MockTiFlashError struct {
	Message string
}

func (me *MockTiFlashError) Error() string {
	return me.Message
}
