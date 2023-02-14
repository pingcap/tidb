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
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/mux"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/ddl/placement"
	"github.com/pingcap/tidb/store/helper"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/dbterror"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/pdapi"
	"github.com/tikv/client-go/v2/tikv"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

// TiFlashReplicaManager manages placement settings and replica progress for TiFlash.
type TiFlashReplicaManager interface {
	// SetTiFlashGroupConfig sets the group index of the tiflash placement rule
	SetTiFlashGroupConfig(ctx context.Context) error
	// SetPlacementRule is a helper function to set placement rule.
	SetPlacementRule(ctx context.Context, rule placement.TiFlashRule) error
	// DeletePlacementRule is to delete placement rule for certain group.
	DeletePlacementRule(ctx context.Context, group string, ruleID string) error
	// GetGroupRules to get all placement rule in a certain group.
	GetGroupRules(ctx context.Context, group string) ([]placement.TiFlashRule, error)
	// PostAccelerateSchedule sends `regions/accelerate-schedule` request.
	PostAccelerateSchedule(ctx context.Context, tableID int64) error
	// GetRegionCountFromPD is a helper function calling `/stats/region`.
	GetRegionCountFromPD(ctx context.Context, tableID int64, regionCount *int) error
	// GetStoresStat gets the TiKV store information by accessing PD's api.
	GetStoresStat(ctx context.Context) (*helper.StoresStat, error)
	// CalculateTiFlashProgress calculates TiFlash replica progress
	CalculateTiFlashProgress(tableID int64, replicaCount uint64, TiFlashStores map[int64]helper.StoreStat) (float64, error)
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
	etcdCli              *clientv3.Client
	sync.RWMutex         // protect tiflashProgressCache
	tiflashProgressCache map[int64]float64
	codec                tikv.Codec
}

// Close is called to close TiFlashReplicaManagerCtx.
func (m *TiFlashReplicaManagerCtx) Close(ctx context.Context) {

}

func getTiFlashPeerWithoutLagCount(tiFlashStores map[int64]helper.StoreStat, tableID int64) (int, error) {
	// storeIDs -> regionID, PD will not create two peer on the same store
	var flashPeerCount int
	for _, store := range tiFlashStores {
		regionReplica := make(map[int64]int)
		err := helper.CollectTiFlashStatus(store.Store.StatusAddress, tableID, &regionReplica)
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
func calculateTiFlashProgress(tableID int64, replicaCount uint64, tiFlashStores map[int64]helper.StoreStat) (float64, error) {
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

	tiflashPeerCount, err := getTiFlashPeerWithoutLagCount(tiFlashStores, tableID)
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

// CalculateTiFlashProgress calculates TiFlash replica progress.
func (m *TiFlashReplicaManagerCtx) CalculateTiFlashProgress(tableID int64, replicaCount uint64, tiFlashStores map[int64]helper.StoreStat) (float64, error) {
	return calculateTiFlashProgress(tableID, replicaCount, tiFlashStores)
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
	res, err := doRequest(ctx,
		"GetRuleGroupConfig",
		m.etcdCli.Endpoints(),
		path.Join(pdapi.Config, "rule_group", placement.TiFlashRuleGroupID),
		"GET",
		nil,
	)

	if err != nil {
		return errors.Trace(err)
	}

	var groupConfig placement.RuleGroupConfig
	shouldUpdate := res == nil
	if res != nil {
		if err = json.Unmarshal(res, &groupConfig); err != nil {
			return errors.Trace(err)
		}

		if groupConfig.Index != placement.RuleIndexTiFlash || groupConfig.Override {
			shouldUpdate = true
		}
	}

	if shouldUpdate {
		groupConfig.ID = placement.TiFlashRuleGroupID
		groupConfig.Index = placement.RuleIndexTiFlash
		groupConfig.Override = false

		body, err := json.Marshal(&groupConfig)
		if err != nil {
			return errors.Trace(err)
		}

		_, err = doRequest(ctx,
			"SetRuleGroupConfig",
			m.etcdCli.Endpoints(),
			path.Join(pdapi.Config, "rule_group"),
			"POST",
			bytes.NewBuffer(body),
		)

		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

// SetPlacementRule is a helper function to set placement rule.
func (m *TiFlashReplicaManagerCtx) SetPlacementRule(ctx context.Context, rule placement.TiFlashRule) error {
	// TiDB 6.6 doesn't support tiflash multi-tenancy yet.
	// TODO(iosmanthus): remove this check after TiDB supports tiflash multi-tenancy.
	if m.codec.GetAPIVersion() == kvrpcpb.APIVersion_V2 {
		return errors.Trace(dbterror.ErrNotSupportedYet.GenWithStackByArgs("set TiFlash replica count while enabling API V2"))
	}
	if err := m.SetTiFlashGroupConfig(ctx); err != nil {
		return err
	}

	if rule.Count == 0 {
		return m.DeletePlacementRule(ctx, rule.GroupID, rule.ID)
	}
	j, _ := json.Marshal(rule)
	buf := bytes.NewBuffer(j)
	res, err := doRequest(ctx, "SetPlacementRule", m.etcdCli.Endpoints(), path.Join(pdapi.Config, "rule"), "POST", buf)
	if err != nil {
		return errors.Trace(err)
	}
	if res == nil {
		return fmt.Errorf("TiFlashReplicaManagerCtx returns error in SetPlacementRule")
	}
	return nil
}

// DeletePlacementRule is to delete placement rule for certain group.
func (m *TiFlashReplicaManagerCtx) DeletePlacementRule(ctx context.Context, group string, ruleID string) error {
	res, err := doRequest(ctx, "DeletePlacementRule", m.etcdCli.Endpoints(), path.Join(pdapi.Config, "rule", group, ruleID), "DELETE", nil)
	if err != nil {
		return errors.Trace(err)
	}
	if res == nil {
		return fmt.Errorf("TiFlashReplicaManagerCtx returns error in DeletePlacementRule")
	}
	return nil
}

// GetGroupRules to get all placement rule in a certain group.
func (m *TiFlashReplicaManagerCtx) GetGroupRules(ctx context.Context, group string) ([]placement.TiFlashRule, error) {
	res, err := doRequest(ctx, "GetGroupRules", m.etcdCli.Endpoints(), path.Join(pdapi.Config, "rules", "group", group), "GET", nil)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if res == nil {
		return nil, fmt.Errorf("TiFlashReplicaManagerCtx returns error in GetGroupRules")
	}

	var rules []placement.TiFlashRule
	err = json.Unmarshal(res, &rules)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return rules, nil
}

// PostAccelerateSchedule sends `regions/accelerate-schedule` request.
func (m *TiFlashReplicaManagerCtx) PostAccelerateSchedule(ctx context.Context, tableID int64) error {
	startKey := tablecodec.GenTableRecordPrefix(tableID)
	endKey := tablecodec.EncodeTablePrefix(tableID + 1)
	startKey = codec.EncodeBytes([]byte{}, startKey)
	endKey = codec.EncodeBytes([]byte{}, endKey)

	input := map[string]string{
		"start_key": hex.EncodeToString(startKey),
		"end_key":   hex.EncodeToString(endKey),
	}
	j, err := json.Marshal(input)
	if err != nil {
		return errors.Trace(err)
	}
	buf := bytes.NewBuffer(j)
	res, err := doRequest(ctx, "PostAccelerateSchedule", m.etcdCli.Endpoints(), "/pd/api/v1/regions/accelerate-schedule", "POST", buf)
	if err != nil {
		return errors.Trace(err)
	}
	if res == nil {
		return fmt.Errorf("TiFlashReplicaManagerCtx returns error in PostAccelerateSchedule")
	}
	return nil
}

// GetRegionCountFromPD is a helper function calling `/stats/region`.
func (m *TiFlashReplicaManagerCtx) GetRegionCountFromPD(ctx context.Context, tableID int64, regionCount *int) error {
	startKey := tablecodec.GenTableRecordPrefix(tableID)
	endKey := tablecodec.EncodeTablePrefix(tableID + 1)
	startKey = codec.EncodeBytes([]byte{}, startKey)
	endKey = codec.EncodeBytes([]byte{}, endKey)

	p := fmt.Sprintf("/pd/api/v1/stats/region?start_key=%s&end_key=%s&count",
		url.QueryEscape(string(startKey)),
		url.QueryEscape(string(endKey)))
	res, err := doRequest(ctx, "GetPDRegionStats", m.etcdCli.Endpoints(), p, "GET", nil)
	if err != nil {
		return errors.Trace(err)
	}
	if res == nil {
		return fmt.Errorf("TiFlashReplicaManagerCtx returns error in GetRegionCountFromPD")
	}
	var stats helper.PDRegionStats
	err = json.Unmarshal(res, &stats)
	if err != nil {
		return errors.Trace(err)
	}
	*regionCount = stats.Count
	return nil
}

// GetStoresStat gets the TiKV store information by accessing PD's api.
func (m *TiFlashReplicaManagerCtx) GetStoresStat(ctx context.Context) (*helper.StoresStat, error) {
	var storesStat helper.StoresStat
	res, err := doRequest(ctx, "GetStoresStat", m.etcdCli.Endpoints(), pdapi.Stores, "GET", nil)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if res == nil {
		return nil, fmt.Errorf("TiFlashReplicaManagerCtx returns error in GetStoresStat")
	}

	err = json.Unmarshal(res, &storesStat)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &storesStat, err
}

type mockTiFlashReplicaManagerCtx struct {
	sync.RWMutex
	// Set to nil if there is no need to set up a mock TiFlash server.
	// Otherwise use NewMockTiFlash to create one.
	tiflash              *MockTiFlash
	tiflashProgressCache map[int64]float64
}

func makeBaseRule() placement.TiFlashRule {
	return placement.TiFlashRule{
		GroupID:  placement.TiFlashRuleGroupID,
		ID:       "",
		Index:    placement.RuleIndexTiFlash,
		Override: false,
		Role:     placement.Learner,
		Count:    2,
		Constraints: []placement.Constraint{
			{
				Key:    "engine",
				Op:     placement.In,
				Values: []string{"tiflash"},
			},
		},
	}
}

// MakeNewRule creates a pd rule for TiFlash.
func MakeNewRule(ID int64, Count uint64, LocationLabels []string) *placement.TiFlashRule {
	ruleID := fmt.Sprintf("table-%v-r", ID)
	startKey := tablecodec.GenTableRecordPrefix(ID)
	endKey := tablecodec.EncodeTablePrefix(ID + 1)
	startKey = codec.EncodeBytes([]byte{}, startKey)
	endKey = codec.EncodeBytes([]byte{}, endKey)

	ruleNew := makeBaseRule()
	ruleNew.ID = ruleID
	ruleNew.StartKeyHex = startKey.String()
	ruleNew.EndKeyHex = endKey.String()
	ruleNew.Count = int(Count)
	ruleNew.LocationLabels = LocationLabels

	return &ruleNew
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
	sync.Mutex
	groupIndex                  int
	StatusAddr                  string
	StatusServer                *httptest.Server
	SyncStatus                  map[int]mockTiFlashTableInfo
	StoreInfo                   map[uint64]helper.StoreBaseStat
	GlobalTiFlashPlacementRules map[string]placement.TiFlashRule
	PdEnabled                   bool
	TiflashDelay                time.Duration
	StartTime                   time.Time
	NotAvailable                bool
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
	router.HandleFunc("/tiflash/sync-status/{tableid:\\d+}", func(w http.ResponseWriter, req *http.Request) {
		tiflash.Lock()
		defer tiflash.Unlock()
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
	router.HandleFunc("/config", func(w http.ResponseWriter, req *http.Request) {
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
		StoreInfo:                   make(map[uint64]helper.StoreBaseStat),
		GlobalTiFlashPlacementRules: make(map[string]placement.TiFlashRule),
		PdEnabled:                   true,
		TiflashDelay:                0,
		StartTime:                   time.Now(),
		NotAvailable:                false,
	}
	tiflash.setUpMockTiFlashHTTPServer()
	return tiflash
}

// HandleSetPlacementRule is mock function for SetTiFlashPlacementRule.
func (tiflash *MockTiFlash) HandleSetPlacementRule(rule placement.TiFlashRule) error {
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
func (tiflash *MockTiFlash) HandleDeletePlacementRule(group string, ruleID string) {
	tiflash.Lock()
	defer tiflash.Unlock()
	delete(tiflash.GlobalTiFlashPlacementRules, ruleID)
}

// HandleGetGroupRules is mock function for GetTiFlashGroupRules.
func (tiflash *MockTiFlash) HandleGetGroupRules(group string) ([]placement.TiFlashRule, error) {
	tiflash.Lock()
	defer tiflash.Unlock()
	var result = make([]placement.TiFlashRule, 0)
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
func (tiflash *MockTiFlash) HandleGetPDRegionRecordStats(_ int64) helper.PDRegionStats {
	return helper.PDRegionStats{
		Count: 1,
	}
}

// AddStore is mock function for adding store info into MockTiFlash.
func (tiflash *MockTiFlash) AddStore(storeID uint64, address string) {
	tiflash.StoreInfo[storeID] = helper.StoreBaseStat{
		ID:             int64(storeID),
		Address:        address,
		State:          0,
		StateName:      "Up",
		Version:        "4.0.0-alpha",
		StatusAddress:  tiflash.StatusAddr,
		GitHash:        "mock-tikv-githash",
		StartTimestamp: tiflash.StartTime.Unix(),
		Labels: []helper.StoreLabel{{
			Key:   "engine",
			Value: "tiflash",
		}},
	}
}

// HandleGetStoresStat is mock function for GetStoresStat.
// It returns address of our mocked TiFlash server.
func (tiflash *MockTiFlash) HandleGetStoresStat() *helper.StoresStat {
	tiflash.Lock()
	defer tiflash.Unlock()
	if len(tiflash.StoreInfo) == 0 {
		// default Store
		return &helper.StoresStat{
			Count: 1,
			Stores: []helper.StoreStat{
				{
					Store: helper.StoreBaseStat{
						ID:             1,
						Address:        "127.0.0.1:3930",
						State:          0,
						StateName:      "Up",
						Version:        "4.0.0-alpha",
						StatusAddress:  tiflash.StatusAddr,
						GitHash:        "mock-tikv-githash",
						StartTimestamp: tiflash.StartTime.Unix(),
						Labels: []helper.StoreLabel{{
							Key:   "engine",
							Value: "tiflash",
						}},
					},
				},
			},
		}
	}
	stores := make([]helper.StoreStat, 0, len(tiflash.StoreInfo))
	for _, storeInfo := range tiflash.StoreInfo {
		stores = append(stores, helper.StoreStat{Store: storeInfo, Status: helper.StoreDetailStat{}})
	}
	return &helper.StoresStat{
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
func isRuleMatch(rule placement.TiFlashRule, startKey string, endKey string, count int, labels []string) bool {
	// Compute startKey
	if rule.StartKeyHex == startKey && rule.EndKeyHex == endKey {
		ok := false
		for _, c := range rule.Constraints {
			if c.Key == "engine" && len(c.Values) == 1 && c.Values[0] == "tiflash" && c.Op == placement.In {
				ok = true
				break
			}
		}
		if !ok {
			return false
		}

		if len(rule.LocationLabels) == len(labels) {
			for i, lb := range labels {
				if lb != rule.LocationLabels[i] {
					return false
				}
			}
		} else {
			return false
		}

		if rule.Count != count {
			return false
		}
		if rule.Role != placement.Learner {
			return false
		}
	} else {
		return false
	}
	return true
}

// CheckPlacementRule find if a given rule precisely matches already set rules.
func (tiflash *MockTiFlash) CheckPlacementRule(rule placement.TiFlashRule) bool {
	tiflash.Lock()
	defer tiflash.Unlock()
	for _, r := range tiflash.GlobalTiFlashPlacementRules {
		if isRuleMatch(rule, r.StartKeyHex, r.EndKeyHex, r.Count, r.LocationLabels) {
			return true
		}
	}
	return false
}

// GetPlacementRule find a rule by name.
func (tiflash *MockTiFlash) GetPlacementRule(ruleName string) (*placement.TiFlashRule, bool) {
	tiflash.Lock()
	defer tiflash.Unlock()
	if r, ok := tiflash.GlobalTiFlashPlacementRules[ruleName]; ok {
		p := r
		return &p, ok
	}
	return nil, false
}

// CleanPlacementRules cleans all placement rules.
func (tiflash *MockTiFlash) CleanPlacementRules() {
	tiflash.Lock()
	defer tiflash.Unlock()
	tiflash.GlobalTiFlashPlacementRules = make(map[string]placement.TiFlashRule)
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

// CalculateTiFlashProgress return truncated string to avoid float64 comparison.
func (m *mockTiFlashReplicaManagerCtx) CalculateTiFlashProgress(tableID int64, replicaCount uint64, tiFlashStores map[int64]helper.StoreStat) (float64, error) {
	return calculateTiFlashProgress(tableID, replicaCount, tiFlashStores)
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
func (m *mockTiFlashReplicaManagerCtx) SetPlacementRule(ctx context.Context, rule placement.TiFlashRule) error {
	m.Lock()
	defer m.Unlock()
	if m.tiflash == nil {
		return nil
	}
	return m.tiflash.HandleSetPlacementRule(rule)
}

// DeletePlacementRule is to delete placement rule for certain group.
func (m *mockTiFlashReplicaManagerCtx) DeletePlacementRule(ctx context.Context, group string, ruleID string) error {
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
func (m *mockTiFlashReplicaManagerCtx) GetGroupRules(ctx context.Context, group string) ([]placement.TiFlashRule, error) {
	m.Lock()
	defer m.Unlock()
	if m.tiflash == nil {
		return []placement.TiFlashRule{}, nil
	}
	return m.tiflash.HandleGetGroupRules(group)
}

// PostAccelerateSchedule sends `regions/accelerate-schedule` request.
func (m *mockTiFlashReplicaManagerCtx) PostAccelerateSchedule(ctx context.Context, tableID int64) error {
	m.Lock()
	defer m.Unlock()
	if m.tiflash == nil {
		return nil
	}
	endKey := tablecodec.EncodeTablePrefix(tableID + 1)
	endKey = codec.EncodeBytes([]byte{}, endKey)
	return m.tiflash.HandlePostAccelerateSchedule(hex.EncodeToString(endKey))
}

// GetRegionCountFromPD is a helper function calling `/stats/region`.
func (m *mockTiFlashReplicaManagerCtx) GetRegionCountFromPD(ctx context.Context, tableID int64, regionCount *int) error {
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
func (m *mockTiFlashReplicaManagerCtx) GetStoresStat(ctx context.Context) (*helper.StoresStat, error) {
	m.Lock()
	defer m.Unlock()
	if m.tiflash == nil {
		return nil, &MockTiFlashError{"MockTiFlash is not accessible"}
	}
	return m.tiflash.HandleGetStoresStat(), nil
}

// Close is called to close mockTiFlashReplicaManager.
func (m *mockTiFlashReplicaManagerCtx) Close(ctx context.Context) {
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
