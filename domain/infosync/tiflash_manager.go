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
	"github.com/pingcap/tidb/ddl/placement"
	"github.com/pingcap/tidb/store/helper"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/pdapi"
	"go.uber.org/zap"
)

// TiFlashPlacementManager manages placement settings for TiFlash.
type TiFlashPlacementManager interface {
	// SetPlacementRule is a helper function to set placement rule.
	SetPlacementRule(ctx context.Context, rule placement.TiFlashRule) error
	// DeletePlacementRule is to delete placement rule for certain group.
	DeletePlacementRule(ctx context.Context, group string, ruleID string) error
	// GetGroupRules to get all placement rule in a certain group.
	GetGroupRules(ctx context.Context, group string) ([]placement.TiFlashRule, error)
	// PostAccelerateSchedule sends `regions/accelerate-schedule` request.
	PostAccelerateSchedule(ctx context.Context, tableID int64) error
	// GetPDRegionRecordStats is a helper function calling `/stats/region`.
	GetPDRegionRecordStats(ctx context.Context, tableID int64, stats *helper.PDRegionStats) error
	// GetStoresStat gets the TiKV store information by accessing PD's api.
	GetStoresStat(ctx context.Context) (*helper.StoresStat, error)
	// Close is to close TiFlashPlacementManager
	Close(ctx context.Context)
}

// TiFlashPDPlacementManager manages placement with pd for TiFlash.
type TiFlashPDPlacementManager struct {
	addrs []string
}

// Close is called to close TiFlashPDPlacementManager.
func (m *TiFlashPDPlacementManager) Close(ctx context.Context) {

}

// SetPlacementRule is a helper function to set placement rule.
func (m *TiFlashPDPlacementManager) SetPlacementRule(ctx context.Context, rule placement.TiFlashRule) error {
	if rule.Count == 0 {
		return m.DeletePlacementRule(ctx, rule.GroupID, rule.ID)
	}
	j, _ := json.Marshal(rule)
	buf := bytes.NewBuffer(j)
	res, err := doRequest(ctx, m.addrs, path.Join(pdapi.Config, "rule"), "POST", buf)
	if err != nil {
		return errors.Trace(err)
	}
	if res == nil {
		return fmt.Errorf("TiFlashPDPlacementManager returns error in SetPlacementRule")
	}
	return nil
}

// DeletePlacementRule is to delete placement rule for certain group.
func (m *TiFlashPDPlacementManager) DeletePlacementRule(ctx context.Context, group string, ruleID string) error {
	res, err := doRequest(ctx, m.addrs, path.Join(pdapi.Config, "rule", group, ruleID), "DELETE", nil)
	if err != nil {
		return errors.Trace(err)
	}
	if res == nil {
		return fmt.Errorf("TiFlashPDPlacementManager returns error in DeletePlacementRule")
	}
	return nil
}

// GetGroupRules to get all placement rule in a certain group.
func (m *TiFlashPDPlacementManager) GetGroupRules(ctx context.Context, group string) ([]placement.TiFlashRule, error) {
	res, err := doRequest(ctx, m.addrs, path.Join(pdapi.Config, "rules", "group", group), "GET", nil)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if res == nil {
		return nil, fmt.Errorf("TiFlashPDPlacementManager returns error in GetGroupRules")
	}

	var rules []placement.TiFlashRule
	err = json.Unmarshal(res, &rules)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return rules, nil
}

// PostAccelerateSchedule sends `regions/accelerate-schedule` request.
func (m *TiFlashPDPlacementManager) PostAccelerateSchedule(ctx context.Context, tableID int64) error {
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
	res, err := doRequest(ctx, m.addrs, "/pd/api/v1/regions/accelerate-schedule", "POST", buf)
	if err != nil {
		return errors.Trace(err)
	}
	if res == nil {
		return fmt.Errorf("TiFlashPDPlacementManager returns error in PostAccelerateSchedule")
	}
	return nil
}

// GetPDRegionRecordStats is a helper function calling `/stats/region`.
func (m *TiFlashPDPlacementManager) GetPDRegionRecordStats(ctx context.Context, tableID int64, stats *helper.PDRegionStats) error {
	startKey := tablecodec.GenTableRecordPrefix(tableID)
	endKey := tablecodec.EncodeTablePrefix(tableID + 1)
	startKey = codec.EncodeBytes([]byte{}, startKey)
	endKey = codec.EncodeBytes([]byte{}, endKey)

	p := fmt.Sprintf("/pd/api/v1/stats/region?start_key=%s&end_key=%s",
		url.QueryEscape(string(startKey)),
		url.QueryEscape(string(endKey)))
	res, err := doRequest(ctx, m.addrs, p, "GET", nil)
	if err != nil {
		return errors.Trace(err)
	}
	if res == nil {
		return fmt.Errorf("TiFlashPDPlacementManager returns error in GetPDRegionRecordStats")
	}

	err = json.Unmarshal(res, stats)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

// GetStoresStat gets the TiKV store information by accessing PD's api.
func (m *TiFlashPDPlacementManager) GetStoresStat(ctx context.Context) (*helper.StoresStat, error) {
	var storesStat helper.StoresStat
	res, err := doRequest(ctx, m.addrs, pdapi.Stores, "GET", nil)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if res == nil {
		return nil, fmt.Errorf("TiFlashPDPlacementManager returns error in GetStoresStat")
	}

	err = json.Unmarshal(res, &storesStat)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &storesStat, err
}

type mockTiFlashPlacementManager struct {
	sync.Mutex
	// Set to nil if there is no need to set up a mock TiFlash server.
	// Otherwise use NewMockTiFlash to create one.
	tiflash *MockTiFlash
}

func makeBaseRule() placement.TiFlashRule {
	return placement.TiFlashRule{
		GroupID:  "tiflash",
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
	StatusAddr                  string
	StatusServer                *httptest.Server
	SyncStatus                  map[int]mockTiFlashTableInfo
	GlobalTiFlashPlacementRules map[string]placement.TiFlashRule
	PdEnabled                   bool
	TiflashDelay                time.Duration
	StartTime                   time.Time
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
		logutil.BgLogger().Info("Mock TiFlash returns", zap.Bool("ok", ok), zap.Int("tableID", tableID))
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
		GlobalTiFlashPlacementRules: make(map[string]placement.TiFlashRule),
		PdEnabled:                   true,
		TiflashDelay:                0,
		StartTime:                   time.Now(),
	}
	tiflash.setUpMockTiFlashHTTPServer()
	return tiflash
}

// HandleSetPlacementRule is mock function for SetTiFlashPlacementRule.
func (tiflash *MockTiFlash) HandleSetPlacementRule(rule placement.TiFlashRule) error {
	tiflash.Lock()
	defer tiflash.Unlock()
	if !tiflash.PdEnabled {
		return errors.New("pd server is manually disabled, just quit")
	}

	tiflash.GlobalTiFlashPlacementRules[rule.ID] = rule
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

// HandleGetPDRegionRecordStats is mock function for GetPDRegionRecordStats.
// It currently always returns 1 Region for convenience.
func (tiflash *MockTiFlash) HandleGetPDRegionRecordStats(_ int64) helper.PDRegionStats {
	return helper.PDRegionStats{
		Count:            1,
		EmptyCount:       1,
		StorageSize:      1,
		StorageKeys:      1,
		StoreLeaderCount: map[uint64]int{1: 1},
		StorePeerCount:   map[uint64]int{1: 1},
	}
}

// HandleGetStoresStat is mock function for GetStoresStat.
// It returns address of our mocked TiFlash server.
func (tiflash *MockTiFlash) HandleGetStoresStat() *helper.StoresStat {
	tiflash.Lock()
	defer tiflash.Unlock()
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

// SetPlacementRule is a helper function to set placement rule.
func (m *mockTiFlashPlacementManager) SetPlacementRule(ctx context.Context, rule placement.TiFlashRule) error {
	m.Lock()
	defer m.Unlock()
	if m.tiflash == nil {
		return nil
	}
	return m.tiflash.HandleSetPlacementRule(rule)
}

// DeletePlacementRule is to delete placement rule for certain group.
func (m *mockTiFlashPlacementManager) DeletePlacementRule(ctx context.Context, group string, ruleID string) error {
	m.Lock()
	defer m.Unlock()
	if m.tiflash == nil {
		return nil
	}
	logutil.BgLogger().Info("Remove TiFlash rule", zap.String("ID", ruleID))
	m.tiflash.HandleDeletePlacementRule(group, ruleID)
	return nil
}

// GetGroupRules to get all placement rule in a certain group.
func (m *mockTiFlashPlacementManager) GetGroupRules(ctx context.Context, group string) ([]placement.TiFlashRule, error) {
	m.Lock()
	defer m.Unlock()
	if m.tiflash == nil {
		return []placement.TiFlashRule{}, nil
	}
	return m.tiflash.HandleGetGroupRules(group)
}

// PostAccelerateSchedule sends `regions/accelerate-schedule` request.
func (m *mockTiFlashPlacementManager) PostAccelerateSchedule(ctx context.Context, tableID int64) error {
	m.Lock()
	defer m.Unlock()
	if m.tiflash == nil {
		return nil
	}
	endKey := tablecodec.EncodeTablePrefix(tableID + 1)
	endKey = codec.EncodeBytes([]byte{}, endKey)
	return m.tiflash.HandlePostAccelerateSchedule(hex.EncodeToString(endKey))
}

// GetPDRegionRecordStats is a helper function calling `/stats/region`.
func (m *mockTiFlashPlacementManager) GetPDRegionRecordStats(ctx context.Context, tableID int64, stats *helper.PDRegionStats) error {
	m.Lock()
	defer m.Unlock()
	if m.tiflash == nil {
		return nil
	}
	*stats = m.tiflash.HandleGetPDRegionRecordStats(tableID)
	return nil
}

// GetStoresStat gets the TiKV store information by accessing PD's api.
func (m *mockTiFlashPlacementManager) GetStoresStat(ctx context.Context) (*helper.StoresStat, error) {
	m.Lock()
	defer m.Unlock()
	if m.tiflash == nil {
		return nil, &MockTiFlashError{"MockTiFlash is not accessible"}
	}
	return m.tiflash.HandleGetStoresStat(), nil
}

// Close is called to close mockTiFlashPlacementManager.
func (m *mockTiFlashPlacementManager) Close(ctx context.Context) {
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
