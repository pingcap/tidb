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

package infosync

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/ddl/placement"
	"github.com/pingcap/tidb/store/helper"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/pdapi"
	"net/url"
	"path"
	"sync"
)

// TiFlashPlacementManager manages placement settings for TiFlash
type TiFlashPlacementManager interface {
	// SetPlacementRule is a helper function to set placement rule.
	SetPlacementRule(ctx context.Context, rule placement.Rule) error
	// DeletePlacementRule is to delete placement rule for certain group.
	DeletePlacementRule(ctx context.Context, ruleID string) error
	// GetGroupRules to get all placement rule in a certain group.
	GetGroupRules(ctx context.Context, group string) ([]placement.Rule, error)
	// PostAccelerateSchedule sends `regions/accelerate-schedule` request.
	PostAccelerateSchedule(ctx context.Context, tableID int64) error
	// GetPDRegionRecordStats is a helper function calling `/stats/region`.
	GetPDRegionRecordStats(ctx context.Context, tableID int64, stats *helper.PDRegionStats) error
	// GetStoresStat gets the TiKV store information by accessing PD's api.
	GetStoresStat(ctx context.Context) (*helper.StoresStat, error)
}

// TiFlashPDPlacementManager manages placement with pd for TiFlash
type TiFlashPDPlacementManager struct {
	addrs []string
}

// SetPlacementRule is a helper function to set placement rule.
func (m *TiFlashPDPlacementManager) SetPlacementRule(ctx context.Context, rule placement.Rule) error {
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
func (m *TiFlashPDPlacementManager) GetGroupRules(ctx context.Context, group string) ([]placement.Rule, error) {
	res, err := doRequest(ctx, m.addrs, path.Join(pdapi.Config, "rules", "group", group), "GET", nil)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if res == nil {
		return nil, fmt.Errorf("TiFlashPDPlacementManager returns error in GetGroupRules")
	}

	var rules []placement.Rule
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
		"start_key": url.QueryEscape(string(startKey)),
		"end_key":   url.QueryEscape(string(endKey)),
	}
	j, err := json.Marshal(input)
	buf := bytes.NewBuffer(j)

	res, err := doRequest(ctx, m.addrs, path.Join(pdapi.Config, "regions", "accelerate-schedule"), "POST", buf)
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

// PlacementManager manages placement settings
type PlacementManager interface {
	// GetRuleBundle is used to get one specific rule bundle from PD.
	GetRuleBundle(ctx context.Context, name string) (*placement.Bundle, error)
	// GetAllRuleBundles is used to get all rule bundles from PD. It is used to load full rules from PD while fullload infoschema.
	GetAllRuleBundles(ctx context.Context) ([]*placement.Bundle, error)
	// PutRuleBundles is used to post specific rule bundles to PD.
	PutRuleBundles(ctx context.Context, bundles []*placement.Bundle) error
}

// PDPlacementManager manages placement with pd
type PDPlacementManager struct {
	addrs []string
}

// GetRuleBundle is used to get one specific rule bundle from PD.
func (m *PDPlacementManager) GetRuleBundle(ctx context.Context, name string) (*placement.Bundle, error) {
	bundle := &placement.Bundle{ID: name}
	res, err := doRequest(ctx, m.addrs, path.Join(pdapi.Config, "placement-rule", name), "GET", nil)
	if err == nil && res != nil {
		err = json.Unmarshal(res, bundle)
	}
	return bundle, err
}

// GetAllRuleBundles is used to get all rule bundles from PD. It is used to load full rules from PD while fullload infoschema.
func (m *PDPlacementManager) GetAllRuleBundles(ctx context.Context) ([]*placement.Bundle, error) {
	var bundles []*placement.Bundle
	res, err := doRequest(ctx, m.addrs, path.Join(pdapi.Config, "placement-rule"), "GET", nil)
	if err == nil && res != nil {
		err = json.Unmarshal(res, &bundles)
	}
	return bundles, err
}

// PutRuleBundles is used to post specific rule bundles to PD.
func (m *PDPlacementManager) PutRuleBundles(ctx context.Context, bundles []*placement.Bundle) error {
	if len(bundles) == 0 {
		return nil
	}

	b, err := json.Marshal(bundles)
	if err != nil {
		return err
	}

	_, err = doRequest(ctx, m.addrs, path.Join(pdapi.Config, "placement-rule")+"?partial=true", "POST", bytes.NewReader(b))
	return err
}

type mockPlacementManager struct {
	sync.Mutex
	bundles map[string]*placement.Bundle
}

func (m *mockPlacementManager) GetRuleBundle(_ context.Context, name string) (*placement.Bundle, error) {
	m.Lock()
	defer m.Unlock()

	if bundle, ok := m.bundles[name]; ok {
		return bundle, nil
	}

	return &placement.Bundle{ID: name}, nil
}

func (m *mockPlacementManager) GetAllRuleBundles(_ context.Context) ([]*placement.Bundle, error) {
	m.Lock()
	defer m.Unlock()

	bundles := make([]*placement.Bundle, 0, len(m.bundles))
	for _, bundle := range m.bundles {
		bundles = append(bundles, bundle)
	}
	return bundles, nil
}

func (m *mockPlacementManager) PutRuleBundles(_ context.Context, bundles []*placement.Bundle) error {
	m.Lock()
	defer m.Unlock()

	if m.bundles == nil {
		m.bundles = make(map[string]*placement.Bundle)
	}

	for _, bundle := range bundles {
		if bundle.IsEmpty() {
			delete(m.bundles, bundle.ID)
		} else {
			m.bundles[bundle.ID] = bundle
		}
	}

	return nil
}
