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
	DeletePlacementRule(ctx context.Context, group string, ruleID string) error
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
	TiFlashPDPlacementManager
	sync.Mutex
}

func makeBaseRule() placement.Rule {
	return placement.Rule{
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
func MakeNewRule(ID int64, Count uint64, LocationLabels []string) *placement.Rule {
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
