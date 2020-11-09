// Copyright 2020 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package placement

import (
	"context"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/util/codec"
	pd "github.com/tikv/pd/client"
)

func checkLabelConstraint(label string) (LabelConstraint, error) {
	r := LabelConstraint{}

	if len(label) < 4 {
		return r, errors.Errorf("label constraint should be in format '{+|-}key=value', but got '%s'", label)
	}

	var op LabelConstraintOp
	switch label[0] {
	case '+':
		op = In
	case '-':
		op = NotIn
	default:
		return r, errors.Errorf("label constraint should be in format '{+|-}key=value', but got '%s'", label)
	}

	kv := strings.Split(label[1:], "=")
	if len(kv) != 2 {
		return r, errors.Errorf("label constraint should be in format '{+|-}key=value', but got '%s'", label)
	}

	key := strings.TrimSpace(kv[0])
	if key == "" {
		return r, errors.Errorf("label constraint should be in format '{+|-}key=value', but got '%s'", label)
	}

	val := strings.TrimSpace(kv[1])
	if val == "" {
		return r, errors.Errorf("label constraint should be in format '{+|-}key=value', but got '%s'", label)
	}

	r.Key = key
	r.Op = op
	r.Values = []string{val}
	return r, nil
}

// CheckLabelConstraints will check labels, and build LabelConstraints for rule.
func CheckLabelConstraints(labels []string) ([]LabelConstraint, error) {
	constraints := make([]LabelConstraint, 0, len(labels))
	for _, str := range labels {
		label, err := checkLabelConstraint(strings.TrimSpace(str))
		if err != nil {
			return constraints, err
		}
		constraints = append(constraints, label)
	}
	return constraints, nil
}

// GroupID accepts a tableID or whatever integer, and encode the integer into a valid GroupID for PD.
func GroupID(id int64) string {
	return fmt.Sprintf("%s%d", BundleIDPrefix, id)
}

// ObjectIDFromGroupID extracts the db/table/partition ID from the group ID
func ObjectIDFromGroupID(groupID string) (int64, error) {
	// If the rule doesn't come from TiDB, skip it.
	if !strings.HasPrefix(groupID, BundleIDPrefix) {
		return 0, nil
	}
	id, err := strconv.ParseInt(groupID[len(BundleIDPrefix):], 10, 64)
	if err != nil || id <= 0 {
		return 0, errors.Errorf("Rule %s doesn't include an id", groupID)
	}
	return id, nil
}

// RestoreLabelConstraintList converts the label constraints to a readable string.
func RestoreLabelConstraintList(constraints []LabelConstraint) (string, error) {
	var sb strings.Builder
	for i, constraint := range constraints {
		sb.WriteByte('"')
		conStr, err := constraint.Restore()
		if err != nil {
			return "", err
		}
		sb.WriteString(conStr)
		sb.WriteByte('"')
		if i < len(constraints)-1 {
			sb.WriteByte(',')
		}
	}
	return sb.String(), nil
}

// BuildPlacementDropBundle builds the bundle to drop placement rules.
func BuildPlacementDropBundle(partitionID int64) *Bundle {
	return &Bundle{
		ID: GroupID(partitionID),
	}
}

// BuildPlacementCopyBundle copies a new bundle from the old, with a new name and a new key range.
func BuildPlacementCopyBundle(oldBundle *Bundle, newID int64) *Bundle {
	newBundle := oldBundle.Clone()
	newBundle.ID = GroupID(newID)
	startKey := hex.EncodeToString(codec.EncodeBytes(nil, tablecodec.GenTablePrefix(newID)))
	endKey := hex.EncodeToString(codec.EncodeBytes(nil, tablecodec.GenTablePrefix(newID+1)))
	for _, rule := range newBundle.Rules {
		rule.GroupID = newBundle.ID
		rule.StartKeyHex = startKey
		rule.EndKeyHex = endKey
	}
	return newBundle
}

func GetLeaderDCLocationByBundle(ctx context.Context, bundle *Bundle, pdClient pd.Client, dcLabelKey string) (map[string]struct{}, error) {
	bundleDCDistributions := make(map[string]struct{}, 1)
	for _, rule := range bundle.Rules {
		if rule.Role != Leader {
			continue
		}
		//TODO: we can execute these in parallel
		ruleDCDistributions, err := getLeaderDCLocationByRule(ctx, rule, pdClient, dcLabelKey)
		if err != nil {
			return nil, err
		}
		for dc := range ruleDCDistributions {
			bundleDCDistributions[dc] = struct{}{}
		}
	}
	return bundleDCDistributions, nil
}

func getLeaderDCLocationByRule(parCtx context.Context, rule *Rule, pdClient pd.Client, dcLabelKey string) (map[string]struct{}, error) {
	getLabelValueByKey := func(labels []*metapb.StoreLabel, key string) string {
		for _, label := range labels {
			if label.Key == key {
				return label.Value
			}
		}
		return ""
	}
	dcLocations := make(map[string]struct{}, 1)
	startKey, err := hex.DecodeString(rule.StartKeyHex)
	if err != nil {
		return nil, fmt.Errorf("failed to decode rule[%v,%v]'s startkey, err: %v", rule.GroupID, rule.ID, err)
	}
	endKey, err := hex.DecodeString(rule.EndKeyHex)
	if err != nil {
		return nil, fmt.Errorf("failed to decode rule[%v,%v]'s endKey, err: %v", rule.GroupID, rule.ID, err)
	}
	ctx, cancel := context.WithCancel(parCtx)
	defer cancel()
	regions, err := pdClient.ScanRegions(ctx, startKey, endKey, -1)
	if err != nil {
		return nil, fmt.Errorf("failed to scan regions, err: %v", err)
	}
	for _, region := range regions {
		if region.Leader == nil {
			return nil, fmt.Errorf("region %v have no leader", region.Meta.GetId())
		}
		// TODO: we can cache the storeInfo
		storeInfo, err := pdClient.GetStore(ctx, region.Leader.StoreId)
		if err != nil {
			return nil, fmt.Errorf("failed to get store %v information, err: %v", region.Leader.StoreId, err)
		}
		value := getLabelValueByKey(storeInfo.GetLabels(), dcLabelKey)
		if len(value) < 1 {
			return nil, fmt.Errorf("no dcLabel")
		}
		dcLocations[value] = struct{}{}
	}
	return dcLocations, nil
}
