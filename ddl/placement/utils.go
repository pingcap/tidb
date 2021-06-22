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
	"encoding/hex"
	"strconv"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/util/codec"
)

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
	startKey := hex.EncodeToString(codec.EncodeBytes(nil, tablecodec.GenTableRecordPrefix(newID)))
	endKey := hex.EncodeToString(codec.EncodeBytes(nil, tablecodec.GenTableRecordPrefix(newID+1)))
	for _, rule := range newBundle.Rules {
		rule.GroupID = newBundle.ID
		rule.StartKeyHex = startKey
		rule.EndKeyHex = endKey
	}
	return newBundle
}

// GetLeaderDCByBundle returns the leader's DC by Bundle if found
func GetLeaderDCByBundle(bundle *Bundle, dcLabelKey string) (string, bool) {
	for _, rule := range bundle.Rules {
		if isValidLeaderRule(rule, dcLabelKey) {
			return rule.Constraints[0].Values[0], true
		}
	}
	return "", false
}

func isValidLeaderRule(rule *Rule, dcLabelKey string) bool {
	if rule.Role == Leader && rule.Count == 1 {
		for _, con := range rule.Constraints {
			if con.Op == In && con.Key == dcLabelKey && len(con.Values) == 1 {
				return true
			}
		}
	}
	return false
}
