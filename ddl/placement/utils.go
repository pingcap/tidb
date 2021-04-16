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

// CheckLabelConstraints will check labels, and build Constraints for rule.
func CheckLabelConstraints(labels []string) ([]Constraint, error) {
	constraints := make([]Constraint, 0, len(labels))
	for _, str := range labels {
		label, err := NewConstraint(strings.TrimSpace(str))
		if err != nil {
			return constraints, err
		}

		pass := true

		for _, cnst := range constraints {
			if label.Key == cnst.Key {
				sameOp := label.Op == cnst.Op
				sameVal := label.Values[0] == cnst.Values[0]
				// no following cases:
				// 1. duplicated constraint
				// 2. no instance can meet: +dc=sh, -dc=sh
				// 3. can not match multiple instances: +dc=sh, +dc=bj
				if sameOp && sameVal {
					pass = false
					break
				} else if (!sameOp && sameVal) || (sameOp && !sameVal && label.Op == In) {
					s1, err := label.Restore()
					if err != nil {
						s1 = err.Error()
					}
					s2, err := cnst.Restore()
					if err != nil {
						s2 = err.Error()
					}
					return constraints, errors.Errorf("conflicting constraints '%s' and '%s'", s1, s2)
				}
			}
		}

		if pass {
			constraints = append(constraints, label)
		}
	}
	return constraints, nil
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
func RestoreLabelConstraintList(constraints []Constraint) (string, error) {
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
			return rule.LabelConstraints[0].Values[0], true
		}
	}
	return "", false
}

func isValidLeaderRule(rule *Rule, dcLabelKey string) bool {
	if rule.Role == Leader && rule.Count == 1 {
		for _, con := range rule.LabelConstraints {
			if con.Op == In && con.Key == dcLabelKey && len(con.Values) == 1 {
				return true
			}
		}
	}
	return false
}
