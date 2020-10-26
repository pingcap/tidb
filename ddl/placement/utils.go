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
	"fmt"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/util/codec"
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
	return fmt.Sprintf("TIDB_DDL_%d", id)
}

// BuildPlacementDropBundle builds the bundle to drop placement rules.
func BuildPlacementDropBundle(partitionID int64) *Bundle {
	return &Bundle{
		ID: GroupID(partitionID),
	}
}

// BuildPlacementTruncateBundle builds the bundle to copy placement rules from old id to new id.
func BuildPlacementTruncateBundle(oldBundle *Bundle, newID int64) *Bundle {
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
