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

package label

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"

	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/util/codec"
	"gopkg.in/yaml.v2"
)

const (
	// IDPrefix is the prefix for label rule ID.
	IDPrefix = "schema"
	ruleType = "key-range"
)

const (
	// RuleIndexDefault is the default index for a rule.
	RuleIndexDefault int = iota
	// RuleIndexDatabase is the index for a rule of database.
	RuleIndexDatabase
	// RuleIndexTable is the index for a rule of table.
	RuleIndexTable
	// RuleIndexPartition is the index for a rule of partition.
	RuleIndexPartition
)

var (
	// TableIDFormat is the format of the label rule ID for a table.
	// The format follows "schema/database_name/table_name".
	TableIDFormat = "%s/%s/%s"
	// PartitionIDFormat is the format of the label rule ID for a partition.
	// The format follows "schema/database_name/table_name/partition_name".
	PartitionIDFormat = "%s/%s/%s/%s"
)

// Rule is used to establish the relationship between labels and a key range.
type Rule struct {
	ID       string        `json:"id"`
	Index    int           `json:"index"`
	Labels   Labels        `json:"labels"`
	RuleType string        `json:"rule_type"`
	Data     []interface{} `json:"data"`
}

// NewRule creates a rule.
func NewRule() *Rule {
	return &Rule{}
}

// ApplyAttributesSpec will transfer attributes defined in AttributesSpec to the labels.
func (r *Rule) ApplyAttributesSpec(spec *ast.AttributesSpec) error {
	if spec.Default {
		r.Labels = []Label{}
		return nil
	}
	// construct a string list
	attrBytes := []byte("[" + spec.Attributes + "]")
	attributes := []string{}
	err := yaml.UnmarshalStrict(attrBytes, &attributes)
	if err != nil {
		return err
	}
	r.Labels, err = NewLabels(attributes)
	return err
}

// String implements fmt.Stringer.
func (r *Rule) String() string {
	t, err := json.Marshal(r)
	if err != nil {
		return ""
	}
	return string(t)
}

// Clone clones a rule.
func (r *Rule) Clone() *Rule {
	newRule := NewRule()
	*newRule = *r
	return newRule
}

// Reset will reset the label rule for a table/partition with a given ID and names.
func (r *Rule) Reset(dbName, tableName, partName string, ids ...int64) *Rule {
	isPartition := partName != ""
	if isPartition {
		r.ID = fmt.Sprintf(PartitionIDFormat, IDPrefix, dbName, tableName, partName)
	} else {
		r.ID = fmt.Sprintf(TableIDFormat, IDPrefix, dbName, tableName)
	}
	if len(r.Labels) == 0 {
		return r
	}
	var hasDBKey, hasTableKey, hasPartitionKey bool
	for i := range r.Labels {
		switch r.Labels[i].Key {
		case dbKey:
			r.Labels[i].Value = dbName
			hasDBKey = true
		case tableKey:
			r.Labels[i].Value = tableName
			hasTableKey = true
		case partitionKey:
			if isPartition {
				r.Labels[i].Value = partName
				hasPartitionKey = true
			}
		default:
		}
	}

	if !hasDBKey {
		r.Labels = append(r.Labels, Label{Key: dbKey, Value: dbName})
	}

	if !hasTableKey {
		r.Labels = append(r.Labels, Label{Key: tableKey, Value: tableName})
	}

	if isPartition && !hasPartitionKey {
		r.Labels = append(r.Labels, Label{Key: partitionKey, Value: partName})
	}
	r.RuleType = ruleType
	r.Data = []interface{}{}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	for i := 0; i < len(ids); i++ {
		data := map[string]string{
			"start_key": hex.EncodeToString(codec.EncodeBytes(nil, tablecodec.GenTablePrefix(ids[i]))),
			"end_key":   hex.EncodeToString(codec.EncodeBytes(nil, tablecodec.GenTablePrefix(ids[i]+1))),
		}
		r.Data = append(r.Data, data)
	}
	// We may support more types later.
	r.Index = RuleIndexTable
	if isPartition {
		r.Index = RuleIndexPartition
	}
	return r
}

// RulePatch is the patch to update the label rules.
type RulePatch struct {
	SetRules    []*Rule  `json:"sets"`
	DeleteRules []string `json:"deletes"`
}

// NewRulePatch returns a patch of rules which need to be set or deleted.
func NewRulePatch(setRules []*Rule, deleteRules []string) *RulePatch {
	return &RulePatch{
		SetRules:    setRules,
		DeleteRules: deleteRules,
	}
}
