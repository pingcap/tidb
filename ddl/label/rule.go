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
// See the License for the specific language governing permissions and
// limitations under the License.

package label

import (
	"encoding/hex"
	"encoding/json"
	"fmt"

	"github.com/pingcap/parser/ast"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/util/codec"
	"gopkg.in/yaml.v2"
)

const (
	// IDPrefix is the prefix for label rule ID.
	IDPrefix = "schema"

	ruleType = "key-range"
)

var (
	// TableIDFormat is the format of the label rule ID for a table.
	// The format follows "schema/database_name/table_name".
	TableIDFormat = "%s/%s/%s"
)

// Rule is used to establish the relationship between labels and a key range.
type Rule struct {
	ID       string      `json:"id"`
	Labels   Labels      `json:"labels"`
	RuleType string      `json:"rule_type"`
	Rule     interface{} `json:"rule"`
}

// NewRule creates a rule.
func NewRule() *Rule {
	return &Rule{}
}

// ApplyAttributesSpec will transfer attributes defined in AttributesSpec to the labels.
func (r *Rule) ApplyAttributesSpec(spec *ast.AttributesSpec) error {
	// construct a string list
	attrBytes := []byte("[" + spec.Attributes + "]")
	attributes := []string{}
	err := yaml.UnmarshalStrict(attrBytes, &attributes)
	if err != nil {
		return err
	}
	r.Labels = NewLabels(attributes)
	return nil
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

// ResetTable will reset the label rule for a table with a given ID and names.
func (r *Rule) ResetTable(id int64, dbName, tableName string) *Rule {
	r.ID = fmt.Sprintf(TableIDFormat, IDPrefix, dbName, tableName)
	r.Labels = append(r.Labels, []Label{
		{Key: dbKey, Value: dbName},
		{Key: tableKey, Value: tableName},
	}...)

	r.RuleType = ruleType
	r.Rule = map[string]string{
		"start_key": hex.EncodeToString(codec.EncodeBytes(nil, tablecodec.GenTableRecordPrefix(id))),
		"end_key":   hex.EncodeToString(codec.EncodeBytes(nil, tablecodec.GenTableRecordPrefix(id+1))),
	}
	return r
}
