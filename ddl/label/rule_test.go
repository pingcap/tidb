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
	"testing"

	"github.com/pingcap/tidb/parser/ast"
	"github.com/stretchr/testify/require"
)

func TestApplyAttributesSpec(t *testing.T) {
	// valid case
	spec := &ast.AttributesSpec{Attributes: "key=value,key1=value1"}
	rule := NewRule()
	err := rule.ApplyAttributesSpec(spec)
	require.NoError(t, err)
	require.Len(t, rule.Labels, 2)
	require.Equal(t, "key", rule.Labels[0].Key)
	require.Equal(t, "value", rule.Labels[0].Value)
	require.Equal(t, "key1", rule.Labels[1].Key)
	require.Equal(t, "value1", rule.Labels[1].Value)

	// invalid cases
	testcases := []string{
		"key=value,,key1=value1",
		"key-value,key1=value1",
		"key=,key1=value1",
		"=value,key1=value1",
	}

	for i := range testcases {
		spec = &ast.AttributesSpec{Attributes: testcases[i]}
		err = rule.ApplyAttributesSpec(spec)
		require.Error(t, err)
	}
}

func TestDefaultOrEmpty(t *testing.T) {
	specs := []*ast.AttributesSpec{{Attributes: ""}, {Default: true}}
	for i := range specs {
		rule := NewRule()
		err := rule.ApplyAttributesSpec(specs[i])
		require.NoError(t, err)

		rule.Reset("db", "t", "", 1)
		require.Len(t, rule.Labels, 0)
	}
}

func TestReset(t *testing.T) {
	spec := &ast.AttributesSpec{Attributes: "key=value"}
	rule := NewRule()
	require.NoError(t, rule.ApplyAttributesSpec(spec))

	rule.Reset("db1", "t1", "", 1, 2, 3)
	require.Equal(t, "schema/db1/t1", rule.ID)
	require.Equal(t, ruleType, rule.RuleType)
	require.Len(t, rule.Labels, 3)
	require.Equal(t, "value", rule.Labels[0].Value)
	require.Equal(t, "db1", rule.Labels[1].Value)
	require.Equal(t, "t1", rule.Labels[2].Value)
	require.Equal(t, rule.Index, 2)

	r := rule.Data[0].(map[string]string)
	require.Equal(t, "7480000000000000ff0100000000000000f8", r["start_key"])
	require.Equal(t, "7480000000000000ff0200000000000000f8", r["end_key"])
	r = rule.Data[1].(map[string]string)
	require.Equal(t, "7480000000000000ff0200000000000000f8", r["start_key"])
	require.Equal(t, "7480000000000000ff0300000000000000f8", r["end_key"])
	r = rule.Data[2].(map[string]string)
	require.Equal(t, "7480000000000000ff0300000000000000f8", r["start_key"])
	require.Equal(t, "7480000000000000ff0400000000000000f8", r["end_key"])

	r1 := rule.Clone()
	require.Equal(t, r1, rule)

	r2 := rule.Reset("db2", "t2", "p2", 2)
	require.Equal(t, "schema/db2/t2/p2", r2.ID)
	require.Len(t, rule.Labels, 4)
	require.Equal(t, "value", rule.Labels[0].Value)
	require.Equal(t, "db2", rule.Labels[1].Value)
	require.Equal(t, "t2", rule.Labels[2].Value)
	require.Equal(t, "p2", rule.Labels[3].Value)
	require.Equal(t, rule.Index, 3)

	r = r2.Data[0].(map[string]string)
	require.Equal(t, "7480000000000000ff0200000000000000f8", r["start_key"])
	require.Equal(t, "7480000000000000ff0300000000000000f8", r["end_key"])

	// default case
	spec = &ast.AttributesSpec{Default: true}
	rule, expected := NewRule(), NewRule()
	expected.ID, expected.Labels = "schema/db3/t3/p3", []Label{}
	require.NoError(t, rule.ApplyAttributesSpec(spec))
	r3 := rule.Reset("db3", "t3", "p3", 3)
	require.Equal(t, r3, expected)
}
