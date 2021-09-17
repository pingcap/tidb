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

	"github.com/pingcap/parser/ast"
	"github.com/stretchr/testify/require"
)

func TestApplyAttributesSpec(t *testing.T) {
	t.Parallel()

	spec := &ast.AttributesSpec{Attributes: "attr1=true,attr2=false"}
	rule := NewRule()
	err := rule.ApplyAttributesSpec(spec)
	require.NoError(t, err)
	require.Len(t, rule.Labels, 2)
	require.Equal(t, "attr1", rule.Labels[0].Key)
	require.Equal(t, "true", rule.Labels[0].Value)
	require.Equal(t, "attr2", rule.Labels[1].Key)
	require.Equal(t, "false", rule.Labels[1].Value)
}

func TestDefaultOrEmpty(t *testing.T) {
	t.Parallel()

	spec := &ast.AttributesSpec{Attributes: ""}
	rule := NewRule()
	err := rule.ApplyAttributesSpec(spec)
	require.NoError(t, err)

	rule.Reset(1, "db", "t")
	require.Len(t, rule.Labels, 0)

	spec = &ast.AttributesSpec{Default: true}
	rule = NewRule()
	err = rule.ApplyAttributesSpec(spec)
	require.NoError(t, err)

	rule.Reset(1, "db", "t")
	require.Len(t, rule.Labels, 0)
}

func TestReset(t *testing.T) {
	t.Parallel()

	spec := &ast.AttributesSpec{Attributes: "attr=true"}
	rule := NewRule()
	require.NoError(t, rule.ApplyAttributesSpec(spec))

	rule.Reset(1, "db1", "t1")
	require.Equal(t, "schema/db1/t1", rule.ID)
	require.Equal(t, ruleType, rule.RuleType)
	require.Len(t, rule.Labels, 3)
	require.Equal(t, "true", rule.Labels[0].Value)
	require.Equal(t, "db1", rule.Labels[1].Value)
	require.Equal(t, "t1", rule.Labels[2].Value)

	r := rule.Rule.(map[string]string)
	require.Equal(t, "7480000000000000ff015f720000000000fa", r["start_key"])
	require.Equal(t, "7480000000000000ff025f720000000000fa", r["end_key"])

	r1 := rule.Clone()
	require.Equal(t, r1, rule)

	r2 := rule.Reset(2, "db2", "t2", "p2")
	require.Equal(t, "schema/db2/t2/p2", r2.ID)
	require.Len(t, rule.Labels, 4)
	require.Equal(t, "true", rule.Labels[0].Value)
	require.Equal(t, "db2", rule.Labels[1].Value)
	require.Equal(t, "t2", rule.Labels[2].Value)
	require.Equal(t, "p2", rule.Labels[3].Value)

	r = r2.Rule.(map[string]string)
	require.Equal(t, "7480000000000000ff025f720000000000fa", r["start_key"])
	require.Equal(t, "7480000000000000ff035f720000000000fa", r["end_key"])
}
