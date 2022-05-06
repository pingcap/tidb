// Copyright 2022 PingCAP, Inc.
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

package column

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRule(t *testing.T) {
	// test invalid rules
	inValidRule := &Rule{"test*", "abc*", "id", "id", "Error", nil, "xxx"}
	require.NotNil(t, inValidRule.Valid())

	inValidRule.TargetColumn = ""
	require.NotNil(t, inValidRule.Valid())

	inValidRule.Expression = AddPrefix
	inValidRule.TargetColumn = "id"
	require.NotNil(t, inValidRule.Valid())

	inValidRule.Arguments = []string{"1"}
	require.Nil(t, inValidRule.Valid())

	inValidRule.Expression = PartitionID
	require.NotNil(t, inValidRule.Valid())

	inValidRule.Arguments = []string{"1", "test_", "t_"}
	require.Nil(t, inValidRule.Valid())
}

func TestHandle(t *testing.T) {
	rules := []*Rule{
		{"Test*", "xxx*", "", "id", AddPrefix, []string{"instance_id:"}, "xx"},
	}

	// initial column mapping
	m, err := NewMapping(false, rules)
	require.NoError(t, err)
	require.Len(t, m.cache.infos, 0)

	// test add prefix, add suffix is similar
	vals, poss, err := m.HandleRowValue("test", "xxx", []string{"age", "id"}, []interface{}{1, "1"})
	require.NoError(t, err)
	require.Equal(t, []interface{}{1, "instance_id:1"}, vals)
	require.Equal(t, []int{-1, 1}, poss)

	// test cache
	vals, poss, err = m.HandleRowValue("test", "xxx", []string{"name"}, []interface{}{1, "1"})
	require.NoError(t, err)
	require.Equal(t, []interface{}{1, "instance_id:1"}, vals)
	require.Equal(t, []int{-1, 1}, poss)

	// test resetCache
	m.resetCache()
	_, _, err = m.HandleRowValue("test", "xxx", []string{"name"}, []interface{}{"1"})
	require.Error(t, err)

	// test DDL
	_, _, err = m.HandleDDL("test", "xxx", []string{"id", "age"}, "create table xxx")
	require.Error(t, err)

	statement, poss, err := m.HandleDDL("abc", "xxx", []string{"id", "age"}, "create table xxx")
	require.NoError(t, err)
	require.Equal(t, "create table xxx", statement)
	require.Nil(t, poss)
}

func TestQueryColumnInfo(t *testing.T) {
	SetPartitionRule(4, 7, 8)
	rules := []*Rule{
		{"test*", "xxx*", "", "id", PartitionID, []string{"8", "test_", "xxx_"}, "xx"},
	}

	// initial column mapping
	m, err := NewMapping(false, rules)
	require.NoError(t, err)

	// test mismatch
	info, err := m.queryColumnInfo("test_2", "t_1", []string{"id", "name"})
	require.NoError(t, err)
	require.True(t, info.ignore)

	// test matched
	info, err = m.queryColumnInfo("test_2", "xxx_1", []string{"id", "name"})
	require.NoError(t, err)
	require.EqualValues(t, &mappingInfo{
		sourcePosition: -1,
		targetPosition: 0,
		rule:           rules[0],
		instanceID:     int64(8 << 59),
		schemaID:       int64(2 << 52),
		tableID:        int64(1 << 44),
	}, info)

	m.resetCache()
	SetPartitionRule(0, 0, 3)
	info, err = m.queryColumnInfo("test_2", "xxx_1", []string{"id", "name"})
	require.NoError(t, err)
	require.EqualValues(t, &mappingInfo{
		sourcePosition: -1,
		targetPosition: 0,
		rule:           rules[0],
		instanceID:     int64(0),
		schemaID:       int64(0),
		tableID:        int64(1 << 60),
	}, info)
}

func TestSetPartitionRule(t *testing.T) {
	SetPartitionRule(4, 7, 8)
	require.Equal(t, 4, instanceIDBitSize)
	require.Equal(t, 7, schemaIDBitSize)
	require.Equal(t, 8, tableIDBitSize)
	require.Equal(t, int64(1<<44), maxOriginID)

	SetPartitionRule(0, 3, 4)
	require.Equal(t, 0, instanceIDBitSize)
	require.Equal(t, 3, schemaIDBitSize)
	require.Equal(t, 4, tableIDBitSize)
	require.Equal(t, int64(1<<56), maxOriginID)
}

func TestComputePartitionID(t *testing.T) {
	SetPartitionRule(4, 7, 8)

	rule := &Rule{
		Arguments: []string{"test", "t"},
	}
	_, _, _, err := computePartitionID("test_1", "t_1", rule)
	require.Error(t, err)
	_, _, _, err = computePartitionID("test", "t", rule)
	require.Error(t, err)

	rule = &Rule{
		Arguments: []string{"2", "test", "t", "_"},
	}
	instanceID, schemaID, tableID, err := computePartitionID("test_1", "t_1", rule)
	require.NoError(t, err)
	require.Equal(t, int64(2<<59), instanceID)
	require.Equal(t, int64(1<<52), schemaID)
	require.Equal(t, int64(1<<44), tableID)

	// test default partition ID to zero
	instanceID, schemaID, tableID, err = computePartitionID("test", "t_3", rule)
	require.NoError(t, err)
	require.Equal(t, int64(2<<59), instanceID)
	require.Equal(t, int64(0), schemaID)
	require.Equal(t, int64(3<<44), tableID)

	instanceID, schemaID, tableID, err = computePartitionID("test_5", "t", rule)
	require.NoError(t, err)
	require.Equal(t, int64(2<<59), instanceID)
	require.Equal(t, int64(5<<52), schemaID)
	require.Equal(t, int64(0), tableID)

	_, _, _, err = computePartitionID("unrelated", "t_6", rule)
	require.Regexp(t, "test_ is not the prefix of unrelated.*", err)

	_, _, _, err = computePartitionID("test", "x", rule)
	require.Regexp(t, "t_ is not the prefix of x.*", err)

	_, _, _, err = computePartitionID("test_0", "t_0xa", rule)
	require.Regexp(t, "the suffix of 0xa can't be converted to int64.*", err)

	_, _, _, err = computePartitionID("test_0", "t_", rule)
	require.Regexp(t, "t_ is not the prefix of t_.*", err) // needs a better error messag

	_, _, _, err = computePartitionID("testx", "t_3", rule)
	require.Regexp(t, "test_ is not the prefix of testx.*", err)

	SetPartitionRule(4, 0, 8)
	rule = &Rule{
		Arguments: []string{"2", "test_", "t_", ""},
	}
	instanceID, schemaID, tableID, err = computePartitionID("test_1", "t_1", rule)
	require.NoError(t, err)
	require.Equal(t, int64(2<<59), instanceID)
	require.Equal(t, int64(0), schemaID)
	require.Equal(t, int64(1<<51), tableID)

	instanceID, schemaID, tableID, err = computePartitionID("test_", "t_", rule)
	require.NoError(t, err)
	require.Equal(t, int64(2<<59), instanceID)
	require.Equal(t, int64(0), schemaID)
	require.Equal(t, int64(0), tableID)

	// test ignore instance ID
	SetPartitionRule(4, 7, 8)
	rule = &Rule{
		Arguments: []string{"", "test_", "t_", ""},
	}
	instanceID, schemaID, tableID, err = computePartitionID("test_1", "t_1", rule)
	require.NoError(t, err)
	require.Equal(t, int64(0), instanceID)
	require.Equal(t, int64(1<<56), schemaID)
	require.Equal(t, int64(1<<48), tableID)

	// test ignore schema ID
	rule = &Rule{
		Arguments: []string{"2", "", "t_", ""},
	}
	instanceID, schemaID, tableID, err = computePartitionID("test_1", "t_1", rule)
	require.NoError(t, err)
	require.Equal(t, int64(2<<59), instanceID)
	require.Equal(t, int64(0), schemaID)
	require.Equal(t, int64(1<<51), tableID)

	// test ignore schema ID
	rule = &Rule{
		Arguments: []string{"2", "test_", "", ""},
	}
	instanceID, schemaID, tableID, err = computePartitionID("test_1", "t_1", rule)
	require.NoError(t, err)
	require.Equal(t, int64(2<<59), instanceID)
	require.Equal(t, int64(1<<52), schemaID)
	require.Equal(t, int64(0), tableID)
}

func TestPartitionID(t *testing.T) {
	SetPartitionRule(4, 7, 8)
	info := &mappingInfo{
		instanceID:     int64(2 << 59),
		schemaID:       int64(1 << 52),
		tableID:        int64(1 << 44),
		targetPosition: 1,
	}

	// test wrong type
	_, err := partitionID(info, []interface{}{1, "ha"})
	require.Error(t, err)

	// test exceed maxOriginID
	_, err = partitionID(info, []interface{}{"ha", 1 << 44})
	require.Error(t, err)

	vals, err := partitionID(info, []interface{}{"ha", 1})
	require.NoError(t, err)
	require.Equal(t, []interface{}{"ha", int64(2<<59 | 1<<52 | 1<<44 | 1)}, vals)

	info.instanceID = 0
	vals, err = partitionID(info, []interface{}{"ha", "123"})
	require.NoError(t, err)
	require.Equal(t, []interface{}{"ha", fmt.Sprintf("%d", int64(1<<52|1<<44|123))}, vals)
}

func TestCaseSensitive(t *testing.T) {
	// we test case insensitive in TestHandle
	rules := []*Rule{
		{"Test*", "xxx*", "", "id", AddPrefix, []string{"instance_id:"}, "xx"},
	}

	// case sensitive
	// initial column mapping
	m, err := NewMapping(true, rules)
	require.NoError(t, err)
	require.Len(t, m.cache.infos, 0)

	// test add prefix, add suffix is similar
	vals, poss, err := m.HandleRowValue("test", "xxx", []string{"age", "id"}, []interface{}{1, "1"})
	require.NoError(t, err)
	require.Equal(t, []interface{}{1, "1"}, vals)
	require.Nil(t, poss)
}
