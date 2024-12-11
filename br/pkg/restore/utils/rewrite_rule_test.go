// Copyright 2024 PingCAP, Inc.
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

package utils_test

import (
	"bytes"
	"testing"

	"github.com/pingcap/errors"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/kvproto/pkg/import_sstpb"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
	"github.com/pingcap/tidb/br/pkg/restore/utils"
	"github.com/pingcap/tidb/br/pkg/rtree"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/stretchr/testify/require"
)

func TestValidateFileRewriteRule(t *testing.T) {
	rules := &utils.RewriteRules{
		Data: []*import_sstpb.RewriteRule{{
			OldKeyPrefix: []byte(tablecodec.EncodeTablePrefix(1)),
			NewKeyPrefix: []byte(tablecodec.EncodeTablePrefix(2)),
		}},
	}

	// Empty start/end key is not allowed.
	err := utils.ValidateFileRewriteRule(
		&backuppb.File{
			Name:     "file_write.sst",
			StartKey: []byte(""),
			EndKey:   []byte(""),
		},
		0,
		rules,
	)
	require.Error(t, err)
	require.Regexp(t, ".*cannot find rewrite rule.*", err.Error())

	// Range is not overlap, no rule found.
	err = utils.ValidateFileRewriteRule(
		&backuppb.File{
			Name:     "file_write.sst",
			StartKey: tablecodec.EncodeTablePrefix(0),
			EndKey:   tablecodec.EncodeTablePrefix(1),
		},
		0,
		rules,
	)
	require.Error(t, err)
	require.Regexp(t, ".*cannot find rewrite rule.*", err.Error())

	// No rule for end key.
	err = utils.ValidateFileRewriteRule(
		&backuppb.File{
			Name:     "file_write.sst",
			StartKey: tablecodec.EncodeTablePrefix(1),
			EndKey:   tablecodec.EncodeTablePrefix(2),
		},
		0,
		rules,
	)
	require.Error(t, err)
	require.Regexp(t, ".*cannot find rewrite rule.*", err.Error())

	// Add a rule for end key.
	rules.Data = append(rules.Data, &import_sstpb.RewriteRule{
		OldKeyPrefix: tablecodec.EncodeTablePrefix(2),
		NewKeyPrefix: tablecodec.EncodeTablePrefix(3),
	})
	err = utils.ValidateFileRewriteRule(
		&backuppb.File{
			Name:     "file_write.sst",
			StartKey: tablecodec.EncodeTablePrefix(1),
			EndKey:   tablecodec.EncodeTablePrefix(2),
		},
		0,
		rules,
	)
	require.Error(t, err)
	require.Regexp(t, ".*rewrite rule mismatch.*", err.Error())

	// Add a bad rule for end key, after rewrite start key > end key.
	rules.Data = append(rules.Data[:1], &import_sstpb.RewriteRule{
		OldKeyPrefix: tablecodec.EncodeTablePrefix(2),
		NewKeyPrefix: tablecodec.EncodeTablePrefix(1),
	})
	err = utils.ValidateFileRewriteRule(
		&backuppb.File{
			Name:     "file_write.sst",
			StartKey: tablecodec.EncodeTablePrefix(1),
			EndKey:   tablecodec.EncodeTablePrefix(2),
		},
		0,
		rules,
	)
	require.Error(t, err)
	require.Regexp(t, ".*rewrite rule mismatch.*", err.Error())

	err = utils.ValidateFileRewriteRule(
		&backuppb.File{
			Name:     "file_write.sst",
			StartKey: tablecodec.EncodeTablePrefix(4),
			EndKey:   tablecodec.EncodeTablePrefix(6),
			TableMetas: []*backuppb.TableMeta{
				{PhysicalId: 4},
				{PhysicalId: 5},
				{PhysicalId: 6},
			},
		},
		2,
		nil,
	)
	require.Error(t, err)
	require.Regexp(t, "table is not in file range.*", err.Error())

	err = utils.ValidateFileRewriteRule(
		&backuppb.File{
			Name:     "file_write.sst",
			StartKey: tablecodec.EncodeTablePrefix(4),
			EndKey:   tablecodec.EncodeTablePrefix(6),
			TableMetas: []*backuppb.TableMeta{
				{PhysicalId: 4},
				{PhysicalId: 5},
				{PhysicalId: 6},
			},
		},
		7,
		nil,
	)
	require.Error(t, err)
	require.Regexp(t, "table is not in file range.*", err.Error())

	err = utils.ValidateFileRewriteRule(
		&backuppb.File{
			Name:     "file_write.sst",
			StartKey: tablecodec.EncodeTablePrefix(4),
			EndKey:   tablecodec.EncodeTablePrefix(6),
			TableMetas: []*backuppb.TableMeta{
				{PhysicalId: 4},
				{PhysicalId: 5},
				{PhysicalId: 6},
			},
		},
		4,
		nil,
	)
	require.NoError(t, err)
}

func tableRewriteRule(tableID int64) *utils.RewriteRules {
	return &utils.RewriteRules{
		Data: []*import_sstpb.RewriteRule{{
			OldKeyPrefix: []byte(tablecodec.EncodeTablePrefix(tableID)),
			NewKeyPrefix: []byte(tablecodec.EncodeTablePrefix(1000 + tableID)),
		}},
	}
}

func rewriteRule(oldKey, newKey []byte) *import_sstpb.RewriteRule {
	return &import_sstpb.RewriteRule{
		OldKeyPrefix: oldKey,
		NewKeyPrefix: newKey,
	}
}

func TestUniqueSortedRewriteRules(t *testing.T) {
	newRewriteRules := utils.UniqueSortedRewriteRules([]*import_sstpb.RewriteRule{
		rewriteRule([]byte("1"), []byte("10")),
		rewriteRule([]byte("1"), []byte("10")),
		rewriteRule([]byte("2"), []byte("20")),
		rewriteRule([]byte("3"), []byte("30")),
		rewriteRule([]byte("3"), []byte("30")),
		rewriteRule([]byte("4"), []byte("40")),
		rewriteRule([]byte("5"), []byte("50")),
		rewriteRule([]byte("5"), []byte("50")),
	})
	require.Len(t, newRewriteRules, 5)
	require.Equal(t, []byte("1"), newRewriteRules[0].OldKeyPrefix)
	require.Equal(t, []byte("2"), newRewriteRules[1].OldKeyPrefix)
	require.Equal(t, []byte("3"), newRewriteRules[2].OldKeyPrefix)
	require.Equal(t, []byte("4"), newRewriteRules[3].OldKeyPrefix)
	require.Equal(t, []byte("5"), newRewriteRules[4].OldKeyPrefix)
}

func TestGetRewriteRawKey(t *testing.T) {
	rewriteRules := utils.RewriteRules{
		Data: []*import_sstpb.RewriteRule{
			{
				NewKeyPrefix: tablecodec.GenTablePrefix(2),
				OldKeyPrefix: tablecodec.GenTablePrefix(1),
			},
			{
				NewKeyPrefix: tablecodec.GenTablePrefix(511),
				OldKeyPrefix: tablecodec.GenTablePrefix(767),
			},
		},
	}
	rewritedKey, err := utils.GetRewriteRawKey(tablecodec.GenTableRecordPrefix(1), &rewriteRules)
	require.NoError(t, err)
	require.Equal(t, codec.EncodeBytes([]byte{}, tablecodec.GenTableRecordPrefix(2)), rewritedKey)
}

func TestRewriteFileKeys(t *testing.T) {
	rewriteRules := utils.RewriteRules{
		Data: []*import_sstpb.RewriteRule{
			{
				NewKeyPrefix: tablecodec.GenTablePrefix(2),
				OldKeyPrefix: tablecodec.GenTablePrefix(1),
			},
			{
				NewKeyPrefix: tablecodec.GenTablePrefix(511),
				OldKeyPrefix: tablecodec.GenTablePrefix(767),
			},
		},
	}
	rawKeyFile := backuppb.File{
		Name:     "backup.sst",
		StartKey: tablecodec.GenTableRecordPrefix(1),
		EndKey:   tablecodec.GenTableRecordPrefix(1).PrefixNext(),
	}
	start, end, err := utils.GetRewriteRawKeys(&rawKeyFile, &rewriteRules)
	require.NoError(t, err)
	_, end, err = codec.DecodeBytes(end, nil)
	require.NoError(t, err)
	_, start, err = codec.DecodeBytes(start, nil)
	require.NoError(t, err)
	require.Equal(t, []byte(tablecodec.GenTableRecordPrefix(2)), start)
	require.Equal(t, []byte(tablecodec.GenTableRecordPrefix(2).PrefixNext()), end)

	encodeKeyFile := backuppb.DataFileInfo{
		Path:     "bakcup.log",
		StartKey: codec.EncodeBytes(nil, tablecodec.GenTableRecordPrefix(1)),
		EndKey:   codec.EncodeBytes(nil, tablecodec.GenTableRecordPrefix(1).PrefixNext()),
	}
	start, end, err = utils.GetRewriteEncodedKeys(&encodeKeyFile, &rewriteRules)
	require.NoError(t, err)
	require.Equal(t, codec.EncodeBytes(nil, tablecodec.GenTableRecordPrefix(2)), start)
	require.Equal(t, codec.EncodeBytes(nil, tablecodec.GenTableRecordPrefix(2).PrefixNext()), end)

	// test for table id 767
	encodeKeyFile767 := backuppb.DataFileInfo{
		Path:     "bakcup.log",
		StartKey: codec.EncodeBytes(nil, tablecodec.GenTableRecordPrefix(767)),
		EndKey:   codec.EncodeBytes(nil, tablecodec.GenTableRecordPrefix(767).PrefixNext()),
	}
	// use raw rewrite should no error but not equal
	start, end, err = utils.GetRewriteRawKeys(&encodeKeyFile767, &rewriteRules)
	require.NoError(t, err)
	require.NotEqual(t, codec.EncodeBytes(nil, tablecodec.GenTableRecordPrefix(511)), start)
	require.NotEqual(t, codec.EncodeBytes(nil, tablecodec.GenTableRecordPrefix(511).PrefixNext()), end)
	// use encode rewrite should no error and equal
	start, end, err = utils.GetRewriteEncodedKeys(&encodeKeyFile767, &rewriteRules)
	require.NoError(t, err)
	require.Equal(t, codec.EncodeBytes(nil, tablecodec.GenTableRecordPrefix(511)), start)
	require.Equal(t, codec.EncodeBytes(nil, tablecodec.GenTableRecordPrefix(511).PrefixNext()), end)
}

func TestRewriteRange(t *testing.T) {
	// Define test cases
	cases := []struct {
		rg            *rtree.Range
		rewriteRules  map[int64]*utils.RewriteRules
		expectedRange *rtree.Range
		expectedError error
	}{
		// Test case 1: No rewrite rules
		{
			rg: &rtree.Range{
				StartKey: []byte("startKey"),
				EndKey:   []byte("endKey"),
			},
			rewriteRules:  nil,
			expectedRange: &rtree.Range{StartKey: []byte("startKey"), EndKey: []byte("endKey")},
			expectedError: nil,
		},
		// Test case 2: Rewrite rule found for both start key and end key
		{
			rg: &rtree.Range{
				StartKey: append(tablecodec.GenTableIndexPrefix(1), []byte("startKey")...),
				EndKey:   append(tablecodec.GenTableIndexPrefix(1), []byte("endKey")...),
			},
			rewriteRules: map[int64]*utils.RewriteRules{
				1: {
					Data: []*import_sstpb.RewriteRule{
						{
							OldKeyPrefix: tablecodec.GenTableIndexPrefix(1),
							NewKeyPrefix: tablecodec.GenTableIndexPrefix(2),
						},
					},
				},
			},
			expectedRange: &rtree.Range{
				StartKey: append(tablecodec.GenTableIndexPrefix(2), []byte("startKey")...),
				EndKey:   append(tablecodec.GenTableIndexPrefix(2), []byte("endKey")...),
			},
			expectedError: nil,
		},
		// Test case 3: Rewrite rule found for end key
		{
			rg: &rtree.Range{
				StartKey: append(tablecodec.GenTableIndexPrefix(1), []byte("startKey")...),
				EndKey:   append(tablecodec.GenTableIndexPrefix(1), []byte("endKey")...),
			},
			rewriteRules: map[int64]*utils.RewriteRules{
				1: {
					Data: []*import_sstpb.RewriteRule{
						{
							OldKeyPrefix: append(tablecodec.GenTableIndexPrefix(1), []byte("endKey")...),
							NewKeyPrefix: append(tablecodec.GenTableIndexPrefix(2), []byte("newEndKey")...),
						},
					},
				},
			},
			expectedRange: &rtree.Range{
				StartKey: append(tablecodec.GenTableIndexPrefix(1), []byte("startKey")...),
				EndKey:   append(tablecodec.GenTableIndexPrefix(2), []byte("newEndKey")...),
			},
			expectedError: nil,
		},
		// Test case 4: Table ID mismatch
		{
			rg: &rtree.Range{
				StartKey: []byte("t1_startKey"),
				EndKey:   []byte("t2_endKey"),
			},
			rewriteRules: map[int64]*utils.RewriteRules{
				0: {
					Data: []*import_sstpb.RewriteRule{
						{
							OldKeyPrefix: []byte("t1_startKey"),
							NewKeyPrefix: []byte("t2_newStartKey"),
						},
					},
				},
			},
			expectedRange: nil,
			expectedError: errors.Annotate(berrors.ErrRestoreTableIDMismatch, "table id mismatch"),
		},
	}

	// Run test cases
	for _, tc := range cases {
		actualRange, actualError := utils.RewriteRange(tc.rg, tc.rewriteRules)
		if tc.expectedError != nil {
			require.EqualError(t, tc.expectedError, actualError.Error())
		} else {
			require.NoError(t, actualError)
		}
		require.Equal(t, tc.expectedRange, actualRange)
	}
}

func TestGetRewriteTableID(t *testing.T) {
	var tableID int64 = 76
	var oldTableID int64 = 80
	{
		rewriteRules := &utils.RewriteRules{
			Data: []*import_sstpb.RewriteRule{
				{
					OldKeyPrefix: tablecodec.EncodeTablePrefix(oldTableID),
					NewKeyPrefix: tablecodec.EncodeTablePrefix(tableID),
				},
			},
		}

		newTableID := utils.GetRewriteTableID(oldTableID, rewriteRules)
		require.Equal(t, tableID, newTableID)
	}

	{
		rewriteRules := &utils.RewriteRules{
			Data: []*import_sstpb.RewriteRule{
				{
					OldKeyPrefix: tablecodec.GenTableRecordPrefix(oldTableID),
					NewKeyPrefix: tablecodec.GenTableRecordPrefix(tableID),
				},
			},
		}

		newTableID := utils.GetRewriteTableID(oldTableID, rewriteRules)
		require.Equal(t, tableID, newTableID)
	}
}

func getNewKeyPrefix(key []byte, rewriteRules *utils.RewriteRules) kv.Key {
	for _, rule := range rewriteRules.Data {
		if bytes.HasPrefix(key, rule.GetOldKeyPrefix()) {
			return rule.GetNewKeyPrefix()
		}
	}
	return nil
}

func generateRewriteTableInfos() (newTableInfo, oldTableInfo *model.TableInfo) {
	newTableInfo = &model.TableInfo{
		ID: 1,
		Indices: []*model.IndexInfo{
			{
				ID:   1,
				Name: pmodel.NewCIStr("i1"),
			},
			{
				ID:   2,
				Name: pmodel.NewCIStr("i2"),
			},
		},
		Partition: &model.PartitionInfo{
			Definitions: []model.PartitionDefinition{
				{
					ID:   100,
					Name: pmodel.NewCIStr("p1"),
				},
				{
					ID:   200,
					Name: pmodel.NewCIStr("p2"),
				},
			},
		},
	}
	oldTableInfo = &model.TableInfo{
		ID: 2,
		Indices: []*model.IndexInfo{
			{
				ID:   1,
				Name: pmodel.NewCIStr("i1"),
			},
			{
				ID:   2,
				Name: pmodel.NewCIStr("i2"),
			},
		},
		Partition: &model.PartitionInfo{
			Definitions: []model.PartitionDefinition{
				{
					ID:   101,
					Name: pmodel.NewCIStr("p1"),
				},
				{
					ID:   201,
					Name: pmodel.NewCIStr("p2"),
				},
			},
		},
	}
	return newTableInfo, oldTableInfo
}

func TestGetRewriteRules(t *testing.T) {
	newTableInfo, oldTableInfo := generateRewriteTableInfos()

	{
		rewriteRules, err := utils.GetRewriteRules(newTableInfo, oldTableInfo, 0)
		require.NoError(t, err)
		require.Equal(t, getNewKeyPrefix(tablecodec.EncodeTablePrefix(2), rewriteRules), tablecodec.EncodeTablePrefix(1))
		require.Equal(t, getNewKeyPrefix(tablecodec.EncodeTablePrefix(101), rewriteRules), tablecodec.EncodeTablePrefix(100))
		require.Equal(t, getNewKeyPrefix(tablecodec.EncodeTablePrefix(201), rewriteRules), tablecodec.EncodeTablePrefix(200))
	}

	{
		rewriteRules, err := utils.GetRewriteRules(newTableInfo, oldTableInfo, 0)
		require.NoError(t, err)
		require.Equal(t, getNewKeyPrefix(tablecodec.GenTableRecordPrefix(2), rewriteRules), tablecodec.GenTableRecordPrefix(1))
		require.Equal(t, getNewKeyPrefix(tablecodec.EncodeTableIndexPrefix(2, 1), rewriteRules), tablecodec.EncodeTableIndexPrefix(1, 1))
		require.Equal(t, getNewKeyPrefix(tablecodec.EncodeTableIndexPrefix(2, 2), rewriteRules), tablecodec.EncodeTableIndexPrefix(1, 2))
		require.Equal(t, getNewKeyPrefix(tablecodec.GenTableRecordPrefix(101), rewriteRules), tablecodec.GenTableRecordPrefix(100))
		require.Equal(t, getNewKeyPrefix(tablecodec.EncodeTableIndexPrefix(101, 1), rewriteRules), tablecodec.EncodeTableIndexPrefix(100, 1))
		require.Equal(t, getNewKeyPrefix(tablecodec.EncodeTableIndexPrefix(101, 2), rewriteRules), tablecodec.EncodeTableIndexPrefix(100, 2))
		require.Equal(t, getNewKeyPrefix(tablecodec.GenTableRecordPrefix(201), rewriteRules), tablecodec.GenTableRecordPrefix(200))
		require.Equal(t, getNewKeyPrefix(tablecodec.EncodeTableIndexPrefix(201, 1), rewriteRules), tablecodec.EncodeTableIndexPrefix(200, 1))
		require.Equal(t, getNewKeyPrefix(tablecodec.EncodeTableIndexPrefix(201, 2), rewriteRules), tablecodec.EncodeTableIndexPrefix(200, 2))
	}
}

func TestGetRewriteRulesMap(t *testing.T) {
	newTableInfo, oldTableInfo := generateRewriteTableInfos()

	{
		rewriteRules := utils.GetRewriteRulesMap(newTableInfo, oldTableInfo, 0, false)
		require.Equal(t, getNewKeyPrefix(tablecodec.EncodeTablePrefix(2), rewriteRules[2]), tablecodec.EncodeTablePrefix(1))
		require.Equal(t, getNewKeyPrefix(tablecodec.EncodeTablePrefix(101), rewriteRules[101]), tablecodec.EncodeTablePrefix(100))
		require.Equal(t, getNewKeyPrefix(tablecodec.EncodeTablePrefix(201), rewriteRules[201]), tablecodec.EncodeTablePrefix(200))
	}

	{
		rewriteRules := utils.GetRewriteRulesMap(newTableInfo, oldTableInfo, 0, true)
		require.Equal(t, getNewKeyPrefix(tablecodec.GenTableRecordPrefix(2), rewriteRules[2]), tablecodec.GenTableRecordPrefix(1))
		require.Equal(t, getNewKeyPrefix(tablecodec.EncodeTableIndexPrefix(2, 1), rewriteRules[2]), tablecodec.EncodeTableIndexPrefix(1, 1))
		require.Equal(t, getNewKeyPrefix(tablecodec.EncodeTableIndexPrefix(2, 2), rewriteRules[2]), tablecodec.EncodeTableIndexPrefix(1, 2))
		require.Equal(t, getNewKeyPrefix(tablecodec.GenTableRecordPrefix(101), rewriteRules[101]), tablecodec.GenTableRecordPrefix(100))
		require.Equal(t, getNewKeyPrefix(tablecodec.EncodeTableIndexPrefix(101, 1), rewriteRules[101]), tablecodec.EncodeTableIndexPrefix(100, 1))
		require.Equal(t, getNewKeyPrefix(tablecodec.EncodeTableIndexPrefix(101, 2), rewriteRules[101]), tablecodec.EncodeTableIndexPrefix(100, 2))
		require.Equal(t, getNewKeyPrefix(tablecodec.GenTableRecordPrefix(201), rewriteRules[201]), tablecodec.GenTableRecordPrefix(200))
		require.Equal(t, getNewKeyPrefix(tablecodec.EncodeTableIndexPrefix(201, 1), rewriteRules[201]), tablecodec.EncodeTableIndexPrefix(200, 1))
		require.Equal(t, getNewKeyPrefix(tablecodec.EncodeTableIndexPrefix(201, 2), rewriteRules[201]), tablecodec.EncodeTableIndexPrefix(200, 2))
	}
}

func TestGetRewriteRuleOfTable(t *testing.T) {
	{
		rewriteRules := utils.GetRewriteRuleOfTable(2, 1, 0, map[int64]int64{1: 1, 2: 2}, false)
		require.Equal(t, getNewKeyPrefix(tablecodec.EncodeTablePrefix(2), rewriteRules), tablecodec.EncodeTablePrefix(1))
	}

	{
		rewriteRules := utils.GetRewriteRuleOfTable(2, 1, 0, map[int64]int64{1: 1, 2: 2}, true)
		require.Equal(t, getNewKeyPrefix(tablecodec.GenTableRecordPrefix(2), rewriteRules), tablecodec.GenTableRecordPrefix(1))
		require.Equal(t, getNewKeyPrefix(tablecodec.EncodeTableIndexPrefix(2, 1), rewriteRules), tablecodec.EncodeTableIndexPrefix(1, 1))
		require.Equal(t, getNewKeyPrefix(tablecodec.EncodeTableIndexPrefix(2, 2), rewriteRules), tablecodec.EncodeTableIndexPrefix(1, 2))
	}
}

type fakeApplyFile struct {
	StartKey []byte
	EndKey   []byte
}

func (f fakeApplyFile) GetStartKey() []byte {
	return f.StartKey
}

func (f fakeApplyFile) GetEndKey() []byte {
	return f.EndKey
}

func rewriteKey(key kv.Key, rule *import_sstpb.RewriteRule) kv.Key {
	if bytes.HasPrefix(key, rule.GetOldKeyPrefix()) {
		return append(rule.GetNewKeyPrefix(), key[len(rule.GetNewKeyPrefix()):]...)
	}
	return nil
}

func TestFindMatchedRewriteRule(t *testing.T) {
	rewriteRules := utils.GetRewriteRuleOfTable(2, 1, 0, map[int64]int64{1: 10}, true)
	{
		applyFile := fakeApplyFile{
			StartKey: tablecodec.EncodeRowKeyWithHandle(2, kv.IntHandle(100)),
			EndKey:   tablecodec.EncodeRowKeyWithHandle(2, kv.IntHandle(200)),
		}
		rule := utils.FindMatchedRewriteRule(applyFile, rewriteRules)
		require.Equal(t, rewriteKey(tablecodec.EncodeRowKeyWithHandle(2, kv.IntHandle(100)), rule),
			tablecodec.EncodeRowKeyWithHandle(1, kv.IntHandle(100)))
	}

	{
		applyFile := fakeApplyFile{
			StartKey: tablecodec.EncodeIndexSeekKey(2, 1, []byte("test-1")),
			EndKey:   tablecodec.EncodeIndexSeekKey(2, 1, []byte("test-2")),
		}
		rule := utils.FindMatchedRewriteRule(applyFile, rewriteRules)
		require.Equal(t, rewriteKey(tablecodec.EncodeIndexSeekKey(2, 1, []byte("test-1")), rule),
			tablecodec.EncodeIndexSeekKey(1, 10, []byte("test-1")))
	}

	{
		applyFile := fakeApplyFile{
			StartKey: tablecodec.EncodeRowKeyWithHandle(1, kv.IntHandle(100)),
			EndKey:   tablecodec.EncodeRowKeyWithHandle(2, kv.IntHandle(200)),
		}
		rule := utils.FindMatchedRewriteRule(applyFile, rewriteRules)
		require.Nil(t, rule)
	}

	{
		applyFile := fakeApplyFile{
			StartKey: tablecodec.EncodeTablePrefix(1),
			EndKey:   tablecodec.EncodeTablePrefix(1),
		}
		rule := utils.FindMatchedRewriteRule(applyFile, rewriteRules)
		require.Nil(t, rule)
	}
}

func TestGetRewriteKeyWithDifferentTable(t *testing.T) {
	applyFile := fakeApplyFile{
		StartKey: tablecodec.EncodeRowKeyWithHandle(1, kv.IntHandle(100)),
		EndKey:   tablecodec.EncodeRowKeyWithHandle(2, kv.IntHandle(200)),
	}
	_, _, err := utils.GetRewriteRawKeys(applyFile, nil)
	require.Error(t, err)
	_, _, err = utils.GetRewriteEncodedKeys(applyFile, nil)
	require.Error(t, err)
}

func tableDetailRewriteRule(tableID int64) *utils.RewriteRules {
	return &utils.RewriteRules{
		Data: []*import_sstpb.RewriteRule{
			{
				OldKeyPrefix: []byte(tablecodec.GenTableRecordPrefix(tableID)),
				NewKeyPrefix: []byte(tablecodec.GenTableRecordPrefix(1000 + tableID)),
			},
			{
				OldKeyPrefix: []byte(tablecodec.EncodeTableIndexPrefix(tableID, 1)),
				NewKeyPrefix: []byte(tablecodec.EncodeTableIndexPrefix(1000+tableID, 1)),
			},
		},
	}
}

func TestGetSortedRewriteRules(t *testing.T) {
	file := &backuppb.File{
		Name:       "test_write.sst",
		StartKey:   tablecodec.GenTableRecordPrefix(1),
		EndKey:     tablecodec.EncodeTableIndexPrefix(3, 1),
		TableMetas: []*backuppb.TableMeta{},
	}
	rules := map[int64]*utils.RewriteRules{
		1: tableDetailRewriteRule(1),
		2: tableDetailRewriteRule(2),
		3: tableDetailRewriteRule(3),
	}
	encodedStartKey, encodedEndKey, sortedRewriteRules := utils.GetSortedRewriteRules(file, rules, false)
	require.Equal(t, codec.EncodeBytes([]byte{}, tablecodec.GenTableRecordPrefix(1001)), encodedStartKey)
	require.Equal(t, codec.EncodeBytes([]byte{}, tablecodec.EncodeTableIndexPrefix(1003, 1)), encodedEndKey)
	require.Equal(t, &import_sstpb.RewriteRule{
		OldKeyPrefix: tablecodec.GenTableRecordPrefix(1),
		NewKeyPrefix: tablecodec.GenTableRecordPrefix(1001),
	}, sortedRewriteRules[0])
	require.Equal(t, &import_sstpb.RewriteRule{
		OldKeyPrefix: tablecodec.EncodeTableIndexPrefix(2, 1),
		NewKeyPrefix: tablecodec.EncodeTableIndexPrefix(1002, 1),
	}, sortedRewriteRules[1])
	require.Equal(t, &import_sstpb.RewriteRule{
		OldKeyPrefix: tablecodec.GenTableRecordPrefix(2),
		NewKeyPrefix: tablecodec.GenTableRecordPrefix(1002),
	}, sortedRewriteRules[2])
	require.Equal(t, &import_sstpb.RewriteRule{
		OldKeyPrefix: tablecodec.EncodeTableIndexPrefix(3, 1),
		NewKeyPrefix: tablecodec.EncodeTableIndexPrefix(1003, 1),
	}, sortedRewriteRules[3])
}

func TestGetSortedRewriteRules2(t *testing.T) {
	file := &backuppb.File{
		Name:       "test_write.sst",
		StartKey:   tablecodec.GenTableRecordPrefix(1),
		EndKey:     tablecodec.EncodeTableIndexPrefix(3, 1),
		TableMetas: []*backuppb.TableMeta{},
	}
	rules := map[int64]*utils.RewriteRules{
		1: tableDetailRewriteRule(1),
		2: tableDetailRewriteRule(2),
		3: tableDetailRewriteRule(3),
	}
	encodedStartKey, encodedEndKey, sortedRewriteRules := utils.GetSortedRewriteRules(file, rules, true)
	require.Equal(t, codec.EncodeBytes([]byte{}, tablecodec.GenTableRecordPrefix(1001)), encodedStartKey)
	require.Equal(t, codec.EncodeBytes([]byte{}, tablecodec.EncodeTableIndexPrefix(1003, 1)), encodedEndKey)
	require.Equal(t, &import_sstpb.RewriteRule{
		OldKeyPrefix: utils.EncodeKeyPrefix(tablecodec.GenTableRecordPrefix(1)),
		NewKeyPrefix: utils.EncodeKeyPrefix(tablecodec.GenTableRecordPrefix(1001)),
	}, sortedRewriteRules[0])
	require.Equal(t, &import_sstpb.RewriteRule{
		OldKeyPrefix: utils.EncodeKeyPrefix(tablecodec.EncodeTableIndexPrefix(2, 1)),
		NewKeyPrefix: utils.EncodeKeyPrefix(tablecodec.EncodeTableIndexPrefix(1002, 1)),
	}, sortedRewriteRules[1])
	require.Equal(t, &import_sstpb.RewriteRule{
		OldKeyPrefix: utils.EncodeKeyPrefix(tablecodec.GenTableRecordPrefix(2)),
		NewKeyPrefix: utils.EncodeKeyPrefix(tablecodec.GenTableRecordPrefix(1002)),
	}, sortedRewriteRules[2])
	require.Equal(t, &import_sstpb.RewriteRule{
		OldKeyPrefix: utils.EncodeKeyPrefix(tablecodec.EncodeTableIndexPrefix(3, 1)),
		NewKeyPrefix: utils.EncodeKeyPrefix(tablecodec.EncodeTableIndexPrefix(1003, 1)),
	}, sortedRewriteRules[3])
}
