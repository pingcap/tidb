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
	"testing"

	"github.com/pingcap/errors"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/kvproto/pkg/import_sstpb"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
	"github.com/pingcap/tidb/br/pkg/restore/utils"
	"github.com/pingcap/tidb/br/pkg/rtree"
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
		rules,
	)
	require.Error(t, err)
	require.Regexp(t, ".*rewrite rule mismatch.*", err.Error())
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
		rewriteRules  *utils.RewriteRules
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
			rewriteRules: &utils.RewriteRules{
				Data: []*import_sstpb.RewriteRule{
					{
						OldKeyPrefix: tablecodec.GenTableIndexPrefix(1),
						NewKeyPrefix: tablecodec.GenTableIndexPrefix(2),
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
			rewriteRules: &utils.RewriteRules{
				Data: []*import_sstpb.RewriteRule{
					{
						OldKeyPrefix: append(tablecodec.GenTableIndexPrefix(1), []byte("endKey")...),
						NewKeyPrefix: append(tablecodec.GenTableIndexPrefix(2), []byte("newEndKey")...),
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
			rewriteRules: &utils.RewriteRules{
				Data: []*import_sstpb.RewriteRule{
					{
						OldKeyPrefix: []byte("t1_startKey"),
						NewKeyPrefix: []byte("t2_newStartKey"),
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
