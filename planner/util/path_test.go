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

package util_test

import (
	"testing"

	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/planner/util"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/collate"
	"github.com/pingcap/tidb/util/ranger"
	"github.com/stretchr/testify/require"
)

func TestCompareCol2Len(t *testing.T) {
	tests := []struct {
		c1         util.Col2Len
		c2         util.Col2Len
		res        int
		comparable bool
	}{
		{
			c1:         util.Col2Len{1: -1, 2: -1, 3: -1},
			c2:         util.Col2Len{1: -1, 2: 10},
			res:        1,
			comparable: true,
		},
		{
			c1:         util.Col2Len{1: 5},
			c2:         util.Col2Len{1: 10, 2: -1},
			res:        -1,
			comparable: true,
		},
		{
			c1:         util.Col2Len{1: -1, 2: -1},
			c2:         util.Col2Len{1: -1, 2: 5, 3: -1},
			res:        0,
			comparable: false,
		},
		{
			c1:         util.Col2Len{1: -1, 2: 10},
			c2:         util.Col2Len{1: -1, 2: 5, 3: -1},
			res:        0,
			comparable: false,
		},
		{
			c1:         util.Col2Len{1: -1, 2: 10},
			c2:         util.Col2Len{1: -1, 2: 10},
			res:        0,
			comparable: true,
		},
		{
			c1:         util.Col2Len{1: -1, 2: -1},
			c2:         util.Col2Len{1: -1, 2: 10},
			res:        0,
			comparable: false,
		},
	}
	for _, tt := range tests {
		res, comparable := util.CompareCol2Len(tt.c1, tt.c2)
		require.Equal(t, tt.res, res)
		require.Equal(t, tt.comparable, comparable)
	}
}

func TestOnlyPointRange(t *testing.T) {
	sctx := core.MockContext()
	nullDatum := types.MinNotNullDatum()
	nullDatum.SetNull()
	nullPointRange := ranger.Range{
		LowVal:    []types.Datum{*nullDatum.Clone()},
		HighVal:   []types.Datum{*nullDatum.Clone()},
		Collators: collate.GetBinaryCollatorSlice(1),
	}
	onePointRange := ranger.Range{
		LowVal:    []types.Datum{types.NewIntDatum(1)},
		HighVal:   []types.Datum{types.NewIntDatum(1)},
		Collators: collate.GetBinaryCollatorSlice(1),
	}
	one2TwoRange := ranger.Range{
		LowVal:    []types.Datum{types.NewIntDatum(1)},
		HighVal:   []types.Datum{types.NewIntDatum(2)},
		Collators: collate.GetBinaryCollatorSlice(1),
	}

	intHandlePath := &util.AccessPath{IsIntHandlePath: true}
	intHandlePath.Ranges = []*ranger.Range{&nullPointRange, &onePointRange}
	require.True(t, intHandlePath.OnlyPointRange(sctx))
	intHandlePath.Ranges = []*ranger.Range{&onePointRange, &one2TwoRange}
	require.False(t, intHandlePath.OnlyPointRange(sctx))

	indexPath := &util.AccessPath{Index: &model.IndexInfo{Columns: make([]*model.IndexColumn, 1)}}
	indexPath.Ranges = []*ranger.Range{&onePointRange}
	require.True(t, indexPath.OnlyPointRange(sctx))
	indexPath.Ranges = []*ranger.Range{&nullPointRange, &onePointRange}
	require.False(t, indexPath.OnlyPointRange(sctx))
	indexPath.Ranges = []*ranger.Range{&onePointRange, &one2TwoRange}
	require.False(t, indexPath.OnlyPointRange(sctx))

	indexPath.Index.Columns = make([]*model.IndexColumn, 2)
	indexPath.Ranges = []*ranger.Range{&onePointRange}
	require.False(t, indexPath.OnlyPointRange(sctx))
}
