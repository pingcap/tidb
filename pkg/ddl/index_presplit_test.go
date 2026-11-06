// Copyright 2026 PingCAP, Inc.
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

package ddl

import (
	"bytes"
	"fmt"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
)

func TestIndexPresplitOptions(t *testing.T) {
	maxRegions := int64(config.GetGlobalConfig().SplitRegionMaxNum)
	for _, tc := range []struct {
		regions int64
		errMsg  string
	}{
		{-1, "Split index region num should be greater than 0"},
		{0, "Split index region num should be greater than 0"},
		{1, ""},
		{maxRegions, ""},
		{maxRegions + 1, "Split index region num exceeded the limit"},
	} {
		t.Run(fmt.Sprintf("regions=%d", tc.regions), func(t *testing.T) {
			opt, err := buildIndexPresplitOpt(&ast.IndexOption{SplitOpt: &ast.SplitOption{Num: tc.regions}})
			if tc.errMsg != "" {
				require.ErrorContains(t, err, tc.errMsg)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.regions, opt.Num)
		})
	}
}

func TestIndexPresplitCountOnly(t *testing.T) {
	idx := &model.IndexInfo{
		ID:    1,
		Name:  pmodel.NewCIStr("idx"),
		State: model.StatePublic,
		Columns: []*model.IndexColumn{
			{Name: pmodel.NewCIStr("b"), Offset: 0},
			{Name: pmodel.NewCIStr("c"), Offset: 1},
		},
	}
	tbl := &model.TableInfo{
		ID:      42,
		Name:    pmodel.NewCIStr("t"),
		State:   model.StatePublic,
		Indices: []*model.IndexInfo{idx},
		Columns: []*model.ColumnInfo{
			{ID: 1, Name: pmodel.NewCIStr("b"), Offset: 0, State: model.StatePublic, FieldType: *types.NewFieldType(mysql.TypeLonglong)},
			{ID: 2, Name: pmodel.NewCIStr("c"), Offset: 1, State: model.StatePublic, FieldType: *types.NewFieldType(mysql.TypeLonglong)},
		},
	}
	sctx := mock.NewContext()
	sctx.GetSessionVars().StmtCtx.SetTimeZone(time.UTC)
	maxRegions := int64(config.GetGlobalConfig().SplitRegionMaxNum)
	for _, regions := range []int64{1, 4, maxRegions} {
		t.Run(fmt.Sprintf("regions=%d", regions), func(t *testing.T) {
			opt, err := buildIndexPresplitOpt(&ast.IndexOption{SplitOpt: &ast.SplitOption{Num: regions}})
			require.NoError(t, err)
			args, err := evalSplitDatumFromArgs(nil, tbl, idx, &model.IndexArg{SplitOpt: opt})
			require.NoError(t, err)
			require.Equal(t, int(regions), args.regionsCnt)
			require.Len(t, args.betweenLower, len(idx.Columns))
			require.Len(t, args.betweenUpper, len(idx.Columns))
			for i := range idx.Columns {
				require.Equal(t, types.KindMinNotNull, args.betweenLower[i].Kind())
				require.Equal(t, types.KindMaxValue, args.betweenUpper[i].Kind())
			}

			keys, err := getSplitIdxKeys(sctx, tbl, idx, args)
			require.NoError(t, err)
			// The first index adds its end boundary and regions-1 interior keys.
			require.Len(t, keys, int(regions))
			start := tablecodec.EncodeTableIndexPrefix(tbl.ID, idx.ID)
			end := tablecodec.EncodeTableIndexPrefix(tbl.ID, idx.ID+1)
			unique := make(map[string]struct{}, len(keys))
			interior := 0
			for _, key := range keys {
				require.Positive(t, bytes.Compare(key, start))
				require.LessOrEqual(t, bytes.Compare(key, end), 0)
				if !bytes.Equal(key, end) {
					interior++
				}
				unique[string(key)] = struct{}{}
			}
			require.Len(t, unique, int(regions))
			require.Equal(t, int(regions)-1, interior)
		})
	}
}
