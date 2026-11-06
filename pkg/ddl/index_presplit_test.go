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
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/pingcap/tidb/pkg/types"
	contextutil "github.com/pingcap/tidb/pkg/util/context"
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

			keys, err := getSplitIdxKeys(sctx, tbl, idx, args, time.UTC)
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

func TestIndexPresplitTimeZone(t *testing.T) {
	tsType := types.NewFieldType(mysql.TypeTimestamp)
	tsType.SetDecimal(types.DefaultFsp)
	idx := &model.IndexInfo{
		ID:      1,
		Name:    pmodel.NewCIStr("idx_ts"),
		State:   model.StatePublic,
		Tp:      pmodel.IndexTypeBtree,
		Columns: []*model.IndexColumn{{Name: pmodel.NewCIStr("ts"), Offset: 0}},
	}
	tbl := &model.TableInfo{
		ID:      43,
		Name:    pmodel.NewCIStr("t_ts"),
		State:   model.StatePublic,
		Indices: []*model.IndexInfo{idx},
		Columns: []*model.ColumnInfo{{
			ID: 1, Name: pmodel.NewCIStr("ts"), Offset: 0, State: model.StatePublic,
			FieldType: *tsType,
		}},
	}
	for _, tc := range []struct {
		name         string
		jobOffset    int
		workerOffset int
	}{
		{"job+08_workerUTC", 8 * 3600, 0},
		{"jobUTC_worker+08", 0, 8 * 3600},
	} {
		t.Run(tc.name, func(t *testing.T) {
			jobTZ := &model.TimeZoneLocation{Name: "UTC", Offset: tc.jobOffset}
			if tc.jobOffset != 0 {
				jobTZ.Name = "UTC+08:00"
			}
			jobLoc, err := jobTZ.GetLocation()
			require.NoError(t, err)
			workerLoc := time.UTC
			if tc.workerOffset != 0 {
				workerLoc = time.FixedZone("worker+08:00", tc.workerOffset)
			}
			reorgMeta := &model.DDLReorgMeta{Location: jobTZ, ReorgTp: model.ReorgTypeTxn}
			exprCtx, err := newReorgExprCtxWithReorgMeta(reorgMeta, contextutil.NewStaticWarnHandler(0))
			require.NoError(t, err)
			for _, style := range []string{"BY", "BETWEEN"} {
				t.Run(style, func(t *testing.T) {
					worker := mock.NewContext()
					worker.GetSessionVars().StmtCtx.SetTimeZone(workerLoc)
					reference := mock.NewContext()
					reference.GetSessionVars().StmtCtx.SetTimeZone(jobLoc)
					opt := &model.IndexArgSplitOpt{ValueLists: [][]string{{"'2026-01-02 12:00:00'"}}}
					if style == "BETWEEN" {
						opt = &model.IndexArgSplitOpt{
							Lower: []string{"'2026-01-02 12:00:00'"},
							Upper: []string{"'2026-01-03 12:00:00'"}, Num: 4,
						}
					}
					indexArg := &model.IndexArg{SplitOpt: opt}
					args, err := evalSplitDatumFromArgs(exprCtx, tbl, idx, indexArg)
					require.NoError(t, err)
					if style == "BY" {
						require.Equal(t, mysql.TypeTimestamp, args.byRows[0][0].GetMysqlTime().Type())
					} else {
						require.Equal(t, mysql.TypeTimestamp, args.betweenLower[0].GetMysqlTime().Type())
					}
					expected, err := getSplitIdxKeys(reference, tbl, idx, args, jobLoc)
					require.NoError(t, err)
					wrongZoneKeys, err := getSplitIdxKeys(worker, tbl, idx, args, workerLoc)
					require.NoError(t, err)
					require.NotEqual(t, expected, wrongZoneKeys, "the timestamp fixture must distinguish the two zones")

					var actual [][]byte
					calls := 0
					testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/beforePresplitIndex", func(keys [][]byte) {
						calls++
						require.Same(t, workerLoc, worker.GetSessionVars().StmtCtx.TimeZone())
						for _, key := range keys {
							actual = append(actual, bytes.Clone(key))
						}
					})
					// Capture real producer keys before the optional storage RPC. The
					// same job must encode them independently of the worker's zone.
					err = preSplitIndexRegions(context.Background(), worker, nil, tbl, []*model.IndexInfo{idx}, reorgMeta,
						&model.ModifyIndexArgs{IndexArgs: []*model.IndexArg{indexArg}})
					require.NoError(t, err)
					require.Equal(t, 1, calls)
					require.Equal(t, expected, actual)
					require.Same(t, workerLoc, worker.GetSessionVars().StmtCtx.TimeZone())
				})
			}
		})
	}
}
