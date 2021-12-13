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

package statistics_test

import (
	"testing"

	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/types"
	"github.com/stretchr/testify/require"
)

func TestEstimateTypeWidth(t *testing.T) {
	t.Parallel()

	var colType *types.FieldType

	colType = &types.FieldType{Tp: mysql.TypeLonglong}
	require.Equal(t, 8, statistics.EstimateTypeWidthForChunk(colType)) // fixed-witch type

	colType = &types.FieldType{Tp: mysql.TypeString, Flen: 31}
	require.Equal(t, 31, statistics.EstimateTypeWidthForChunk(colType)) // colLen <= 32

	colType = &types.FieldType{Tp: mysql.TypeString, Flen: 999}
	require.Equal(t, 515, statistics.EstimateTypeWidthForChunk(colType)) // colLen < 1000

	colType = &types.FieldType{Tp: mysql.TypeString, Flen: 2000}
	require.Equal(t, 516, statistics.EstimateTypeWidthForChunk(colType)) // colLen < 1000

	colType = &types.FieldType{Tp: mysql.TypeString}
	require.Equal(t, 32, statistics.EstimateTypeWidthForChunk(colType)) // value after guessing
}

func TestGetAvgRowSizeWithoutColStats(t *testing.T) {
	t.Parallel()
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	se, err := session.CreateSession4Test(store)
	require.NoError(t, err)
	coll := &statistics.HistColl{}
	col := &expression.Column{}
	se.GetSessionVars().EnableChunkRPC = true

	col.RetType = &types.FieldType{Tp: mysql.TypeString, Flen: 999}
	// 515 + 1/8
	require.Equal(t, float64(515.125), coll.GetAvgRowSize(se, []*expression.Column{col}, false, false))

	col.RetType = &types.FieldType{Tp: mysql.TypeString}
	// 32 + 1/8
	require.Equal(t, float64(32.125), coll.GetAvgRowSize(se, []*expression.Column{col}, false, false))

	col.RetType = &types.FieldType{Tp: mysql.TypeFloat}
	// 4 + 1/8
	require.Equal(t, float64(4.125), coll.GetAvgRowSize(se, []*expression.Column{col}, false, false))

	se.GetSessionVars().EnableChunkRPC = false
	// 8 + 1
	require.Equal(t, float64(9), coll.GetAvgRowSize(se, []*expression.Column{col}, false, false))
}
