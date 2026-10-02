// Copyright 2023 PingCAP, Inc.
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

package globalstats

import (
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tidb/pkg/util/sqlkiller"
	"github.com/stretchr/testify/require"
)

func TestMergePartTopN2GlobalTopNWithoutHists(t *testing.T) {
	loc := time.UTC
	sc := stmtctx.NewStmtCtxWithTimeZone(loc)
	version := 1
	killer := sqlkiller.SQLKiller{}

	// Prepare TopNs.
	topNs := make([]*statistics.TopN, 0, 10)
	for i := 0; i < 10; i++ {
		// Construct TopN, should be key(1, 1) -> 2, key(1, 2) -> 2, key(1, 3) -> 3.
		topN := statistics.NewTopN(3)
		{
			key1, err := codec.EncodeKey(sc.TimeZone(), nil, types.NewIntDatum(1), types.NewIntDatum(1))
			require.NoError(t, err)
			topN.AppendTopN(key1, 2)
			key2, err := codec.EncodeKey(sc.TimeZone(), nil, types.NewIntDatum(1), types.NewIntDatum(2))
			require.NoError(t, err)
			topN.AppendTopN(key2, 2)
			key3, err := codec.EncodeKey(sc.TimeZone(), nil, types.NewIntDatum(1), types.NewIntDatum(3))
			require.NoError(t, err)
			topN.AppendTopN(key3, 3)
		}
		topNs = append(topNs, topN)
	}

	// Test merge 2 topN with nil hists.
	globalTopN, leftTopN, _, err := MergePartTopN2GlobalTopN(loc, version, topNs, 2, nil, false, &killer)
	require.NoError(t, err)
	require.Len(t, globalTopN.TopN, 2, "should only have 2 topN")
	require.Equal(t, uint64(50), globalTopN.TotalCount(), "should have 50 rows")
	require.Len(t, leftTopN, 1, "should have 1 left topN")
}

func TestMergePartTopN2GlobalTopNWithHists(t *testing.T) {
	loc := time.UTC
	sc := stmtctx.NewStmtCtxWithTimeZone(loc)
	version := 1
	killer := sqlkiller.SQLKiller{}

	// Prepare TopNs.
	topNs := make([]*statistics.TopN, 0, 10)
	for i := 0; i < 10; i++ {
		// Construct TopN, should be key1 -> 2, key2 -> 2, key3 -> 3.
		topN := statistics.NewTopN(3)
		{
			key1, err := codec.EncodeKey(sc.TimeZone(), nil, types.NewIntDatum(1))
			require.NoError(t, err)
			topN.AppendTopN(key1, 2)
			key2, err := codec.EncodeKey(sc.TimeZone(), nil, types.NewIntDatum(2))
			require.NoError(t, err)
			topN.AppendTopN(key2, 2)
			if i%2 == 0 {
				key3, err := codec.EncodeKey(sc.TimeZone(), nil, types.NewIntDatum(3))
				require.NoError(t, err)
				topN.AppendTopN(key3, 3)
			}
		}
		topNs = append(topNs, topN)
	}

	// Prepare Hists.
	hists := make([]*statistics.Histogram, 0, 10)
	for i := 0; i < 10; i++ {
		// Construct Hist
		h := statistics.NewHistogram(1, 10, 0, 0, types.NewFieldType(mysql.TypeTiny), chunk.InitialCapacity, 0)
		h.Bounds.AppendInt64(0, 1)
		h.Buckets = append(h.Buckets, statistics.Bucket{Repeat: 10, Count: 20})
		h.Bounds.AppendInt64(0, 2)
		h.Buckets = append(h.Buckets, statistics.Bucket{Repeat: 10, Count: 30})
		h.Bounds.AppendInt64(0, 3)
		h.Buckets = append(h.Buckets, statistics.Bucket{Repeat: 10, Count: 30})
		h.Bounds.AppendInt64(0, 4)
		h.Buckets = append(h.Buckets, statistics.Bucket{Repeat: 10, Count: 40})
		hists = append(hists, h)
	}

	// Test merge 2 topN.
	globalTopN, leftTopN, _, err := MergePartTopN2GlobalTopN(loc, version, topNs, 2, hists, false, &killer)
	require.NoError(t, err)
	require.Len(t, globalTopN.TopN, 2, "should only have 2 topN")
	require.Equal(t, uint64(55), globalTopN.TotalCount(), "should have 55")
	require.Len(t, leftTopN, 1, "should have 1 left topN")
}
