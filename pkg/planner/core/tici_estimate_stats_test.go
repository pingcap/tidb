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

package core

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/pingcap/tidb/pkg/util/ranger"
	"github.com/pingcap/tipb/go-tipb"
	"github.com/stretchr/testify/require"
)

type ticiStatsTestStore struct {
	kv.Storage
	count   uint64
	err     error
	req     *kv.TiCIEstimateCountRequest
	timeout time.Duration
}

func (s *ticiStatsTestStore) EstimateTiCICount(_ context.Context, req *kv.TiCIEstimateCountRequest, timeout time.Duration) (uint64, error) {
	s.req, s.timeout = req, timeout
	return s.count, s.err
}

func TestTiCISearchPathEstimateStats(t *testing.T) {
	sctx := mock.NewContext()
	store := &ticiStatsTestStore{Storage: &mock.Store{Client: &mock.Client{}}, count: 321}
	sctx.Store = store
	sctx.GetSessionVars().TimeZone = time.FixedZone("test-zone", 28800)
	ds := logicalop.DataSource{TableInfo: &model.TableInfo{ID: 42}, PhysicalTableID: 43}.Init(sctx.GetPlanCtx(), 0)
	ds.StatisticTable = &statistics.Table{HistColl: statistics.HistColl{RealtimeCount: 10000}}
	path := &util.AccessPath{
		Index:        &model.IndexInfo{ID: 7, FullTextInfo: &model.FullTextIndexInfo{}},
		FtsQueryInfo: &tipb.FTSQueryInfo{},
		Ranges:       ranger.FullIntRange(false),
	}
	deriveSearchPathStats(ds, path)
	require.Equal(t, float64(321), path.CountAfterAccess)
	require.Equal(t, int64(43), store.req.TableID)
	require.Equal(t, int64(7), store.req.IndexID)
	require.Same(t, path.FtsQueryInfo, store.req.FTSQueryInfo)
	require.Positive(t, store.req.KeyRanges.TotalRangeNum())
	require.Equal(t, int64(28800), store.req.TimeZoneOffset)
	require.Equal(t, 50*time.Millisecond, store.timeout)

	ds.PhysicalTableID = 0
	store.count = 20000
	deriveSearchPathStats(ds, path)
	require.Equal(t, float64(10000), path.CountAfterAccess)
	require.Equal(t, int64(42), store.req.TableID)

	store.count = 0
	deriveSearchPathStats(ds, path)
	require.Zero(t, path.CountAfterAccess)

	store.err = errors.New("estimate unavailable")
	deriveSearchPathStats(ds, path)
	require.Equal(t, float64(1000), path.CountAfterAccess)

	sctx.Store = &mock.Store{}
	deriveSearchPathStats(ds, path)
	require.Equal(t, float64(1000), path.CountAfterAccess)
}
