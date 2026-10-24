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

package executor_test

import (
	"context"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/kvproto/pkg/coprocessor"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/store/mockstore"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/testutils"
	"github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/tikvrpc"
)

func TestIntegrationCopCache(t *testing.T) {
	originConfig := config.GetGlobalConfig()
	config.StoreGlobalConfig(config.NewConfig())
	defer config.StoreGlobalConfig(originConfig)

	cli := &copCacheTestClient{
		RegionProperityClient: &testkit.RegionProperityClient{},
		cacheVersion:          123,
	}
	hijackClient := func(c tikv.Client) tikv.Client {
		cli.Client = c
		return cli
	}
	var cluster testutils.Cluster
	store, dom := testkit.CreateMockStoreAndDomain(t,
		mockstore.WithClusterInspector(func(c testutils.Cluster) {
			mockstore.BootstrapWithSingleStore(c)
			cluster = c
		}),
		mockstore.WithClientHijacker(hijackClient))

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int primary key)")

	tblInfo, err := dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t"))
	require.NoError(t, err)
	tid := tblInfo.Meta().ID
	tk.MustExec(`insert into t values(1),(2),(3),(4),(5),(6),(7),(8),(9),(10),(11),(12)`)
	tableStart := tablecodec.GenTableRecordPrefix(tid)
	tableStartPrefixNext := tableStart.PrefixNext()
	if kerneltype.IsNextGen() {
		tableStart = store.GetCodec().EncodeKey(tableStart)
		tableStartPrefixNext = store.GetCodec().EncodeKey(tableStartPrefixNext)
	}
	cluster.SplitKeys(tableStart, tableStartPrefixNext, 6)

	cli.enableCopCacheMock.Store(true)
	rows := tk.MustQuery("explain analyze select * from t where t.a < 10").Rows()
	require.Equal(t, "9", rows[0][2])
	require.Contains(t, rows[0][5], "cop_task: {num: 5")
	require.Contains(t, rows[0][5], "copr_cache_hit_ratio: 0.00")

	rows = tk.MustQuery("explain analyze select * from t").Rows()
	require.Equal(t, "12", rows[0][2])
	require.Contains(t, rows[0][5], "cop_task: {num: 6")
	hitRatioIdx := strings.Index(rows[0][5].(string), "copr_cache_hit_ratio:") + len("copr_cache_hit_ratio: ")
	require.GreaterOrEqual(t, hitRatioIdx, len("copr_cache_hit_ratio: "))
	hitRatio, err := strconv.ParseFloat(rows[0][5].(string)[hitRatioIdx:hitRatioIdx+4], 64)
	require.NoError(t, err)
	require.Greater(t, hitRatio, float64(0))

	// Test for cop cache disabled.
	cli.enableCopCacheMock.Store(false)
	cfg := config.NewConfig()
	cfg.TiKVClient.CoprCache.CapacityMB = 0
	config.StoreGlobalConfig(cfg)
	rows = tk.MustQuery("explain analyze select * from t where t.a < 10").Rows()
	require.Equal(t, "9", rows[0][2])
	require.Contains(t, rows[0][5], "copr_cache: disabled")
}

type copCacheTestClient struct {
	*testkit.RegionProperityClient
	cacheVersion       uint64
	enableCopCacheMock atomic.Bool
}

func (c *copCacheTestClient) SendRequest(ctx context.Context, addr string, req *tikvrpc.Request, timeout time.Duration) (*tikvrpc.Response, error) {
	if req.Type != tikvrpc.CmdCop {
		return c.RegionProperityClient.SendRequest(ctx, addr, req, timeout)
	}

	copReq := req.Cop()
	if !c.enableCopCacheMock.Load() || copReq == nil || !copReq.IsCacheEnabled {
		return c.RegionProperityClient.SendRequest(ctx, addr, req, timeout)
	}
	if copReq.CacheIfMatchVersion == c.cacheVersion {
		return &tikvrpc.Response{Resp: &coprocessor.Response{
			IsCacheHit:       true,
			CacheLastVersion: c.cacheVersion,
		}}, nil
	}

	resp, err := c.RegionProperityClient.SendRequest(ctx, addr, req, timeout)
	if err != nil || resp == nil {
		return resp, err
	}
	copResp, ok := resp.Resp.(*coprocessor.Response)
	if !ok {
		return resp, err
	}
	// Keep the cop-cache behavior local to this test instead of depending on
	// source-rewritten failpoints in the mock store.
	copResp.CanBeCached = true
	copResp.CacheLastVersion = c.cacheVersion
	if copResp.ExecDetailsV2 != nil {
		if copResp.ExecDetailsV2.TimeDetailV2 == nil {
			copResp.ExecDetailsV2.TimeDetailV2 = &kvrpcpb.TimeDetailV2{}
		}
		copResp.ExecDetailsV2.TimeDetailV2.ProcessWallTimeNs = uint64((500 * time.Millisecond).Nanoseconds())
	}
	if copResp.ExecDetails == nil {
		copResp.ExecDetails = &kvrpcpb.ExecDetails{}
	}
	copResp.ExecDetails.TimeDetail = &kvrpcpb.TimeDetail{ProcessWallTimeMs: 500}
	return resp, err
}
