// Copyright 2025 PingCAP, Inc.
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
	"testing"

	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	pdhttp "github.com/tikv/pd/client/http"
)

func (cli *mockPDCli) GetRegionDistributionByKeyRange(ctx context.Context, keyRange *pdhttp.KeyRange, engine string) (*pdhttp.RegionDistributions, error) {
	args := cli.Called(ctx, keyRange, engine)
	return args.Get(0).(*pdhttp.RegionDistributions), args.Error(1)
}

func TestShowTableDistributions(t *testing.T) {
	re := require.New(t)
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	cli := &mockPDCli{}
	recoverCli := infosync.SetPDHttpCliForTest(cli)
	defer recoverCli()
	mockGetDistributions := func(tblName, partition string, distributions *pdhttp.RegionDistributions) *mock.Call {
		is := tk.Session().GetDomainInfoSchema()
		tbl, err := is.TableInfoByName(ast.NewCIStr("test"), ast.NewCIStr(tblName))
		require.NoError(t, err)
		tblID := tbl.ID
		if partition != "" {
			tblID = tbl.GetPartitionInfo().GetPartitionIDByName(partition)
		}
		startKey, endKey := tablecodec.GetTableHandleKeyRange(tblID)
		keyRange := pdhttp.NewKeyRange(startKey, endKey)
		return cli.On("GetRegionDistributionByKeyRange", mock.Anything, keyRange, "").
			Return(distributions, nil)
	}
	distributions := &pdhttp.RegionDistributions{}
	distributions.RegionDistributions = append(distributions.RegionDistributions, &pdhttp.RegionDistribution{
		StoreID:               1,
		EngineType:            "tikv",
		RegionLeaderCount:     1,
		RegionPeerCount:       3,
		RegionWriteBytes:      100,
		RegionWriteKeys:       10,
		RegionWriteQuery:      1,
		RegionLeaderReadBytes: 1000,
		RegionLeaderReadKeys:  100,
		RegionLeaderReadQuery: 10,
		RegionPeerReadKeys:    10000,
		RegionPeerReadBytes:   1000,
		RegionPeerReadQuery:   100,
	})
	tk.MustExec("create table t1(a int)")
	mockGetDistributions("t1", "", distributions)
	ret := tk.MustQuery("show table t1 distributions").Rows()
	re.Len(ret, 1)
	re.Len(ret[0], 13)

	// test for partition table distributions
	tk.MustExec("create table tp1 (id int) PARTITION BY RANGE (id) (" +
		"PARTITION p0 VALUES LESS THAN (100)," +
		"PARTITION p1 VALUES LESS THAN (1000)," +
		"PARTITION p2 VALUES LESS THAN (10000)" +
		")")
	mockGetDistributions("tp1", "p0", distributions)
	mockGetDistributions("tp1", "p1", distributions)
	tk.MustQuery("show table tp1 partition(p0) distributions").Check(testkit.Rows("1 tikv 1 3 100 10 1 " +
		"1000 100 10 1000 10000 100"))
	tk.MustQuery("show table tp1 partition(p0,p1) distributions").Check(testkit.Rows("1 tikv 1 3 100 10 1 "+
		"1000 100 10 1000 10000 100", "1 tikv 1 3 100 10 1 1000 100 10 1000 10000 100"))
}
