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

package brietest

import (
	"context"
	"testing"

	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/sessionctx/binloginfo"
	"github.com/pingcap/tidb/store/mockstore/mockcopr"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/tests/realtikvtest"
	"github.com/pingcap/tipb/go-binlog"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

type mockBinlogPump struct {
}

var _ binlog.PumpClient = &mockBinlogPump{}

func (p *mockBinlogPump) WriteBinlog(_ context.Context, _ *binlog.WriteBinlogReq, _ ...grpc.CallOption) (*binlog.WriteBinlogResp, error) {
	return &binlog.WriteBinlogResp{}, nil
}

func (p *mockBinlogPump) PullBinlogs(_ context.Context, _ *binlog.PullBinlogReq, _ ...grpc.CallOption) (binlog.Pump_PullBinlogsClient, error) {
	return mockPumpPullBinlogsClient{mockcopr.MockGRPCClientStream()}, nil
}

type mockPumpPullBinlogsClient struct {
	grpc.ClientStream
}

func (m mockPumpPullBinlogsClient) Recv() (*binlog.PullBinlogResp, error) {
	return nil, nil
}

func TestForCoverage(t *testing.T) {
	// Just for test coverage.
	store, clean := realtikvtest.CreateMockStoreAndSetup(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (id int auto_increment, v int, index (id))")
	tk.MustExec("insert t values ()")
	tk.MustExec("insert t values ()")
	tk.MustExec("insert t values ()")

	// Normal request will not cover txn.Seek.
	tk.MustExec("admin check table t")

	// Cover dirty table operations in StateTxn.
	tk.Session().GetSessionVars().BinlogClient = binloginfo.MockPumpsClient(&mockBinlogPump{})
	tk.MustExec("begin")
	tk.MustExec("truncate table t")
	tk.MustExec("insert t values ()")
	tk.MustExec("delete from t where id = 2")
	tk.MustExec("update t set v = 5 where id = 2")
	tk.MustExec("insert t values ()")
	tk.MustExec("rollback")

	require.NoError(t, tk.Session().SetCollation(mysql.DefaultCollationID))

	tk.MustExec("show processlist")
	_, err := tk.Session().FieldList("t")
	require.NoError(t, err)
}
