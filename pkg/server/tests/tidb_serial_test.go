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

package tests

import (
	"context"
	"testing"

	"github.com/pingcap/failpoint"
	tmysql "github.com/pingcap/tidb/pkg/parser/mysql"
	util2 "github.com/pingcap/tidb/pkg/server/internal/util"
	"github.com/pingcap/tidb/pkg/server/tests/servertestkit"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

// this test will change `kv.TxnTotalSizeLimit` which may affect other test suites,
// so we must make it running in serial.
func TestLoadData1(t *testing.T) {
	ts := servertestkit.CreateTidbTestSuite(t)

	ts.RunTestLoadDataWithColumnList(t, ts.Server)
	ts.RunTestLoadData(t, ts.Server)
	ts.RunTestLoadDataWithSelectIntoOutfile(t)
	ts.RunTestLoadDataForSlowLog(t)
}

func TestLoadDataInTransaction(t *testing.T) {
	ts := servertestkit.CreateTidbTestSuite(t)

	ts.RunTestLoadDataInTransaction(t)
}

func TestConfigDefaultValue(t *testing.T) {
	ts := servertestkit.CreateTidbTestSuite(t)

	ts.RunTestsOnNewDB(t, nil, "config", func(dbt *testkit.DBTestKit) {
		rows := dbt.MustQuery("select @@tidb_slow_log_threshold;")
		ts.CheckRows(t, rows, "300")
	})
}

// Fix issue#22540. Change tidb_dml_batch_size,
// then check if load data into table with auto random column works properly.
func TestLoadDataAutoRandom(t *testing.T) {
	err := failpoint.Enable("github.com/pingcap/tidb/pkg/executor/BeforeCommitWork", "sleep(1000)")
	require.NoError(t, err)
	defer func() {
		//nolint:errcheck
		_ = failpoint.Disable("github.com/pingcap/tidb/pkg/executor/BeforeCommitWork")
	}()
	ts := servertestkit.CreateTidbTestSuite(t)

	ts.RunTestLoadDataAutoRandom(t)
}

func TestLoadDataAutoRandomWithSpecialTerm(t *testing.T) {
	ts := servertestkit.CreateTidbTestSuite(t)

	ts.RunTestLoadDataAutoRandomWithSpecialTerm(t)
}

func TestExplainFor(t *testing.T) {
	ts := servertestkit.CreateTidbTestSuite(t)

	ts.RunTestExplainForConn(t)
}

func TestStmtCount(t *testing.T) {
	cfg := util2.NewTestConfig()
	cfg.Port = 0
	cfg.Status.ReportStatus = true
	cfg.Status.StatusPort = 0
	cfg.Status.RecordDBLabel = false
	cfg.Performance.TCPKeepAlive = true
	ts := servertestkit.CreateTidbTestSuiteWithCfg(t, cfg)

	ts.RunTestStmtCount(t)
}

func TestDBStmtCount(t *testing.T) {
	ts := servertestkit.CreateTidbTestSuite(t)

	ts.RunTestDBStmtCount(t)
}

func TestLoadDataListPartition(t *testing.T) {
	ts := servertestkit.CreateTidbTestSuite(t)

	ts.RunTestLoadDataForListPartition(t)
	ts.RunTestLoadDataForListPartition2(t)
	ts.RunTestLoadDataForListColumnPartition(t)
	ts.RunTestLoadDataForListColumnPartition2(t)
}

func TestPrepareExecute(t *testing.T) {
	ts := servertestkit.CreateTidbTestSuite(t)

	qctx, err := ts.Tidbdrv.OpenCtx(uint64(0), 0, uint8(tmysql.DefaultCollationID), "test", nil, nil)
	require.NoError(t, err)

	ctx := context.Background()
	_, err = qctx.Execute(ctx, "use test")
	require.NoError(t, err)
	_, err = qctx.Execute(ctx, "create table t1(id int primary key, v int)")
	require.NoError(t, err)
	_, err = qctx.Execute(ctx, "insert into t1 values(1, 100)")
	require.NoError(t, err)

	stmt, _, _, err := qctx.Prepare("select * from t1 where id=1")
	require.NoError(t, err)
	rs, err := stmt.Execute(ctx, nil)
	require.NoError(t, err)
	req := rs.NewChunk(nil)
	require.NoError(t, rs.Next(ctx, req))
	require.Equal(t, 2, req.NumCols())
	require.Equal(t, req.NumCols(), len(rs.Columns()))
	require.Equal(t, 1, req.NumRows())
	require.Equal(t, int64(1), req.GetRow(0).GetInt64(0))
	require.Equal(t, int64(100), req.GetRow(0).GetInt64(1))

	// issue #33509
	_, err = qctx.Execute(ctx, "alter table t1 drop column v")
	require.NoError(t, err)

	rs, err = stmt.Execute(ctx, nil)
	require.NoError(t, err)
	req = rs.NewChunk(nil)
	require.NoError(t, rs.Next(ctx, req))
	require.Equal(t, 1, req.NumCols())
	require.Equal(t, req.NumCols(), len(rs.Columns()))
	require.Equal(t, 1, req.NumRows())
	require.Equal(t, int64(1), req.GetRow(0).GetInt64(0))
}

func TestDefaultCharacterAndCollation(t *testing.T) {
	ts := servertestkit.CreateTidbTestSuite(t)

	// issue #21194
	// 255 is the collation id of mysql client 8 default collation_connection
	qctx, err := ts.Tidbdrv.OpenCtx(uint64(0), 0, uint8(255), "test", nil, nil)
	require.NoError(t, err)
	testCase := []struct {
		variable string
		except   string
	}{
		{"collation_connection", "utf8mb4_0900_ai_ci"},
		{"character_set_connection", "utf8mb4"},
		{"character_set_client", "utf8mb4"},
	}

	for _, tc := range testCase {
		sVars, b := qctx.GetSessionVars().GetSystemVar(tc.variable)
		require.True(t, b)
		require.Equal(t, tc.except, sVars)
	}
}
