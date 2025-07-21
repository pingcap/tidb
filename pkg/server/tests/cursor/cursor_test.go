// Copyright 2024 PingCAP, Inc.
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

package cursor

import (
	"context"
	"os"
	"testing"
	"time"

	mysqlcursor "github.com/YangKeao/go-mysql-driver"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/executor"
	util2 "github.com/pingcap/tidb/pkg/server/internal/util"
	"github.com/pingcap/tidb/pkg/server/tests/servertestkit"
	"github.com/stretchr/testify/require"
)

func TestCursorExceedQuota(t *testing.T) {
	cfg := util2.NewTestConfig()
	cfg.TempStoragePath = t.TempDir()

	cfg.Port = 0
	cfg.Status.StatusPort = 0
	cfg.TempStorageQuota = 1000
	executor.GlobalDiskUsageTracker.SetBytesLimit(cfg.TempStorageQuota)
	config.StoreGlobalConfig(cfg)
	ts := servertestkit.CreateTidbTestSuiteWithCfg(t, cfg)

	mysqldriver := &mysqlcursor.MySQLDriver{}
	rawConn, err := mysqldriver.Open(ts.GetDSNWithCursor(10))
	require.NoError(t, err)
	conn := rawConn.(mysqlcursor.Connection)

	_, err = conn.ExecContext(context.Background(), "drop table if exists t1", nil)
	require.NoError(t, err)
	_, err = conn.ExecContext(context.Background(), "CREATE TABLE `t1` (`c1` varchar(100));", nil)
	require.NoError(t, err)
	rowCount := 1000
	for i := 0; i < rowCount; i++ {
		_, err = conn.ExecContext(context.Background(), "insert into t1 (c1) values ('201801');", nil)
		require.NoError(t, err)
	}

	_, err = conn.ExecContext(context.Background(), "set tidb_mem_quota_query = 1;", nil)
	require.NoError(t, err)
	_, err = conn.ExecContext(context.Background(), "set global tidb_enable_tmp_storage_on_oom = 'ON'", nil)
	require.NoError(t, err)

	rawStmt, err := conn.Prepare("SELECT * FROM test.t1")
	require.NoError(t, err)
	stmt := rawStmt.(mysqlcursor.Statement)

	_, err = stmt.QueryContext(context.Background(), nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "Out Of Quota For Local Temporary Space!")

	require.NoError(t, conn.Close())

	time.Sleep(time.Second)

	tempStoragePath := cfg.TempStoragePath
	files, err := os.ReadDir(tempStoragePath)
	require.NoError(t, err)
	require.Empty(t, files)
}
