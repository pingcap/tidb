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
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/testkit"
	"github.com/stretchr/testify/require"
)

func TestTiDBLastTxnInfoCommitMode(t *testing.T) {
	defer config.RestoreFunc()()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.TiKVClient.AsyncCommit.SafeWindow = time.Second
	})

	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int primary key, v int)")
	tk.MustExec("insert into t values (1, 1)")

	tk.MustExec("set @@tidb_enable_async_commit = 1")
	tk.MustExec("set @@tidb_enable_1pc = 0")
	tk.MustExec("update t set v = v + 1 where a = 1")
	rows := tk.MustQuery("select json_extract(@@tidb_last_txn_info, '$.txn_commit_mode'), json_extract(@@tidb_last_txn_info, '$.async_commit_fallback'), json_extract(@@tidb_last_txn_info, '$.one_pc_fallback')").Rows()
	t.Log(rows)
	require.Equal(t, `"async_commit"`, rows[0][0])
	require.Equal(t, "false", rows[0][1])
	require.Equal(t, "false", rows[0][2])

	tk.MustExec("set @@tidb_enable_async_commit = 0")
	tk.MustExec("set @@tidb_enable_1pc = 1")
	tk.MustExec("update t set v = v + 1 where a = 1")
	rows = tk.MustQuery("select json_extract(@@tidb_last_txn_info, '$.txn_commit_mode'), json_extract(@@tidb_last_txn_info, '$.async_commit_fallback'), json_extract(@@tidb_last_txn_info, '$.one_pc_fallback')").Rows()
	require.Equal(t, `"1pc"`, rows[0][0])
	require.Equal(t, "false", rows[0][1])
	require.Equal(t, "false", rows[0][2])

	tk.MustExec("set @@tidb_enable_async_commit = 0")
	tk.MustExec("set @@tidb_enable_1pc = 0")
	tk.MustExec("update t set v = v + 1 where a = 1")
	rows = tk.MustQuery("select json_extract(@@tidb_last_txn_info, '$.txn_commit_mode'), json_extract(@@tidb_last_txn_info, '$.async_commit_fallback'), json_extract(@@tidb_last_txn_info, '$.one_pc_fallback')").Rows()
	require.Equal(t, `"2pc"`, rows[0][0])
	require.Equal(t, "false", rows[0][1])
	require.Equal(t, "false", rows[0][2])

	require.NoError(t, failpoint.Enable("tikvclient/invalidMaxCommitTS", "return"))
	defer func() {
		require.NoError(t, failpoint.Disable("tikvclient/invalidMaxCommitTS"))
	}()

	tk.MustExec("set @@tidb_enable_async_commit = 1")
	tk.MustExec("set @@tidb_enable_1pc = 0")
	tk.MustExec("update t set v = v + 1 where a = 1")
	rows = tk.MustQuery("select json_extract(@@tidb_last_txn_info, '$.txn_commit_mode'), json_extract(@@tidb_last_txn_info, '$.async_commit_fallback'), json_extract(@@tidb_last_txn_info, '$.one_pc_fallback')").Rows()
	t.Log(rows)
	require.Equal(t, `"2pc"`, rows[0][0])
	require.Equal(t, "true", rows[0][1])
	require.Equal(t, "false", rows[0][2])

	tk.MustExec("set @@tidb_enable_async_commit = 0")
	tk.MustExec("set @@tidb_enable_1pc = 1")
	tk.MustExec("update t set v = v + 1 where a = 1")
	rows = tk.MustQuery("select json_extract(@@tidb_last_txn_info, '$.txn_commit_mode'), json_extract(@@tidb_last_txn_info, '$.async_commit_fallback'), json_extract(@@tidb_last_txn_info, '$.one_pc_fallback')").Rows()
	t.Log(rows)
	require.Equal(t, `"2pc"`, rows[0][0])
	require.Equal(t, "false", rows[0][1])
	require.Equal(t, "true", rows[0][2])

	tk.MustExec("set @@tidb_enable_async_commit = 1")
	tk.MustExec("set @@tidb_enable_1pc = 1")
	tk.MustExec("update t set v = v + 1 where a = 1")
	rows = tk.MustQuery("select json_extract(@@tidb_last_txn_info, '$.txn_commit_mode'), json_extract(@@tidb_last_txn_info, '$.async_commit_fallback'), json_extract(@@tidb_last_txn_info, '$.one_pc_fallback')").Rows()
	t.Log(rows)
	require.Equal(t, `"2pc"`, rows[0][0])
	require.Equal(t, "true", rows[0][1])
	require.Equal(t, "true", rows[0][2])
}
