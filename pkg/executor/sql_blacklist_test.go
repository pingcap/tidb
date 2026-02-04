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
	"testing"

	"github.com/pingcap/tidb/pkg/errno"
	"github.com/pingcap/tidb/pkg/testkit"
)

func TestSQLBlacklistDigestAndKeyword(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t(id int, v int)")

	tk.MustExec("delete from mysql.sql_blacklist")
	tk.MustExec("begin")
	tk.MustExec("insert into mysql.sql_blacklist values ('digest', 'delete from t where id = ?')")
	tk.MustExec("commit")
	tk.MustGetErrCode("delete from t where id = 1", errno.ErrSQLDeniedByBlacklist)

	tk.MustExec("delete from mysql.sql_blacklist")
	tk.MustExec("insert into mysql.sql_blacklist values ('keyword', 'modify column, NULL')")
	tk.MustGetErrCode("alter table t modify column v bigint null default -1", errno.ErrSQLDeniedByBlacklist)
}

func TestAdminReloadSQLBlacklist(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t(id int)")

	tk.MustExec("delete from mysql.sql_blacklist")
	tk.MustExec("admin reload sql_blacklist")
	tk.MustExec("delete from t where id = 1")

	tk.MustExec("begin")
	tk.MustExec("insert into mysql.sql_blacklist values ('digest', 'delete from t where id = ?')")
	sessVars := tk.Session().GetSessionVars()
	sessVars.TxnCtx.SQLBlacklistUpdated = false
	sessVars.TxnCtx.TableDeltaMap = nil
	tk.MustExec("commit")

	// Force admin reload to apply the new rule.
	tk.MustExec("delete from t where id = 1")
	tk.MustExec("admin reload sql_blacklist")
	tk.MustGetErrCode("delete from t where id = 1", errno.ErrSQLDeniedByBlacklist)
}
