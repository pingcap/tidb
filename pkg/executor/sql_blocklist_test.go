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

package executor_test

import (
	"testing"

	"github.com/pingcap/tidb/pkg/errno"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func TestSQLBlocklistDigestAndKeyword(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t(id int, v int)")

	tk.MustExec("delete from mysql.sql_blocklist")
	tk.MustExec("begin")
	tk.MustExec("insert into mysql.sql_blocklist(type, value) values ('digest', 'delete from t where id = ?')")
	tk.MustExec("commit")
	tk.MustGetErrCode("delete from t where id = 1", errno.ErrSQLDeniedByBlocklist)

	tk.MustExec("delete from mysql.sql_blocklist")
	tk.MustExec("insert into mysql.sql_blocklist(type, value) values ('keyword', 'modify column, NULL')")
	tk.MustGetErrCode("alter table t modify column v bigint null default -1", errno.ErrSQLDeniedByBlocklist)
}

func TestSQLBlocklistUserScope(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tkRoot := testkit.NewTestKit(t, store)
	require.NoError(t, tkRoot.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil, nil))
	tkRoot.MustExec("create user app")
	tkRoot.MustExec("grant all on test.* to app")
	tkRoot.MustExec("use test")
	tkRoot.MustExec("create table t(id int)")

	tkApp := testkit.NewTestKit(t, store)
	require.NoError(t, tkApp.Session().Auth(&auth.UserIdentity{Username: "app", Hostname: "%"}, nil, nil, nil))
	tkApp.MustExec("use test")

	tkRoot.MustExec("delete from mysql.sql_blocklist")
	tkRoot.MustExec("insert into mysql.sql_blocklist(type, value, username) values ('digest', 'select * from t where id = ?', 'app')")
	tkRoot.MustExec("admin reload sql_blocklist")
	tkRoot.MustExec("select * from t where id = 1")
	tkApp.MustGetErrCode("select * from t where id = 1", errno.ErrSQLDeniedByBlocklist)

	tkRoot.MustExec("delete from mysql.sql_blocklist")
	tkRoot.MustExec("insert into mysql.sql_blocklist(type, value, username) values ('digest', 'select * from t where id = ?', '*')")
	tkRoot.MustExec("admin reload sql_blocklist")
	tkRoot.MustGetErrCode("select * from t where id = 1", errno.ErrSQLDeniedByBlocklist)
	tkApp.MustGetErrCode("select * from t where id = 1", errno.ErrSQLDeniedByBlocklist)
}

func TestAdminReloadSQLBlocklist(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t(id int)")

	tk.MustExec("delete from mysql.sql_blocklist")
	tk.MustExec("admin reload sql_blocklist")
	tk.MustExec("delete from t where id = 1")

	tk.MustExec("begin")
	tk.MustExec("insert into mysql.sql_blocklist(type, value) values ('digest', 'delete from t where id = ?')")
	sessVars := tk.Session().GetSessionVars()
	sessVars.TxnCtx.SQLBlocklistUpdated = false
	sessVars.TxnCtx.TableDeltaMap = nil
	tk.MustExec("commit")

	// Force admin reload to apply the new rule.
	tk.MustExec("delete from t where id = 1")
	tk.MustExec("admin reload sql_blocklist")
	tk.MustGetErrCode("delete from t where id = 1", errno.ErrSQLDeniedByBlocklist)
}
