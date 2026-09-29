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

package session

import (
	"context"
	"testing"
	"time"

	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/pkg/config/diagnosticmode"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/util/dbterror/plannererrors"
	"github.com/pingcap/tidb/pkg/util/diagnosticclient"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/tikvrpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type diagnosticRPCClient struct {
	tikv.Client
	sendCount int
}

func (c *diagnosticRPCClient) SendRequest(_ context.Context, _ string, _ *tikvrpc.Request, _ time.Duration) (*tikvrpc.Response, error) {
	c.sendCount++
	return &tikvrpc.Response{}, nil
}

func TestDiagnosticSQLAllowlist(t *testing.T) {
	p := parser.New()
	tests := []struct {
		name string
		sql  string
		want bool
	}{
		{name: "select", sql: "explain select 1", want: true},
		{name: "union", sql: "explain select 1 union all select 2", want: true},
		{name: "cte", sql: "explain with cte as (select 1 as a) select a from cte", want: true},
		{name: "stored generated column", sql: "explain select stored_col from t where stored_col > 0", want: true},
		{name: "virtual generated column", sql: "explain select virtual_col from t where virtual_col > 0", want: true},
		{name: "inner join", sql: "explain select * from t1 join t2 on t1.a = t2.a", want: true},
		{name: "left outer join", sql: "explain select * from t1 left join t2 on t1.a = t2.a", want: true},
		{name: "right outer join", sql: "explain select * from t1 right join t2 on t1.a = t2.a", want: true},
		{name: "union all", sql: "explain select a from t1 union all select a from t2", want: true},
		{name: "union distinct", sql: "explain select a from t1 union select a from t2", want: true},
		{name: "tiflash table scan", sql: "explain select /*+ read_from_storage(tiflash[t]) */ * from t", want: true},
		{name: "tiflash mpp one phase aggregation", sql: "explain select /*+ read_from_storage(tiflash[t]), mpp_1phase_agg() */ count(*) from t group by a", want: true},
		{name: "tiflash mpp two phase aggregation", sql: "explain select /*+ read_from_storage(tiflash[t]), mpp_2phase_agg() */ count(*) from t group by a", want: true},
		{name: "tiflash mpp broadcast join", sql: "explain select /*+ read_from_storage(tiflash[t1, t2]), broadcast_join(t1, t2) */ * from t1 join t2 on t1.a = t2.a", want: true},
		{name: "tiflash mpp shuffle join", sql: "explain select /*+ read_from_storage(tiflash[t1, t2]), shuffle_join(t1, t2) */ * from t1 join t2 on t1.a = t2.a", want: true},
		{name: "scalar subquery", sql: "explain select (select 1)", want: false},
		{name: "where scalar subquery", sql: "explain select * from rider_reputation where lifetime_score < (select min(country_code) from rider_reputation)", want: false},
		{name: "exists subquery", sql: "explain select 1 where exists (select 1)", want: false},
		{name: "use", sql: "use test", want: true},
		{name: "show databases", sql: "show databases", want: true},
		{name: "show tables", sql: "show tables", want: true},
		{name: "show create table", sql: "show create table mysql.user", want: true},
		{name: "show create database", sql: "show create database test", want: true},
		{name: "show where assignment", sql: "show tables where @x := 1", want: false},
		{name: "show regions", sql: "show table t regions", want: false},
		{name: "show backups", sql: "show backups", want: false},
		{name: "analyze", sql: "explain analyze select 1", want: false},
		{name: "insert", sql: "explain insert into t values (1)", want: false},
		{name: "update", sql: "explain update t set a = 1", want: false},
		{name: "delete", sql: "explain delete from t", want: false},
		{name: "show unsupported", sql: "show profile", want: false},
		{name: "transaction", sql: "begin", want: false},
		{name: "ddl", sql: "create table t (a int)", want: false},
		{name: "prepared", sql: "prepare s from 'select 1'", want: false},
		{name: "lock", sql: "explain select * from t for update", want: false},
		{name: "outfile", sql: "explain select 1 into outfile '/tmp/diagnostic.out'", want: false},
		{name: "nextval", sql: "explain select nextval(seq)", want: false},
		{name: "setval", sql: "explain select setval(seq, 1)", want: false},
		{name: "advisory lock", sql: "explain select get_lock('diagnostic', 0)", want: false},
		{name: "last insert id assignment", sql: "explain select last_insert_id(1)", want: false},
		{name: "set var hint", sql: "explain select /*+ set_var(max_execution_time=1) */ 1", want: false},
		{name: "variable assignment", sql: "explain select @x := 1", want: false},
		{name: "nested assignment", sql: "explain select * from (select @x := 1 as a) t", want: false},
		{name: "union values", sql: "explain select 1 union all values row(2)", want: false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			stmt, err := p.ParseOneStmt(test.sql, "", "")
			require.NoError(t, err)
			require.Equal(t, test.want, isDiagnosticSQLAllowed(stmt))
		})
	}

	stmt, err := p.ParseOneStmt("explain select 1", "", "")
	require.NoError(t, err)
	require.IsType(t, &ast.ExplainStmt{}, stmt)
}

func TestDiagnosticSQLExecutionGuard(t *testing.T) {
	store, dom := CreateStoreAndBootstrap(t)
	t.Cleanup(func() {
		dom.Close()
		require.NoError(t, store.Close())
	})
	se := CreateSessionAndSetID(t, store)
	t.Cleanup(se.Close)

	t.Run("diagnostic mode", func(t *testing.T) {
		restore := diagnosticmode.SetForTest(true)
		t.Cleanup(restore)

		resultSets, err := se.Execute(context.Background(), "explain select 1")
		require.NoError(t, err)
		require.Len(t, resultSets, 1)
		require.NoError(t, resultSets[0].Close())

		for _, sql := range []string{
			"use test",
			"show databases",
			"show tables",
			"show create table mysql.user",
		} {
			resultSets, err = se.Execute(context.Background(), sql)
			require.NoError(t, err, sql)
			for _, resultSet := range resultSets {
				require.NoError(t, resultSet.Close(), sql)
			}
		}

		for _, sql := range []string{
			"select 1",
			"insert into t values (1)",
			"begin",
			"create table t (a int)",
			"explain analyze select 1",
			"explain select 1 for update",
			"set global tidb_distsql_scan_concurrency=5;",
			"explain select * from t where t.a < (select max(b) from t);",
			// DDL and DML statements from the SQL coverage cases.
			"drop database if exists tidb_sql_test",
			"create database tidb_sql_test",
			"alter database tidb_sql_test character set utf8mb4 collate utf8mb4_bin",
			"update dml_t set score = score + 1 where id = 1",
			"replace into dml_t(id, name, score, category) values (2, 'Bob-replaced', 99, 1)",
			"delete from dml_t where id = 100",
			"batch on id limit 2 delete from batch_t where v < 40",
			"create view high_score_view as select id, name, score from dml_t where score >= 80",
			"drop view high_score_view",
			"create sequence test_seq start with 1 increment by 1",
			"alter sequence test_seq increment by 2",
			"drop sequence test_seq",
			"truncate table truncate_t",
			"drop table truncate_t",
			"drop table ddl_t_copy2",

			// ALTER TABLE cases.
			"alter table t add column col_add int default 100",
			"alter table t add column col_first int default 0 first",
			"alter table t add column col_after int default 0 after a",
			"alter table t add column if not exists col_if_not_exists int",
			"alter table t add column col_multi_1 int default 1, add column col_multi_2 varchar(20) default 'x'",
			"alter table t modify column c varchar(100) default 'abc'",
			"alter table t modify column col_after int default 0 after b",
			"alter table t change column col_add col_changed bigint default 100",
			"alter table t rename column col_changed to col_renamed",
			"alter table t alter column b set default 999",
			"alter table t alter column b drop default",
			"alter table t drop column col_renamed",
			"alter table t drop column if exists col_if_not_exists",
			"alter table t add index idx_c(c)",
			"alter table unique_test add unique index uk_email(email)",
			"alter table t add index if not exists idx_d(d)",
			"alter table t alter index idx_c invisible",
			"alter table t alter index idx_c visible",
			"alter table t rename index idx_c to idx_c_new",
			"alter table t drop index idx_c_new",
			"alter table t drop index if exists idx_d",
			"alter table pk_test add primary key(id)",
			"alter table pk_test drop primary key",
			"alter table t add constraint chk_a check (a >= 0)",
			"alter table t alter check chk_a not enforced",
			"alter table t alter check chk_a enforced",
			"alter table t drop check chk_a",
			"alter table child_t add constraint fk_child_parent foreign key(parent_id) references parent_t(id) on delete cascade on update cascade",
			"alter table child_t drop foreign key fk_child_parent",
			"alter table t convert to character set utf8mb4 collate utf8mb4_bin",
			"alter table t default character set utf8mb4 collate utf8mb4_bin",
			"alter table rename_test rename to rename_test_new",
			"alter table rename_test_new rename to rename_test",
			"alter table t auto_increment = 10000",
			"alter table t auto_id_cache = 100",
			"alter table shard_test shard_row_id_bits = 4",
			"alter table cache_test cache",
			"alter table cache_test nocache",
			"alter table ttl_test ttl = created_at + interval 365 day",
			"alter table ttl_test ttl_enable = 'OFF'",
			"alter table ttl_test ttl_enable = 'ON'",
			"alter table ttl_test ttl_job_interval = '2h'",
			"alter table ttl_test remove ttl",
			"alter table t add column virtual_sum bigint generated always as (a + b) virtual",
			"alter table t add column stored_sum bigint generated always as (a + b) stored",
			"alter table t add index idx_virtual_sum(virtual_sum)",
			"alter table partition_add_test add partition (partition p2 values less than (300))",
			"alter table partition_test truncate partition p0",
			"alter table partition_add_test drop partition p2",
			"alter table partition_test reorganize partition p2 into (partition p2a values less than (250), partition p2b values less than (300))",
			"alter table partition_test exchange partition p0 with table exchange_test with validation",
			"alter table partition_test analyze partition p1",
			"alter table partition_test analyze partition p1 index primary",
			"alter table t disable keys",
			"alter table t enable keys",
			"alter table t algorithm=instant, add column algo_test int",
			"alter table t lock=none, add column lock_test int",
			"alter table t force",
			"alter table t add column multi_a int, add column multi_b varchar(20), add index idx_multi_a(multi_a)",
			"alter table t drop index idx_multi_a, drop column multi_a, drop column multi_b",

			// EXPLAIN statements that are not read-only SELECT plans.
			"explain delete from t1 where a = 20",
			"explain update t1 set b = b + 1 where a = 20",
			"explain insert into t1(id, a, b, name) values (10, 100, 1000, 'insert-test')",
			"explain insert into t1(id, a, b, name) select id + 100, a, b, name from t1 where id <= 2",
			"explain insert into t1(id, a, b, name) values (1, 99, 999, 'dup') on duplicate key update a = values(a), b = values(b)",
			"explain replace into t1(id, a, b, name) values (1, 100, 1000, 'replace-test')",
			"explain format = 'brief' delete from t1 where b > 200",
			"explain format = 'verbose' update t1 set b = b + 100 where a >= 20",
			"explain format = 'brief' insert into t1(id, a, b, name) select id + 200, a, b, name from t1",
			"explain format = 'brief' replace into t1 values (10, 10, 10, 'x')",
			"explain analyze select * from t1 where a >= 20",
			"explain analyze delete from explain_delete_test where id = 1",
			"explain analyze update explain_update_test set b = b + 1 where id = 1",
			"explain analyze insert into explain_insert_test values (1, 10, 100, 'inserted')",
			"explain analyze replace into explain_replace_test values (1, 100, 1000, 'after')",

			// Transaction, import, privilege, ANALYZE, and ADMIN statements.
			"commit",
			"start transaction",
			"begin pessimistic",
			"begin optimistic",
			"start transaction with consistent snapshot",
			"start transaction read write",
			"start transaction read only",
			"rollback",
			"savepoint sp1",
			"set autocommit = 0",
			"import into import_target from select id, value, name from import_src",
			"drop user if exists 'sql_test_user'@'%'",
			"drop user if exists 'sql_test_user_renamed'@'%'",
			"drop role if exists 'sql_test_read_role'@'%'",
			"drop role if exists 'sql_test_write_role'@'%'",
			"create user 'sql_test_user'@'%' identified by 'TestPassword_123!'",
			"grant select on misc_sql_test.* to 'sql_test_user'@'%'",
			"grant insert, update, delete on misc_sql_test.txn_test to 'sql_test_user'@'%'",
			"grant select on misc_sql_test.import_src to 'sql_test_user'@'%'",
			"grant select on misc_sql_test.import_target to 'sql_test_user'@'%' with grant option",
			"revoke delete on misc_sql_test.txn_test from 'sql_test_user'@'%'",
			"revoke grant option on misc_sql_test.import_target from 'sql_test_user'@'%'",
			"alter user 'sql_test_user'@'%' identified by 'TestPassword_456!'",
			"set password for 'sql_test_user'@'%' = 'TestPassword_789!'",
			"create role 'sql_test_read_role'@'%'",
			"create role 'sql_test_write_role'@'%'",
			"grant select on misc_sql_test.* to 'sql_test_read_role'@'%'",
			"grant insert, update, delete on misc_sql_test.* to 'sql_test_write_role'@'%'",
			"grant 'sql_test_read_role'@'%', 'sql_test_write_role'@'%' to 'sql_test_user'@'%'",
			"analyze table t1",
			"analyze table t1, t2",
			"analyze table t1 index",
			"analyze table t1 index idx_a",
			"analyze table t1 index idx_a, idx_b",
			"analyze table t1 columns a",
			"analyze table t1 columns a, b, d",
			"analyze table t1 predicate columns",
			"admin recover index t1 idx_a",
			"admin cleanup index t1 idx_a",
			"admin cleanup table lock t1",
			"admin cleanup table lock t1, t2",
			"admin reload expr_pushdown_blacklist",
			"admin reload opt_rule_blacklist",
			"admin reload bindings",
			"admin reload stats_extended",
			"admin reload statistics",
			"admin flush bindings",
			"admin capture bindings",
			"admin evolve bindings",
			"admin flush session plan_cache",
			"admin flush instance plan_cache",
		} {
			_, err = se.Execute(context.Background(), sql)
			require.Error(t, err, sql)
			require.True(t, terror.ErrorEqual(err, plannererrors.ErrSQLInReadOnlyMode), sql)
		}

		// Binary protocol prepare is rejected before it can prepare a transaction;
		// it cannot be used to bypass the SQL allowlist.
		_, _, _, err = se.PrepareStmt("select 1")
		require.Error(t, err)
		require.True(t, terror.ErrorEqual(err, plannererrors.ErrSQLInReadOnlyMode))

		// Internal restricted SQL is used by diagnostic startup and must remain
		// available even while the process-wide diagnostic mode is enabled.
		rs, err := se.ExecuteInternal(kv.WithInternalSourceType(context.Background(), kv.InternalTxnOthers), "select 1")
		require.NoError(t, err)
		require.NoError(t, rs.Close())

		// The diagnostic KV RPC wrapper forwards read RPCs in its allowlist and
		// rejects write RPCs before they reach the underlying client.
		rpcClient := &diagnosticRPCClient{}
		guardedRPCClient := diagnosticclient.WrapKV(rpcClient)
		_, err = guardedRPCClient.SendRequest(
			context.Background(),
			"store-address",
			tikvrpc.NewRequest(tikvrpc.CmdGet, &kvrpcpb.GetRequest{}),
			time.Second,
		)
		require.NoError(t, err)
		require.Equal(t, 1, rpcClient.sendCount)

		_, err = guardedRPCClient.SendRequest(
			context.Background(),
			"store-address",
			tikvrpc.NewRequest(tikvrpc.CmdPrewrite, &kvrpcpb.PrewriteRequest{}),
			time.Second,
		)
		require.Error(t, err)
		require.Equal(t, codes.PermissionDenied, status.Code(err))
		require.Equal(t, 1, rpcClient.sendCount)
	})

	t.Run("normal mode", func(t *testing.T) {
		restore := diagnosticmode.SetForTest(false)
		t.Cleanup(restore)
		resultSets, err := se.Execute(context.Background(), "select 1")
		require.NoError(t, err)
		require.Len(t, resultSets, 1)
		require.NoError(t, resultSets[0].Close())
	})
}
