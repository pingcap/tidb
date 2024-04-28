// Copyright 2016 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package ast_test

import (
	"fmt"
	"testing"

	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type visitor struct{}

func (v visitor) Enter(in ast.Node) (ast.Node, bool) {
	return in, false
}

func (v visitor) Leave(in ast.Node) (ast.Node, bool) {
	return in, true
}

type visitor1 struct {
	visitor
}

func (visitor1) Enter(in ast.Node) (ast.Node, bool) {
	return in, true
}

func TestMiscVisitorCover(t *testing.T) {
	valueExpr := ast.NewValueExpr(42, mysql.DefaultCharset, mysql.DefaultCollationName)
	stmts := []ast.Node{
		&ast.AdminStmt{},
		&ast.AlterUserStmt{},
		&ast.BeginStmt{},
		&ast.BinlogStmt{},
		&ast.CommitStmt{},
		&ast.CompactTableStmt{Table: &ast.TableName{}},
		&ast.CreateUserStmt{},
		&ast.DeallocateStmt{},
		&ast.DoStmt{},
		&ast.ExecuteStmt{UsingVars: []ast.ExprNode{valueExpr}},
		&ast.ExplainStmt{Stmt: &ast.ShowStmt{}},
		&ast.GrantStmt{},
		&ast.PrepareStmt{SQLVar: &ast.VariableExpr{Value: valueExpr}},
		&ast.RollbackStmt{},
		&ast.SetPwdStmt{},
		&ast.SetStmt{Variables: []*ast.VariableAssignment{
			{
				Value: valueExpr,
			},
		}},
		&ast.UseStmt{},
		&ast.AnalyzeTableStmt{
			TableNames: []*ast.TableName{
				{},
			},
		},
		&ast.FlushStmt{},
		&ast.PrivElem{},
		&ast.VariableAssignment{Value: valueExpr},
		&ast.KillStmt{},
		&ast.DropStatsStmt{
			Tables: []*ast.TableName{
				{},
			},
		},
		&ast.ShutdownStmt{},
	}

	for _, v := range stmts {
		v.Accept(visitor{})
		v.Accept(visitor1{})
	}
}

func TestDDLVisitorCoverMisc(t *testing.T) {
	sql := `
create table t (c1 smallint unsigned, c2 int unsigned);
alter table t add column a smallint unsigned after b;
alter table t add column (a int, constraint check (a > 0));
create index t_i on t (id);
create database test character set utf8;
drop database test;
drop index t_i on t;
drop table t;
truncate t;
create table t (
jobAbbr char(4) not null,
constraint foreign key (jobabbr) references ffxi_jobtype (jobabbr) on delete cascade on update cascade
);
`
	parse := parser.New()
	stmts, _, err := parse.Parse(sql, "", "")
	require.NoError(t, err)
	for _, stmt := range stmts {
		stmt.Accept(visitor{})
		stmt.Accept(visitor1{})
	}
}

func TestDMLVistorCover(t *testing.T) {
	sql := `delete from somelog where user = 'jcole' order by timestamp_column limit 1;
delete t1, t2 from t1 inner join t2 inner join t3 where t1.id=t2.id and t2.id=t3.id;
select * from t where exists(select * from t k where t.c = k.c having sum(c) = 1);
insert into t_copy select * from t where t.x > 5;
(select /*+ TIDB_INLJ(t1) */ a from t1 where a=10 and b=1) union (select /*+ TIDB_SMJ(t2) */ a from t2 where a=11 and b=2) order by a limit 10;
update t1 set col1 = col1 + 1, col2 = col1;
show create table t;
load data infile '/tmp/t.csv' into table t fields terminated by 'ab' enclosed by 'b';
import into t from '/file.csv'`

	p := parser.New()
	stmts, _, err := p.Parse(sql, "", "")
	require.NoError(t, err)
	for _, stmt := range stmts {
		stmt.Accept(visitor{})
		stmt.Accept(visitor1{})
	}
}

// test Change Pump or drainer status sql parser
func TestChangeStmt(t *testing.T) {
	sql := `change pump to node_state='paused' for node_id '127.0.0.1:8249';
change drainer to node_state='paused' for node_id '127.0.0.1:8249';
shutdown;`

	p := parser.New()
	stmts, _, err := p.Parse(sql, "", "")
	require.NoError(t, err)
	for _, stmt := range stmts {
		stmt.Accept(visitor{})
		stmt.Accept(visitor1{})
	}
}

func TestSensitiveStatement(t *testing.T) {
	positive := []ast.StmtNode{
		&ast.SetPwdStmt{},
		&ast.CreateUserStmt{},
		&ast.AlterUserStmt{},
		&ast.GrantStmt{},
	}
	for i, stmt := range positive {
		_, ok := stmt.(ast.SensitiveStmtNode)
		require.Truef(t, ok, "%d, %#v fail", i, stmt)
	}

	negative := []ast.StmtNode{
		&ast.DropUserStmt{},
		&ast.RevokeStmt{},
		&ast.AlterTableStmt{},
		&ast.CreateDatabaseStmt{},
		&ast.CreateIndexStmt{},
		&ast.CreateTableStmt{},
		&ast.DropDatabaseStmt{},
		&ast.DropIndexStmt{},
		&ast.DropTableStmt{},
		&ast.RenameTableStmt{},
		&ast.TruncateTableStmt{},
	}
	for _, stmt := range negative {
		_, ok := stmt.(ast.SensitiveStmtNode)
		require.False(t, ok)
	}
}

func TestUserSpec(t *testing.T) {
	hashString := "*3D56A309CD04FA2EEF181462E59011F075C89548"
	u := ast.UserSpec{
		User: &auth.UserIdentity{
			Username: "test",
		},
		AuthOpt: &ast.AuthOption{
			ByAuthString: false,
			AuthString:   "xxx",
			HashString:   hashString,
		},
	}
	pwd, ok := u.EncodedPassword()
	require.True(t, ok)
	require.Equal(t, u.AuthOpt.HashString, pwd)

	u.AuthOpt.HashString = "not-good-password-format"
	_, ok = u.EncodedPassword()
	require.False(t, ok)

	u.AuthOpt.ByAuthString = true
	pwd, ok = u.EncodedPassword()
	require.True(t, ok)
	require.Equal(t, hashString, pwd)

	u.AuthOpt.AuthString = ""
	pwd, ok = u.EncodedPassword()
	require.True(t, ok)
	require.Equal(t, "", pwd)
}

func TestTableOptimizerHintRestore(t *testing.T) {
	testCases := []NodeRestoreTestCase{
		{"USE_INDEX(t1 c1)", "USE_INDEX(`t1` `c1`)"},
		{"USE_INDEX(test.t1 c1)", "USE_INDEX(`test`.`t1` `c1`)"},
		{"USE_INDEX(@sel_1 t1 c1)", "USE_INDEX(@`sel_1` `t1` `c1`)"},
		{"USE_INDEX(t1@sel_1 c1)", "USE_INDEX(`t1`@`sel_1` `c1`)"},
		{"USE_INDEX(test.t1@sel_1 c1)", "USE_INDEX(`test`.`t1`@`sel_1` `c1`)"},
		{"USE_INDEX(test.t1@sel_1 partition(p0) c1)", "USE_INDEX(`test`.`t1`@`sel_1` PARTITION(`p0`) `c1`)"},
		{"FORCE_INDEX(t1 c1)", "FORCE_INDEX(`t1` `c1`)"},
		{"FORCE_INDEX(test.t1 c1)", "FORCE_INDEX(`test`.`t1` `c1`)"},
		{"FORCE_INDEX(@sel_1 t1 c1)", "FORCE_INDEX(@`sel_1` `t1` `c1`)"},
		{"FORCE_INDEX(t1@sel_1 c1)", "FORCE_INDEX(`t1`@`sel_1` `c1`)"},
		{"FORCE_INDEX(test.t1@sel_1 c1)", "FORCE_INDEX(`test`.`t1`@`sel_1` `c1`)"},
		{"FORCE_INDEX(test.t1@sel_1 partition(p0) c1)", "FORCE_INDEX(`test`.`t1`@`sel_1` PARTITION(`p0`) `c1`)"},
		{"IGNORE_INDEX(t1 c1)", "IGNORE_INDEX(`t1` `c1`)"},
		{"IGNORE_INDEX(@sel_1 t1 c1)", "IGNORE_INDEX(@`sel_1` `t1` `c1`)"},
		{"IGNORE_INDEX(t1@sel_1 c1)", "IGNORE_INDEX(`t1`@`sel_1` `c1`)"},
		{"IGNORE_INDEX(t1@sel_1 partition(p0, p1) c1)", "IGNORE_INDEX(`t1`@`sel_1` PARTITION(`p0`, `p1`) `c1`)"},
		{"ORDER_INDEX(t1 c1)", "ORDER_INDEX(`t1` `c1`)"},
		{"ORDER_INDEX(test.t1 c1)", "ORDER_INDEX(`test`.`t1` `c1`)"},
		{"ORDER_INDEX(@sel_1 t1 c1)", "ORDER_INDEX(@`sel_1` `t1` `c1`)"},
		{"ORDER_INDEX(t1@sel_1 c1)", "ORDER_INDEX(`t1`@`sel_1` `c1`)"},
		{"ORDER_INDEX(test.t1@sel_1 c1)", "ORDER_INDEX(`test`.`t1`@`sel_1` `c1`)"},
		{"ORDER_INDEX(test.t1@sel_1 partition(p0) c1)", "ORDER_INDEX(`test`.`t1`@`sel_1` PARTITION(`p0`) `c1`)"},
		{"NO_ORDER_INDEX(t1 c1)", "NO_ORDER_INDEX(`t1` `c1`)"},
		{"NO_ORDER_INDEX(test.t1 c1)", "NO_ORDER_INDEX(`test`.`t1` `c1`)"},
		{"NO_ORDER_INDEX(@sel_1 t1 c1)", "NO_ORDER_INDEX(@`sel_1` `t1` `c1`)"},
		{"NO_ORDER_INDEX(t1@sel_1 c1)", "NO_ORDER_INDEX(`t1`@`sel_1` `c1`)"},
		{"NO_ORDER_INDEX(test.t1@sel_1 c1)", "NO_ORDER_INDEX(`test`.`t1`@`sel_1` `c1`)"},
		{"NO_ORDER_INDEX(test.t1@sel_1 partition(p0) c1)", "NO_ORDER_INDEX(`test`.`t1`@`sel_1` PARTITION(`p0`) `c1`)"},
		{"TIDB_SMJ(`t1`)", "TIDB_SMJ(`t1`)"},
		{"TIDB_SMJ(t1)", "TIDB_SMJ(`t1`)"},
		{"TIDB_SMJ(t1,t2)", "TIDB_SMJ(`t1`, `t2`)"},
		{"TIDB_SMJ(@sel1 t1,t2)", "TIDB_SMJ(@`sel1` `t1`, `t2`)"},
		{"TIDB_SMJ(t1@sel1,t2@sel2)", "TIDB_SMJ(`t1`@`sel1`, `t2`@`sel2`)"},
		{"TIDB_INLJ(t1,t2)", "TIDB_INLJ(`t1`, `t2`)"},
		{"TIDB_INLJ(@sel1 t1,t2)", "TIDB_INLJ(@`sel1` `t1`, `t2`)"},
		{"TIDB_INLJ(t1@sel1,t2@sel2)", "TIDB_INLJ(`t1`@`sel1`, `t2`@`sel2`)"},
		{"TIDB_HJ(t1,t2)", "TIDB_HJ(`t1`, `t2`)"},
		{"TIDB_HJ(@sel1 t1,t2)", "TIDB_HJ(@`sel1` `t1`, `t2`)"},
		{"TIDB_HJ(t1@sel1,t2@sel2)", "TIDB_HJ(`t1`@`sel1`, `t2`@`sel2`)"},
		{"MERGE_JOIN(t1,t2)", "MERGE_JOIN(`t1`, `t2`)"},
		{"BROADCAST_JOIN(t1,t2)", "BROADCAST_JOIN(`t1`, `t2`)"},
		{"INL_HASH_JOIN(t1,t2)", "INL_HASH_JOIN(`t1`, `t2`)"},
		{"INL_MERGE_JOIN(t1,t2)", "INL_MERGE_JOIN(`t1`, `t2`)"},
		{"INL_JOIN(t1,t2)", "INL_JOIN(`t1`, `t2`)"},
		{"HASH_JOIN(t1,t2)", "HASH_JOIN(`t1`, `t2`)"},
		{"HASH_JOIN_BUILD(t1)", "HASH_JOIN_BUILD(`t1`)"},
		{"HASH_JOIN_PROBE(t1)", "HASH_JOIN_PROBE(`t1`)"},
		{"LEADING(t1)", "LEADING(`t1`)"},
		{"LEADING(t1, c1)", "LEADING(`t1`, `c1`)"},
		{"LEADING(t1, c1, t2)", "LEADING(`t1`, `c1`, `t2`)"},
		{"LEADING(@sel1 t1, c1)", "LEADING(@`sel1` `t1`, `c1`)"},
		{"LEADING(@sel1 t1)", "LEADING(@`sel1` `t1`)"},
		{"LEADING(@sel1 t1, c1, t2)", "LEADING(@`sel1` `t1`, `c1`, `t2`)"},
		{"LEADING(t1@sel1)", "LEADING(`t1`@`sel1`)"},
		{"LEADING(t1@sel1, c1)", "LEADING(`t1`@`sel1`, `c1`)"},
		{"LEADING(t1@sel1, c1, t2)", "LEADING(`t1`@`sel1`, `c1`, `t2`)"},
		{"MAX_EXECUTION_TIME(3000)", "MAX_EXECUTION_TIME(3000)"},
		{"MAX_EXECUTION_TIME(@sel1 3000)", "MAX_EXECUTION_TIME(@`sel1` 3000)"},
		{"USE_INDEX_MERGE(t1 c1)", "USE_INDEX_MERGE(`t1` `c1`)"},
		{"USE_INDEX_MERGE(@sel1 t1 c1)", "USE_INDEX_MERGE(@`sel1` `t1` `c1`)"},
		{"USE_INDEX_MERGE(t1@sel1 c1)", "USE_INDEX_MERGE(`t1`@`sel1` `c1`)"},
		{"USE_TOJA(TRUE)", "USE_TOJA(TRUE)"},
		{"USE_TOJA(FALSE)", "USE_TOJA(FALSE)"},
		{"USE_TOJA(@sel1 TRUE)", "USE_TOJA(@`sel1` TRUE)"},
		{"USE_CASCADES(TRUE)", "USE_CASCADES(TRUE)"},
		{"USE_CASCADES(FALSE)", "USE_CASCADES(FALSE)"},
		{"USE_CASCADES(@sel1 TRUE)", "USE_CASCADES(@`sel1` TRUE)"},
		{"QUERY_TYPE(OLAP)", "QUERY_TYPE(OLAP)"},
		{"QUERY_TYPE(OLTP)", "QUERY_TYPE(OLTP)"},
		{"QUERY_TYPE(@sel1 OLTP)", "QUERY_TYPE(@`sel1` OLTP)"},
		{"NTH_PLAN(10)", "NTH_PLAN(10)"},
		{"NTH_PLAN(@sel1 30)", "NTH_PLAN(@`sel1` 30)"},
		{"MEMORY_QUOTA(1 GB)", "MEMORY_QUOTA(1024 MB)"},
		{"MEMORY_QUOTA(@sel1 1 GB)", "MEMORY_QUOTA(@`sel1` 1024 MB)"},
		{"HASH_AGG()", "HASH_AGG()"},
		{"HASH_AGG(@sel1)", "HASH_AGG(@`sel1`)"},
		{"STREAM_AGG()", "STREAM_AGG()"},
		{"STREAM_AGG(@sel1)", "STREAM_AGG(@`sel1`)"},
		{"AGG_TO_COP()", "AGG_TO_COP()"},
		{"AGG_TO_COP(@sel_1)", "AGG_TO_COP(@`sel_1`)"},
		{"LIMIT_TO_COP()", "LIMIT_TO_COP()"},
		{"MERGE()", "MERGE()"},
		{"STRAIGHT_JOIN()", "STRAIGHT_JOIN()"},
		{"NO_INDEX_MERGE()", "NO_INDEX_MERGE()"},
		{"NO_INDEX_MERGE(@sel1)", "NO_INDEX_MERGE(@`sel1`)"},
		{"READ_CONSISTENT_REPLICA()", "READ_CONSISTENT_REPLICA()"},
		{"READ_CONSISTENT_REPLICA(@sel1)", "READ_CONSISTENT_REPLICA(@`sel1`)"},
		{"QB_NAME(sel1)", "QB_NAME(`sel1`)"},
		{"READ_FROM_STORAGE(@sel TIFLASH[t1, t2])", "READ_FROM_STORAGE(@`sel` TIFLASH[`t1`, `t2`])"},
		{"READ_FROM_STORAGE(@sel TIFLASH[t1 partition(p0)])", "READ_FROM_STORAGE(@`sel` TIFLASH[`t1` PARTITION(`p0`)])"},
		{"TIME_RANGE('2020-02-02 10:10:10','2020-02-02 11:10:10')", "TIME_RANGE('2020-02-02 10:10:10', '2020-02-02 11:10:10')"},
		{"RESOURCE_GROUP(rg1)", "RESOURCE_GROUP(`rg1`)"},
		{"RESOURCE_GROUP(`default`)", "RESOURCE_GROUP(`default`)"},
	}
	extractNodeFunc := func(node ast.Node) ast.Node {
		return node.(*ast.SelectStmt).TableHints[0]
	}
	runNodeRestoreTest(t, testCases, "select /*+ %s */ * from t1 join t2", extractNodeFunc)
}

func TestChangeStmtRestore(t *testing.T) {
	testCases := []NodeRestoreTestCase{
		{"CHANGE PUMP TO NODE_STATE ='paused' FOR NODE_ID '127.0.0.1:9090'", "CHANGE PUMP TO NODE_STATE ='paused' FOR NODE_ID '127.0.0.1:9090'"},
		{"CHANGE DRAINER TO NODE_STATE ='paused' FOR NODE_ID '127.0.0.1:9090'", "CHANGE DRAINER TO NODE_STATE ='paused' FOR NODE_ID '127.0.0.1:9090'"},
	}
	extractNodeFunc := func(node ast.Node) ast.Node {
		return node.(*ast.ChangeStmt)
	}
	runNodeRestoreTest(t, testCases, "%s", extractNodeFunc)
}

func TestBRIESecureText(t *testing.T) {
	testCases := []struct {
		input   string
		secured string
	}{
		{
			input:   "restore database * from 'local:///tmp/br01' snapshot = 23333",
			secured: `^\QRESTORE DATABASE * FROM 'local:///tmp/br01' SNAPSHOT = 23333\E$`,
		},
		{
			input:   "backup database * to 's3://bucket/prefix?region=us-west-2'",
			secured: `^\QBACKUP DATABASE * TO 's3://bucket/prefix?region=us-west-2'\E$`,
		},
		{
			// we need to use regexp to match to avoid the random ordering since a map was used.
			// unfortunately Go's regexp doesn't support lookahead assertion, so the test case below
			// has false positives.
			input:   "backup database * to 's3://bucket/prefix?access-key=abcdefghi&secret-access-key=123&force-path-style=true'",
			secured: `^\QBACKUP DATABASE * TO 's3://bucket/prefix?\E((access-key=xxxxxx|force-path-style=true|secret-access-key=xxxxxx)(&|'$)){3}`,
		},
		{
			input:   "backup database * to 'gcs://bucket/prefix?access-key=irrelevant&credentials-file=/home/user/secrets.txt'",
			secured: `^\QBACKUP DATABASE * TO 'gcs://bucket/prefix?\E((access-key=irrelevant|credentials-file=/home/user/secrets\.txt)(&|'$)){2}`,
		},
	}

	p := parser.New()
	for _, tc := range testCases {
		comment := fmt.Sprintf("input = %s", tc.input)
		node, err := p.ParseOneStmt(tc.input, "", "")
		require.NoError(t, err, comment)
		n, ok := node.(ast.SensitiveStmtNode)
		require.True(t, ok, comment)
		require.Regexp(t, tc.secured, n.SecureText(), comment)
	}
}

func TestCompactTableStmtRestore(t *testing.T) {
	testCases := []NodeRestoreTestCase{
		{"alter table abc compact tiflash replica", "ALTER TABLE `abc` COMPACT TIFLASH REPLICA"},
		{"alter table abc compact", "ALTER TABLE `abc` COMPACT"},
		{"alter table test.abc compact", "ALTER TABLE `test`.`abc` COMPACT"},
	}
	extractNodeFunc := func(node ast.Node) ast.Node {
		return node.(*ast.CompactTableStmt)
	}
	runNodeRestoreTest(t, testCases, "%s", extractNodeFunc)
}

func TestPlanReplayerStmtRestore(t *testing.T) {
	testCases := []NodeRestoreTestCase{
		{"plan replayer dump with stats as of timestamp '2023-06-28 12:34:00' explain select * from t where a > 10",
			"PLAN REPLAYER DUMP WITH STATS AS OF TIMESTAMP _UTF8MB4'2023-06-28 12:34:00' EXPLAIN SELECT * FROM `t` WHERE `a`>10"},
		{"plan replayer dump explain analyze select * from t where a > 10",
			"PLAN REPLAYER DUMP EXPLAIN ANALYZE SELECT * FROM `t` WHERE `a`>10"},
		{"plan replayer dump with stats as of timestamp 12345 explain analyze select * from t where a > 10",
			"PLAN REPLAYER DUMP WITH STATS AS OF TIMESTAMP 12345 EXPLAIN ANALYZE SELECT * FROM `t` WHERE `a`>10"},
		{"plan replayer dump explain analyze 'test'",
			"PLAN REPLAYER DUMP EXPLAIN ANALYZE 'test'"},
		{"plan replayer dump with stats as of timestamp '12345' explain analyze 'test2'",
			"PLAN REPLAYER DUMP WITH STATS AS OF TIMESTAMP _UTF8MB4'12345' EXPLAIN ANALYZE 'test2'"},
	}
	extractNodeFunc := func(node ast.Node) ast.Node {
		return node.(*ast.PlanReplayerStmt)
	}
	runNodeRestoreTest(t, testCases, "%s", extractNodeFunc)
}

func TestRedactURL(t *testing.T) {
	type args struct {
		str string
	}
	tests := []struct {
		args args
		want string
	}{
		{args{""}, ""},
		{args{":"}, ":"},
		{args{"~/file"}, "~/file"},
		{args{"gs://bucket/file"}, "gs://bucket/file"},
		// gs don't have access-key/secret-access-key, so it will NOT be redacted
		{args{"gs://bucket/file?access-key=123"}, "gs://bucket/file?access-key=123"},
		{args{"gs://bucket/file?secret-access-key=123"}, "gs://bucket/file?secret-access-key=123"},
		{args{"s3://bucket/file"}, "s3://bucket/file"},
		{args{"s3://bucket/file?other-key=123"}, "s3://bucket/file?other-key=123"},
		{args{"s3://bucket/file?access-key=123"}, "s3://bucket/file?access-key=xxxxxx"},
		{args{"s3://bucket/file?secret-access-key=123"}, "s3://bucket/file?secret-access-key=xxxxxx"},
		// underline
		{args{"s3://bucket/file?access_key=123"}, "s3://bucket/file?access_key=xxxxxx"},
		{args{"s3://bucket/file?secret_access_key=123"}, "s3://bucket/file?secret_access_key=xxxxxx"},
	}
	for _, tt := range tests {
		t.Run(tt.args.str, func(t *testing.T) {
			got := ast.RedactURL(tt.args.str)
			if got != tt.want {
				t.Errorf("RedactURL() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDeniedByBDR(t *testing.T) {
	testCases := []struct {
		role     ast.BDRRole
		action   model.ActionType
		expected bool
	}{
		// Roles for ActionCreateSchema
		{ast.BDRRolePrimary, model.ActionCreateSchema, false},
		{ast.BDRRoleSecondary, model.ActionCreateSchema, true},
		{ast.BDRRoleNone, model.ActionCreateSchema, false},

		// Roles for ActionDropSchema
		{ast.BDRRolePrimary, model.ActionDropSchema, true},
		{ast.BDRRoleSecondary, model.ActionDropSchema, true},
		{ast.BDRRoleNone, model.ActionDropSchema, false},

		// Roles for ActionCreateTable
		{ast.BDRRolePrimary, model.ActionCreateTable, false},
		{ast.BDRRoleSecondary, model.ActionCreateTable, true},
		{ast.BDRRoleNone, model.ActionCreateTable, false},

		// Roles for ActionDropTable
		{ast.BDRRolePrimary, model.ActionDropTable, true},
		{ast.BDRRoleSecondary, model.ActionDropTable, true},
		{ast.BDRRoleNone, model.ActionDropTable, false},

		// Roles for ActionAddColumn
		{ast.BDRRolePrimary, model.ActionAddColumn, false},
		{ast.BDRRoleSecondary, model.ActionAddColumn, true},
		{ast.BDRRoleNone, model.ActionAddColumn, false},

		// Roles for ActionDropColumn
		{ast.BDRRolePrimary, model.ActionDropColumn, true},
		{ast.BDRRoleSecondary, model.ActionDropColumn, true},
		{ast.BDRRoleNone, model.ActionDropColumn, false},

		// Roles for ActionAddIndex
		{ast.BDRRolePrimary, model.ActionAddIndex, false},
		{ast.BDRRoleSecondary, model.ActionAddIndex, true},
		{ast.BDRRoleNone, model.ActionAddIndex, false},

		// Roles for ActionDropIndex
		{ast.BDRRolePrimary, model.ActionDropIndex, false},
		{ast.BDRRoleSecondary, model.ActionDropIndex, true},
		{ast.BDRRoleNone, model.ActionDropIndex, false},

		// Roles for ActionAddForeignKey
		{ast.BDRRolePrimary, model.ActionAddForeignKey, true},
		{ast.BDRRoleSecondary, model.ActionAddForeignKey, true},
		{ast.BDRRoleNone, model.ActionAddForeignKey, false},

		// Roles for ActionDropForeignKey
		{ast.BDRRolePrimary, model.ActionDropForeignKey, true},
		{ast.BDRRoleSecondary, model.ActionDropForeignKey, true},
		{ast.BDRRoleNone, model.ActionDropForeignKey, false},

		// Roles for ActionTruncateTable
		{ast.BDRRolePrimary, model.ActionTruncateTable, true},
		{ast.BDRRoleSecondary, model.ActionTruncateTable, true},
		{ast.BDRRoleNone, model.ActionTruncateTable, false},

		// Roles for ActionModifyColumn
		{ast.BDRRolePrimary, model.ActionModifyColumn, false},
		{ast.BDRRoleSecondary, model.ActionModifyColumn, true},
		{ast.BDRRoleNone, model.ActionModifyColumn, false},

		// Roles for ActionRebaseAutoID
		{ast.BDRRolePrimary, model.ActionRebaseAutoID, true},
		{ast.BDRRoleSecondary, model.ActionRebaseAutoID, true},
		{ast.BDRRoleNone, model.ActionRebaseAutoID, false},

		// Roles for ActionRenameTable
		{ast.BDRRolePrimary, model.ActionRenameTable, true},
		{ast.BDRRoleSecondary, model.ActionRenameTable, true},
		{ast.BDRRoleNone, model.ActionRenameTable, false},

		// Roles for ActionSetDefaultValue
		{ast.BDRRolePrimary, model.ActionSetDefaultValue, false},
		{ast.BDRRoleSecondary, model.ActionSetDefaultValue, true},
		{ast.BDRRoleNone, model.ActionSetDefaultValue, false},

		// Roles for ActionShardRowID
		{ast.BDRRolePrimary, model.ActionShardRowID, true},
		{ast.BDRRoleSecondary, model.ActionShardRowID, true},
		{ast.BDRRoleNone, model.ActionShardRowID, false},

		// Roles for ActionModifyTableComment
		{ast.BDRRolePrimary, model.ActionModifyTableComment, false},
		{ast.BDRRoleSecondary, model.ActionModifyTableComment, true},
		{ast.BDRRoleNone, model.ActionModifyTableComment, false},

		// Roles for ActionRenameIndex
		{ast.BDRRolePrimary, model.ActionRenameIndex, false},
		{ast.BDRRoleSecondary, model.ActionRenameIndex, true},
		{ast.BDRRoleNone, model.ActionRenameIndex, false},

		// Roles for ActionAddTablePartition
		{ast.BDRRolePrimary, model.ActionAddTablePartition, false},
		{ast.BDRRoleSecondary, model.ActionAddTablePartition, true},
		{ast.BDRRoleNone, model.ActionAddTablePartition, false},

		// Roles for ActionDropTablePartition
		{ast.BDRRolePrimary, model.ActionDropTablePartition, true},
		{ast.BDRRoleSecondary, model.ActionDropTablePartition, true},
		{ast.BDRRoleNone, model.ActionDropTablePartition, false},

		// Roles for ActionCreateView
		{ast.BDRRolePrimary, model.ActionCreateView, false},
		{ast.BDRRoleSecondary, model.ActionCreateView, true},
		{ast.BDRRoleNone, model.ActionCreateView, false},

		// Roles for ActionModifyTableCharsetAndCollate
		{ast.BDRRolePrimary, model.ActionModifyTableCharsetAndCollate, true},
		{ast.BDRRoleSecondary, model.ActionModifyTableCharsetAndCollate, true},
		{ast.BDRRoleNone, model.ActionModifyTableCharsetAndCollate, false},

		// Roles for ActionTruncateTablePartition
		{ast.BDRRolePrimary, model.ActionTruncateTablePartition, true},
		{ast.BDRRoleSecondary, model.ActionTruncateTablePartition, true},
		{ast.BDRRoleNone, model.ActionTruncateTablePartition, false},

		// Roles for ActionDropView
		{ast.BDRRolePrimary, model.ActionDropView, false},
		{ast.BDRRoleSecondary, model.ActionDropView, true},
		{ast.BDRRoleNone, model.ActionDropView, false},

		// Roles for ActionRecoverTable
		{ast.BDRRolePrimary, model.ActionRecoverTable, true},
		{ast.BDRRoleSecondary, model.ActionRecoverTable, true},
		{ast.BDRRoleNone, model.ActionRecoverTable, false},

		// Roles for ActionModifySchemaCharsetAndCollate
		{ast.BDRRolePrimary, model.ActionModifySchemaCharsetAndCollate, true},
		{ast.BDRRoleSecondary, model.ActionModifySchemaCharsetAndCollate, true},
		{ast.BDRRoleNone, model.ActionModifySchemaCharsetAndCollate, false},

		// Roles for ActionLockTable
		{ast.BDRRolePrimary, model.ActionLockTable, true},
		{ast.BDRRoleSecondary, model.ActionLockTable, true},
		{ast.BDRRoleNone, model.ActionLockTable, false},

		// Roles for ActionUnlockTable
		{ast.BDRRolePrimary, model.ActionUnlockTable, true},
		{ast.BDRRoleSecondary, model.ActionUnlockTable, true},
		{ast.BDRRoleNone, model.ActionUnlockTable, false},

		// Roles for ActionRepairTable
		{ast.BDRRolePrimary, model.ActionRepairTable, true},
		{ast.BDRRoleSecondary, model.ActionRepairTable, true},
		{ast.BDRRoleNone, model.ActionRepairTable, false},

		// Roles for ActionSetTiFlashReplica
		{ast.BDRRolePrimary, model.ActionSetTiFlashReplica, true},
		{ast.BDRRoleSecondary, model.ActionSetTiFlashReplica, true},
		{ast.BDRRoleNone, model.ActionSetTiFlashReplica, false},

		// Roles for ActionUpdateTiFlashReplicaStatus
		{ast.BDRRolePrimary, model.ActionUpdateTiFlashReplicaStatus, true},
		{ast.BDRRoleSecondary, model.ActionUpdateTiFlashReplicaStatus, true},
		{ast.BDRRoleNone, model.ActionUpdateTiFlashReplicaStatus, false},

		// Roles for ActionAddPrimaryKey
		{ast.BDRRolePrimary, model.ActionAddPrimaryKey, true},
		{ast.BDRRoleSecondary, model.ActionAddPrimaryKey, true},
		{ast.BDRRoleNone, model.ActionAddPrimaryKey, false},

		// Roles for ActionDropPrimaryKey
		{ast.BDRRolePrimary, model.ActionDropPrimaryKey, false},
		{ast.BDRRoleSecondary, model.ActionDropPrimaryKey, true},
		{ast.BDRRoleNone, model.ActionDropPrimaryKey, false},

		// Roles for ActionCreateSequence
		{ast.BDRRolePrimary, model.ActionCreateSequence, true},
		{ast.BDRRoleSecondary, model.ActionCreateSequence, true},
		{ast.BDRRoleNone, model.ActionCreateSequence, false},

		// Roles for ActionAlterSequence
		{ast.BDRRolePrimary, model.ActionAlterSequence, true},
		{ast.BDRRoleSecondary, model.ActionAlterSequence, true},
		{ast.BDRRoleNone, model.ActionAlterSequence, false},

		// Roles for ActionDropSequence
		{ast.BDRRolePrimary, model.ActionDropSequence, true},
		{ast.BDRRoleSecondary, model.ActionDropSequence, true},
		{ast.BDRRoleNone, model.ActionDropSequence, false},

		// Roles for ActionModifyTableAutoIdCache
		{ast.BDRRolePrimary, model.ActionModifyTableAutoIdCache, true},
		{ast.BDRRoleSecondary, model.ActionModifyTableAutoIdCache, true},
		{ast.BDRRoleNone, model.ActionModifyTableAutoIdCache, false},

		// Roles for ActionRebaseAutoRandomBase
		{ast.BDRRolePrimary, model.ActionRebaseAutoRandomBase, true},
		{ast.BDRRoleSecondary, model.ActionRebaseAutoRandomBase, true},
		{ast.BDRRoleNone, model.ActionRebaseAutoRandomBase, false},

		// Roles for ActionAlterIndexVisibility
		{ast.BDRRolePrimary, model.ActionAlterIndexVisibility, false},
		{ast.BDRRoleSecondary, model.ActionAlterIndexVisibility, true},
		{ast.BDRRoleNone, model.ActionAlterIndexVisibility, false},

		// Roles for ActionExchangeTablePartition
		{ast.BDRRolePrimary, model.ActionExchangeTablePartition, true},
		{ast.BDRRoleSecondary, model.ActionExchangeTablePartition, true},
		{ast.BDRRoleNone, model.ActionExchangeTablePartition, false},

		// Roles for ActionAddCheckConstraint
		{ast.BDRRolePrimary, model.ActionAddCheckConstraint, true},
		{ast.BDRRoleSecondary, model.ActionAddCheckConstraint, true},
		{ast.BDRRoleNone, model.ActionAddCheckConstraint, false},

		// Roles for ActionDropCheckConstraint
		{ast.BDRRolePrimary, model.ActionDropCheckConstraint, true},
		{ast.BDRRoleSecondary, model.ActionDropCheckConstraint, true},
		{ast.BDRRoleNone, model.ActionDropCheckConstraint, false},

		// Roles for ActionAlterCheckConstraint
		{ast.BDRRolePrimary, model.ActionAlterCheckConstraint, true},
		{ast.BDRRoleSecondary, model.ActionAlterCheckConstraint, true},
		{ast.BDRRoleNone, model.ActionAlterCheckConstraint, false},

		// Roles for ActionRenameTables
		{ast.BDRRolePrimary, model.ActionRenameTables, true},
		{ast.BDRRoleSecondary, model.ActionRenameTables, true},
		{ast.BDRRoleNone, model.ActionRenameTables, false},

		// Roles for ActionAlterTableAttributes
		{ast.BDRRolePrimary, model.ActionAlterTableAttributes, true},
		{ast.BDRRoleSecondary, model.ActionAlterTableAttributes, true},
		{ast.BDRRoleNone, model.ActionAlterTableAttributes, false},

		// Roles for ActionAlterTablePartitionAttributes
		{ast.BDRRolePrimary, model.ActionAlterTablePartitionAttributes, true},
		{ast.BDRRoleSecondary, model.ActionAlterTablePartitionAttributes, true},
		{ast.BDRRoleNone, model.ActionAlterTablePartitionAttributes, false},

		// Roles for ActionCreatePlacementPolicy
		{ast.BDRRolePrimary, model.ActionCreatePlacementPolicy, false},
		{ast.BDRRoleSecondary, model.ActionCreatePlacementPolicy, false},
		{ast.BDRRoleNone, model.ActionCreatePlacementPolicy, false},

		// Roles for ActionAlterPlacementPolicy
		{ast.BDRRolePrimary, model.ActionAlterPlacementPolicy, false},
		{ast.BDRRoleSecondary, model.ActionAlterPlacementPolicy, false},
		{ast.BDRRoleNone, model.ActionAlterPlacementPolicy, false},

		// Roles for ActionDropPlacementPolicy
		{ast.BDRRolePrimary, model.ActionDropPlacementPolicy, false},
		{ast.BDRRoleSecondary, model.ActionDropPlacementPolicy, false},
		{ast.BDRRoleNone, model.ActionDropPlacementPolicy, false},

		// Roles for ActionAlterTablePartitionPlacement
		{ast.BDRRolePrimary, model.ActionAlterTablePartitionPlacement, true},
		{ast.BDRRoleSecondary, model.ActionAlterTablePartitionPlacement, true},
		{ast.BDRRoleNone, model.ActionAlterTablePartitionPlacement, false},

		// Roles for ActionModifySchemaDefaultPlacement
		{ast.BDRRolePrimary, model.ActionModifySchemaDefaultPlacement, true},
		{ast.BDRRoleSecondary, model.ActionModifySchemaDefaultPlacement, true},
		{ast.BDRRoleNone, model.ActionModifySchemaDefaultPlacement, false},

		// Roles for ActionAlterTablePlacement
		{ast.BDRRolePrimary, model.ActionAlterTablePlacement, true},
		{ast.BDRRoleSecondary, model.ActionAlterTablePlacement, true},
		{ast.BDRRoleNone, model.ActionAlterTablePlacement, false},

		// Roles for ActionAlterCacheTable
		{ast.BDRRolePrimary, model.ActionAlterCacheTable, true},
		{ast.BDRRoleSecondary, model.ActionAlterCacheTable, true},
		{ast.BDRRoleNone, model.ActionAlterCacheTable, false},

		// Roles for ActionAlterTableStatsOptions
		{ast.BDRRolePrimary, model.ActionAlterTableStatsOptions, true},
		{ast.BDRRoleSecondary, model.ActionAlterTableStatsOptions, true},
		{ast.BDRRoleNone, model.ActionAlterTableStatsOptions, false},

		// Roles for ActionAlterNoCacheTable
		{ast.BDRRolePrimary, model.ActionAlterNoCacheTable, true},
		{ast.BDRRoleSecondary, model.ActionAlterNoCacheTable, true},
		{ast.BDRRoleNone, model.ActionAlterNoCacheTable, false},

		// Roles for ActionCreateTables
		{ast.BDRRolePrimary, model.ActionCreateTables, false},
		{ast.BDRRoleSecondary, model.ActionCreateTables, true},
		{ast.BDRRoleNone, model.ActionCreateTables, false},

		// Roles for ActionMultiSchemaChange
		{ast.BDRRolePrimary, model.ActionMultiSchemaChange, true},
		{ast.BDRRoleSecondary, model.ActionMultiSchemaChange, true},
		{ast.BDRRoleNone, model.ActionMultiSchemaChange, false},

		// Roles for ActionFlashbackCluster
		{ast.BDRRolePrimary, model.ActionFlashbackCluster, true},
		{ast.BDRRoleSecondary, model.ActionFlashbackCluster, true},
		{ast.BDRRoleNone, model.ActionFlashbackCluster, false},

		// Roles for ActionRecoverSchema
		{ast.BDRRolePrimary, model.ActionRecoverSchema, true},
		{ast.BDRRoleSecondary, model.ActionRecoverSchema, true},
		{ast.BDRRoleNone, model.ActionRecoverSchema, false},

		// Roles for ActionReorganizePartition
		{ast.BDRRolePrimary, model.ActionReorganizePartition, true},
		{ast.BDRRoleSecondary, model.ActionReorganizePartition, true},
		{ast.BDRRoleNone, model.ActionReorganizePartition, false},

		// Roles for ActionAlterTTLInfo
		{ast.BDRRolePrimary, model.ActionAlterTTLInfo, false},
		{ast.BDRRoleSecondary, model.ActionAlterTTLInfo, true},
		{ast.BDRRoleNone, model.ActionAlterTTLInfo, false},

		// Roles for ActionAlterTTLRemove
		{ast.BDRRolePrimary, model.ActionAlterTTLRemove, false},
		{ast.BDRRoleSecondary, model.ActionAlterTTLRemove, true},
		{ast.BDRRoleNone, model.ActionAlterTTLRemove, false},

		// Roles for ActionCreateResourceGroup
		{ast.BDRRolePrimary, model.ActionCreateResourceGroup, false},
		{ast.BDRRoleSecondary, model.ActionCreateResourceGroup, false},
		{ast.BDRRoleNone, model.ActionCreateResourceGroup, false},

		// Roles for ActionAlterResourceGroup
		{ast.BDRRolePrimary, model.ActionAlterResourceGroup, false},
		{ast.BDRRoleSecondary, model.ActionAlterResourceGroup, false},
		{ast.BDRRoleNone, model.ActionAlterResourceGroup, false},

		// Roles for ActionDropResourceGroup
		{ast.BDRRolePrimary, model.ActionDropResourceGroup, false},
		{ast.BDRRoleSecondary, model.ActionDropResourceGroup, false},
		{ast.BDRRoleNone, model.ActionDropResourceGroup, false},

		// Roles for ActionAlterTablePartitioning
		{ast.BDRRolePrimary, model.ActionAlterTablePartitioning, true},
		{ast.BDRRoleSecondary, model.ActionAlterTablePartitioning, true},
		{ast.BDRRoleNone, model.ActionAlterTablePartitioning, false},

		// Roles for ActionRemovePartitioning
		{ast.BDRRolePrimary, model.ActionRemovePartitioning, true},
		{ast.BDRRoleSecondary, model.ActionRemovePartitioning, true},
		{ast.BDRRoleNone, model.ActionRemovePartitioning, false},
	}

	for _, tc := range testCases {
		assert.Equal(t, tc.expected, ast.DeniedByBDR(tc.role, tc.action, nil), fmt.Sprintf("role: %v, action: %v", tc.role, tc.action))
	}

	// test special cases
	testCases2 := []struct {
		role     ast.BDRRole
		action   model.ActionType
		job      *model.Job
		expected bool
	}{
		{
			role:   ast.BDRRolePrimary,
			action: model.ActionAddPrimaryKey,
			job: &model.Job{
				Type: model.ActionAddPrimaryKey,
				Args: []interface{}{true},
			},
			expected: true,
		},
		{
			role:   ast.BDRRolePrimary,
			action: model.ActionAddIndex,
			job: &model.Job{
				Type: model.ActionAddIndex,
				Args: []interface{}{true},
			},
			expected: true,
		},
		{
			role:   ast.BDRRolePrimary,
			action: model.ActionAddIndex,
			job: &model.Job{
				Type: model.ActionAddIndex,
				Args: []interface{}{false},
			},
			expected: false,
		},
	}

	for _, tc := range testCases2 {
		assert.Equal(t, tc.expected, ast.DeniedByBDR(tc.role, tc.action, tc.job), fmt.Sprintf("role: %v, action: %v", tc.role, tc.action))
	}
}
