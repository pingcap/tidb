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
	. "github.com/pingcap/check"
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
	. "github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/auth"
	"github.com/pingcap/parser/mysql"
)

var _ = Suite(&testMiscSuite{})

type testMiscSuite struct {
}

type visitor struct{}

func (v visitor) Enter(in Node) (Node, bool) {
	return in, false
}

func (v visitor) Leave(in Node) (Node, bool) {
	return in, true
}

type visitor1 struct {
	visitor
}

func (visitor1) Enter(in Node) (Node, bool) {
	return in, true
}

func (ts *testMiscSuite) TestMiscVisitorCover(c *C) {
	valueExpr := NewValueExpr(42, mysql.DefaultCharset, mysql.DefaultCollationName)
	stmts := []Node{
		&AdminStmt{},
		&AlterUserStmt{},
		&BeginStmt{},
		&BinlogStmt{},
		&CommitStmt{},
		&CreateUserStmt{},
		&DeallocateStmt{},
		&DoStmt{},
		&ExecuteStmt{UsingVars: []ExprNode{valueExpr}},
		&ExplainStmt{Stmt: &ShowStmt{}},
		&GrantStmt{},
		&PrepareStmt{SQLVar: &VariableExpr{Value: valueExpr}},
		&RollbackStmt{},
		&SetPwdStmt{},
		&SetStmt{Variables: []*VariableAssignment{
			{
				Value: valueExpr,
			},
		}},
		&UseStmt{},
		&AnalyzeTableStmt{
			TableNames: []*TableName{
				{},
			},
		},
		&FlushStmt{},
		&PrivElem{},
		&VariableAssignment{Value: valueExpr},
		&KillStmt{},
		&DropStatsStmt{Table: &TableName{}},
		&ShutdownStmt{},
	}

	for _, v := range stmts {
		v.Accept(visitor{})
		v.Accept(visitor1{})
	}
}

func (ts *testMiscSuite) TestDDLVisitorCover(c *C) {
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
	c.Assert(err, IsNil)
	for _, stmt := range stmts {
		stmt.Accept(visitor{})
		stmt.Accept(visitor1{})
	}
}

func (ts *testMiscSuite) TestDMLVistorCover(c *C) {
	sql := `delete from somelog where user = 'jcole' order by timestamp_column limit 1;
delete t1, t2 from t1 inner join t2 inner join t3 where t1.id=t2.id and t2.id=t3.id;
select * from t where exists(select * from t k where t.c = k.c having sum(c) = 1);
insert into t_copy select * from t where t.x > 5;
(select /*+ TIDB_INLJ(t1) */ a from t1 where a=10 and b=1) union (select /*+ TIDB_SMJ(t2) */ a from t2 where a=11 and b=2) order by a limit 10;
update t1 set col1 = col1 + 1, col2 = col1;
show create table t;
load data infile '/tmp/t.csv' into table t fields terminated by 'ab' enclosed by 'b';`

	p := parser.New()
	stmts, _, err := p.Parse(sql, "", "")
	c.Assert(err, IsNil)
	for _, stmt := range stmts {
		stmt.Accept(visitor{})
		stmt.Accept(visitor1{})
	}
}

// test Change Pump or drainer status sql parser
func (ts *testMiscSuite) TestChangeStmt(c *C) {
	sql := `change pump to node_state='paused' for node_id '127.0.0.1:8249';
change drainer to node_state='paused' for node_id '127.0.0.1:8249';
shutdown;`

	p := parser.New()
	stmts, _, err := p.Parse(sql, "", "")
	c.Assert(err, IsNil)
	for _, stmt := range stmts {
		stmt.Accept(visitor{})
		stmt.Accept(visitor1{})
	}
}

func (ts *testMiscSuite) TestSensitiveStatement(c *C) {
	positive := []StmtNode{
		&SetPwdStmt{},
		&CreateUserStmt{},
		&AlterUserStmt{},
		&GrantStmt{},
	}
	for i, stmt := range positive {
		_, ok := stmt.(SensitiveStmtNode)
		c.Assert(ok, IsTrue, Commentf("%d, %#v fail", i, stmt))
	}

	negative := []StmtNode{
		&DropUserStmt{},
		&RevokeStmt{},
		&AlterTableStmt{},
		&CreateDatabaseStmt{},
		&CreateIndexStmt{},
		&CreateTableStmt{},
		&DropDatabaseStmt{},
		&DropIndexStmt{},
		&DropTableStmt{},
		&RenameTableStmt{},
		&TruncateTableStmt{},
	}
	for _, stmt := range negative {
		_, ok := stmt.(SensitiveStmtNode)
		c.Assert(ok, IsFalse)
	}
}

func (ts *testMiscSuite) TestUserSpec(c *C) {
	hashString := "*3D56A309CD04FA2EEF181462E59011F075C89548"
	u := UserSpec{
		User: &auth.UserIdentity{
			Username: "test",
		},
		AuthOpt: &AuthOption{
			ByAuthString: false,
			AuthString:   "xxx",
			HashString:   hashString,
		},
	}
	pwd, ok := u.EncodedPassword()
	c.Assert(ok, IsTrue)
	c.Assert(pwd, Equals, u.AuthOpt.HashString)

	u.AuthOpt.HashString = "not-good-password-format"
	pwd, ok = u.EncodedPassword()
	c.Assert(ok, IsFalse)

	u.AuthOpt.ByAuthString = true
	pwd, ok = u.EncodedPassword()
	c.Assert(ok, IsTrue)
	c.Assert(pwd, Equals, hashString)

	u.AuthOpt.AuthString = ""
	pwd, ok = u.EncodedPassword()
	c.Assert(ok, IsTrue)
	c.Assert(pwd, Equals, "")
}

func (ts *testMiscSuite) TestTableOptimizerHintRestore(c *C) {
	testCases := []NodeRestoreTestCase{
		{"USE_INDEX(t1 c1)", "USE_INDEX(`t1` `c1`)"},
		{"USE_INDEX(test.t1 c1)", "USE_INDEX(`test`.`t1` `c1`)"},
		{"USE_INDEX(@sel_1 t1 c1)", "USE_INDEX(@`sel_1` `t1` `c1`)"},
		{"USE_INDEX(t1@sel_1 c1)", "USE_INDEX(`t1`@`sel_1` `c1`)"},
		{"USE_INDEX(test.t1@sel_1 c1)", "USE_INDEX(`test`.`t1`@`sel_1` `c1`)"},
		{"USE_INDEX(test.t1@sel_1 partition(p0) c1)", "USE_INDEX(`test`.`t1`@`sel_1` PARTITION(`p0`) `c1`)"},
		{"IGNORE_INDEX(t1 c1)", "IGNORE_INDEX(`t1` `c1`)"},
		{"IGNORE_INDEX(@sel_1 t1 c1)", "IGNORE_INDEX(@`sel_1` `t1` `c1`)"},
		{"IGNORE_INDEX(t1@sel_1 c1)", "IGNORE_INDEX(`t1`@`sel_1` `c1`)"},
		{"IGNORE_INDEX(t1@sel_1 partition(p0, p1) c1)", "IGNORE_INDEX(`t1`@`sel_1` PARTITION(`p0`, `p1`) `c1`)"},
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
		{"INL_JOIN(t1,t2)", "INL_JOIN(`t1`, `t2`)"},
		{"HASH_JOIN(t1,t2)", "HASH_JOIN(`t1`, `t2`)"},
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
		{"TOPN_TO_COP()", "TOPN_TO_COP()"},
		{"NO_INDEX_MERGE()", "NO_INDEX_MERGE()"},
		{"NO_INDEX_MERGE(@sel1)", "NO_INDEX_MERGE(@`sel1`)"},
		{"READ_CONSISTENT_REPLICA()", "READ_CONSISTENT_REPLICA()"},
		{"READ_CONSISTENT_REPLICA(@sel1)", "READ_CONSISTENT_REPLICA(@`sel1`)"},
		{"QB_NAME(sel1)", "QB_NAME(`sel1`)"},
		{"READ_FROM_STORAGE(@sel TIFLASH[t1, t2])", "READ_FROM_STORAGE(@`sel` TIFLASH[`t1`, `t2`])"},
		{"READ_FROM_STORAGE(@sel TIFLASH[t1 partition(p0)])", "READ_FROM_STORAGE(@`sel` TIFLASH[`t1` PARTITION(`p0`)])"},
		{"TIME_RANGE('2020-02-02 10:10:10','2020-02-02 11:10:10')", "TIME_RANGE('2020-02-02 10:10:10', '2020-02-02 11:10:10')"},
	}
	extractNodeFunc := func(node Node) Node {
		return node.(*SelectStmt).TableHints[0]
	}
	RunNodeRestoreTest(c, testCases, "select /*+ %s */ * from t1 join t2", extractNodeFunc)
}

func (ts *testMiscSuite) TestChangeStmtRestore(c *C) {
	testCases := []NodeRestoreTestCase{
		{"CHANGE PUMP TO NODE_STATE ='paused' FOR NODE_ID '127.0.0.1:9090'", "CHANGE PUMP TO NODE_STATE ='paused' FOR NODE_ID '127.0.0.1:9090'"},
		{"CHANGE DRAINER TO NODE_STATE ='paused' FOR NODE_ID '127.0.0.1:9090'", "CHANGE DRAINER TO NODE_STATE ='paused' FOR NODE_ID '127.0.0.1:9090'"},
	}
	extractNodeFunc := func(node Node) Node {
		return node.(*ChangeStmt)
	}
	RunNodeRestoreTest(c, testCases, "%s", extractNodeFunc)
}

func (ts *testMiscSuite) TestBRIESecureText(c *C) {
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

	parser := parser.New()
	for _, tc := range testCases {
		comment := Commentf("input = %s", tc.input)
		node, err := parser.ParseOneStmt(tc.input, "", "")
		c.Assert(err, IsNil, comment)
		n, ok := node.(ast.SensitiveStmtNode)
		c.Assert(ok, IsTrue, comment)
		c.Assert(n.SecureText(), Matches, tc.secured, comment)
	}
}
