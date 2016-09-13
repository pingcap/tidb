package ast_test

import (
	. "github.com/pingcap/check"
	. "github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/parser"
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
	stmts := []Node{
		(&AdminStmt{}),
		(&BeginStmt{}),
		(&BinlogStmt{}),
		(&CommitStmt{}),
		(&CreateUserStmt{}),
		(&DeallocateStmt{}),
		(&DoStmt{}),
		(&ExecuteStmt{UsingVars: []ExprNode{&ValueExpr{}}}),
		(&ExplainStmt{Stmt: &ShowStmt{}}),
		(&GrantStmt{}),
		(&PrepareStmt{SQLVar: &VariableExpr{Value: &ValueExpr{}}}),
		(&RollbackStmt{}),
		(&SetPwdStmt{}),
		(&SetStmt{Variables: []*VariableAssignment{
			{
				Value: &ValueExpr{},
			},
		}}),
		(&UseStmt{}),
		(&AnalyzeTableStmt{
			TableNames: []*TableName{
				{},
			},
		}),
		(&FlushTableStmt{}),
		(&PrivElem{}),
		(&VariableAssignment{Value: &ValueExpr{}}),
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
create index t_i on t (id);
create database test CHARACTER SET utf8;
drop database test;
drop index t_i on t;
drop table t;
truncate t;
CREATE TABLE t (
jobAbbr char(4) NOT NULL,
CONSTRAINT FOREIGN KEY (jobAbbr) REFERENCES ffxi_jobType (jobAbbr) ON DELETE CASCADE ON UPDATE CASCADE
);
`
	parser := parser.New()
	stmts, err := parser.Parse(sql, "", "")
	c.Assert(err, IsNil)
	for _, stmt := range stmts {
		stmt.Accept(visitor{})
		stmt.Accept(visitor1{})
	}
}

func (ts *testMiscSuite) TestDMLVistorCover(c *C) {
	sql := `DELETE FROM somelog WHERE user = 'jcole' ORDER BY timestamp_column LIMIT 1;
delete t1, t2 from t1 inner join t2 inner join t3 where t1.id=t2.id and t2.id=t3.id;
select * from t where exists(select * from t k where t.c = k.c having sum(c) = 1);
INSERT INTO t_copy SELECT * FROM t WHERE t.x > 5;
(SELECT a FROM t1 WHERE a=10 AND B=1) UNION (SELECT a FROM t2 WHERE a=11 AND B=2) ORDER BY a LIMIT 10;
UPDATE t1 SET col1 = col1 + 1, col2 = col1;
SHOW CREATE TABLE t;
load data infile '/tmp/t.csv' into table t fields terminated by 'ab' enclosed by 'b';`

	parser := parser.New()
	stmts, err := parser.Parse(sql, "", "")
	c.Assert(err, IsNil)
	for _, stmt := range stmts {
		stmt.Accept(visitor{})
		stmt.Accept(visitor1{})
	}
}
