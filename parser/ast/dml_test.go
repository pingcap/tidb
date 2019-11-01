// Copyright 2017 PingCAP, Inc.
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
	. "github.com/pingcap/parser/ast"
)

var _ = Suite(&testDMLSuite{})

type testDMLSuite struct {
}

func (ts *testDMLSuite) TestDMLVisitorCover(c *C) {
	ce := &checkExpr{}

	tableRefsClause := &TableRefsClause{TableRefs: &Join{Left: &TableSource{Source: &TableName{}}, On: &OnCondition{Expr: ce}}}

	stmts := []struct {
		node             Node
		expectedEnterCnt int
		expectedLeaveCnt int
	}{
		{&DeleteStmt{TableRefs: tableRefsClause, Tables: &DeleteTableList{}, Where: ce,
			Order: &OrderByClause{}, Limit: &Limit{Count: ce, Offset: ce}}, 4, 4},
		{&ShowStmt{Table: &TableName{}, Column: &ColumnName{}, Pattern: &PatternLikeExpr{Expr: ce, Pattern: ce}, Where: ce}, 3, 3},
		{&LoadDataStmt{Table: &TableName{}, Columns: []*ColumnName{{}}, FieldsInfo: &FieldsClause{}, LinesInfo: &LinesClause{}}, 0, 0},
		{&Assignment{Column: &ColumnName{}, Expr: ce}, 1, 1},
		{&ByItem{Expr: ce}, 1, 1},
		{&GroupByClause{Items: []*ByItem{{Expr: ce}, {Expr: ce}}}, 2, 2},
		{&HavingClause{Expr: ce}, 1, 1},
		{&Join{Left: &TableSource{Source: &TableName{}}}, 0, 0},
		{&Limit{Count: ce, Offset: ce}, 2, 2},
		{&OnCondition{Expr: ce}, 1, 1},
		{&OrderByClause{Items: []*ByItem{{Expr: ce}, {Expr: ce}}}, 2, 2},
		{&SelectField{Expr: ce, WildCard: &WildCardField{}}, 1, 1},
		{&TableName{}, 0, 0},
		{tableRefsClause, 1, 1},
		{&TableSource{Source: &TableName{}}, 0, 0},
		{&WildCardField{}, 0, 0},

		// TODO: cover childrens
		{&InsertStmt{Table: tableRefsClause}, 1, 1},
		{&UnionStmt{}, 0, 0},
		{&UpdateStmt{TableRefs: tableRefsClause}, 1, 1},
		{&SelectStmt{}, 0, 0},
		{&FieldList{}, 0, 0},
		{&UnionSelectList{}, 0, 0},
		{&WindowSpec{}, 0, 0},
		{&PartitionByClause{}, 0, 0},
		{&FrameClause{}, 0, 0},
		{&FrameBound{}, 0, 0},
	}

	for _, v := range stmts {
		ce.reset()
		v.node.Accept(checkVisitor{})
		c.Check(ce.enterCnt, Equals, v.expectedEnterCnt)
		c.Check(ce.leaveCnt, Equals, v.expectedLeaveCnt)
		v.node.Accept(visitor1{})
	}
}

func (tc *testDMLSuite) TestTableNameRestore(c *C) {
	testCases := []NodeRestoreTestCase{
		{"dbb.`tbb1`", "`dbb`.`tbb1`"},
		{"`tbb2`", "`tbb2`"},
		{"tbb3", "`tbb3`"},
		{"dbb.`hello-world`", "`dbb`.`hello-world`"},
		{"`dbb`.`hello-world`", "`dbb`.`hello-world`"},
		{"`dbb.HelloWorld`", "`dbb.HelloWorld`"},
	}
	extractNodeFunc := func(node Node) Node {
		return node.(*CreateTableStmt).Table
	}
	RunNodeRestoreTest(c, testCases, "CREATE TABLE %s (id VARCHAR(128) NOT NULL);", extractNodeFunc)
}

func (tc *testDMLSuite) TestTableNameIndexHintsRestore(c *C) {
	testCases := []NodeRestoreTestCase{
		{"t use index (hello)", "`t` USE INDEX (`hello`)"},
		{"t use index (hello, world)", "`t` USE INDEX (`hello`, `world`)"},
		{"t use index ()", "`t` USE INDEX ()"},
		{"t use key ()", "`t` USE INDEX ()"},
		{"t ignore key ()", "`t` IGNORE INDEX ()"},
		{"t force key ()", "`t` FORCE INDEX ()"},
		{"t use index for order by (idx1)", "`t` USE INDEX FOR ORDER BY (`idx1`)"},

		{"t use index (hello, world, yes) force key (good)", "`t` USE INDEX (`hello`, `world`, `yes`) FORCE INDEX (`good`)"},
		{"t use index (hello, world, yes) use index for order by (good)", "`t` USE INDEX (`hello`, `world`, `yes`) USE INDEX FOR ORDER BY (`good`)"},
		{"t ignore key (hello, world, yes) force key (good)", "`t` IGNORE INDEX (`hello`, `world`, `yes`) FORCE INDEX (`good`)"},

		{"t use index for group by (idx1) use index for order by (idx2)", "`t` USE INDEX FOR GROUP BY (`idx1`) USE INDEX FOR ORDER BY (`idx2`)"},
		{"t use index for group by (idx1) ignore key for order by (idx2)", "`t` USE INDEX FOR GROUP BY (`idx1`) IGNORE INDEX FOR ORDER BY (`idx2`)"},
		{"t use index for group by (idx1) ignore key for group by (idx2)", "`t` USE INDEX FOR GROUP BY (`idx1`) IGNORE INDEX FOR GROUP BY (`idx2`)"},
		{"t use index for order by (idx1) ignore key for group by (idx2)", "`t` USE INDEX FOR ORDER BY (`idx1`) IGNORE INDEX FOR GROUP BY (`idx2`)"},

		{"t use index for order by (idx1) ignore key for group by (idx2) use index (idx3)", "`t` USE INDEX FOR ORDER BY (`idx1`) IGNORE INDEX FOR GROUP BY (`idx2`) USE INDEX (`idx3`)"},
		{"t use index for order by (idx1) ignore key for group by (idx2) use index (idx3)", "`t` USE INDEX FOR ORDER BY (`idx1`) IGNORE INDEX FOR GROUP BY (`idx2`) USE INDEX (`idx3`)"},

		{"t use index (`foo``bar`) force index (`baz``1`, `xyz`)", "`t` USE INDEX (`foo``bar`) FORCE INDEX (`baz``1`, `xyz`)"},
		{"t force index (`foo``bar`) ignore index (`baz``1`, xyz)", "`t` FORCE INDEX (`foo``bar`) IGNORE INDEX (`baz``1`, `xyz`)"},
		{"t ignore index (`foo``bar`) force key (`baz``1`, xyz)", "`t` IGNORE INDEX (`foo``bar`) FORCE INDEX (`baz``1`, `xyz`)"},
		{"t ignore index (`foo``bar`) ignore key for group by (`baz``1`, xyz)", "`t` IGNORE INDEX (`foo``bar`) IGNORE INDEX FOR GROUP BY (`baz``1`, `xyz`)"},
		{"t ignore index (`foo``bar`) ignore key for order by (`baz``1`, xyz)", "`t` IGNORE INDEX (`foo``bar`) IGNORE INDEX FOR ORDER BY (`baz``1`, `xyz`)"},

		{"t use index for group by (`foo``bar`) use index for order by (`baz``1`, `xyz`)", "`t` USE INDEX FOR GROUP BY (`foo``bar`) USE INDEX FOR ORDER BY (`baz``1`, `xyz`)"},
		{"t use index for group by (`foo``bar`) ignore key for order by (`baz``1`, `xyz`)", "`t` USE INDEX FOR GROUP BY (`foo``bar`) IGNORE INDEX FOR ORDER BY (`baz``1`, `xyz`)"},
		{"t use index for group by (`foo``bar`) ignore key for group by (`baz``1`, `xyz`)", "`t` USE INDEX FOR GROUP BY (`foo``bar`) IGNORE INDEX FOR GROUP BY (`baz``1`, `xyz`)"},
		{"t use index for order by (`foo``bar`) ignore key for group by (`baz``1`, `xyz`)", "`t` USE INDEX FOR ORDER BY (`foo``bar`) IGNORE INDEX FOR GROUP BY (`baz``1`, `xyz`)"},

		{"t tt use index for order by (`foo``bar`) ignore key for group by (`baz``1`, `xyz`)", "`t` AS `tt` USE INDEX FOR ORDER BY (`foo``bar`) IGNORE INDEX FOR GROUP BY (`baz``1`, `xyz`)"},
		{"t as tt use index for order by (`foo``bar`) ignore key for group by (`baz``1`, `xyz`)", "`t` AS `tt` USE INDEX FOR ORDER BY (`foo``bar`) IGNORE INDEX FOR GROUP BY (`baz``1`, `xyz`)"},
	}
	extractNodeFunc := func(node Node) Node {
		return node.(*SelectStmt).From.TableRefs.Left
	}
	RunNodeRestoreTest(c, testCases, "SELECT * FROM %s", extractNodeFunc)
}

func (tc *testDMLSuite) TestLimitRestore(c *C) {
	testCases := []NodeRestoreTestCase{
		{"limit 10", "LIMIT 10"},
		{"limit 10,20", "LIMIT 10,20"},
		{"limit 20 offset 10", "LIMIT 10,20"},
	}
	extractNodeFunc := func(node Node) Node {
		return node.(*SelectStmt).Limit
	}
	RunNodeRestoreTest(c, testCases, "SELECT 1 %s", extractNodeFunc)
}

func (tc *testDMLSuite) TestWildCardFieldRestore(c *C) {
	testCases := []NodeRestoreTestCase{
		{"*", "*"},
		{"t.*", "`t`.*"},
		{"testdb.t.*", "`testdb`.`t`.*"},
	}
	extractNodeFunc := func(node Node) Node {
		return node.(*SelectStmt).Fields.Fields[0].WildCard
	}
	RunNodeRestoreTest(c, testCases, "SELECT %s", extractNodeFunc)
}

func (tc *testDMLSuite) TestSelectFieldRestore(c *C) {
	testCases := []NodeRestoreTestCase{
		{"*", "*"},
		{"t.*", "`t`.*"},
		{"testdb.t.*", "`testdb`.`t`.*"},
		{"col as a", "`col` AS `a`"},
		{"col + 1 a", "`col`+1 AS `a`"},
	}
	extractNodeFunc := func(node Node) Node {
		return node.(*SelectStmt).Fields.Fields[0]
	}
	RunNodeRestoreTest(c, testCases, "SELECT %s", extractNodeFunc)
}

func (tc *testDMLSuite) TestFieldListRestore(c *C) {
	testCases := []NodeRestoreTestCase{
		{"*", "*"},
		{"t.*", "`t`.*"},
		{"testdb.t.*", "`testdb`.`t`.*"},
		{"col as a", "`col` AS `a`"},
		{"`t`.*, s.col as a", "`t`.*, `s`.`col` AS `a`"},
	}
	extractNodeFunc := func(node Node) Node {
		return node.(*SelectStmt).Fields
	}
	RunNodeRestoreTest(c, testCases, "SELECT %s", extractNodeFunc)
}

func (tc *testDMLSuite) TestTableSourceRestore(c *C) {
	testCases := []NodeRestoreTestCase{
		{"tbl", "`tbl`"},
		{"tbl as t", "`tbl` AS `t`"},
		{"(select * from tbl) as t", "(SELECT * FROM `tbl`) AS `t`"},
		{"(select * from a union select * from b) as t", "(SELECT * FROM `a` UNION SELECT * FROM `b`) AS `t`"},
	}
	extractNodeFunc := func(node Node) Node {
		return node.(*SelectStmt).From.TableRefs.Left
	}
	RunNodeRestoreTest(c, testCases, "select * from %s", extractNodeFunc)
}

func (tc *testDMLSuite) TestOnConditionRestore(c *C) {
	testCases := []NodeRestoreTestCase{
		{"on t1.a=t2.a", "ON `t1`.`a`=`t2`.`a`"},
		{"on t1.a=t2.a and t1.b=t2.b", "ON `t1`.`a`=`t2`.`a` AND `t1`.`b`=`t2`.`b`"},
	}
	extractNodeFunc := func(node Node) Node {
		return node.(*SelectStmt).From.TableRefs.On
	}
	RunNodeRestoreTest(c, testCases, "select * from t1 join t2 %s", extractNodeFunc)
}

func (tc *testDMLSuite) TestJoinRestore(c *C) {
	testCases := []NodeRestoreTestCase{
		{"t1 natural join t2", "`t1` NATURAL JOIN `t2`"},
		{"t1 natural left join t2", "`t1` NATURAL LEFT JOIN `t2`"},
		{"t1 natural right outer join t2", "`t1` NATURAL RIGHT JOIN `t2`"},
		{"t1 straight_join t2", "`t1` STRAIGHT_JOIN `t2`"},
		{"t1 straight_join t2 on t1.a>t2.a", "`t1` STRAIGHT_JOIN `t2` ON `t1`.`a`>`t2`.`a`"},
		{"t1 cross join t2", "`t1` JOIN `t2`"},
		{"t1 cross join t2 on t1.a>t2.a", "`t1` JOIN `t2` ON `t1`.`a`>`t2`.`a`"},
		{"t1 inner join t2 using (b)", "`t1` JOIN `t2` USING (`b`)"},
		{"t1 join t2 using (b,c) left join t3 on t1.a>t3.a", "(`t1` JOIN `t2` USING (`b`,`c`)) LEFT JOIN `t3` ON `t1`.`a`>`t3`.`a`"},
		{"t1 natural join t2 right outer join t3 using (b,c)", "(`t1` NATURAL JOIN `t2`) RIGHT JOIN `t3` USING (`b`,`c`)"},
		{"(a al left join b bl on al.a1 > bl.b1) join (a ar right join b br on ar.a1 > br.b1)", "(`a` AS `al` LEFT JOIN `b` AS `bl` ON `al`.`a1`>`bl`.`b1`) JOIN (`a` AS `ar` RIGHT JOIN `b` AS `br` ON `ar`.`a1`>`br`.`b1`)"},
		{"a al left join b bl on al.a1 > bl.b1, a ar right join b br on ar.a1 > br.b1", "(`a` AS `al` LEFT JOIN `b` AS `bl` ON `al`.`a1`>`bl`.`b1`) JOIN (`a` AS `ar` RIGHT JOIN `b` AS `br` ON `ar`.`a1`>`br`.`b1`)"},
		{"t1, t2", "(`t1`) JOIN `t2`"},
		{"t1, t2, t3", "((`t1`) JOIN `t2`) JOIN `t3`"},
	}
	extractNodeFunc := func(node Node) Node {
		return node.(*SelectStmt).From.TableRefs
	}
	RunNodeRestoreTest(c, testCases, "select * from %s", extractNodeFunc)
}

func (ts *testDMLSuite) TestTableRefsClauseRestore(c *C) {
	testCases := []NodeRestoreTestCase{
		{"t", "`t`"},
		{"t1 join t2", "`t1` JOIN `t2`"},
		{"t1, t2", "(`t1`) JOIN `t2`"},
	}
	extractNodeFunc := func(node Node) Node {
		return node.(*SelectStmt).From
	}
	RunNodeRestoreTest(c, testCases, "select * from %s", extractNodeFunc)
}

func (tc *testDMLSuite) TestDeleteTableListRestore(c *C) {
	testCases := []NodeRestoreTestCase{
		{"t1,t2", "`t1`,`t2`"},
	}
	extractNodeFunc := func(node Node) Node {
		return node.(*DeleteStmt).Tables
	}
	RunNodeRestoreTest(c, testCases, "DELETE %s FROM t1, t2;", extractNodeFunc)
	RunNodeRestoreTest(c, testCases, "DELETE FROM %s USING t1, t2;", extractNodeFunc)
}

func (tc *testDMLSuite) TestDeleteTableIndexHintRestore(c *C) {
	testCases := []NodeRestoreTestCase{
		{"DELETE FROM t1 USE key (`fld1`) WHERE fld=1",
			"DELETE FROM `t1` USE INDEX (`fld1`) WHERE `fld`=1"},
		{"DELETE FROM t1 as tbl USE key (`fld1`) WHERE tbl.fld=2",
			"DELETE FROM `t1` AS `tbl` USE INDEX (`fld1`) WHERE `tbl`.`fld`=2"},
	}

	extractNodeFunc := func(node Node) Node {
		return node.(*DeleteStmt)
	}
	RunNodeRestoreTest(c, testCases, "%s", extractNodeFunc)
}

func (tc *testExpressionsSuite) TestByItemRestore(c *C) {
	testCases := []NodeRestoreTestCase{
		{"a", "`a`"},
		{"a desc", "`a` DESC"},
		{"NULL", "NULL"},
	}
	extractNodeFunc := func(node Node) Node {
		return node.(*SelectStmt).OrderBy.Items[0]
	}
	RunNodeRestoreTest(c, testCases, "select * from t order by %s", extractNodeFunc)
}

func (tc *testExpressionsSuite) TestGroupByClauseRestore(c *C) {
	testCases := []NodeRestoreTestCase{
		{"GROUP BY a,b desc", "GROUP BY `a`,`b` DESC"},
		{"GROUP BY 1 desc,b", "GROUP BY 1 DESC,`b`"},
	}
	extractNodeFunc := func(node Node) Node {
		return node.(*SelectStmt).GroupBy
	}
	RunNodeRestoreTest(c, testCases, "select * from t %s", extractNodeFunc)
}

func (tc *testDMLSuite) TestOrderByClauseRestore(c *C) {
	testCases := []NodeRestoreTestCase{
		{"ORDER BY a", "ORDER BY `a`"},
		{"ORDER BY a,b", "ORDER BY `a`,`b`"},
	}
	extractNodeFunc := func(node Node) Node {
		return node.(*SelectStmt).OrderBy
	}
	RunNodeRestoreTest(c, testCases, "SELECT 1 FROM t1 %s", extractNodeFunc)

	extractNodeFromUnionStmtFunc := func(node Node) Node {
		return node.(*UnionStmt).OrderBy
	}
	RunNodeRestoreTest(c, testCases, "SELECT 1 FROM t1 UNION SELECT 2 FROM t2 %s", extractNodeFromUnionStmtFunc)
}

func (tc *testDMLSuite) TestAssignmentRestore(c *C) {
	testCases := []NodeRestoreTestCase{
		{"a=1", "`a`=1"},
		{"b=1+2", "`b`=1+2"},
	}
	extractNodeFunc := func(node Node) Node {
		return node.(*UpdateStmt).List[0]
	}
	RunNodeRestoreTest(c, testCases, "UPDATE t1 SET %s", extractNodeFunc)
}

func (ts *testDMLSuite) TestHavingClauseRestore(c *C) {
	testCases := []NodeRestoreTestCase{
		{"HAVING a", "HAVING `a`"},
		{"HAVING NULL", "HAVING NULL"},
		{"HAVING a>b", "HAVING `a`>`b`"},
	}
	extractNodeFunc := func(node Node) Node {
		return node.(*SelectStmt).Having
	}
	RunNodeRestoreTest(c, testCases, "select 1 from t1 group by 1 %s", extractNodeFunc)
}

func (ts *testDMLSuite) TestFrameBoundRestore(c *C) {
	testCases := []NodeRestoreTestCase{
		{"CURRENT ROW", "CURRENT ROW"},
		{"UNBOUNDED PRECEDING", "UNBOUNDED PRECEDING"},
		{"1 PRECEDING", "1 PRECEDING"},
		{"? PRECEDING", "? PRECEDING"},
		{"INTERVAL 5 DAY PRECEDING", "INTERVAL 5 DAY PRECEDING"},
		{"UNBOUNDED FOLLOWING", "UNBOUNDED FOLLOWING"},
		{"1 FOLLOWING", "1 FOLLOWING"},
		{"? FOLLOWING", "? FOLLOWING"},
		{"INTERVAL '2:30' MINUTE_SECOND FOLLOWING", "INTERVAL '2:30' MINUTE_SECOND FOLLOWING"},
	}
	extractNodeFunc := func(node Node) Node {
		return &node.(*SelectStmt).Fields.Fields[0].Expr.(*WindowFuncExpr).Spec.Frame.Extent.Start
	}
	RunNodeRestoreTest(c, testCases, "select avg(val) over (rows between %s and current row) from t", extractNodeFunc)
}

func (ts *testDMLSuite) TestFrameClauseRestore(c *C) {
	testCases := []NodeRestoreTestCase{
		{"ROWS CURRENT ROW", "ROWS BETWEEN CURRENT ROW AND CURRENT ROW"},
		{"ROWS UNBOUNDED PRECEDING", "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW"},
		{"ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING", "ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING"},
		{"RANGE BETWEEN ? PRECEDING AND ? FOLLOWING", "RANGE BETWEEN ? PRECEDING AND ? FOLLOWING"},
		{"RANGE BETWEEN INTERVAL 5 DAY PRECEDING AND INTERVAL '2:30' MINUTE_SECOND FOLLOWING", "RANGE BETWEEN INTERVAL 5 DAY PRECEDING AND INTERVAL '2:30' MINUTE_SECOND FOLLOWING"},
	}
	extractNodeFunc := func(node Node) Node {
		return node.(*SelectStmt).Fields.Fields[0].Expr.(*WindowFuncExpr).Spec.Frame
	}
	RunNodeRestoreTest(c, testCases, "select avg(val) over (%s) from t", extractNodeFunc)
}

func (ts *testDMLSuite) TestPartitionByClauseRestore(c *C) {
	testCases := []NodeRestoreTestCase{
		{"PARTITION BY a", "PARTITION BY `a`"},
		{"PARTITION BY NULL", "PARTITION BY NULL"},
		{"PARTITION BY a, b", "PARTITION BY `a`, `b`"},
	}
	extractNodeFunc := func(node Node) Node {
		return node.(*SelectStmt).Fields.Fields[0].Expr.(*WindowFuncExpr).Spec.PartitionBy
	}
	RunNodeRestoreTest(c, testCases, "select avg(val) over (%s rows current row) from t", extractNodeFunc)
}

func (ts *testDMLSuite) TestWindowSpecRestore(c *C) {
	testCases := []NodeRestoreTestCase{
		{"w as ()", "`w` AS ()"},
		{"w as (w1)", "`w` AS (`w1`)"},
		{"w as (w1 order by country)", "`w` AS (`w1` ORDER BY `country`)"},
		{"w as (partition by a order by b rows current row)", "`w` AS (PARTITION BY `a` ORDER BY `b` ROWS BETWEEN CURRENT ROW AND CURRENT ROW)"},
	}
	extractNodeFunc := func(node Node) Node {
		return &node.(*SelectStmt).WindowSpecs[0]
	}
	RunNodeRestoreTest(c, testCases, "select rank() over w from t window %s", extractNodeFunc)

	testCases = []NodeRestoreTestCase{
		{"w", "`w`"},
		{"()", "()"},
		{"(w)", "(`w`)"},
		{"(w PARTITION BY country)", "(`w` PARTITION BY `country`)"},
		{"(PARTITION BY a ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING)", "(PARTITION BY `a` ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING)"},
	}
	extractNodeFunc = func(node Node) Node {
		return &node.(*SelectStmt).Fields.Fields[0].Expr.(*WindowFuncExpr).Spec
	}
	RunNodeRestoreTest(c, testCases, "select rank() over %s from t window w as (order by a)", extractNodeFunc)
}

func (ts *testDMLSuite) TestFulltextSearchModifier(c *C) {
	c.Assert(FulltextSearchModifier(FulltextSearchModifierNaturalLanguageMode).IsBooleanMode(), IsFalse)
	c.Assert(FulltextSearchModifier(FulltextSearchModifierNaturalLanguageMode).IsNaturalLanguageMode(), IsTrue)
	c.Assert(FulltextSearchModifier(FulltextSearchModifierNaturalLanguageMode).WithQueryExpansion(), IsFalse)
}
