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

package core

import (
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/stretchr/testify/require"
)

func tableNamesAsStr(tableNames []*ast.TableName) string {
	names := []string{}
	for _, tn := range tableNames {
		names = append(names, fmt.Sprintf("[%s.%s]", tn.Schema.L, tn.Name.L))
	}
	return strings.Join(names, ",")
}

func sortTableNames(tableNames []*ast.TableName) {
	sort.Slice(tableNames, func(i, j int) bool {
		if tableNames[i].Schema.L == tableNames[j].Schema.L {
			return tableNames[i].Name.L < tableNames[j].Name.L
		}
		return tableNames[i].Schema.L < tableNames[j].Schema.L
	})
}

func TestExtractTableList(t *testing.T) {
	cases := []struct {
		sql    string
		asName bool
		expect []*ast.TableName
	}{
		{
			sql: "WITH t AS (SELECT * FROM t2) SELECT * FROM t, t1, mysql.user WHERE t1.a = mysql.user.username",
			expect: []*ast.TableName{
				{Name: model.NewCIStr("t")},
				{Name: model.NewCIStr("t1")},
				{Name: model.NewCIStr("t2")},
				{Name: model.NewCIStr("user"), Schema: model.NewCIStr("mysql")},
			},
		},
		{
			sql: "SELECT (SELECT a,b,c FROM t1) AS t WHERE t.a = 1",
			expect: []*ast.TableName{
				{Name: model.NewCIStr("t1")},
			},
		},
		{
			sql: "SELECT * FROM t, v AS w",
			expect: []*ast.TableName{
				{Name: model.NewCIStr("t")},
				{Name: model.NewCIStr("v")},
			},
		},
		{
			sql:    "SELECT * FROM t, v AS w",
			asName: true,
			expect: []*ast.TableName{
				{Name: model.NewCIStr("t")},
				{Name: model.NewCIStr("w")},
			},
		},
		{
			sql: `SELECT
					  AVG(all_scores.avg_score) AS avg_score,
					  student_name
					FROM
					  (
					    SELECT
					      student_id,
					      AVG(score) AS avg_score
					    FROM
					      scores
					    GROUP BY
					      student_id
					  ) AS all_scores
					  JOIN students ON students.student_id = all_scores.student_id
					GROUP BY
					  student_id
					ORDER BY
					  avg_score DESC`,
			expect: []*ast.TableName{
				{Name: model.NewCIStr("scores")},
				{Name: model.NewCIStr("students")},
			},
		},
		{
			sql: "DELETE FROM x.y z WHERE z.a > 0",
			expect: []*ast.TableName{
				{Name: model.NewCIStr("y"), Schema: model.NewCIStr("x")},
			},
		},
		{
			sql: "WITH t AS (SELECT * FROM v) DELETE FROM x.y z WHERE z.a > t.c",
			expect: []*ast.TableName{
				{Name: model.NewCIStr("y"), Schema: model.NewCIStr("x")},
				{Name: model.NewCIStr("v")},
			},
		},
		{
			sql: "DELETE FROM `t1` AS `t2` USE INDEX (`fld1`) WHERE `t2`.`fld`=2",
			expect: []*ast.TableName{
				{Name: model.NewCIStr("t1")},
			},
		},
		{
			sql:    "DELETE FROM `t1` AS `t2` USE INDEX (`fld1`) WHERE `t2`.`fld`=2",
			asName: true,
			expect: []*ast.TableName{
				{Name: model.NewCIStr("t2")},
			},
		},
		{
			sql: "UPDATE t1 USE INDEX(idx_a) JOIN t2 SET t1.price=t2.price WHERE t1.id=t2.id;",
			expect: []*ast.TableName{
				{Name: model.NewCIStr("t1")},
				{Name: model.NewCIStr("t2")},
			},
		},
		{
			sql: "INSERT INTO t (a,b,c) SELECT x,y,z FROM t1;",
			expect: []*ast.TableName{
				{Name: model.NewCIStr("t")},
				{Name: model.NewCIStr("t1")},
			},
		},
		{
			sql: "WITH t AS (SELECT * FROM v) SELECT a FROM t UNION SELECT b FROM t1",
			expect: []*ast.TableName{
				{Name: model.NewCIStr("v")},
				{Name: model.NewCIStr("t")},
				{Name: model.NewCIStr("t1")},
			},
		},
		{
			sql: "LOAD DATA INFILE '/a.csv' FORMAT 'sql file' INTO TABLE `t`",
			expect: []*ast.TableName{
				{Name: model.NewCIStr("t")},
			},
		},
		{
			sql: "batch on c limit 10 delete from t where t.c = 10",
			expect: []*ast.TableName{
				{Name: model.NewCIStr("t")},
			},
		},
		{
			sql: "split table t1 between () and () regions 10",
			expect: []*ast.TableName{
				{Name: model.NewCIStr("t1")},
			},
		},
		{
			sql: "show create table t",
			expect: []*ast.TableName{
				{Name: model.NewCIStr("t")},
			},
		},
		{
			sql: "show create database test",
			expect: []*ast.TableName{
				{Schema: model.NewCIStr("test")},
			},
		},
		{
			sql: "create database test",
			expect: []*ast.TableName{
				{Schema: model.NewCIStr("test")},
			},
		},
		{
			sql: "FLASHBACK DATABASE t1 TO t2",
			expect: []*ast.TableName{
				{Schema: model.NewCIStr("t1")},
				{Schema: model.NewCIStr("t2")},
			},
		},
		{
			sql: "flashback table t,t1,test.t2 to timestamp '2021-05-26 16:45:26'",
			expect: []*ast.TableName{
				{Name: model.NewCIStr("t")},
				{Name: model.NewCIStr("t1")},
				{Name: model.NewCIStr("t2"), Schema: model.NewCIStr("test")},
			},
		},
		{
			sql: "flashback database test to timestamp '2021-05-26 16:45:26'",
			expect: []*ast.TableName{
				{Schema: model.NewCIStr("test")},
			},
		},
		{
			sql: "flashback table t TO t1",
			expect: []*ast.TableName{
				{Name: model.NewCIStr("t")},
				{Name: model.NewCIStr("t1")},
			},
		},
		{
			sql: "create table t",
			expect: []*ast.TableName{
				{Name: model.NewCIStr("t")},
			},
		},
		{
			sql: "RENAME TABLE t TO t1, test.t2 TO test.t3",
			expect: []*ast.TableName{
				{Name: model.NewCIStr("t")},
				{Name: model.NewCIStr("t1")},
				{Name: model.NewCIStr("t2"), Schema: model.NewCIStr("test")},
				{Name: model.NewCIStr("t3"), Schema: model.NewCIStr("test")},
			},
		},
		{
			sql: "drop table test.t, t1",
			expect: []*ast.TableName{
				{Name: model.NewCIStr("t1")},
				{Name: model.NewCIStr("t"), Schema: model.NewCIStr("test")},
			},
		},
		{
			sql: "create view v as (select * from t)",
			expect: []*ast.TableName{
				{Name: model.NewCIStr("v")},
				{Name: model.NewCIStr("t")},
			},
		},
		{
			sql: "create sequence if not exists seq no cycle",
			expect: []*ast.TableName{
				{Name: model.NewCIStr("seq")},
			},
		},
		{
			sql: "CREATE INDEX idx ON t ( a ) VISIBLE INVISIBLE",
			expect: []*ast.TableName{
				{Name: model.NewCIStr("t")},
			},
		},
		{
			sql: "LOCK TABLE t1 WRITE, t2 READ",
			expect: []*ast.TableName{
				{Name: model.NewCIStr("t1")},
				{Name: model.NewCIStr("t2")},
			},
		},
		{
			sql: "grant select on test.* to u1",
			expect: []*ast.TableName{
				{Schema: model.NewCIStr("test")},
			},
		},
		{
			sql: "BACKUP TABLE a.b,c.d,e TO 'noop://'",
			expect: []*ast.TableName{
				{Name: model.NewCIStr("b"), Schema: model.NewCIStr("a")},
				{Name: model.NewCIStr("d"), Schema: model.NewCIStr("c")},
				{Name: model.NewCIStr("e")},
			},
		},
		{
			sql: "TRACE SELECT (SELECT a,b,c FROM t1) AS t WHERE t.a = 1",
			expect: []*ast.TableName{
				{Name: model.NewCIStr("t1")},
			},
		},
		{
			sql: "EXPLAIN SELECT (SELECT a,b,c FROM t1) AS t WHERE t.a = 1",
			expect: []*ast.TableName{
				{Name: model.NewCIStr("t1")},
			},
		},
		{
			sql: "PLAN REPLAYER DUMP EXPLAIN SELECT (SELECT a,b,c FROM t1) AS t WHERE t.a = 1",
			expect: []*ast.TableName{
				{Name: model.NewCIStr("t1")},
			},
		},
		{
			sql: "ALTER TABLE t COMPACT",
			expect: []*ast.TableName{
				{Name: model.NewCIStr("t")},
			},
		},
	}
	p := parser.New()
	for i, c := range cases {
		stmtNode, err := p.ParseOneStmt(c.sql, "", "")
		require.NoError(t, err, "case %d sql: %s", i, c.sql)
		tableNames := ExtractTableList(stmtNode, c.asName)
		require.Len(t, tableNames, len(c.expect), "case %d sql: %s, len: %d, actual: %s", i, c.sql, len(tableNames), tableNamesAsStr(tableNames))
		sortTableNames(tableNames)
		sortTableNames(c.expect)
		for j, tn := range tableNames {
			require.Equal(t, c.expect[j].Schema.L, tn.Schema.L, "case %d sql: %s, j: %d, actual: %s", i, c.sql, j, tableNamesAsStr(tableNames))
			require.Equal(t, c.expect[j].Name.L, tn.Name.L, "case %d sql: %s, j: %d, actual: %s", i, c.sql, j, tableNamesAsStr(tableNames))
		}
	}
}
