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

package funcdep_test

import (
	"testing"

	"github.com/pingcap/tidb/testkit"
	"github.com/stretchr/testify/require"
)

func TestOnlyFullGroupByOldCases(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)

	tk.MustExec("set @@session.tidb_enable_new_only_full_group_by_check = 'on';")

	// test case 1
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("drop table if exists t2")
	tk.MustExec("drop view if exists v1")
	tk.MustExec("CREATE TABLE t1 (  c1 INT,  c2 INT,  c4 DATE,  c5 VARCHAR(1));")
	tk.MustExec("CREATE TABLE t2 (  c1 INT,  c2 INT,  c3 INT,  c5 VARCHAR(1));")
	tk.MustExec("CREATE VIEW v1 AS  SELECT alias1.c4 AS field1  FROM t1 AS alias1  INNER JOIN t1 AS alias2  ON 1 GROUP BY field1 ORDER BY alias1.c5;")
	_, err := tk.Exec("SELECT * FROM v1;")
	require.NotNil(t, err)
	require.Equal(t, err.Error(), "[planner:1055]Expression #2 of SELECT list is not in GROUP BY clause and contains nonaggregated column 'test.alias1.c5' which is not functionally dependent on columns in GROUP BY clause; this is incompatible with sql_mode=only_full_group_by")

	// test case 2
	tk.MustExec("drop table if exists t1")
	tk.MustExec("drop table if exists t2")
	tk.MustExec("drop view if exists v1")
	tk.MustExec("CREATE TABLE t1 (c1 INT, c2 INT, c4 DATE, c5 VARCHAR(1));")
	tk.MustExec("CREATE TABLE t2 (c1 INT, c2 INT, c3 INT, c5 VARCHAR(1));")
	tk.MustExec("CREATE definer='root'@'localhost' VIEW v1 AS  SELECT alias1.c4 AS field1, alias1.c4 AS field2  FROM t1 AS alias1  INNER JOIN t1 AS alias2 ON (alias2.c1 = alias1.c2) WHERE ( NOT EXISTS (  SELECT SQ1_alias1.c5 AS SQ1_field1   FROM t2 AS SQ1_alias1  WHERE SQ1_alias1.c3 < alias1.c1 ))   AND (alias1.c5 = alias1.c5    AND alias1.c5 = 'd'   ) GROUP BY field1, field2 ORDER BY alias1.c5, field1, field2")
	tk.MustQuery("SELECT * FROM v1;")

	// test case 3
	// need to resolve the name resolver problem first (view and base table's column can not refer each other)
	// see some cases in 21

	// test case 4
	tk.MustExec("drop table if exists t1")
	tk.MustExec("drop table if exists t2")
	tk.MustExec("drop view if exists v2")
	tk.MustExec("CREATE TABLE t1 ( col_varchar_10_utf8 VARCHAR(10) CHARACTER SET utf8,  col_int_key INT,  pk INT PRIMARY KEY);")
	tk.MustExec("CREATE TABLE t2 ( col_varchar_10_utf8 VARCHAR(10) CHARACTER SET utf8 DEFAULT NULL, col_int_key INT DEFAULT NULL,  pk INT PRIMARY KEY);")
	tk.MustExec("CREATE ALGORITHM=MERGE definer='root'@'localhost' VIEW v2 AS SELECT t2.pk, COALESCE(t2.pk, 3) AS coa FROM t1 LEFT JOIN t2 ON 0;")
	tk.MustQuery("SELECT v2.pk, v2.coa FROM t1 LEFT JOIN v2 AS v2 ON 0 GROUP BY v2.pk;")

	// test case 5
	tk.MustExec("drop table if exists t")
	tk.MustExec("CREATE TABLE t ( a INT, c INT GENERATED ALWAYS AS (a+2), d INT GENERATED ALWAYS AS (c+2) );")
	tk.MustQuery("SELECT c FROM t GROUP BY a;")
	tk.MustQuery("SELECT d FROM t GROUP BY c;")
	tk.MustQuery("SELECT d FROM t GROUP BY a;")
	tk.MustQuery("SELECT 1+c FROM t GROUP BY a;")
	tk.MustQuery("SELECT 1+d FROM t GROUP BY c;")
	tk.MustQuery("SELECT 1+d FROM t GROUP BY a;")
	tk.MustQuery("SELECT t1.d FROM t as t1, t as t2 WHERE t2.d=t1.c GROUP BY t2.a;")
	_, err = tk.Exec("SELECT t1.d FROM t as t1, t as t2 WHERE t2.d>t1.c GROUP BY t2.a;")
	require.NotNil(t, err)
	require.Equal(t, err.Error(), "[planner:1055]Expression #1 of SELECT list is not in GROUP BY clause and contains nonaggregated column 'test.t1.d' which is not functionally dependent on columns in GROUP BY clause; this is incompatible with sql_mode=only_full_group_by")

	// test case 6
	tk.MustExec("drop table if exists t")
	tk.MustExec("CREATE TABLE t ( a INT, c INT GENERATED ALWAYS AS (a+2), d INT GENERATED ALWAYS AS (c+2) );")
	_, err = tk.Exec("SELECT t1.d FROM t as t1, t as t2 WHERE t2.d>t1.c GROUP BY t2.a;")
	require.NotNil(t, err)
	require.Equal(t, err.Error(), "[planner:1055]Expression #1 of SELECT list is not in GROUP BY clause and contains nonaggregated column 'test.t1.d' which is not functionally dependent on columns in GROUP BY clause; this is incompatible with sql_mode=only_full_group_by")
	_, err = tk.Exec("SELECT (SELECT t1.c FROM t as t1 GROUP BY -3) FROM t as t2;")
	require.NotNil(t, err)
	require.Equal(t, err.Error(), "[planner:1055]Expression #1 of SELECT list is not in GROUP BY clause and contains nonaggregated column 'test.t1.c' which is not functionally dependent on columns in GROUP BY clause; this is incompatible with sql_mode=only_full_group_by")
	_, err = tk.Exec("SELECT DISTINCT t1.a FROM t as t1 ORDER BY t1.d LIMIT 1;")
	require.NotNil(t, err)
	require.Equal(t, err.Error(), "[planner:3065]Expression #1 of ORDER BY clause is not in SELECT list, references column 'test.t.d' which is not in SELECT list; this is incompatible with DISTINCT")
	_, err = tk.Exec("SELECT DISTINCT t1.a FROM t as t1 ORDER BY t1.d LIMIT 1;")
	require.NotNil(t, err)
	require.Equal(t, err.Error(), "[planner:3065]Expression #1 of ORDER BY clause is not in SELECT list, references column 'test.t.d' which is not in SELECT list; this is incompatible with DISTINCT")
	_, err = tk.Exec("SELECT (SELECT DISTINCT t1.a FROM t as t1 ORDER BY t1.d LIMIT 1) FROM t as t2;")
	require.NotNil(t, err)
	require.Equal(t, err.Error(), "[planner:3065]Expression #1 of ORDER BY clause is not in SELECT list, references column 'test.t.d' which is not in SELECT list; this is incompatible with DISTINCT")

	// test case 7
	tk.MustExec("drop table if exists t")
	tk.MustExec("CREATE TABLE t(a INT NULL, b INT NOT NULL, c INT, UNIQUE(a,b));")
	tk.MustQuery("SELECT a,b,c FROM t WHERE a IS NOT NULL GROUP BY a,b;")
	tk.MustQuery("SELECT a,b,c FROM t WHERE NOT (a IS NULL) GROUP BY a,b;")
	tk.MustQuery("SELECT a,b,c FROM t WHERE a > 3 GROUP BY a,b;")
	tk.MustQuery("SELECT a,b,c FROM t WHERE a = 3 GROUP BY b;")
	tk.MustQuery("SELECT a,b,c FROM t WHERE a BETWEEN 3 AND 6 GROUP BY a,b;")
	tk.MustQuery("SELECT a,b,c FROM t WHERE a <> 3 GROUP BY a,b;")
	tk.MustQuery("SELECT a,b,c FROM t WHERE a IN (3,4) GROUP BY a,b;")
	tk.MustQuery("SELECT a,b,c FROM t WHERE a IN (SELECT b FROM t) GROUP BY a,b;")
	tk.MustQuery("SELECT a,b,c FROM t WHERE a IS TRUE GROUP BY a,b;")
	tk.MustQuery("SELECT a,b,c FROM t WHERE (a <> 3) IS TRUE GROUP BY a,b;")
	tk.MustQuery("SELECT a,b,c FROM t WHERE a IS FALSE GROUP BY a,b;")
	tk.MustQuery("SELECT a,b,c FROM t WHERE (a <> 3) IS FALSE GROUP BY a,b;")
	tk.MustQuery("SELECT a,b,c FROM t WHERE a LIKE \"%abc%\" GROUP BY a,b;")
	// todo: eval not-null refuse NOT wrapper.
	// tk.MustQuery("SELECT a,b,c FROM t WHERE NOT(a IN (3,4)) GROUP BY a,b;")
	// tk.MustQuery("SELECT a,b,c FROM t WHERE a NOT IN (3,4) GROUP BY a,b;")
	// tk.MustQuery("SELECT a,b,c FROM t WHERE a NOT LIKE \"%abc%\" GROUP BY a,b;")
	_, err = tk.Exec("SELECT a,b,c FROM t WHERE a<=>NULL GROUP BY b;")
	require.NotNil(t, err)
	require.Equal(t, err.Error(), "[planner:1055]Expression #3 of SELECT list is not in GROUP BY clause and contains nonaggregated column 'test.t.c' which is not functionally dependent on columns in GROUP BY clause; this is incompatible with sql_mode=only_full_group_by")
	// is-not-true will let the null value pass, so evaluating it won't derive to a with not-null attribute.
	_, err = tk.Exec("SELECT a,b,c FROM t WHERE a IS NOT TRUE GROUP BY a,b;")
	require.NotNil(t, err)
	require.Equal(t, err.Error(), "[planner:1055]Expression #3 of SELECT list is not in GROUP BY clause and contains nonaggregated column 'test.t.c' which is not functionally dependent on columns in GROUP BY clause; this is incompatible with sql_mode=only_full_group_by")

	// test case 8
	tk.MustExec("drop table if exists t1")
	tk.MustExec("drop table if exists t2")
	tk.MustExec("drop table if exists t3")
	tk.MustExec("CREATE TABLE t1 (a INT, b INT);")
	tk.MustExec("CREATE TABLE t2 (b INT);")
	tk.MustExec("CREATE TABLE t3 (b INT NULL, c INT NULL, d INT NULL, e INT NULL, UNIQUE KEY (b,d,e));")
	tk.MustQuery("SELECT * FROM t1, t2, t3 WHERE t2.b = t1.b AND t2.b = t3.b AND t3.d = 1 AND t3.e = 1 AND t3.d IS NOT NULL AND t1.a = 2 GROUP BY t1.b;")

	// test case 9
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1(a int, b int not null, c int not null, d int, unique key(b,c), unique key(b,d));")
	_, err = tk.Exec("select (select sin(a)) as z from t1 group by d,b;")
	require.NotNil(t, err)
	require.Equal(t, err.Error(), "[planner:1055]Expression #1 of SELECT list is not in GROUP BY clause and contains nonaggregated column 'z' which is not functionally dependent on columns in GROUP BY clause; this is incompatible with sql_mode=only_full_group_by")

	// test case 10 & 11
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1(a int, b int not null, c int not null, d int, unique key(b,c), unique key(b,d));")
	tk.MustExec("select t3.a from t1, t1 as t2, t1 as t3 where  t3.b=t2.b and t3.c=t1.d and  t2.b=t1.b and t2.c=t1.c group by t1.b,t1.c")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("drop table if exists t3")
	tk.MustExec("create table t1(a int, b int not null, c int not null, d int, unique key(b,c), unique key(b,d));")
	tk.MustExec("create table t3(pk int primary key, b int);")
	tk.MustQuery("select t3.b from  t1,t1 as t2,t3  where t3.pk=t2.d and t2.b=t1.b and t2.c=t1.a  group by t1.b,t1.c;")

	// test case 12
	tk.MustExec("drop table if exists t1")
	tk.MustExec("drop table if exists t2")
	tk.MustExec("create table t1(a int,b int not null,c int not null,d int, unique key(b,c), unique key(b,d));")
	tk.MustExec("create table t2 like t1")
	tk.MustQuery("select t1.a,t2.c from t1 left join t2 on t1.a=t2.c and cos(t2.c+t2.b)>0.5 and sin(t1.a+t2.d)<0.9 group by t1.a;")
	tk.MustQuery("select t1.a,t2.d from t1 left join t2 on t1.a=t2.c and t1.d=t2.b and cos(t2.c+t2.b)>0.5 and sin(t1.a+t2.d)<0.9 group by t1.a,t1.d;")

	// test case 17
	tk.MustExec("drop table if exists customer1")
	tk.MustExec("drop table if exists customer2")
	tk.MustExec("drop view if exists customer")
	tk.MustExec("create table customer1(pk int primary key, a int);")
	tk.MustExec("create table customer2(pk int primary key, b int);")
	tk.MustExec("CREATE algorithm=merge definer='root'@'localhost' VIEW customer as SELECT pk,a,b FROM customer1 JOIN customer2 USING (pk);")
	tk.MustQuery("select customer.pk, customer.b from customer group by customer.pk;")
	// classic cases
	tk.MustQuery("select customer1.a, count(*) from customer1 left join customer2 on customer1.a=customer2.b where customer2.pk in (7,9) group by customer2.b;")
	tk.MustQuery("select customer1.a, count(*) from customer1 left join customer2 on customer1.a=1 where customer2.pk in (7,9) group by customer2.b;")
	// c2.pk reject the null from both inner side of the left join.
	tk.MustQuery("select c1.a, count(*) from customer2 c3 left join (customer1 c1 left join customer2 c2 on c1.a=c2.b) on c3.b=c1.a where c2.pk in (7,9) group by c2.b;")
	tk.MustQuery("select c3.b, count(*) from customer2 c3 left join (customer1 c1 left join customer2 c2 on c1.a=1) on c3.b=c1.a where c2.pk in (7,9) group by c2.b;")
	tk.MustQuery("select c1.a, count(*) from customer2 c3 left join (customer1 c1 left join customer2 c2 on c1.a=1) on c3.b=c1.a where c2.pk in (7,9) group by c2.b;")
	tk.MustQuery("select c1.a, c3.b, count(*) from customer2 c3 left join (customer1 c1 left join customer2 c2 on c1.a=1) on c3.b=1 where c2.pk in (7,9) group by c2.b;")
	// inner join nested with outer join.
	tk.MustQuery("select c1.a, c3.b, count(*) from customer2 c3 join (customer1 c1 left join customer2 c2 on c1.a=1) on c3.b=1 where c2.pk in (7,9) group by c2.b;")
	tk.MustQuery("select c1.a, c3.b, count(*) from customer2 c3  join (customer1 c1 left join customer2 c2 on c1.a=1) on c3.b=c1.a where c2.pk in (7,9) group by c2.b;")
	tk.MustQuery("select c1.a, c3.b, count(*) from customer2 c3  join (customer1 c1 left join customer2 c2 on c1.a=c2.b) on c3.b=c1.a where c2.pk in (7,9) group by c2.b;")
	// outer join nested with inner join.
	// TODO: inner side's strict FD and equiv FD can be saved.
	//tk.MustQuery("select c1.a, c3.b, count(*) from customer2 c3  left join (customer1 c1 inner join customer2 c2 on c1.a=c2.b) on c3.b=c1.a where c2.pk in (7,9) group by c2.b;")
	//tk.MustQuery("select c1.a, c3.b, count(*) from customer2 c3  left join (customer1 c1 inner join customer2 c2 on c1.a=1) on c3.b=c1.a where c2.pk in (7,9) group by c2.b;")
	//tk.MustQuery("select c1.a, c3.b, count(*) from customer2 c3  left join (customer1 c1 inner join customer2 c2 on c1.a=1 and c1.a=c2.b) on c3.b=c1.a where c2.pk in (7,9) group by c2.b;")

	tk.MustExec("drop view if exists customer")
	// this left join can extend left pk to all cols.
	tk.MustExec("CREATE algorithm=merge definer='root'@'localhost' VIEW customer as SELECT pk,a,b FROM customer1 LEFT JOIN customer2 USING (pk);")
	tk.MustQuery("select customer.pk, customer.b from customer group by customer.pk;")

	// test case 18
	tk.MustExec("drop table if exists t1")
	tk.MustExec("drop table if exists t2")
	tk.MustExec("create table t1(pk int primary key, a int);")
	tk.MustExec("create table t2(pk int primary key, b int);")
	tk.MustQuery("select t1.pk, t2.b from t1 join t2 on t1.pk=t2.pk group by t1.pk;")
	tk.MustQuery("select t1.pk, t2.b from t1 join t2 using(pk) group by t1.pk;")
	tk.MustQuery("select t1.pk, t2.b from t1 natural join t2 group by t1.pk;")
	tk.MustQuery("select t1.pk, t2.b from t1 left join t2 using(pk) group by t1.pk;")
	tk.MustQuery("select t1.pk, t2.b from t1 natural left join t2 group by t1.pk;")
	tk.MustQuery("select t1.pk, t2.b from t2 right join t1 using(pk) group by t1.pk;")
	tk.MustQuery("select t1.pk, t2.b from t2 natural right join t1 group by t1.pk;")

	// test case 20
	tk.MustExec("drop table t1")
	tk.MustExec("create table t1(pk int primary key, a int);")
	tk.MustQuery("select t3.a from t1 left join (t1 as t2 left join t1 as t3 on 1) on 1 group by t3.pk;")
	tk.MustQuery("select (select t1.a from t1 as t2 limit 1) from t1 group by pk;")

	// test case 21
	tk.MustExec("drop table if exists t1, t2")
	tk.MustExec("create table t1(a int, b int);")
	// TODO: to be fixed.
	//tk.MustExec("drop view if exists v1;")
	//tk.MustExec("create view v1 as select a as a, 2*a as b, coalesce(a,3) as c from t1;")
	//err = tk.ExecToErr("select v1.b from t1 left join v1 on 1 group by v1.a")
	//require.NotNil(t, err)
	//require.Equal(t, err.Error(), "[planner:1055]Expression #1 of SELECT list is not in GROUP BY clause and contains nonaggregated column 'z' which is not functionally dependent on columns in GROUP BY clause; this is incompatible with sql_mode=only_full_group_by")
	tk.MustExec("create table t2(c int, d int);")
	err = tk.ExecToErr("select t4.d from t1 left join (t2 as t3 join t2 as t4 on t4.d=3) on t1.a=10 group by \"\";")
	require.NotNil(t, err)
	require.Equal(t, err.Error(), "[planner:1055]Expression #1 of SELECT list is not in GROUP BY clause and contains nonaggregated column 'test.t4.d' which is not functionally dependent on columns in GROUP BY clause; this is incompatible with sql_mode=only_full_group_by")
	tk.MustExec("select t4.d from t1 join (t2 as t3 left join t2 as t4 on t4.d=3) on t1.a=10 group by \"\";")
	err = tk.ExecToErr("select t4.d from t1 join (t2 as t3 left join t2 as t4 on t4.d=3 and t4.c+t3.c=2) on t1.a=10 group by \"\";")
	require.NotNil(t, err)
	require.Equal(t, err.Error(), "[planner:1055]Expression #1 of SELECT list is not in GROUP BY clause and contains nonaggregated column 'test.t4.d' which is not functionally dependent on columns in GROUP BY clause; this is incompatible with sql_mode=only_full_group_by")
	//tk.MustExec("drop table t1")
	//tk.MustExec("drop view v1")
	//tk.MustExec("create table t1(a int not null, b int)")
	//tk.MustExec("create view v1 as select a as a, 2*a as b, coalesce(a,3) as c from t1")
	//tk.MustExec("select v1.b from t1 left join v1 on 1 group by v1.a")

	// test issue #25196
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1 (i1 integer, c1 integer);")
	tk.MustExec("insert into t1 values (2, 41), (1, 42), (3, 43), (0, null);")
	tk.MustExec("drop table if exists t2")
	tk.MustExec("create table t2 (i2 integer, c2 integer, f2 float);")
	tk.MustExec("insert into t2 values (0, 43, null), (1, null, 0.1), (3, 42, 0.01), (2, 73, 0.12), (null, 41, -0.1), (null, null, null);")
	err = tk.ExecToErr("SELECT * FROM t2 AS _tmp_1 JOIN (SELECT max(_tmp_3.f2) AS _tmp_4,min(_tmp_3.i2) AS _tmp_5 FROM t2 AS _tmp_3 WHERE _tmp_3.f2>=_tmp_3.c2 GROUP BY _tmp_3.c2 ORDER BY _tmp_3.i2) AS _tmp_2 WHERE _tmp_2._tmp_5=100;")
	require.NotNil(t, err)
	require.Equal(t, err.Error(), "[planner:1055]Expression #3 of SELECT list is not in GROUP BY clause and contains nonaggregated column 'test._tmp_3.i2' which is not functionally dependent on columns in GROUP BY clause; this is incompatible with sql_mode=only_full_group_by")

	// test issue #22301 and #33056
	tk.MustExec("drop table if exists t1, t2")
	tk.MustExec("create table t1 (a int);")
	tk.MustExec("create table t2 (a int, b int);")
	tk.MustQuery("select t1.a from t1 join t2 on t2.a=t1.a group by t2.a having min(t2.b) > 0;")
	tk.MustQuery("select t2.a, count(t2.b) from t1 join t2 using (a) where t1.a = 1;")
	tk.MustQuery("select count(t2.b) from t1 join t2 using (a) order by t2.a;")

	// test issue #30024
	tk.MustExec("drop table if exists t1,t2;")
	tk.MustExec("CREATE TABLE t1 (a INT, b INT, c INT DEFAULT 0);")
	tk.MustExec("INSERT INTO t1 (a, b) VALUES (3,3), (2,2), (3,3), (2,2), (3,3), (4,4);")
	tk.MustExec("CREATE TABLE t2 (a INT, b INT, c INT DEFAULT 0);")
	tk.MustExec("INSERT INTO t2 (a, b) VALUES (3,3), (2,2), (3,3), (2,2), (3,3), (4,4);")
	tk.MustQuery("SELECT t1.a FROM t1 GROUP BY t1.a HAVING t1.a IN (SELECT t2.a FROM t2 ORDER BY SUM(t1.b));")

	// a normal case
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1(a int not null, b int not null, index(a))")
	err = tk.ExecToErr("select b from t1 group by a")
	require.NotNil(t, err)
	require.Equal(t, err.Error(), "[planner:1055]Expression #1 of SELECT list is not in GROUP BY clause and contains nonaggregated column 'test.t1.b' which is not functionally dependent on columns in GROUP BY clause; this is incompatible with sql_mode=only_full_group_by")
}
