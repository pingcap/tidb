// Copyright 2023 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package ast_test

import (
	"fmt"
	"testing"

	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/stretchr/testify/require"
)

func TestProcedureVisitorCover(t *testing.T) {
	stmts := []ast.Node{
		&ast.StoreParameter{},
		&ast.ProcedureDecl{},
	}
	for _, v := range stmts {
		v.Accept(visitor{})
		v.Accept(visitor1{})
	}
	stmts2 := []ast.StmtNode{
		&ast.ProcedureBlock{},
		&ast.ProcedureInfo{ProcedureBody: &ast.ProcedureBlock{}},
		&ast.DropProcedureStmt{},
	}
	for _, v := range stmts2 {
		v.Accept(visitor{})
		v.Accept(visitor1{})
	}
}
func TestProcedure(t *testing.T) {
	p := parser.New()
	testcases := []string{"create procedure proc_2() begin declare s varchar(100) DEFAULT FROM_UNIXTIME(1447430881);select s;SELECT * FROM `t1`;SELECT * FROM `t2`;INSERT INTO `t1` VALUES (111);END;",
		"create procedure if not exists proc_2() begin declare s varchar(100) DEFAULT FROM_UNIXTIME(1447430881);select s;SELECT * FROM `t1`;SELECT * FROM `t2`;INSERT INTO `t1` VALUES (111);END;",
		"create procedure if not exists proc_2(in id int,inout id2 int,out id3 int) begin declare s varchar(100) DEFAULT FROM_UNIXTIME(1447430881);select s;SELECT * FROM `t1`;SELECT * FROM `t2`;INSERT INTO `t1` VALUES (111);END;",
		"create procedure proc_2(in id bigint,in id2 varchar(100),in id3 decimal(30,2)) begin declare s varchar(100) DEFAULT FROM_UNIXTIME(1447430881);select s;SELECT * FROM `t1`;SELECT * FROM `t2`;INSERT INTO `t1` VALUES (111);END;",
		"create procedure proc_2(in id double,in id2 float,out id3 char(10),in id4 binary) begin declare s varchar(100) DEFAULT FROM_UNIXTIME(1447430881);select s;SELECT * FROM `t1`;SELECT * FROM `t2`;INSERT INTO `t1` VALUES (111);END;",
		"create procedure proc_2(in id VARBINARY(30),in id2 BLOB,out id3 TEXT,in id4 ENUM('1','2')) begin declare s varchar(100) DEFAULT FROM_UNIXTIME(1447430881);select s;SELECT * FROM `t1`;SELECT * FROM `t2`;INSERT INTO `t1` VALUES (111);END;",
		"create procedure proc_2(in id SET('1','2')) begin declare s varchar(100) DEFAULT FROM_UNIXTIME(1447430881);select s;SELECT * FROM `t1`;SELECT * FROM `t2`;INSERT INTO `t1` VALUES (111);END;",
		"create procedure proc_2(in id SET('1','2')) begin declare s varchar(100) DEFAULT FROM_UNIXTIME(1447430881);select a.id,a.username,a.password,a.age,a.sex from user a where a.id > 10 and a.id < 50;" +
			"select us.subject,count(us.user_id),sum(us.score),avg(us.score),max(us.score),min(us.score) from user_score us where us.score > 90 group by us.subject;END;",
		"create procedure proc_2(in id SET('1','2')) select *,rank() over (partition by subject order by score desc) as ranking from user_score;select *,rank() over (partition by subject order by score desc) as ranking from user_score;",
		"create procedure proc_2(in id SET('1','2')) begin select us.*,sum(us.score) over (order by us.id) as current_sum," +
			"avg(us.score) over (order by us.id) as current_avg,count(us.score) over (order by us.id) as current_count,max(us.score) over (order by us.id) as current_max,min(us.score) over (order by us.id) as current_min from user_score us;" +
			"select us.*,sum(us.score) over (order by us.id) as current_sum, avg(us.score) over (order by us.id) as current_avg,count(us.score) over (order by us.id) as current_count,max(us.score) over (order by us.id) as current_max," +
			"min(us.score) over (order by us.id) as current_min,u.username ,ua.address,CONCAT(u.username, \"-\" ,ua.address) as userinfo from user_score us left join user u on u.id = us.user_id left join user_address ua on ua.id = us.user_id; end;",
		"create procedure proc_2() begin SELECT DISTINCT us.user_id,u.username ,ua.address,CONCAT(u.username, \"-\" ,ua.address) as userinfo, sum(us.score) from user_score us left join user u on u.id = us.user_id" +
			"left join user_address ua on ua.id = us.user_id group by us.user_id,u.username;" +
			"select a.subject,a.id,a.user_id,u.username, a.score,a.rownum from (select id,user_id,subject,score,row_number() over (order by score desc) as rownum from user_score) as a left join user u on a.user_id = u.id" +
			"inner join user_score as b on a.id=b.id where a.rownum<=10 order by a.rownum ;" +
			"select a.subject,a.id,a.score,a.rownum from (" +
			"select id,subject,score,row_number() over (partition by subject order by score desc) as rownum from user_score) as a inner join user_score as b on a.id=b.id where a.rownum<=10 order by a.subject ;" +
			"select *,u.username,ua.address,CONCAT(u.username, \"-\" ,ua.address) as userinfo,avg(us.score) over (order by us.id rows 2 preceding) as current_avg,sum(score) over (order by us.id rows 2 preceding) as current_sum from user_score us" +
			" left join user u on u.id = us.user_id left join user_address ua on ua.id = us.user_id;" +
			"select a.id,a.username,a.password,a.age,a.sex from user a where a.id in (select user_id from user_score where score > 90);	end;",
		"create procedure proc_2() begin select us.user_id,u.username,us.subject,us.score from user_score us left join user u on u.id = us.user_id where us.score > 90 group by us.user_id,us.subject,us.score;" +
			"select us.user_id,u.username,us.subject,us.score from user_score us join user u on u.id = us.user_id where us.score > 90 group by us.user_id,us.subject,us.score;" +
			"select a.id,a.username,a.password,a.age,a.sex,ad.address,CONCAT(a.username, \"-\" ,ad.address) as userinfo from user a left join user_address ad on a.id = ad.user_id where a.id > 10 and a.id < 50;" +
			"select a.id,a.username,a.password,a.age,a.sex,ad.score from user a right join user_score ad on a.id = ad.user_id where a.id > 10 and a.id < 50;" +
			"select a.id,a.username,a.password,a.age,a.sex,ad.score from user a left join user_score ad on a.id = ad.user_id where a.id in (select user_id from user_score where score > 90 and score < 99 ) " +
			"union select a.id,a.username,a.password,a.age,a.sex,ad.score from user a left join user_score ad on a.id = ad.user_id where a.id in (select user_id from user_score where score > 30 and score < 70 ); end;",
		`create procedure proc_2() begin select * from t1; if i > 1 then select 2 ; end if ;end`,
		`create procedure proc_2() begin select * from t1; if i > 1 then select 2; else select id from t2  ; end if ; end`,
		`create procedure proc_2() begin select * from t1; if i > 1 then select 2; elseif i = 3 then select 4; else select 5 ; end if; end`,
		`create procedure proc_2(id int) begin while id < 10 do set id = id + 1; select 1; end while; end`,
		`create procedure proc_2() begin declare test1 CURSOR for select 1; end;`,
		`create procedure proc_2() begin declare test1 CURSOR for select 1; select 1;open test1; end;`,
		`create procedure proc_2() begin declare a int;declare test1 CURSOR for select 1; select 1;open test1; fetch test1 into a; close test1;end;`,
		`create procedure proc_2() begin declare a int;declare exit handler for 1111 select 1 ; end;`,
		`create procedure proc_2() begin declare a int;declare exit handler for 1111,1112 select 1 ; end;`,
		`create procedure proc_2() begin declare a int;declare exit handler for SQLWARNING,NOT FOUND,SQLEXCEPTION select 1 ; end;`,
		`create procedure proc_2() begin declare a int;declare continue handler for SQLWARNING,NOT FOUND,SQLEXCEPTION select 1 ; end;`,
		`create procedure proc_2() begin declare a int;declare continue handler for sqlstate 'ssss' select 1 ; end;`,
		`create procedure proc_2() begin declare a int;declare continue handler for sqlstate 'ssss' begin select 1; end; end;`,
		`create procedure proc_2() begin declare a int;declare continue handler for sqlstate 'ssss' while id < 10 do set id = id + 1; select 1; end while; end;`,
		`create procedure proc_2() begin declare a int;declare continue handler for sqlstate 'ssss' if i > 1 then select 2; elseif i = 3 then select 4;end if; end;`,
		`create procedure proc_2() case now() when "1980-10-01" Then select 1; end case;`,
		`create procedure proc_2() case now() when "1980-10-01" Then select 1; when "1980-10-01" then select 2; end case;`,
		`create procedure proc_2() case now() when "1980-10-01" Then select 1; when "1980-10-01" then select 2; else select 3; end case;`,
		`create procedure proc_2(id int) case when id = 1 Then select 1; end case;`,
		`create procedure proc_2(id int) case when id = 1 Then select 1; when id = 2 then select 2; end case;`,
		`create procedure proc_2(id int) case when id = 1 Then select 1; when id = 2 then select 2; else select 3; end case;`,
		`create procedure proc_2(id int) begin REPEAT set id = id + 1; select 1; UNTIL id < 10 end REPEAT; end`,
		`create procedure proc_2() labelname: begin declare a int;declare continue handler for SQLWARNING,NOT FOUND,SQLEXCEPTION select 1 ; end;`,
		`create procedure proc_2() labelname: begin declare a int;declare continue handler for SQLWARNING,NOT FOUND,SQLEXCEPTION select 1 ; end labelname;`,
		`create procedure proc_2() begin labelname: while id < 10 do set id = id + 1; select 1; end while; end`,
		`create procedure proc_2() begin labelname: while id < 10 do set id = id + 1; select 1; end while labelname; end`,
		`create procedure proc_2(id int) begin labelname: REPEAT set id = id + 1; select 1; UNTIL id < 10 end REPEAT labelname; end`,
	}
	for _, testcase := range testcases {
		stmt, _, err := p.Parse(testcase, "", "")
		if err != nil {
			fmt.Println(testcase)
		}
		require.NoError(t, err)
		_, ok := stmt[0].(*ast.ProcedureInfo)
		require.True(t, ok, testcase)
	}
}

func TestShowCreateProcedure(t *testing.T) {
	p := parser.New()
	stmt, _, err := p.Parse("show create procedure proc_2", "", "")
	require.NoError(t, err)
	_, ok := stmt[0].(*ast.ShowStmt)
	require.True(t, ok)
	stmt, _, err = p.Parse("drop procedure proc_2", "", "")
	require.NoError(t, err)
	_, ok = stmt[0].(*ast.DropProcedureStmt)
	require.True(t, ok)
}

func TestProcedureVisitor(t *testing.T) {
	sqls := []string{
		"create procedure proc_2(in id bigint,in id2 varchar(100),in id3 decimal(30,2)) begin declare s varchar(100) DEFAULT FROM_UNIXTIME(1447430881);select s;SELECT * FROM `t1`;SELECT * FROM `t2`;INSERT INTO `t1` VALUES (111);END;",
		"show create procedure proc_2;",
		"drop procedure proc_2;",
	}
	parse := parser.New()
	for _, sql := range sqls {
		stmts, _, err := parse.Parse(sql, "", "")
		require.NoError(t, err)
		for _, stmt := range stmts {
			stmt.Accept(visitor{})
			stmt.Accept(visitor1{})
		}
	}
}

func TestProcedureRestore(t *testing.T) {
	testCases := []NodeRestoreTestCase{
		{"CREATE PROCEDURE `proc_2`( IN `id` BIGINT(20), IN `id2` VARCHAR(100), IN `id3` DECIMAL(30,2)) BEGIN DECLARE `s` VARCHAR(100) DEFAULT FROM_UNIXTIME(1447430881);SELECT `s`;SELECT * FROM `t1`;SELECT * FROM `t2`;INSERT INTO `t1` VALUES (111); END",
			"CREATE PROCEDURE `proc_2`( IN `id` BIGINT(20), IN `id2` VARCHAR(100), IN `id3` DECIMAL(30,2)) BEGIN DECLARE `s` VARCHAR(100) DEFAULT FROM_UNIXTIME(1447430881);SELECT `s`;SELECT * FROM `t1`;SELECT * FROM `t2`;INSERT INTO `t1` VALUES (111); END",
		},
		{
			"CREATE PROCEDURE `proc_2`() BEGIN SELECT * FROM `t1`;IF `i`>1 THEN SELECT 2;ELSEIF `i`=3 THEN SELECT 4;ELSE SELECT 5;END IF; END",
			"CREATE PROCEDURE `proc_2`() BEGIN SELECT * FROM `t1`;IF `i`>1 THEN SELECT 2;ELSEIF `i`=3 THEN SELECT 4;ELSE SELECT 5;END IF; END",
		},
		{
			"CREATE PROCEDURE `proc_2`() BEGIN SELECT * FROM `t1`;IF `i`>1 THEN SELECT 2;ELSEIF `i`=3 THEN SELECT 4;END IF; END",
			"CREATE PROCEDURE `proc_2`() BEGIN SELECT * FROM `t1`;IF `i`>1 THEN SELECT 2;ELSEIF `i`=3 THEN SELECT 4;END IF; END",
		},
		{
			"CREATE PROCEDURE `proc_2`() BEGIN SELECT * FROM `t1`;IF `i`>1 THEN SELECT 2;END IF; END",
			"CREATE PROCEDURE `proc_2`() BEGIN SELECT * FROM `t1`;IF `i`>1 THEN SELECT 2;END IF; END",
		},
		{
			"CREATE PROCEDURE `proc_2`() BEGIN SELECT * FROM `t1`;IF `i`>1 THEN SELECT 2;ELSE SELECT 5;END IF; END",
			"CREATE PROCEDURE `proc_2`() BEGIN SELECT * FROM `t1`;IF `i`>1 THEN SELECT 2;ELSE SELECT 5;END IF; END",
		},
		{
			"CREATE PROCEDURE `proc_2`( IN `id` INT(11)) BEGIN WHILE `id`<10 DO SET @@SESSION.`id`=`id`+1;SELECT 1;END WHILE; END",
			"CREATE PROCEDURE `proc_2`( IN `id` INT(11)) BEGIN WHILE `id`<10 DO SET @@SESSION.`id`=`id`+1;SELECT 1;END WHILE; END",
		},
		{
			"CREATE PROCEDURE `proc_2`() BEGIN DECLARE `a` INT(11);DECLARE TEST1 CURSOR FOR SELECT 1;SELECT 1;OPEN TEST1;FETCH TEST1 INTO A;CLOSE TEST1; END",
			"CREATE PROCEDURE `proc_2`() BEGIN DECLARE `a` INT(11);DECLARE TEST1 CURSOR FOR SELECT 1;SELECT 1;OPEN TEST1;FETCH TEST1 INTO A;CLOSE TEST1; END",
		},
		{
			"CREATE PROCEDURE `proc_2`() BEGIN DECLARE `a` INT(11);DECLARE EXIT HANDLER FOR SQLWARNING, NOT FOUND, SQLEXCEPTION SELECT 1; END",
			"CREATE PROCEDURE `proc_2`() BEGIN DECLARE `a` INT(11);DECLARE EXIT HANDLER FOR SQLWARNING, NOT FOUND, SQLEXCEPTION SELECT 1; END",
		},
		{
			"CREATE PROCEDURE `proc_2`( INOUT `id` BIGINT(20), OUT `id1` BIGINT(20)) BEGIN DECLARE `a` INT(11);DECLARE CONTINUE HANDLER FOR 1211, SQLSTATE 'xdw' SELECT 1; END",
			"CREATE PROCEDURE `proc_2`( INOUT `id` BIGINT(20), OUT `id1` BIGINT(20)) BEGIN DECLARE `a` INT(11);DECLARE CONTINUE HANDLER FOR 1211, SQLSTATE 'xdw' SELECT 1; END",
		},
		{
			"CREATE PROCEDURE `proc_2`() BEGIN DECLARE `a` INT(11);DECLARE CONTINUE HANDLER FOR SQLSTATE 'ssss' WHILE `id`<10 DO SET @@SESSION.`id`=`id`+1;SELECT 1;END WHILE; END",
			"CREATE PROCEDURE `proc_2`() BEGIN DECLARE `a` INT(11);DECLARE CONTINUE HANDLER FOR SQLSTATE 'ssss' WHILE `id`<10 DO SET @@SESSION.`id`=`id`+1;SELECT 1;END WHILE; END",
		},
		{
			"CREATE PROCEDURE `proc_2`() CASE NOW() WHEN _UTF8MB4'1980-10-01' THEN SELECT 1; END CASE",
			"CREATE PROCEDURE `proc_2`() CASE NOW() WHEN _UTF8MB4'1980-10-01' THEN SELECT 1; END CASE",
		},
		{
			"CREATE PROCEDURE `proc_2`() CASE NOW() WHEN _UTF8MB4'1980-10-01' THEN SELECT 1;WHEN _UTF8MB4'1980-10-02' THEN SELECT 2; END CASE",
			"CREATE PROCEDURE `proc_2`() CASE NOW() WHEN _UTF8MB4'1980-10-01' THEN SELECT 1;WHEN _UTF8MB4'1980-10-02' THEN SELECT 2; END CASE",
		},
		{
			"CREATE PROCEDURE `proc_2`() CASE NOW() WHEN _UTF8MB4'1980-10-01' THEN SELECT 1;WHEN _UTF8MB4'1980-10-02' THEN SELECT 2; ELSE SELECT 3; END CASE",
			"CREATE PROCEDURE `proc_2`() CASE NOW() WHEN _UTF8MB4'1980-10-01' THEN SELECT 1;WHEN _UTF8MB4'1980-10-02' THEN SELECT 2; ELSE SELECT 3; END CASE",
		},
		{
			"CREATE PROCEDURE `proc_2`() CASE WHEN _UTF8MB4'1980-10-01' THEN SELECT 1; END CASE",
			"CREATE PROCEDURE `proc_2`() CASE WHEN _UTF8MB4'1980-10-01' THEN SELECT 1; END CASE",
		},
		{
			"CREATE PROCEDURE `proc_2`() CASE WHEN _UTF8MB4'1980-10-01' THEN SELECT 1;WHEN _UTF8MB4'1980-10-02' THEN SELECT 2; END CASE",
			"CREATE PROCEDURE `proc_2`() CASE WHEN _UTF8MB4'1980-10-01' THEN SELECT 1;WHEN _UTF8MB4'1980-10-02' THEN SELECT 2; END CASE",
		},
		{
			"CREATE PROCEDURE `proc_2`() CASE WHEN _UTF8MB4'1980-10-01' THEN SELECT 1;WHEN _UTF8MB4'1980-10-02' THEN SELECT 2; ELSE SELECT 3; END CASE",
			"CREATE PROCEDURE `proc_2`() CASE WHEN _UTF8MB4'1980-10-01' THEN SELECT 1;WHEN _UTF8MB4'1980-10-02' THEN SELECT 2; ELSE SELECT 3; END CASE",
		},
		{
			"CREATE PROCEDURE `proc_2`() `labelname`: BEGIN DECLARE `a` INT(11);DECLARE CONTINUE HANDLER FOR SQLWARNING, NOT FOUND, SQLEXCEPTION SELECT 1; END `labelname`",
			"CREATE PROCEDURE `proc_2`() `labelname`: BEGIN DECLARE `a` INT(11);DECLARE CONTINUE HANDLER FOR SQLWARNING, NOT FOUND, SQLEXCEPTION SELECT 1; END `labelname`",
		},
		{
			"CREATE PROCEDURE `proc_2`() BEGIN `labelname`: WHILE `id`<10 DO SET @@SESSION.`id`=`id`+1;SELECT 1;END WHILE `labelname`; END",
			"CREATE PROCEDURE `proc_2`() BEGIN `labelname`: WHILE `id`<10 DO SET @@SESSION.`id`=`id`+1;SELECT 1;END WHILE `labelname`; END",
		},
		{
			"CREATE PROCEDURE `proc_2`( IN `id` INT(11)) BEGIN `labelname`: REPEAT SET @@SESSION.`id`=`id`+1;SELECT 1;UNTIL `id`<10 END REPEAT `labelname`; END",
			"CREATE PROCEDURE `proc_2`( IN `id` INT(11)) BEGIN `labelname`: REPEAT SET @@SESSION.`id`=`id`+1;SELECT 1;UNTIL `id`<10 END REPEAT `labelname`; END",
		},
	}
	extractNodeFunc := func(node ast.Node) ast.Node {
		return node.(*ast.ProcedureInfo)
	}
	runNodeRestoreTest(t, testCases, "%s", extractNodeFunc)
}
