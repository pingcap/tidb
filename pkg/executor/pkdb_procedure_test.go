// Copyright 2022-2023 PingCAP, Inc.

package executor_test

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/util/sqlescape"
	"github.com/stretchr/testify/require"
)

func TestCreateShowDropProcedure(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.InProcedure()
	// no database
	tk.MustGetErrCode("create procedure sp_test() begin select @a; end;", 1046)
	tk.MustGetErrCode("create procedure test2.sp_test() begin select @a; end;", 1049)
	tk.MustExec("create procedure test.sp_test() begin select @a; end;")
	tk.MustQuery("show create procedure test.sp_test").Check(testkit.Rows("sp_test ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION " +
		" CREATE PROCEDURE `sp_test`()\nbegin select @a; end utf8mb4 utf8mb4_bin utf8mb4_bin"))
	tk.MustExec("use test")
	tk.MustGetErrCode("create procedure sp_test() begin select@a; end;", 1304)
	tk.MustExec("create procedure if not exists sp_test() begin select@b; end;")
	tk.MustQuery("show create procedure test.sp_test").Check(testkit.Rows("sp_test ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION " +
		" CREATE PROCEDURE `sp_test`()\nbegin select @a; end utf8mb4 utf8mb4_bin utf8mb4_bin"))
	// in/out/inout
	tk.MustExec("create procedure if not exists sp_test1(id int) begin select@b; end;")
	tk.MustQuery("show create procedure sp_test1").Check(testkit.Rows("sp_test1 ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION " +
		" CREATE PROCEDURE `sp_test1`(id int)\nbegin select@b; end utf8mb4 utf8mb4_bin utf8mb4_bin"))
	tk.MustExec("create procedure if not exists sp_test2(in id int) begin select@b; end;")
	tk.MustQuery("show create procedure sp_test2").Check(testkit.Rows("sp_test2 ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION " +
		" CREATE PROCEDURE `sp_test2`(in id int)\nbegin select@b; end utf8mb4 utf8mb4_bin utf8mb4_bin"))
	tk.MustExec("create procedure if not exists sp_test3(out id int) begin select@b; end;")
	tk.MustQuery("show create procedure sp_test3").Check(testkit.Rows("sp_test3 ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION " +
		" CREATE PROCEDURE `sp_test3`(out id int)\nbegin select@b; end utf8mb4 utf8mb4_bin utf8mb4_bin"))
	tk.MustExec("create procedure if not exists sp_test4(inout id int) begin select@b; end;")
	tk.MustQuery("show create procedure sp_test4").Check(testkit.Rows("sp_test4 ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION " +
		" CREATE PROCEDURE `sp_test4`(inout id int)\nbegin select@b; end utf8mb4 utf8mb4_bin utf8mb4_bin"))
	tk.MustExec("create procedure if not exists sp_test5(id int,in id1 int,out id2 varchar(100),inout id3 int) begin select@b; end;")
	tk.MustQuery("show create procedure sp_test5").Check(testkit.Rows("sp_test5 ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION " +
		" CREATE PROCEDURE `sp_test5`(id int,in id1 int,out id2 varchar(100),inout id3 int)\nbegin select@b; end utf8mb4 utf8mb4_bin utf8mb4_bin"))
	// Duplicate input parameter name.
	tk.MustGetErrCode("create procedure sp_test6(in id int, out id int) begin select @a; end;", 1330)
	// parameter does not exist.
	tk.MustGetErrCode("create procedure sp_test6() begin set a = 1; end;", 1193)
	// Variables should be applied before sql.
	tk.MustGetErrCode("create procedure sp_test6() begin set @a = 1;declare s varchar(100); end;", 1064)
	// sp variables can only be declared inside.
	tk.MustGetErrCode("create procedure sp_test6() begin set @a = 1; end;declare s varchar(100);", 1064)
	// drop procedure
	tk.MustExec("drop procedure sp_test1")
	tk.MustExec("drop procedure sp_test2")
	tk.MustExec("drop procedure sp_test3")
	tk.MustExec("drop procedure sp_test4")
	tk.MustExec("drop procedure sp_test5")
	err := tk.QueryToErr("show create procedure sp_test1")
	require.EqualError(t, err, "[executor:1305]PROCEDURE test.sp_test1 does not exist")

	testcases := []string{"create procedure proc_1() begin declare s varchar(100) DEFAULT FROM_UNIXTIME(1447430881);select s;SELECT * FROM `t1`;SELECT * FROM `t2`;INSERT INTO `t1` VALUES (111);END;",
		"create procedure if not exists proc_2() begin declare s varchar(100) DEFAULT FROM_UNIXTIME(1447430881);select s;SELECT * FROM `t1`;SELECT * FROM `t2`;INSERT INTO `t1` VALUES (111);END;",
		"create procedure if not exists proc_3(in id int,inout id2 int,out id3 int) begin declare s varchar(100) DEFAULT FROM_UNIXTIME(1447430881);select s;SELECT * FROM `t1`;SELECT * FROM `t2`;INSERT INTO `t1` VALUES (111);END;",
		"create procedure proc_4(in id bigint,in id2 varchar(100),in id3 decimal(30,2)) begin declare s varchar(100) DEFAULT FROM_UNIXTIME(1447430881);select s;SELECT * FROM `t1`;SELECT * FROM `t2`;INSERT INTO `t1` VALUES (111);END;",
		"create procedure proc_5(in id double,in id2 float,out id3 char(10),in id4 binary) begin declare s varchar(100) DEFAULT FROM_UNIXTIME(1447430881);select s;SELECT * FROM `t1`;SELECT * FROM `t2`;INSERT INTO `t1` VALUES (111);END;",
		"create procedure proc_6(in id VARBINARY(30),in id2 BLOB,out id3 TEXT,in id4 ENUM('1','2')) begin declare s varchar(100) DEFAULT FROM_UNIXTIME(1447430881);select s;SELECT * FROM `t1`;SELECT * FROM `t2`;INSERT INTO `t1` VALUES (111);END;",
		"create procedure proc_7(in id SET('1','2')) begin declare s varchar(100) DEFAULT FROM_UNIXTIME(1447430881);select s;SELECT * FROM `t1`;SELECT * FROM `t2`;INSERT INTO `t1` VALUES (111);END;",
		"create procedure proc_8(in id SET('1','2')) begin declare s varchar(100) DEFAULT FROM_UNIXTIME(1447430881);select a.id,a.username,a.password,a.age,a.sex from user a where a.id > 10 and a.id < 50;" +
			"select us.subject,count(us.user_id),sum(us.score),avg(us.score),max(us.score),min(us.score) from user_score us where us.score > 90 group by us.subject;END;",
		"create procedure proc_9(in id SET('1','2')) begin select *,rank() over (partition by subject order by score desc) as ranking from user_score;select *,rank() over (partition by subject order by score desc) as ranking from user_score;end",
		"create procedure proc_10(in id SET('1','2')) begin select us.*,sum(us.score) over (order by us.id) as current_sum," +
			"avg(us.score) over (order by us.id) as current_avg,count(us.score) over (order by us.id) as current_count,max(us.score) over (order by us.id) as current_max,min(us.score) over (order by us.id) as current_min from user_score us;" +
			"select us.*,sum(us.score) over (order by us.id) as current_sum, avg(us.score) over (order by us.id) as current_avg,count(us.score) over (order by us.id) as current_count,max(us.score) over (order by us.id) as current_max," +
			"min(us.score) over (order by us.id) as current_min,u.username ,ua.address,CONCAT(u.username, \"-\" ,ua.address) as userinfo from user_score us left join user u on u.id = us.user_id left join user_address ua on ua.id = us.user_id; end;",
		"create procedure proc_11() begin SELECT DISTINCT us.user_id,u.username ,ua.address,CONCAT(u.username, \"-\" ,ua.address) as userinfo, sum(us.score) from user_score us left join user u on u.id = us.user_id" +
			"left join user_address ua on ua.id = us.user_id group by us.user_id,u.username;" +
			"select a.subject,a.id,a.user_id,u.username, a.score,a.rownum from (select id,user_id,subject,score,row_number() over (order by score desc) as rownum from user_score) as a left join user u on a.user_id = u.id" +
			"inner join user_score as b on a.id=b.id where a.rownum<=10 order by a.rownum ;" +
			"select a.subject,a.id,a.score,a.rownum from (" +
			"select id,subject,score,row_number() over (partition by subject order by score desc) as rownum from user_score) as a inner join user_score as b on a.id=b.id where a.rownum<=10 order by a.subject ;" +
			"select *,u.username,ua.address,CONCAT(u.username, \"-\" ,ua.address) as userinfo,avg(us.score) over (order by us.id rows 2 preceding) as current_avg,sum(score) over (order by us.id rows 2 preceding) as current_sum from user_score us" +
			" left join user u on u.id = us.user_id left join user_address ua on ua.id = us.user_id;" +
			"select a.id,a.username,a.password,a.age,a.sex from user a where a.id in (select user_id from user_score where score > 90);    end;",
		"create procedure proc_12() begin select us.user_id,u.username,us.subject,us.score from user_score us left join user u on u.id = us.user_id where us.score > 90 group by us.user_id,us.subject,us.score;" +
			"select us.user_id,u.username,us.subject,us.score from user_score us join user u on u.id = us.user_id where us.score > 90 group by us.user_id,us.subject,us.score;" +
			"select a.id,a.username,a.password,a.age,a.sex,ad.address,CONCAT(a.username, \"-\" ,ad.address) as userinfo from user a left join user_address ad on a.id = ad.user_id where a.id > 10 and a.id < 50;" +
			"select a.id,a.username,a.password,a.age,a.sex,ad.score from user a right join user_score ad on a.id = ad.user_id where a.id > 10 and a.id < 50;" +
			"select a.id,a.username,a.password,a.age,a.sex,ad.score from user a left join user_score ad on a.id = ad.user_id where a.id in (select user_id from user_score where score > 90 and score < 99 ) " +
			"union select a.id,a.username,a.password,a.age,a.sex,ad.score from user a left join user_score ad on a.id = ad.user_id where a.id in (select user_id from user_score where score > 30 and score < 70 ); end;",
		// block recursion
		"create procedure proc_13() begin select @a; begin select @b; end; end",
		"create procedure proc_14() begin select @a; insert into t2 select * from t1; begin select @b;update t2 set id = 1; end; end",
		// test declared variable type
		"create procedure proc_15() begin declare s varchar(100) DEFAULT FROM_UNIXTIME(1447430881); select @a; insert into t2 select * from t1; begin declare s varchar(100); select @b;update t2 set id = 1; end; end",
		"create procedure proc_16() begin declare s varchar(100) ; select @a; insert into t2 select * from t1; begin declare s varchar(100); select @b;update t2 set id = 1; end; end",
		"create procedure proc_17() begin declare s char(100) ;select @a; insert into t2 select * from t1; begin declare s varchar(100); select @b;update t2 set id = 1; end; end",
		// test default
		"create procedure proc_18() begin declare s bigint default @a;select @a; insert into t2 select * from t1; begin declare s varchar(100); select @b;update t2 set id = 1; end; end",
		// test insert select
		"create procedure proc_19() begin select @a; insert into t2 select * from t1; begin declare s varchar(100);begin declare s varchar(100);select @b;update t2 set id = 1; end; select @b;update t2 set id = 1; end; end",
	}
	res := []string{
		" CREATE PROCEDURE `proc_1`()\nbegin declare s varchar(100) DEFAULT FROM_UNIXTIME(1447430881);select s;SELECT * FROM `t1`;SELECT * FROM `t2`;INSERT INTO `t1` VALUES (111);END",
		" CREATE PROCEDURE `proc_2`()\nbegin declare s varchar(100) DEFAULT FROM_UNIXTIME(1447430881);select s;SELECT * FROM `t1`;SELECT * FROM `t2`;INSERT INTO `t1` VALUES (111);END",
		" CREATE PROCEDURE `proc_3`(in id int,inout id2 int,out id3 int)\nbegin declare s varchar(100) DEFAULT FROM_UNIXTIME(1447430881);select s;SELECT * FROM `t1`;SELECT * FROM `t2`;INSERT INTO `t1` VALUES (111);END",
		" CREATE PROCEDURE `proc_4`(in id bigint,in id2 varchar(100),in id3 decimal(30,2))\nbegin declare s varchar(100) DEFAULT FROM_UNIXTIME(1447430881);select s;SELECT * FROM `t1`;SELECT * FROM `t2`;INSERT INTO `t1` VALUES (111);END",
		" CREATE PROCEDURE `proc_5`(in id double,in id2 float,out id3 char(10),in id4 binary)\nbegin declare s varchar(100) DEFAULT FROM_UNIXTIME(1447430881);select s;SELECT * FROM `t1`;SELECT * FROM `t2`;INSERT INTO `t1` VALUES (111);END",
		" CREATE PROCEDURE `proc_6`(in id VARBINARY(30),in id2 BLOB,out id3 TEXT,in id4 ENUM('1','2'))\nbegin declare s varchar(100) DEFAULT FROM_UNIXTIME(1447430881);select s;SELECT * FROM `t1`;SELECT * FROM `t2`;INSERT INTO `t1` VALUES (111);END",
		" CREATE PROCEDURE `proc_7`(in id SET('1','2'))\nbegin declare s varchar(100) DEFAULT FROM_UNIXTIME(1447430881);select s;SELECT * FROM `t1`;SELECT * FROM `t2`;INSERT INTO `t1` VALUES (111);END",
		" CREATE PROCEDURE `proc_8`(in id SET('1','2'))\nbegin declare s varchar(100) DEFAULT FROM_UNIXTIME(1447430881);select a.id,a.username,a.password,a.age,a.sex from user a where a.id > 10 and a.id < 50;" +
			"select us.subject,count(us.user_id),sum(us.score),avg(us.score),max(us.score),min(us.score) from user_score us where us.score > 90 group by us.subject;END",
		" CREATE PROCEDURE `proc_9`(in id SET('1','2'))\nbegin select *,rank() over (partition by subject order by score desc) as ranking from user_score;select *,rank() over (partition by subject order by score desc) as ranking from user_score;end",
		" CREATE PROCEDURE `proc_10`(in id SET('1','2'))\nbegin select us.*,sum(us.score) over (order by us.id) as current_sum," +
			"avg(us.score) over (order by us.id) as current_avg,count(us.score) over (order by us.id) as current_count,max(us.score) over (order by us.id) as current_max,min(us.score) over (order by us.id) as current_min from user_score us;" +
			"select us.*,sum(us.score) over (order by us.id) as current_sum, avg(us.score) over (order by us.id) as current_avg,count(us.score) over (order by us.id) as current_count,max(us.score) over (order by us.id) as current_max," +
			"min(us.score) over (order by us.id) as current_min,u.username ,ua.address,CONCAT(u.username, \"-\" ,ua.address) as userinfo from user_score us left join user u on u.id = us.user_id left join user_address ua on ua.id = us.user_id; end",
		" CREATE PROCEDURE `proc_11`()\nbegin SELECT DISTINCT us.user_id,u.username ,ua.address,CONCAT(u.username, \"-\" ,ua.address) as userinfo, sum(us.score) from user_score us left join user u on u.id = us.user_id" +
			"left join user_address ua on ua.id = us.user_id group by us.user_id,u.username;" +
			"select a.subject,a.id,a.user_id,u.username, a.score,a.rownum from (select id,user_id,subject,score,row_number() over (order by score desc) as rownum from user_score) as a left join user u on a.user_id = u.id" +
			"inner join user_score as b on a.id=b.id where a.rownum<=10 order by a.rownum ;" +
			"select a.subject,a.id,a.score,a.rownum from (" +
			"select id,subject,score,row_number() over (partition by subject order by score desc) as rownum from user_score) as a inner join user_score as b on a.id=b.id where a.rownum<=10 order by a.subject ;" +
			"select *,u.username,ua.address,CONCAT(u.username, \"-\" ,ua.address) as userinfo,avg(us.score) over (order by us.id rows 2 preceding) as current_avg,sum(score) over (order by us.id rows 2 preceding) as current_sum from user_score us" +
			" left join user u on u.id = us.user_id left join user_address ua on ua.id = us.user_id;" +
			"select a.id,a.username,a.password,a.age,a.sex from user a where a.id in (select user_id from user_score where score > 90);    end",
		" CREATE PROCEDURE `proc_12`()\nbegin select us.user_id,u.username,us.subject,us.score from user_score us left join user u on u.id = us.user_id where us.score > 90 group by us.user_id,us.subject,us.score;" +
			"select us.user_id,u.username,us.subject,us.score from user_score us join user u on u.id = us.user_id where us.score > 90 group by us.user_id,us.subject,us.score;" +
			"select a.id,a.username,a.password,a.age,a.sex,ad.address,CONCAT(a.username, \"-\" ,ad.address) as userinfo from user a left join user_address ad on a.id = ad.user_id where a.id > 10 and a.id < 50;" +
			"select a.id,a.username,a.password,a.age,a.sex,ad.score from user a right join user_score ad on a.id = ad.user_id where a.id > 10 and a.id < 50;" +
			"select a.id,a.username,a.password,a.age,a.sex,ad.score from user a left join user_score ad on a.id = ad.user_id where a.id in (select user_id from user_score where score > 90 and score < 99 ) " +
			"union select a.id,a.username,a.password,a.age,a.sex,ad.score from user a left join user_score ad on a.id = ad.user_id where a.id in (select user_id from user_score where score > 30 and score < 70 ); end",
		" CREATE PROCEDURE `proc_13`()\nbegin select @a; begin select @b; end; end",
		" CREATE PROCEDURE `proc_14`()\nbegin select @a; insert into t2 select * from t1; begin select @b;update t2 set id = 1; end; end",
		" CREATE PROCEDURE `proc_15`()\nbegin declare s varchar(100) DEFAULT FROM_UNIXTIME(1447430881); select @a; insert into t2 select * from t1; begin declare s varchar(100); select @b;update t2 set id = 1; end; end",
		" CREATE PROCEDURE `proc_16`()\nbegin declare s varchar(100) ; select @a; insert into t2 select * from t1; begin declare s varchar(100); select @b;update t2 set id = 1; end; end",
		" CREATE PROCEDURE `proc_17`()\nbegin declare s char(100) ;select @a; insert into t2 select * from t1; begin declare s varchar(100); select @b;update t2 set id = 1; end; end",
		" CREATE PROCEDURE `proc_18`()\nbegin declare s bigint default @a;select @a; insert into t2 select * from t1; begin declare s varchar(100); select @b;update t2 set id = 1; end; end",
		" CREATE PROCEDURE `proc_19`()\nbegin select @a; insert into t2 select * from t1; begin declare s varchar(100);begin declare s varchar(100);select @b;update t2 set id = 1; end; select @b;update t2 set id = 1; end; end",
	}
	sqlmod := " ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION "
	collateStr := " utf8mb4 utf8mb4_bin utf8mb4_bin"
	sql := "show create procedure "
	for i, testcase := range testcases {
		tk.MustExec(testcase)
		name := "proc_" + strconv.Itoa(i+1)
		tk.MustQuery(sql + name).Check(testkit.Rows(name + sqlmod + res[i] + collateStr))
		tk.MustExec("drop procedure " + name)
	}

	tk.MustExec("create procedure sP_test1(id int) begin select@b; end;")
	tk.MustQuery("show create procedure sp_test1").Check(testkit.Rows("sP_test1 ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION  CREATE PROCEDURE `sP_test1`(id int)\nbegin select@b; end utf8mb4 utf8mb4_bin utf8mb4_bin"))
	tk.MustExec("drop procedure sp_test1")
	tk.MustGetErrCode("drop procedure proc_1", 1305)
	tk.MustExec("drop procedure if exists proc_1")
	tk.MustExec("set @@sql_mode = 'ANSI_QUOTES'")
	tk.MustExec("create procedure sP_test2(id int) begin select@b; end;")
	tk.MustQuery("show create procedure sp_test2").Check(testkit.Rows("sP_test2 ANSI_QUOTES  CREATE PROCEDURE `sP_test2`(id int)\nbegin select@b; end utf8mb4 utf8mb4_bin utf8mb4_bin"))
}

func TestProcedureSwitch(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustQuery("show variables like 'tidb_enable_procedure'").Check(testkit.Rows("tidb_enable_procedure OFF"))
	tk.InProcedure()
	tk.MustQuery("show variables like 'tidb_enable_procedure'").Check(testkit.Rows("tidb_enable_procedure ON"))
	tk.MustExec("use test")
	tk.MustExec("create table t1 (id int)")
	tk.MustGetErrCode("set tidb_enable_procedure = OFF ", 1229)
	tk.MustExec("set global tidb_enable_procedure = OFF")
	tk.MustGetErrMsg("create procedure t1() begin insert into t1 value(@a); end", "if enterprise edition, please set global tidb_enable_procedure = ON")
	tk.MustGetErrCode("call t1", 1305)
	tk.MustExec("set global tidb_enable_procedure = ON")
	tk.MustExec("create procedure t1() begin insert into t1 value(@a); end")
	tk.MustExec("set global tidb_enable_procedure = OFF")
	tk.MustGetErrMsg("call t1", "if enterprise edition, please set global tidb_enable_procedure = ON")
}
func TestBaseCall(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.InProcedure()
	tk.MustExec("use test")
	tk.MustExec("create table t1 (id int)")
	tk.MustExec("create procedure t1() begin insert into t1 value(@a); end")
	tk.MustExec("set @a = 1")
	tk.MustExec("call t1")
	tk.MustQuery("select * from t1 order by id").Check(testkit.Rows("1"))
	tk.MustExec("set @a = 2")
	tk.MustExec("call t1()")
	tk.MustQuery("select * from t1 order by id").Check(testkit.Rows("1", "2"))
	tk.MustExec("create procedure t2() begin select * from t1 order by id; end")
	tk.MustExec("call t2")
	for _, res := range tk.Res {
		res.Check(testkit.Rows("1", "2"))
	}
	tk.ClearProcedureRes()
	tk.MustExec("call t2()")
	for _, res := range tk.Res {
		res.Check(testkit.Rows("1", "2"))
	}
	tk.ClearProcedureRes()
	tk.MustExec("create procedure t3() begin update t1 set id = id + 1; end")
	tk.MustExec("call t3")
	tk.MustExec("call t2")
	for _, res := range tk.Res {
		res.Check(testkit.Rows("2", "3"))
	}
	tk.ClearProcedureRes()
	tk.MustExec("call t2()")
	for _, res := range tk.Res {
		res.Check(testkit.Rows("2", "3"))
	}
	tk.ClearProcedureRes()
	tk.MustExec("call t3()")
	tk.MustExec("call t2")
	for _, res := range tk.Res {
		res.Check(testkit.Rows("3", "4"))
	}
	tk.ClearProcedureRes()
	tk.MustExec("call t2()")
	for _, res := range tk.Res {
		res.Check(testkit.Rows("3", "4"))
	}
	tk.ClearProcedureRes()
	tk.MustExec("create procedure t4() begin select * from t1 order by id; select * from t1 order by id; select * from t1 order by id; end")
	tk.MustExec("call t4()")
	require.Equal(t, 3, len(tk.Res))
	for _, res := range tk.Res {
		res.Check(testkit.Rows("3", "4"))
	}
	tk.ClearProcedureRes()
	tk.MustExec("create procedure t5() begin select * from t1 order by id; update t1 set id = id + 1; select * from t1 order by id; end")
	tk.MustExec("call t5()")
	require.Equal(t, 2, len(tk.Res))
	tk.Res[0].Check(testkit.Rows("3", "4"))
	tk.Res[1].Check(testkit.Rows("4", "5"))
	tk.ClearProcedureRes()
	tk.MustExec("create procedure t6() begin select * from t1 order by id; update t1 set id = id + 1; begin update t1 set id = id + 1; select * from t1 order by id;insert into t1 value(1);end;select * from t1 order by id; end")
	tk.MustExec("call t6()")
	require.Equal(t, 3, len(tk.Res))
	tk.Res[0].Check(testkit.Rows("4", "5"))
	tk.Res[1].Check(testkit.Rows("6", "7"))
	tk.Res[2].Check(testkit.Rows("1", "6", "7"))
	tk.ClearProcedureRes()
	tk.MustExec("create table t2 (id int)")
	tk.MustExec("create procedure t7() insert into t2 select * from t1")
	tk.MustExec("call t7()")
	tk.MustQuery("select * from t2 order by id").Check(testkit.Rows("1", "6", "7"))
	tk.MustExec("truncate table t2")
	tk.MustExec("create procedure t8() insert into t2 select * from t1;insert into t2 select * from t1")
	tk.MustQuery("select * from t2 order by id").Check(testkit.Rows("1", "6", "7"))
	tk.MustQuery("show create procedure t8").Check(testkit.Rows("t8 ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION " +
		" CREATE PROCEDURE `t8`()\ninsert into t2 select * from t1 utf8mb4 utf8mb4_bin utf8mb4_bin"))

}

func TestCallSelect(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("CREATE TABLE `user` ( `id` int(11) NOT NULL, `username` VARCHAR(30) DEFAULT NULL, `password` VARCHAR(30) DEFAULT NULL, " +
		"`age` int(11) NOT NULL, `sex` int(11) NOT NULL, PRIMARY KEY (`id`), KEY `username` (`username`) ) ENGINE=InnoDB;")
	tk.MustExec("CREATE TABLE `user_score` ( `id` int(11) NOT NULL, `subject` int(11) NOT NULL, `user_id` int(11) NOT NULL, " +
		"`score` int(11) NOT NULL, PRIMARY KEY (`id`) ) ENGINE=InnoDB;")
	tk.MustExec("CREATE TABLE `user_address` ( `id` int(11) NOT NULL, `user_id` int(11) NOT NULL, `address` VARCHAR(30) DEFAULT NULL, " +
		"PRIMARY KEY (`id`), KEY `address` (`address`) ) ENGINE=InnoDB;")
	tk.InProcedure()
	tk.MustExec(`create procedure insert_data(i int, s_i int)  begin insert into user values(i, CONCAT("username-", i),CONCAT("password-", i),FLOOR( 15 + RAND() * 23),Mod(i,2));
    insert into user_score values(s_i, 1, i, FLOOR( 40 + i * 100));
    set s_i=s_i+1;
    insert into user_score values(s_i, 2, i, FLOOR( 40 + i * 100));
    set s_i=s_i+1;
    insert into user_score values(s_i, 3, i, FLOOR( 40 + i * 100));
    set s_i=s_i+1;
    insert into user_score values(s_i, 4, i, FLOOR( 40 + i * 100));
    set s_i=s_i+1;
    insert into user_score values(s_i, 5, i, FLOOR( 40 + i * 100));
    set s_i=s_i+1;
    insert into user_address values(i, i, CONCAT("useraddress-", i));
    set i=i+1;  end;`)
	for i := 1; i <= 100; i = i + 5 {
		sql := fmt.Sprintf("call insert_data(%d,%d)", i, i)
		tk.MustExec(sql)
	}

	tk.MustExec(`create procedure sp_select() begin
    select a.id,a.username,a.password,a.age,a.sex from user a where a.id > 10 and a.id < 50 order by a.id;

    select us.subject,count(us.user_id),sum(us.score),avg(us.score),max(us.score),min(us.score) from user_score us
        where us.score > 90 group by us.subject order by us.subject;

    select *,rank() over (partition by subject order by score desc) as ranking from user_score;

    select us.*,sum(us.score) over (order by us.id) as current_sum,
       avg(us.score) over (order by us.id) as current_avg,
       count(us.score) over (order by us.id) as current_count,
       max(us.score) over (order by us.id) as current_max,
       min(us.score) over (order by us.id) as current_min from user_score us;

    select us.*,sum(us.score) over (order by us.id) as current_sum,
       avg(us.score) over (order by us.id) as current_avg,
       count(us.score) over (order by us.id) as current_count,
       max(us.score) over (order by us.id) as current_max,
       min(us.score) over (order by us.id) as current_min,
       u.username ,ua.address,CONCAT(u.username, "-" ,ua.address) as userinfo
       from user_score us left join user u on u.id = us.user_id left join user_address ua on ua.id = us.user_id;

    SELECT DISTINCT us.user_id,u.username ,ua.address,CONCAT(u.username, "-" ,ua.address) as userinfo,
        sum(us.score) from user_score us left join user u on u.id = us.user_id
        left join user_address ua on ua.id = us.user_id group by us.user_id,u.username;

    select a.subject,a.id,a.user_id,u.username, a.score,a.rownum from ( select id,user_id,subject,score,row_number() over (order by score desc) as rownum from user_score) as a
        left join user u on a.user_id = u.id inner join user_score as b on a.id=b.id where a.rownum<=10 order by a.rownum ;

    select a.subject,a.id,a.score,a.rownum from (
        select id,subject,score,row_number() over (partition by subject order by score desc) as rownum from user_score) as a
        inner join user_score as b on a.id=b.id where a.rownum<=10 order by a.subject ;

    select *,u.username,ua.address,CONCAT(u.username, "-" ,ua.address) as userinfo,
        avg(us.score) over (order by us.id rows 2 preceding) as current_avg,
        sum(score) over (order by us.id rows 2 preceding) as current_sum from user_score us
        left join user u on u.id = us.user_id left join user_address ua on ua.id = us.user_id;

    select a.id,a.username,a.password,a.age,a.sex from user a where a.id in (select user_id from user_score where score > 90);

    select us.user_id,u.username,us.subject,us.score from user_score us left join user u on u.id = us.user_id
        where us.score > 90 group by us.user_id,us.subject,us.score;

    select us.user_id,u.username,us.subject,us.score from user_score us join user u on u.id = us.user_id
         where us.score > 90 group by us.user_id,us.subject,us.score;

    select a.id,a.username,a.password,a.age,a.sex,ad.address,CONCAT(a.username, "-" ,ad.address) as userinfo from user a
         left join user_address ad on a.id = ad.user_id where a.id > 10 and a.id < 50;

    select a.id,a.username,a.password,a.age,a.sex,ad.score from user a right join user_score ad on a.id = ad.user_id
        where a.id > 10 and a.id < 50;

    select a.id,a.username,a.password,a.age,a.sex,ad.score from user a
        left join user_score ad on a.id = ad.user_id where a.id in (select user_id from user_score where score > 90 and score < 99 )
        union select a.id,a.username,a.password,a.age,a.sex,ad.score from user a
        left join user_score ad on a.id = ad.user_id
        where a.id in (select user_id from user_score where score > 30 and score < 70 );
    end;
    `)
	tk.MustExec(`call sp_select`)
	require.Equal(t, 15, len(tk.Res))
	tk.MustQuery("select a.id,a.username,a.password,a.age,a.sex from user a where a.id > 10 and a.id < 50 order by a.id;").Check(tk.Res[0].Rows())
	tk.MustQuery(`select us.subject,count(us.user_id),sum(us.score),avg(us.score),max(us.score),min(us.score) from user_score us
	where us.score > 90 group by us.subject order by us.subject;`).Sort().Check(tk.Res[1].Sort().Rows())
	tk.MustQuery(`select *,rank() over (partition by subject order by score desc) as ranking from user_score;`).Sort().Check(tk.Res[2].Sort().Rows())
	tk.MustQuery(`select us.*,sum(us.score) over (order by us.id) as current_sum,
	avg(us.score) over (order by us.id) as current_avg,
	count(us.score) over (order by us.id) as current_count,
	max(us.score) over (order by us.id) as current_max,
	min(us.score) over (order by us.id) as current_min from user_score us;`).Sort().Check(tk.Res[3].Sort().Rows())
	tk.MustQuery(`select us.*,sum(us.score) over (order by us.id) as current_sum,
	avg(us.score) over (order by us.id) as current_avg,
	count(us.score) over (order by us.id) as current_count,
	max(us.score) over (order by us.id) as current_max,
	min(us.score) over (order by us.id) as current_min,
	u.username ,ua.address,CONCAT(u.username, "-" ,ua.address) as userinfo
	from user_score us left join user u on u.id = us.user_id left join user_address ua on ua.id = us.user_id;`).Sort().Check(tk.Res[4].Sort().Rows())
	tk.MustQuery(`SELECT DISTINCT us.user_id,u.username ,ua.address,CONCAT(u.username, "-" ,ua.address) as userinfo,
	sum(us.score) from user_score us left join user u on u.id = us.user_id
	left join user_address ua on ua.id = us.user_id group by us.user_id,u.username;`).Sort().Check(tk.Res[5].Sort().Rows())
	tk.MustQuery(`select a.subject,a.id,a.user_id,u.username, a.score,a.rownum from ( select id,user_id,subject,score,row_number() over (order by score desc) as rownum from user_score) as a
	left join user u on a.user_id = u.id inner join user_score as b on a.id=b.id where a.rownum<=10 order by a.rownum ;`).Sort().Check(tk.Res[6].Sort().Rows())
	tk.MustQuery(`select a.subject,a.id,a.score,a.rownum from (
        select id,subject,score,row_number() over (partition by subject order by score desc) as rownum from user_score) as a
        inner join user_score as b on a.id=b.id where a.rownum<=10 order by a.subject ;`).Sort().Check(tk.Res[7].Sort().Rows())
	tk.MustQuery(`select *,u.username,ua.address,CONCAT(u.username, "-" ,ua.address) as userinfo,
	avg(us.score) over (order by us.id rows 2 preceding) as current_avg,
	sum(score) over (order by us.id rows 2 preceding) as current_sum from user_score us
	left join user u on u.id = us.user_id left join user_address ua on ua.id = us.user_id;`).Sort().Check(tk.Res[8].Sort().Rows())
	tk.MustQuery(`select a.id,a.username,a.password,a.age,a.sex from user a where a.id in (select user_id from user_score where score > 90);`).Sort().Check(tk.Res[9].Sort().Rows())
	tk.MustQuery(`select us.user_id,u.username,us.subject,us.score from user_score us left join user u on u.id = us.user_id
	where us.score > 90 group by us.user_id,us.subject,us.score;`).Sort().Check(tk.Res[10].Sort().Rows())
	tk.MustQuery(`select us.user_id,u.username,us.subject,us.score from user_score us join user u on u.id = us.user_id
	where us.score > 90 group by us.user_id,us.subject,us.score;`).Sort().Check(tk.Res[11].Sort().Rows())
	tk.MustQuery(`select a.id,a.username,a.password,a.age,a.sex,ad.address,CONCAT(a.username, "-" ,ad.address) as userinfo from user a
	left join user_address ad on a.id = ad.user_id where a.id > 10 and a.id < 50;`).Sort().Check(tk.Res[12].Sort().Rows())
	tk.MustQuery(`select a.id,a.username,a.password,a.age,a.sex,ad.score from user a right join user_score ad on a.id = ad.user_id
	where a.id > 10 and a.id < 50;`).Sort().Check(tk.Res[13].Sort().Rows())
	tk.MustQuery(`select a.id,a.username,a.password,a.age,a.sex,ad.score from user a
	left join user_score ad on a.id = ad.user_id where a.id in (select user_id from user_score where score > 90 and score < 99 )
	union select a.id,a.username,a.password,a.age,a.sex,ad.score from user a
	left join user_score ad on a.id = ad.user_id
	where a.id in (select user_id from user_score where score > 30 and score < 70 );`).Sort().Check(tk.Res[14].Sort().Rows())
}

func TestSelect(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.InProcedure()
	tk.MustExec("use test")
	initEnv(tk)
	name := "user_pro"
	selectSQL := "select a.id,a.username,a.password,a.age,a.sex " +
		"from user a where a.id > 10 and a.id < 50 order by id"
	pSQL, cSQL := procedureSQL(name, selectSQL)
	runTestCases(t, store, pSQL, cSQL, selectSQL)

	name = "score_pro"
	selectSQL = "select us.subject,count(us.user_id),sum(us.score),avg(us.score),max(us.score),min(us.score) " +
		"from user_score us where us.score > 90 group by us.subject"
	pSQL, cSQL = procedureSQL(name, selectSQL)
	runTestCases(t, store, pSQL, cSQL, selectSQL)

	name = "user_score_rank_pro"
	selectSQL = "select *,rank() over (partition by subject order by score desc) as ranking " +
		"from user_score"
	pSQL, cSQL = procedureSQL(name, selectSQL)
	runTestCases(t, store, pSQL, cSQL, selectSQL)

	name = "user_win_pro"
	selectSQL = "select us.*,sum(us.score) over (order by us.id) as current_sum,avg(us.score) over (order by us.id) as current_avg," +
		"count(us.score) over (order by us.id) as current_count,max(us.score) over (order by us.id) as current_max," +
		"min(us.score) over (order by us.id) as current_min from user_score us"
	pSQL, cSQL = procedureSQL(name, selectSQL)
	runTestCases(t, store, pSQL, cSQL, selectSQL)

	name = "user_win_join_pro"
	selectSQL = "select us.*,sum(us.score) over (order by us.id) as current_sum,avg(us.score) over (order by us.id) as current_avg," +
		"count(us.score) over (order by us.id) as current_count,max(us.score) over (order by us.id) as current_max," +
		"min(us.score) over (order by us.id) as current_min,u.username ,ua.address,CONCAT(u.username, \"-\" ,ua.address) as userinfo " +
		"from user_score us left join user u on u.id = us.user_id left join user_address ua on ua.id = us.user_id"
	pSQL, cSQL = procedureSQL(name, selectSQL)
	runTestCases(t, store, pSQL, cSQL, selectSQL)

	name = "user_join_groupBy_pro"
	selectSQL = "SELECT DISTINCT us.user_id,u.username ,ua.address,CONCAT(u.username, \"-\" ,ua.address) as userinfo," +
		"sum(us.score) from user_score us left join user u on u.id = us.user_id left join user_address ua on ua.id = us.user_id " +
		"group by us.user_id,u.username order by us.user_id"
	pSQL, cSQL = procedureSQL(name, selectSQL)
	runTestCases(t, store, pSQL, cSQL, selectSQL)

	name = "user_score_top10_pro"
	selectSQL = "select a.subject,a.id,a.user_id,u.username, a.score,a.rownum " +
		"from (" +
		"select id,user_id,subject,score,row_number() over (order by score desc) as rownum " +
		"from user_score) as a left join user u on a.user_id = u.id " +
		"inner join user_score as b on a.id=b.id " +
		"where a.rownum<=10 order by a.rownum"
	pSQL, cSQL = procedureSQL(name, selectSQL)
	runTestCases(t, store, pSQL, cSQL, selectSQL)

	name = "user_fun_pro"
	selectSQL = "select *,u.username,ua.address,CONCAT(u.username, \"-\" ,ua.address) as userinfo," +
		"avg(us.score) over (order by us.id rows 2 preceding) as current_avg, " +
		"sum(score) over (order by us.id rows 2 preceding) as current_sum " +
		"from user_score us left join user u on u.id = us.user_id " +
		"left join user_address ua on ua.id = us.user_id order by u.id"
	pSQL, cSQL = procedureSQL(name, selectSQL)
	runTestCases(t, store, pSQL, cSQL, selectSQL)

	name = "user_sub_sel_pro"
	selectSQL = "select a.id,a.username,a.password,a.age,a.sex " +
		"from user a " +
		"where a.id in (select user_id from user_score where score > 90) order by a.age desc,a.id"
	pSQL, cSQL = procedureSQL(name, selectSQL)
	runTestCases(t, store, pSQL, cSQL, selectSQL)

	name = "user_left_join_groupBy_pro"
	selectSQL = "select users.subject,sum(users.score) " +
		"from (" +
		"select us.user_id,u.username,us.subject,us.score " +
		"from user_score us " +
		"left join user u on u.id = us.user_id where us.score > 90 ) as users " +
		"group by users.subject"
	pSQL, cSQL = procedureSQL(name, selectSQL)
	runTestCases(t, store, pSQL, cSQL, selectSQL)

	name = "user_join_pro"
	selectSQL = "select users.subject,sum(users.score) " +
		"from (" +
		"select us.user_id,u.username,us.subject,us.score " +
		"from user_score us " +
		"join user u on u.id = us.user_id where us.score > 90 ) as users " +
		"group by users.subject"
	pSQL, cSQL = procedureSQL(name, selectSQL)
	runTestCases(t, store, pSQL, cSQL, selectSQL)

	name = "user_left_join_pro"
	selectSQL = "select a.id,a.username,a.password,a.age,a.sex,ad.address," +
		"CONCAT(a.username, \"-\" ,ad.address) as userinfo " +
		"from user a " +
		"left join user_address ad on a.id = ad.user_id " +
		"where a.id > 10 and a.id < 50  order by a.id"
	pSQL, cSQL = procedureSQL(name, selectSQL)
	runTestCases(t, store, pSQL, cSQL, selectSQL)

	name = "user_right_join_pro"
	selectSQL = "select a.id,a.username,a.password,a.age,a.sex,ad.score " +
		"from user a " +
		"right join user_score ad on a.id = ad.user_id " +
		"where a.id > 10 and a.id < 50 " +
		"order by ad.score desc,a.age"
	pSQL, cSQL = procedureSQL(name, selectSQL)
	runTestCases(t, store, pSQL, cSQL, selectSQL)

	name = "union_pro"
	selectSQL = "select * " +
		"from (" +
		"select a.id,a.username,a.password,a.age,a.sex,ad.score " +
		"from user a " +
		"left join user_score ad on a.id = ad.user_id " +
		"where a.id in (" +
		"select user_id " +
		"from user_score " +
		"where score > 90 and score < 99 " +
		"order by ad.score desc,a.age) " +
		"union " +
		"select a.id,a.username,a.password,a.age,a.sex,ad.score " +
		"from user a " +
		"left join user_score ad on a.id = ad.user_id " +
		"where a.id in (" +
		"select user_id " +
		"from user_score " +
		"where score > 30 and score < 70)) user_info " +
		"order by user_info.score desc,user_info.age"
	pSQL, cSQL = procedureSQL(name, selectSQL)
	runTestCases(t, store, pSQL, cSQL, selectSQL)

	name = "user_top10_pro"
	selectSQL = "select a.subject,a.id,a.score,a.rownum " +
		"from (" +
		"select id,subject,score,row_number() over (partition by subject order by score desc) as rownum " +
		"from user_score) as a inner join user_score as b on a.id=b.id where a.rownum<=10 order by a.subject"
	pSQL, cSQL = procedureSQL(name, selectSQL)
	runTestCases(t, store, pSQL, cSQL, selectSQL)

	name = "user_info_pro"
	selectSQL = "select rank() over (partition by user_info.subject_1 order by user_info.score_1 desc) as ranking," +
		"avg(user_info.score_1) over (order by user_info.id rows 2 preceding) as current_avg," +
		"sum(user_info.score_1) over (order by user_info.id rows 2 preceding) as current_sum," +
		"sum(user_info.score_1) over (order by user_info.id) as score_1_sum," +
		"avg(user_info.score_1) over (order by user_info.id) as score_1_avg," +
		"count(user_info.score_1) over (order by user_info.id) as score_1_count," +
		"max(user_info.score_1) over (order by user_info.id) as score_1_max," +
		"min(user_info.score_1) over (order by user_info.id) as score_1_min," +
		"user_info.* " +
		"from (" +
		"select u.id,u.username,us1.subject as subject_1,us1.score as score_1,us2.subject as subject_2,us2.score as score_2," +
		"us3.subject as subject_3,us3.score as score_3,us4.subject as subject_4,us4.score as score_4,us5.subject as subject_5," +
		"us5.score as score_5,ua.address " +
		"from user u " +
		"left join user_score us1 on us1.user_id = u.id and us1.subject = 1 " +
		"left join user_score us2 on us2.user_id = u.id and us2.subject = 2 " +
		"left join user_score us3 on us3.user_id = u.id and us3.subject = 3 " +
		"left join user_score us4 on us4.user_id = u.id and us4.subject = 4 " +
		"left join user_score us5 on us5.user_id = u.id and us5.subject = 5 " +
		"left join test.user_address ua on u.id = ua.user_id) as user_info"
	pSQL, cSQL = procedureSQL(name, selectSQL)
	runTestCases(t, store, pSQL, cSQL, selectSQL)
	destroyEnv(tk)
}

func TestSelectInsert(t *testing.T) {
	testcases := []struct {
		name            string
		insertSelectSQL string
		selectSQL       string
	}{
		{
			"build_user_info_pro",
			"INSERT " +
				"INTO user_info (id,user_id,username,password,age,sex,address," +
				"subject_1,score_1," +
				"subject_2,score_2," +
				"subject_3,score_3," +
				"subject_4,score_4," +
				"subject_5,score_5) " +
				"SELECT * " +
				"FROM ( select u.id,us1.user_id,u.username,u.password,u.age,u.sex," +
				"ua.address, " +
				"us1.subject as subject_1,us1.score as score_1, " +
				"us2.subject as subject_2,us2.score as score_2, " +
				"us3.subject as subject_3,us3.score as score_3, " +
				"us4.subject as subject_4,us4.score as score_4, " +
				"us5.subject as subject_5,us5.score as score_5 " +
				"from " +
				"user u " +
				"left join user_score us1 on us1.user_id = u.id and us1.subject = 1 " +
				"left join user_score us2 on us2.user_id = u.id and us2.subject = 2 " +
				"left join user_score us3 on us3.user_id = u.id and us3.subject = 3 " +
				"left join user_score us4 on us4.user_id = u.id and us4.subject = 4 " +
				"left join user_score us5 on us5.user_id = u.id and us5.subject = 5 " +
				"left join test.user_address ua on u.id = ua.user_id) " +
				"as user_info",
			"select u.id,us1.user_id,u.username,u.password,u.age,u.sex,ua.address," +
				"us1.subject as subject_1,us1.score as score_1," +
				"us2.subject as subject_2,us2.score as score_2," +
				"us3.subject as subject_3,us3.score as score_3," +
				"us4.subject as subject_4,us4.score as score_4," +
				"us5.subject as subject_5,us5.score as score_5 " +
				"from user u " +
				"left join user_score us1 on us1.user_id = u.id and us1.subject = 1 " +
				"left join user_score us2 on us2.user_id = u.id and us2.subject = 2 " +
				"left join user_score us3 on us3.user_id = u.id and us3.subject = 3 " +
				"left join user_score us4 on us4.user_id = u.id and us4.subject = 4 " +
				"left join user_score us5 on us5.user_id = u.id and us5.subject = 5 " +
				"left join test.user_address ua on u.id = ua.user_id",
		},
	}
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.InProcedure()
	tk.MustExec("use test")
	initEnv(tk)
	procedureSQL, callSQL := procedureSQL(testcases[0].name, testcases[0].insertSelectSQL)
	newTk := testkit.NewTestKit(t, store)
	newTk.InProcedure()
	newTk.MustExec("use test")
	newTk.MustExec(procedureSQL)
	newTk.MustExec(callSQL)
	userInfoRows := newTk.MustQuery("select * from user_info").Rows()
	selectRows := newTk.MustQuery(testcases[0].selectSQL).Rows()
	require.Equal(t, len(userInfoRows), len(selectRows))
	newTk.MustExec("create PROCEDURE update_user_info(in id_in int,out username_out varchar(50)) " +
		"begin UPDATE user_info ui SET ui.username = (SELECT CONCAT(u.username, \"-\" ,ua.address) " +
		"FROM user u left join user_address ua on u.id = ua.user_id WHERE u.id = 1) " +
		"where ui.user_id = id_in;set username_out = (select username from user_info where user_id = id_in);" +
		"end;")
	newTk.MustExec("call update_user_info(1,@username_out)")
	userInfoNameRows := newTk.MustQuery("select username from user_info where user_id = 1").Rows()
	userNameRows := newTk.MustQuery("select @username_out").Rows()
	require.Equal(t, userInfoNameRows[0], userNameRows[0])
	destroyEnv(tk)
}

func runTestCases(t *testing.T, store kv.Storage, procedure, runProcedure, selectSQL string) {
	tk := testkit.NewTestKit(t, store)
	tk.InProcedure()
	tk.MustExec("use test")
	tk.MustExec(procedure)
	tk.MustExec(runProcedure)
	procedureRows := tk.Res[0].Sort().Rows()
	selectRows := tk.MustQuery(selectSQL).Sort().Rows()
	require.Equal(t, len(procedureRows), len(selectRows))
	require.Equal(t, procedureRows[0], selectRows[0])

}

func procedureSQL(procedureName, selectSQL string) (string, string) {
	sqlTemplate := "create procedure procedureName() begin selectSQL; end"
	sqlTemplate = strings.Replace(sqlTemplate, "procedureName", procedureName, 1)
	sqlTemplate = strings.Replace(sqlTemplate, "selectSQL", selectSQL, 1)

	callSQLTemplate := "call procedureName()"
	callSQLTemplate = strings.Replace(callSQLTemplate, "procedureName", procedureName, 1)
	return sqlTemplate, callSQLTemplate
}

func createTable(tk *testkit.TestKit) {
	tk.MustExec("CREATE TABLE IF NOT EXISTS `user` (`id` int(11) NOT NULL,`username` VARCHAR(30) DEFAULT NULL,`password` VARCHAR(30) DEFAULT NULL,`age` int(11) NOT NULL,`sex` int(11) NOT NULL,PRIMARY KEY (`id`),KEY `username` (`username`))")
	tk.MustExec("CREATE TABLE IF NOT EXISTS `user_score` (`id` int(11) NOT NULL,`subject` int(11) NOT NULL,`user_id` int(11) NOT NULL,`score` int(11) NOT NULL,PRIMARY KEY (`id`))")
	tk.MustExec("CREATE TABLE IF NOT EXISTS `user_address` (`id` int(11) NOT NULL,`user_id` int(11) NOT NULL,`address` VARCHAR(30) DEFAULT NULL,PRIMARY KEY (`id`),KEY `address` (`address`))")
	tk.MustExec("CREATE TABLE IF NOT EXISTS `user_info` (" +
		"`id` int(11) NOT NULL," +
		"`user_id` int(11) NOT NULL," +
		"`username` VARCHAR(30) DEFAULT NULL," +
		"`password` VARCHAR(30) DEFAULT NULL," +
		"`age` int(11) NOT NULL," +
		"`sex` int(11) NOT NULL," +
		"`address` VARCHAR(30) DEFAULT NULL," +
		"`subject_1` int(11) DEFAULT NULL,`score_1` int(11) DEFAULT NULL," +
		"`subject_2` int(11) DEFAULT NULL,`score_2` int(11) DEFAULT NULL," +
		"`subject_3` int(11) DEFAULT NULL,`score_3` int(11) DEFAULT NULL," +
		"`subject_4` int(11) DEFAULT NULL,`score_4` int(11) DEFAULT NULL," +
		"`subject_5` int(11) DEFAULT NULL,`score_5` int(11) DEFAULT NULL)")
}

func dropTable(tk *testkit.TestKit) {
	tk.MustExec("drop table IF EXISTS `user`")
	tk.MustExec("drop table IF EXISTS `user_score`")
	tk.MustExec("drop table IF EXISTS `user_address`")
	tk.MustExec("drop table IF EXISTS `user_info`")
}

func dropProcedure(tk *testkit.TestKit) {
	tk.MustExec("DROP PROCEDURE IF EXISTS user_pro")
	tk.MustExec("DROP PROCEDURE IF EXISTS score_pro")
	tk.MustExec("DROP PROCEDURE IF EXISTS user_score_rank_pro")
	tk.MustExec("DROP PROCEDURE IF EXISTS user_win_pro")
	tk.MustExec("DROP PROCEDURE IF EXISTS user_win_join_pro")
	tk.MustExec("DROP PROCEDURE IF EXISTS user_join_groupBy_pro")
	tk.MustExec("DROP PROCEDURE IF EXISTS user_score_top10_pro")
	tk.MustExec("DROP PROCEDURE IF EXISTS user_fun_pro")
	tk.MustExec("DROP PROCEDURE IF EXISTS user_sub_sel_pro")
	tk.MustExec("DROP PROCEDURE IF EXISTS user_left_join_groupBy_pro")
	tk.MustExec("DROP PROCEDURE IF EXISTS user_join_pro")
	tk.MustExec("DROP PROCEDURE IF EXISTS user_left_join_pro")
	tk.MustExec("DROP PROCEDURE IF EXISTS user_right_join_pro")
	tk.MustExec("DROP PROCEDURE IF EXISTS union_pro")
	tk.MustExec("DROP PROCEDURE IF EXISTS user_top10_pro")
	tk.MustExec("DROP PROCEDURE IF EXISTS user_info_pro")
	tk.MustExec("DROP PROCEDURE IF EXISTS build_user_info_pro")
}

func initEnv(tk *testkit.TestKit) {
	dropTable(tk)
	dropProcedure(tk)
	createTable(tk)
	tk.MustExec("CREATE PROCEDURE insert_user (IN id INTEGER) BEGIN insert into user values(id, CONCAT('username-', id),CONCAT('password-', id),FLOOR( 15 + RAND() * 23),Mod(id,2)); end")
	tk.MustExec("CREATE PROCEDURE insert_user_score(IN scoreID INTEGER,IN subjectId INTEGER,IN id INTEGER) BEGIN insert into user_score values(scoreID, subjectId, id, FLOOR( 40 + RAND() * 100)); end")
	tk.MustExec("CREATE PROCEDURE insert_user_address (IN id INTEGER) BEGIN insert into user_address values(id, id, CONCAT('useraddress-', id)); end")
	scoreID := 0
	for i := 0; i < 100; i++ {
		userSQLTemplate := "call insert_user(%?)"
		userSQL := new(strings.Builder)
		sqlescape.MustFormatSQL(userSQL, userSQLTemplate, i)
		tk.MustExec(userSQL.String())
		userScoreSQLTemplate := "call insert_user_score(%?,%?,%?)"
		userScoreSQL := new(strings.Builder)
		sqlescape.MustFormatSQL(userScoreSQL, userScoreSQLTemplate, scoreID, 1, i)
		tk.MustExec(userScoreSQL.String())
		scoreID++
		userScoreSQL = new(strings.Builder)
		sqlescape.MustFormatSQL(userScoreSQL, userScoreSQLTemplate, scoreID, 2, i)
		tk.MustExec(userScoreSQL.String())
		scoreID++
		userScoreSQL = new(strings.Builder)
		sqlescape.MustFormatSQL(userScoreSQL, userScoreSQLTemplate, scoreID, 3, i)
		tk.MustExec(userScoreSQL.String())
		scoreID++
		userScoreSQL = new(strings.Builder)
		sqlescape.MustFormatSQL(userScoreSQL, userScoreSQLTemplate, scoreID, 4, i)
		tk.MustExec(userScoreSQL.String())
		scoreID++
		userScoreSQL = new(strings.Builder)
		sqlescape.MustFormatSQL(userScoreSQL, userScoreSQLTemplate, scoreID, 5, i)
		tk.MustExec(userScoreSQL.String())
		scoreID++
		userAddressSQLTemplate := "call insert_user_address(%?)"
		userAddressSQL := new(strings.Builder)
		sqlescape.MustFormatSQL(userAddressSQL, userAddressSQLTemplate, i)
		tk.MustExec(userAddressSQL.String())
	}
}

func destroyEnv(tk *testkit.TestKit) {
	dropTable(tk)
	dropProcedure(tk)
}

func TestCallInOutParam(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.InProcedure()
	tk.MustExec("use test")
	tk.MustExec("create PROCEDURE var1(in sp1 int) begin select sp1 ; end;")
	// data
	tk.MustExec("call var1(1)")
	tk.Res[0].Check(testkit.Rows("1"))
	tk.ClearProcedureRes()
	// str
	tk.MustExec("call var1('1')")
	tk.Res[0].Check(testkit.Rows("1"))
	tk.ClearProcedureRes()
	// variables
	tk.MustExec("set @a = 1;call var1(@a)")
	tk.Res[0].Check(testkit.Rows("1"))
	tk.ClearProcedureRes()
	// function
	tk.MustExec("call var1(CHAR_LENGTH('1bcdsbcz'))")
	tk.Res[0].Check(testkit.Rows("8"))
	tk.ClearProcedureRes()
	// error
	tk.MustGetErrCode("call var1('1bcdsbcz')", 1292)
	tk.MustExec("set sql_mode = ''")
	tk.MustExec("call var1('1bcdsbcz')")
	tk.Res[0].Check(testkit.Rows("1"))
	tk.ClearProcedureRes()
	tk.MustExec("set sql_mode = default")
	// min/max
	tk.MustExec("call var1(2147483647)")
	tk.Res[0].Check(testkit.Rows("2147483647"))
	tk.ClearProcedureRes()
	tk.MustGetErrCode("call var1(2147483648)", 1690)
	tk.MustExec("call var1(-2147483648)")
	tk.Res[0].Check(testkit.Rows("-2147483648"))
	tk.ClearProcedureRes()
	tk.MustGetErrCode("call var1(-2147483649)", 1690)
	tk.MustExec("call var1(21.242)")
	tk.Res[0].Check(testkit.Rows("21"))
	tk.ClearProcedureRes()
	//max/min
	tk.MustExec("call var1(CHAR_LENGTH('shdauhcuiahds'))")
	tk.Res[0].Check(testkit.Rows("13"))
	tk.ClearProcedureRes()

	tk.MustExec("create PROCEDURE var2(in sp1 varchar(10)) begin select sp1 ; end;")
	tk.MustExec("call var2(1)")
	tk.Res[0].Check(testkit.Rows("1"))
	tk.ClearProcedureRes()
	tk.MustExec("call var2('22345')")
	tk.Res[0].Check(testkit.Rows("22345"))
	tk.ClearProcedureRes()
	tk.MustGetErrCode("call var2('012345678910')", 1406)
	tk.ClearProcedureRes()
	tk.MustExec("set @a = '2222';call var2(@a)")
	tk.Res[0].Check(testkit.Rows("2222"))
	tk.ClearProcedureRes()
	tk.MustExec("set @a = 2222;call var2(@a)")
	tk.Res[0].Check(testkit.Rows("2222"))
	tk.ClearProcedureRes()
	tk.MustExec("call var2(LOWER('FTYGIPJO'))")
	tk.Res[0].Check(testkit.Rows("ftygipjo"))
	tk.ClearProcedureRes()

	tk.MustExec("create PROCEDURE var3(sp1 varchar(10),in sp2 float) begin select sp1,sp2 ; end;")
	tk.MustExec("call var3(1,2.1)")
	tk.Res[0].Check(testkit.Rows("1 2.1"))
	tk.ClearProcedureRes()
	tk.MustExec(" set @a = 1.2; call var3(@a,@a)")
	tk.Res[0].Check(testkit.Rows("1.2 1.2"))
	tk.ClearProcedureRes()

	tk.MustExec("create PROCEDURE var4(in sp1 datetime,out sp2 datetime) begin select sp1; set sp2 = sp1; end;")
	tk.MustExec("call var4('2023-02-03 11:34:22',@a)")
	tk.Res[0].Check(testkit.Rows("2023-02-03 11:34:22"))
	tk.ClearProcedureRes()
	tk.MustQuery("select @a").Check(testkit.Rows("2023-02-03 11:34:22"))
	// Enum
	tk.MustExec("create PROCEDURE var5(in sp1 Enum('a','b','c'),out sp2  Enum('a','b','c')) begin select sp1; set sp2 = sp1; end;")
	tk.MustExec("call var5('b',@a)")
	tk.Res[0].Check(testkit.Rows("b"))
	tk.ClearProcedureRes()
	tk.MustQuery("select @a").Check(testkit.Rows("b"))
	// inout
	tk.MustExec("create PROCEDURE var6(inout sp1 varchar(100)) begin select sp1; end;")
	tk.MustGetErrCode("call var6('b')", 1414)
	tk.MustGetErrCode("call var6(now())", 1414)
	tk.MustExec("set @a =cd; call var6(@a)")
	tk.Res[0].Check(testkit.Rows("cd"))
	tk.ClearProcedureRes()
	tk.MustQuery("select @a").Check(testkit.Rows("cd"))
	// SET
	tk.MustExec("create PROCEDURE var7(in sp1 SET('a','b','c'),out sp2  SET('a','b','c')) begin select sp1; set sp2 = sp1; end;")
	tk.MustExec("call var7('b,a',@a)")
	tk.Res[0].Check(testkit.Rows("a,b"))
	tk.ClearProcedureRes()
	// timestamp
	tk.MustExec("create PROCEDURE var8(in sp1 timestamp,out sp2  timestamp) begin select sp1; set sp2 = sp1; end;")
	tk.MustExec("set timestamp = 1675499582;call var8(now(),@a)")
	tk.Res[0].Check(tk.MustQuery("select now()").Rows())
	tk.ClearProcedureRes()
	//tk.MustQuery("select @a").Check(testkit.Rows("2023-02-04 16:33:02"))
	tk.MustQuery("select @a").Check(tk.MustQuery("select now()").Rows())
}

func TestCallVarParam(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.InProcedure()
	tk.MustExec("use test")
	// int
	sql := `create PROCEDURE var1() begin declare id int;set id = 1; select id; end;`
	tk.MustExec(sql)
	tk.MustExec("call var1")
	tk.Res[0].Check(testkit.Rows("1"))
	tk.ClearProcedureRes()
	// varchar
	sql = `create PROCEDURE var2() begin declare id varchar(10);set id = 1; select id; end;`
	tk.MustExec(sql)
	tk.MustExec("call var2")
	tk.Res[0].Check(testkit.Rows("1"))
	tk.ClearProcedureRes()
	// bigint
	sql = `create PROCEDURE var3() begin declare id bigint;set id = 1; select id; end;`
	tk.MustExec(sql)
	tk.MustExec("call var3")
	tk.Res[0].Check(testkit.Rows("1"))
	tk.ClearProcedureRes()
	sql = `create PROCEDURE var4() begin declare id char(10);set id = 1; select id; end;`
	tk.MustExec(sql)
	tk.MustExec("call var4")
	tk.Res[0].Check(testkit.Rows("1"))
	tk.ClearProcedureRes()
	// decimal
	sql = `create PROCEDURE var5() begin declare id decimal(10,2);set id = 1.2; select id; end;`
	tk.MustExec(sql)
	tk.MustExec("call var5")
	tk.Res[0].Check(testkit.Rows("1.20"))
	tk.ClearProcedureRes()
	// datetime
	sql = `create PROCEDURE var6() begin declare id datetime;set id = "2023-02-03 11:34:22"; select id; end;`
	tk.MustExec(sql)
	tk.MustExec("call var6")
	tk.Res[0].Check(testkit.Rows("2023-02-03 11:34:22"))
	tk.ClearProcedureRes()
	// TIMESTAMP
	sql = `create PROCEDURE var7() begin declare id TIMESTAMP;set id = "2023-02-03 11:34:22"; select id; end;`
	tk.MustExec(sql)
	tk.MustExec("call var7")
	tk.Res[0].Check(testkit.Rows("2023-02-03 11:34:22"))
	tk.ClearProcedureRes()
	// bit
	sql = `create PROCEDURE var8() begin declare id bit;set id = 0; select id; end;`
	tk.MustExec(sql)
	tk.MustExec("call var8")
	tk.Res[0].Check(testkit.Rows("0"))
	tk.ClearProcedureRes()
	// variables cover
	sql = `create PROCEDURE var9() begin declare id bit;set id = 0; select id;
	begin declare id int;set id = 1; select id;  end; select id; end;`
	tk.MustExec(sql)
	tk.MustExec("call var9")
	tk.Res[0].Check(testkit.Rows("0"))
	tk.Res[1].Check(testkit.Rows("1"))
	tk.Res[2].Check(testkit.Rows("0"))
	tk.ClearProcedureRes()
	sql = `create PROCEDURE var10() begin declare id1 varchar(10);declare id2 varchar(10);set id1 = 'ss';select id1; set id2 = 'ss';select id2; begin
	declare id1 varchar(10);declare id2 varchar(10);set id1 = 1; set id2 = 2; select id1; select id2;  end; select id1; select id2; end;`
	tk.MustExec(sql)
	tk.MustExec("call var10")
	tk.Res[0].Check(testkit.Rows("ss"))
	tk.Res[1].Check(testkit.Rows("ss"))
	tk.Res[2].Check(testkit.Rows("1"))
	tk.Res[3].Check(testkit.Rows("2"))
	tk.Res[4].Check(testkit.Rows("ss"))
	tk.Res[5].Check(testkit.Rows("ss"))
	tk.ClearProcedureRes()
	sql = `create PROCEDURE var11() begin declare id1 varchar(10);declare id2 varchar(10);set id1 = 'ss';select id1; set id2 = 'ss';select id2; begin
	declare id1 varchar(10);declare id2 varchar(10);set id1 = 1; set id2 = 2; select id1; select id2; begin declare id1 varchar(10); set id1 ='vv'; select id1; select id2; end;end; select id1; select id2; end;`
	tk.MustExec(sql)
	tk.MustExec("call var11")
	tk.Res[0].Check(testkit.Rows("ss"))
	tk.Res[1].Check(testkit.Rows("ss"))
	tk.Res[2].Check(testkit.Rows("1"))
	tk.Res[3].Check(testkit.Rows("2"))
	tk.Res[4].Check(testkit.Rows("vv"))
	tk.Res[5].Check(testkit.Rows("2"))
	tk.Res[6].Check(testkit.Rows("ss"))
	tk.Res[7].Check(testkit.Rows("ss"))
	tk.ClearProcedureRes()
}

func TestCallVarDef(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.InProcedure()
	tk.MustExec("use test")
	// int
	sql := `create PROCEDURE var1() begin declare id varchar(10) default 1; select id; end;`
	tk.MustExec(sql)
	tk.MustExec("call var1")
	tk.Res[0].Check(testkit.Rows("1"))
	tk.ClearProcedureRes()

	sql = `create PROCEDURE var2() begin declare id varchar(10) default "1"; select id; end;`
	tk.MustExec(sql)
	tk.MustExec("call var2")
	tk.Res[0].Check(testkit.Rows("1"))
	tk.ClearProcedureRes()

	sql = `create PROCEDURE var3() begin declare id varchar(20) default now(); select id; end;`
	tk.MustExec(sql)
	tk.MustExec("set timestamp = 1675499582;call var3")
	tk.Res[0].Check(tk.MustQuery("select now()").Rows())
	tk.ClearProcedureRes()

	sql = `create PROCEDURE var4() begin declare id varchar(2) default now(); select id; end;`
	tk.MustExec(sql)
	tk.MustGetErrCode("set timestamp = 1675499582;call var4", 1406)
	tk.ClearProcedureRes()

	sql = `set @a = 3;create PROCEDURE var5() begin declare id varchar(2) default @a; select id; end;`
	tk.MustExec(sql)
	tk.MustExec("call var5")
	tk.Res[0].Check(testkit.Rows("3"))
	tk.ClearProcedureRes()

	sql = `create PROCEDURE var6() begin declare id varchar(2) default 3; declare id2 varchar(2) default id;select id; select id2;end;`
	tk.MustExec(sql)
	tk.MustExec("call var6")
	tk.Res[0].Check(testkit.Rows("3"))
	tk.Res[1].Check(testkit.Rows("3"))
	tk.ClearProcedureRes()

	sql = `create PROCEDURE var7() begin declare id varchar(2) default 3; set id = 100;end;`
	tk.MustExec(sql)
	tk.MustGetErrCode("call var7", 1406)
	tk.ClearProcedureRes()

	sql = `create PROCEDURE var8() begin declare id varchar(2) default 3; set id := 9; select id;end;`
	tk.MustExec(sql)
	tk.MustExec("call var8")
	tk.Res[0].Check(testkit.Rows("9"))
	tk.ClearProcedureRes()

	sql = `set @a = 30;create PROCEDURE var9() begin declare id varchar(2) default @a; select id;end;`
	tk.MustExec(sql)
	tk.MustExec("call var9")
	tk.Res[0].Check(testkit.Rows("30"))
	tk.ClearProcedureRes()

	sql = `create procedure zap(x int, out y int)
	begin
	declare z int;
	set z = x+1, y = z;
	end`
	tk.MustExec(sql)
	tk.MustExec("call zap(7,@zap)")
	tk.MustQuery("select @zap").Check(testkit.Rows("8"))
}

func TestCallInOutInSQL(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.InProcedure()
	tk.MustExec("use test")
	tk.MustExec("set timestamp = 1675499582")
	tk.MustExec("CREATE TABLE `user` ( `id` int(11) NOT NULL, `username` VARCHAR(30) DEFAULT NULL, `password` VARCHAR(30) DEFAULT NULL, " +
		"`age` int(11) NOT NULL, `sex` int(11) NOT NULL, PRIMARY KEY (`id`), KEY `username` (`username`) ) ENGINE=InnoDB;")
	tk.MustExec("CREATE TABLE `user_score` ( `id` int(11) NOT NULL, `subject` int(11) NOT NULL, `user_id` int(11) NOT NULL, " +
		"`score` int(11) NOT NULL, PRIMARY KEY (`id`) ) ENGINE=InnoDB;")
	tk.MustExec("CREATE TABLE `user_address` ( `id` int(11) NOT NULL, `user_id` int(11) NOT NULL, `address` VARCHAR(30) DEFAULT NULL, " +
		"PRIMARY KEY (`id`), KEY `address` (`address`) ) ENGINE=InnoDB;")
	tk.MustExec(`create procedure insert_data(i int, s_i int)  begin insert into user values(i, CONCAT("username-", i),CONCAT("password-", i),FLOOR( 15 ),Mod(i,2));
		insert into user_score values(s_i, 1, i, FLOOR( 40 + i * 100));
		set s_i=s_i+1;
		insert into user_score values(s_i, 2, i, FLOOR( 40 + i * 100));
		set s_i=s_i+1;
		insert into user_score values(s_i, 3, i, FLOOR( 40 + i * 100));
		set s_i=s_i+1;
		insert into user_score values(s_i, 4, i, FLOOR( 40 + i * 100));
		set s_i=s_i+1;
		insert into user_score values(s_i, 5, i, FLOOR( 40 + i * 100));
		set s_i=s_i+1;
		insert into user_address values(i, i, CONCAT("useraddress-", i));
		set i=i+1;  end;`)
	for i := 1; i <= 100; i = i + 1 {
		sql := fmt.Sprintf("call insert_data(%d,%d)", i, 5*i)
		tk.MustExec(sql)
	}
	tk.MustExec(`create procedure select1(id int)begin select * from user where user.id > id; end;`)
	sql := fmt.Sprintf("call select1(%d)", 3)
	tk.MustExec(sql)
	tk.MustQuery("select * from user where user.id >3").Sort().Check(tk.Res[0].Sort().Rows())
	tk.ClearProcedureRes()

	tk.MustExec(`create procedure select2(id int) select * from user where user.id > id;`)
	sql = fmt.Sprintf("call select2(%d)", 3)
	tk.MustExec(sql)
	tk.MustQuery("select * from user where user.id >3").Sort().Check(tk.Res[0].Sort().Rows())
	tk.ClearProcedureRes()

	tk.MustExec(`create procedure select3(id int)begin select * from user where user.id > id; set id = 10;
	select * from user where user.id > id; end;`)
	sql = fmt.Sprintf("call select3(%d)", 3)
	tk.MustExec(sql)
	tk.MustQuery("select * from user where user.id >3").Sort().Check(tk.Res[0].Sort().Rows())
	tk.MustQuery("select * from user where user.id >10").Sort().Check(tk.Res[1].Sort().Rows())
	tk.ClearProcedureRes()

	tk.MustExec(`create procedure select4(id int)begin select * from user where user.id > id; set id := 10;
	select * from user where user.id > id; end;`)
	sql = fmt.Sprintf("call select4(%d)", 3)
	tk.MustExec(sql)
	tk.MustQuery("select * from user where user.id >3").Sort().Check(tk.Res[0].Sort().Rows())
	tk.MustQuery("select * from user where user.id >10").Sort().Check(tk.Res[1].Sort().Rows())
	tk.ClearProcedureRes()

	tk.MustExec(`create procedure select5(id int)begin select * from user where user.id > id; set id := 10;
	select * from user where user.id > id; end;`)
	sql = fmt.Sprintf("call select5(%d)", 3)
	tk.MustExec(sql)
	tk.MustQuery("select * from user where user.id >3").Sort().Check(tk.Res[0].Sort().Rows())
	tk.MustQuery("select * from user where user.id >10").Sort().Check(tk.Res[1].Sort().Rows())
	tk.ClearProcedureRes()

	tk.MustExec(`create procedure select6(id int,name char(100))begin update user set username = name where user.id = id;
	select * from user where user.id = id; end;`)
	sql = fmt.Sprintf("call select6(%d,'%s')", 3, "test")
	tk.MustExec(sql)
	require.Equal(t, len(tk.Res), 1)
	tk.Res[0].Check(testkit.Rows("3 test password-3 15 1"))
	tk.ClearProcedureRes()

	tk.MustExec(`create procedure select7(id int,tablename char(100))begin update  tablename set username = 'sss' where user.id = id;
	select * from user where user.id = id; end;`)
	sql = fmt.Sprintf("call select7(%d,'%s')", 3, "test")
	tk.MustGetErrCode(sql, 1146)
	tk.ClearProcedureRes()

	tk.MustExec(`create procedure select8(id int,tablename char(100))begin
	select * from user where user.id = id into outfile 'test.txt'; end;`)
	sql = fmt.Sprintf("call select8(%d,'%s')", 3, "test")
	tk.MustExec(sql)
	tk.ClearProcedureRes()
	os.Remove("test.txt")

	tk.MustExec(`create procedure select9(id int,name char(100))begin update user set username = LOWER(name) where user.id = id;
	select * from user where user.id = id; end;`)
	sql = fmt.Sprintf("call select9(%d,'%s')", 3, "TAAA")
	tk.MustExec(sql)
	require.Equal(t, len(tk.Res), 1)
	tk.Res[0].Check(testkit.Rows("3 taaa password-3 15 1"))
	tk.ClearProcedureRes()
	tk.MustExec("update mysql.routines set created ='2023-02-09 19:10:30', last_altered = '2023-02-09 19:10:30'")

	tk.MustQuery("show procedure status").Check(testkit.Rows("test insert_data PROCEDURE  2023-02-09 19:10:30.000000 2023-02-09 19:10:30.000000 DEFINER  utf8mb4 utf8mb4_bin utf8mb4_bin", "test select1 PROCEDURE  2023-02-09 19:10:30.000000 2023-02-09 19:10:30.000000 DEFINER  utf8mb4 utf8mb4_bin utf8mb4_bin",
		"test select2 PROCEDURE  2023-02-09 19:10:30.000000 2023-02-09 19:10:30.000000 DEFINER  utf8mb4 utf8mb4_bin utf8mb4_bin", "test select3 PROCEDURE  2023-02-09 19:10:30.000000 2023-02-09 19:10:30.000000 DEFINER  utf8mb4 utf8mb4_bin utf8mb4_bin",
		"test select4 PROCEDURE  2023-02-09 19:10:30.000000 2023-02-09 19:10:30.000000 DEFINER  utf8mb4 utf8mb4_bin utf8mb4_bin", "test select5 PROCEDURE  2023-02-09 19:10:30.000000 2023-02-09 19:10:30.000000 DEFINER  utf8mb4 utf8mb4_bin utf8mb4_bin",
		"test select6 PROCEDURE  2023-02-09 19:10:30.000000 2023-02-09 19:10:30.000000 DEFINER  utf8mb4 utf8mb4_bin utf8mb4_bin", "test select7 PROCEDURE  2023-02-09 19:10:30.000000 2023-02-09 19:10:30.000000 DEFINER  utf8mb4 utf8mb4_bin utf8mb4_bin",
		"test select8 PROCEDURE  2023-02-09 19:10:30.000000 2023-02-09 19:10:30.000000 DEFINER  utf8mb4 utf8mb4_bin utf8mb4_bin", "test select9 PROCEDURE  2023-02-09 19:10:30.000000 2023-02-09 19:10:30.000000 DEFINER  utf8mb4 utf8mb4_bin utf8mb4_bin"))
	tk.MustQuery("show procedure status like '%insert%'").Check(testkit.Rows("test insert_data PROCEDURE  2023-02-09 19:10:30.000000 2023-02-09 19:10:30.000000 DEFINER  utf8mb4 utf8mb4_bin utf8mb4_bin"))
	tk.MustQuery("show procedure status like 'select9'").Check(testkit.Rows("test select9 PROCEDURE  2023-02-09 19:10:30.000000 2023-02-09 19:10:30.000000 DEFINER  utf8mb4 utf8mb4_bin utf8mb4_bin"))
	tk.MustQuery("show procedure status where DB = 'test'").Check(testkit.Rows("test insert_data PROCEDURE  2023-02-09 19:10:30.000000 2023-02-09 19:10:30.000000 DEFINER  utf8mb4 utf8mb4_bin utf8mb4_bin", "test select1 PROCEDURE  2023-02-09 19:10:30.000000 2023-02-09 19:10:30.000000 DEFINER  utf8mb4 utf8mb4_bin utf8mb4_bin",
		"test select2 PROCEDURE  2023-02-09 19:10:30.000000 2023-02-09 19:10:30.000000 DEFINER  utf8mb4 utf8mb4_bin utf8mb4_bin", "test select3 PROCEDURE  2023-02-09 19:10:30.000000 2023-02-09 19:10:30.000000 DEFINER  utf8mb4 utf8mb4_bin utf8mb4_bin",
		"test select4 PROCEDURE  2023-02-09 19:10:30.000000 2023-02-09 19:10:30.000000 DEFINER  utf8mb4 utf8mb4_bin utf8mb4_bin", "test select5 PROCEDURE  2023-02-09 19:10:30.000000 2023-02-09 19:10:30.000000 DEFINER  utf8mb4 utf8mb4_bin utf8mb4_bin",
		"test select6 PROCEDURE  2023-02-09 19:10:30.000000 2023-02-09 19:10:30.000000 DEFINER  utf8mb4 utf8mb4_bin utf8mb4_bin", "test select7 PROCEDURE  2023-02-09 19:10:30.000000 2023-02-09 19:10:30.000000 DEFINER  utf8mb4 utf8mb4_bin utf8mb4_bin",
		"test select8 PROCEDURE  2023-02-09 19:10:30.000000 2023-02-09 19:10:30.000000 DEFINER  utf8mb4 utf8mb4_bin utf8mb4_bin", "test select9 PROCEDURE  2023-02-09 19:10:30.000000 2023-02-09 19:10:30.000000 DEFINER  utf8mb4 utf8mb4_bin utf8mb4_bin"))
	tk.MustQuery("show procedure status where Type = 'PROCEDURE'").Check(testkit.Rows("test insert_data PROCEDURE  2023-02-09 19:10:30.000000 2023-02-09 19:10:30.000000 DEFINER  utf8mb4 utf8mb4_bin utf8mb4_bin", "test select1 PROCEDURE  2023-02-09 19:10:30.000000 2023-02-09 19:10:30.000000 DEFINER  utf8mb4 utf8mb4_bin utf8mb4_bin",
		"test select2 PROCEDURE  2023-02-09 19:10:30.000000 2023-02-09 19:10:30.000000 DEFINER  utf8mb4 utf8mb4_bin utf8mb4_bin", "test select3 PROCEDURE  2023-02-09 19:10:30.000000 2023-02-09 19:10:30.000000 DEFINER  utf8mb4 utf8mb4_bin utf8mb4_bin",
		"test select4 PROCEDURE  2023-02-09 19:10:30.000000 2023-02-09 19:10:30.000000 DEFINER  utf8mb4 utf8mb4_bin utf8mb4_bin", "test select5 PROCEDURE  2023-02-09 19:10:30.000000 2023-02-09 19:10:30.000000 DEFINER  utf8mb4 utf8mb4_bin utf8mb4_bin",
		"test select6 PROCEDURE  2023-02-09 19:10:30.000000 2023-02-09 19:10:30.000000 DEFINER  utf8mb4 utf8mb4_bin utf8mb4_bin", "test select7 PROCEDURE  2023-02-09 19:10:30.000000 2023-02-09 19:10:30.000000 DEFINER  utf8mb4 utf8mb4_bin utf8mb4_bin",
		"test select8 PROCEDURE  2023-02-09 19:10:30.000000 2023-02-09 19:10:30.000000 DEFINER  utf8mb4 utf8mb4_bin utf8mb4_bin", "test select9 PROCEDURE  2023-02-09 19:10:30.000000 2023-02-09 19:10:30.000000 DEFINER  utf8mb4 utf8mb4_bin utf8mb4_bin"))
	tk.MustQuery("show function status").Check(testkit.Rows())

	tk.MustExec(`create procedure select10(iD int,name char(100))begin update user set username = name where user.id = id;
	select * from user where user.id = id; end;`)
	sql = fmt.Sprintf("call select10(%d,'%s')", 3, "TAAe")
	tk.MustExec(sql)
	require.Equal(t, len(tk.Res), 1)
	tk.Res[0].Check(testkit.Rows("3 TAAe password-3 15 1"))
	tk.ClearProcedureRes()
	tk.MustExec(`create procedure select11(iD int,name char(100))begin select case when IFNULL(name,'0') != '0' THEN name else 0 end ;
			select * from user where user.id = id; end;`)
	sql = fmt.Sprintf("call select11(%d,'%s')", 3, "TAAe")
	tk.MustExec(sql)
	require.Equal(t, len(tk.Res), 2)
	tk.Res[0].Check(testkit.Rows("TAAe"))
	tk.Res[1].Check(testkit.Rows("3 TAAe password-3 15 1"))

	tk.MustExec(`create table t3 ( d date, i int, f double, s varchar(32) )`)
	tk.MustExec("drop procedure if exists nullset")
	tk.MustExec(`create procedure nullset()
	begin
	  declare ld date;
	  declare li int;
	  declare lf double;
	  declare ls varchar(32);

	  set ld = null, li = null, lf = null, ls = null;
	  insert into t3 values (ld, li, lf, ls);

	  insert into t3 (i, f, s) values ((ld is null), 1,    "ld is null"),
									  ((li is null), 1,    "li is null"),
					  ((li = 0),     null, "li = 0"),
					  ((lf is null), 1,    "lf is null"),
					  ((lf = 0),     null, "lf = 0"),
					  ((ls is null), 1,    "ls is null");
	end`)
	tk.MustExec(`call nullset()`)
	tk.MustQuery(`select * from t3`).Sort().Check(testkit.Rows("<nil> 1 1 ld is null", "<nil> 1 1 lf is null", "<nil> 1 1 li is null",
		"<nil> 1 1 ls is null", "<nil> <nil> <nil> <nil>", "<nil> <nil> <nil> lf = 0", "<nil> <nil> <nil> li = 0"))
}

func TestCallIfElseSQL(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.InProcedure()
	tk.MustExec("use test")
	tk.MustExec(`create procedure poc_test1(iD int)begin if id > 1 then select 1; end if; end;`)
	tk.MustExec("call poc_test1(1)")
	require.Equal(t, len(tk.Res), 0)
	tk.MustExec("call poc_test1(2)")
	require.Equal(t, len(tk.Res), 1)
	tk.Res[0].Check(testkit.Rows("1"))
	tk.ClearProcedureRes()
	tk.MustExec(`create procedure poc_test2(iD int)begin if id > 1 then select 1;else select 2 ;end if; end;`)
	tk.MustExec("call poc_test2(1)")
	require.Equal(t, len(tk.Res), 1)
	tk.Res[0].Check(testkit.Rows("2"))
	tk.ClearProcedureRes()
	tk.MustExec("call poc_test2(2)")
	require.Equal(t, len(tk.Res), 1)
	tk.Res[0].Check(testkit.Rows("1"))
	tk.ClearProcedureRes()

	tk.MustExec(`create procedure poc_test3(iD int)begin
	if id > 10 then select 1;
	elseif id >2 then select 3;
	elseif id >0 then select 4;
	else select 10;end if; end;`)
	tk.MustExec("call poc_test3(1)")
	require.Equal(t, len(tk.Res), 1)
	tk.Res[0].Check(testkit.Rows("4"))
	tk.ClearProcedureRes()
	tk.MustExec("call poc_test3(3)")
	require.Equal(t, len(tk.Res), 1)
	tk.Res[0].Check(testkit.Rows("3"))
	tk.ClearProcedureRes()
	tk.MustExec("call poc_test3(11)")
	require.Equal(t, len(tk.Res), 1)
	tk.Res[0].Check(testkit.Rows("1"))
	tk.ClearProcedureRes()
	tk.MustExec("call poc_test3(0)")
	require.Equal(t, len(tk.Res), 1)
	tk.Res[0].Check(testkit.Rows("10"))
	tk.ClearProcedureRes()

	tk.MustExec(`create procedure poc_test4(iD int)begin if id > 10 then select 1;
	            if id >12  then select 8;end if;
				elseif id >2 then select 3;
				elseif id >0 then select 4;
				else select 10;end if; end;`)
	tk.MustExec("call poc_test4(1)")
	require.Equal(t, len(tk.Res), 1)
	tk.Res[0].Check(testkit.Rows("4"))
	tk.ClearProcedureRes()
	tk.MustExec("call poc_test4(3)")
	require.Equal(t, len(tk.Res), 1)
	tk.Res[0].Check(testkit.Rows("3"))
	tk.ClearProcedureRes()
	tk.MustExec("call poc_test4(11)")
	require.Equal(t, len(tk.Res), 1)
	tk.Res[0].Check(testkit.Rows("1"))
	tk.ClearProcedureRes()
	tk.MustExec("call poc_test4(13)")
	require.Equal(t, len(tk.Res), 2)
	tk.Res[0].Check(testkit.Rows("1"))
	tk.Res[1].Check(testkit.Rows("8"))
	tk.ClearProcedureRes()
	tk.MustExec("call poc_test4(0)")
	require.Equal(t, len(tk.Res), 1)
	tk.Res[0].Check(testkit.Rows("10"))
	tk.ClearProcedureRes()

	tk.MustExec(`create procedure poc_test5(iD varchar(20))begin if char_length(id) > 10 then select id;
				else select 10;end if; end;`)
	tk.MustExec("call poc_test5('sde')")
	require.Equal(t, len(tk.Res), 1)
	tk.Res[0].Check(testkit.Rows("10"))
	tk.ClearProcedureRes()
	tk.MustExec("call poc_test5('1111111111111')")
	require.Equal(t, len(tk.Res), 1)
	tk.Res[0].Check(testkit.Rows("1111111111111"))
	tk.ClearProcedureRes()

	tk.MustExec(`create procedure poc_test6(iD varchar(20))begin declare ss varchar(10);set ss = id; if char_length(ss) > 5 then select SUBSTRING(ss,5);
				else select ss;end if; end;`)
	tk.MustExec("call poc_test6('sde')")
	require.Equal(t, len(tk.Res), 1)
	tk.Res[0].Check(testkit.Rows("sde"))
	tk.ClearProcedureRes()
	tk.MustGetErrCode("call poc_test6('1111111111111')", 1406)
	tk.ClearProcedureRes()
	tk.MustExec("call poc_test6('1111111111')")
	require.Equal(t, len(tk.Res), 1)
	tk.Res[0].Check(testkit.Rows("111111"))
	tk.ClearProcedureRes()
	// undefine variable
	tk.MustExec(`create procedure poc_test7(iD varchar(20))begin if char_length(ss) > 5 then select SUBSTRING(ss,5);
	else select ss;end if;begin  declare ss varchar(10);end; end;`)
	tk.MustGetErrCode("call poc_test7('sde')", 1054)
	tk.MustExec("drop procedure poc_test7")
	tk.MustExec(`create procedure poc_test7(id varchar(20))begin if char_length(id) > 5 then select id;
	begin  declare ss varchar(10);set ss = 'ssss';select ss;end; else select id;end if; end;`)
	tk.MustExec("call poc_test7('sde')")
	tk.Res[0].Check(testkit.Rows("sde"))
	tk.ClearProcedureRes()
	tk.MustExec("call poc_test7('sde111')")
	tk.Res[0].Check(testkit.Rows("sde111"))
	tk.Res[1].Check(testkit.Rows("ssss"))
	tk.ClearProcedureRes()
}

func TestCallWhileSQL(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.InProcedure()
	tk.MustExec("use test")
	tk.MustExec("create table t1 (id int key,id2 int)")
	tk.MustExec(`create procedure poc_test1(start int,num int)begin declare id int default start; while id < start + num  do  set id = id + 1; insert into t1 value(id,id); end while; end;`)
	tk.MustExec("call poc_test1(0,10)")
	require.Equal(t, len(tk.Res), 0)
	tk.MustQuery("select count(*) from t1").Check(testkit.Rows("10"))
	tk.ClearProcedureRes()
	tk.MustGetErrCode("call poc_test1(1,10)", 1062)
	tk.MustExec("call poc_test1(10,100)")
	tk.MustQuery("select count(*) from t1").Check(testkit.Rows("110"))
	tk.MustQuery("show create procedure poc_test1").Check(testkit.Rows("poc_test1 ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION  " +
		"CREATE PROCEDURE `poc_test1`(start int,num int)\nbegin declare id int default start; while id < start + num  do  set id = id + 1; insert into t1 value(id,id); end while; end utf8mb4 utf8mb4_bin utf8mb4_bin"))
	tk.MustExec(`create procedure poc_test2(start int,num int)begin declare id int default start; while id < start + num  do  set id = id + 1;begin select count(*) from t1 limit 1;if id > 10 then delete from t1 limit 3;end if; end; insert into t1 value(id,id); end while; end;`)
	tk.MustExec("call poc_test2(110,10)")
	require.Equal(t, len(tk.Res), 10)
	tk.Res[0].Check(testkit.Rows("110"))
	tk.Res[1].Check(testkit.Rows("108"))
	tk.Res[2].Check(testkit.Rows("106"))
	tk.Res[3].Check(testkit.Rows("104"))
	tk.Res[4].Check(testkit.Rows("102"))
	tk.Res[5].Check(testkit.Rows("100"))
	tk.Res[6].Check(testkit.Rows("98"))
	tk.Res[7].Check(testkit.Rows("96"))
	tk.Res[8].Check(testkit.Rows("94"))
	tk.Res[9].Check(testkit.Rows("92"))
	tk.ClearProcedureRes()
	tk.MustExec("create table t2 (id int key,id2 char(1))")
	tk.MustExec(`create procedure poc_test3(start varchar(100))begin declare id int default 0; while id < CHAR_LENGTH(start)  do  set id = id + 1; insert into t2 value(id,SUBSTRING(start,id,1)); end while; end;`)
	tk.MustExec("call poc_test3('njshau')")
	tk.MustQuery("select id2 from t2").Check(testkit.Rows("n", "j", "s", "h", "a", "u"))
	tk.MustExec("truncate table t2")
	tk.MustExec("call poc_test3(NULL)")
	tk.MustQuery("select id2 from t2").Check(testkit.Rows())
	tk.MustExec("call poc_test3('231r1sjxancxac')")
	tk.MustQuery("select id2 from t2").Check(testkit.Rows("2", "3", "1", "r", "1", "s", "j", "x", "a", "n", "c", "x", "a", "c"))
}

func TestCallRepeatStructure(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.InProcedure()
	tk.MustExec("use test")

	// general repeat structure.
	tk.MustExec(`create procedure dorepeat(p1 int) begin set @x = 0; repeat set @x = @x + 1; until @x > p1 end repeat; end;`)
	tk.MustExec("call dorepeat(1000)")
	tk.MustQuery("select @x").Check(testkit.Rows("1001"))
	tk.MustExec("drop procedure dorepeat")

	// repeat structure with SQL.
	tk.MustExec("create table score (id int primary key ,grade int)")
	tk.MustExec("set @total_count =0")
	tk.MustExec("set @limit_total_grade = 500")
	tk.MustExec(`create procedure proc_cursor(in limit_total_grade int, out total_count int )
	begin
		declare sum_grade int default 0;
		declare cursor_grade int default 0;
		declare score_count int default 0;
		declare v_done boolean default false;

		declare score_cursor cursor for select grade from score order by grade ;
		declare continue handler for not found set v_done=true;

		open score_cursor;
		repeat
			fetch score_cursor into cursor_grade;
			if v_done = false then
				set sum_grade = sum_grade + cursor_grade;
				set score_count = score_count + 1;
			end if;
			until sum_grade > limit_total_grade || v_done = true
		end repeat ;
		set total_count = score_count;
		close score_cursor;
	end;`)

	// repeat structure with no rows.
	tk.MustExec("call proc_cursor(@limit_total_grade,@total_count)")
	tk.MustQuery("select @total_count").Check(testkit.Rows("0"))

	tk.MustExec("insert into score values (1,100),(2,200),(3,300),(4,400)")
	// repeat structure end with limit_total_grade.
	tk.MustExec("call proc_cursor(@limit_total_grade,@total_count)")
	tk.MustQuery("select @total_count").Check(testkit.Rows("3"))

	// repeat structure end with all rows.
	tk.MustExec("set @limit_total_grade =10000")
	tk.MustExec("call proc_cursor(@limit_total_grade,@total_count)")
	tk.MustQuery("select @total_count").Check(testkit.Rows("4"))

	tk.MustExec("drop procedure proc_cursor")
	tk.MustExec("drop table score")
}

func TestCallFetchInto(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.InProcedure()
	tk.MustExec("use test")
	// Variable or condition declaration after cursor or handler declaration
	tk.MustGetErrCode("create procedure poc_test1() begin  declare xx CURSOR for select 1;declare id int ; open xx; fetch xx into id;select id;end;", 1337)
	// Duplicate cursor
	tk.MustGetErrCode("create procedure poc_test1() begin  declare xx CURSOR for select 1;declare xx CURSOR for select 2; open xx; fetch xx into id;select id;end;", 1333)
	// Undeclared variable
	tk.MustGetErrCode("create procedure poc_test1() begin  declare xx CURSOR for select 1;open xx; fetch xx into id;select id;end;", 1327)
	// not support variable begin @
	tk.MustGetErrCode("create procedure poc_test1() begin  declare xx CURSOR for select 1;open xx; fetch xx into @id;select id;end;", 1064)
	// fetch not support insert
	tk.MustGetErrCode("create procedure poc_test1() begin  declare xx CURSOR for insert into t1 value(1,1);open xx; fetch xx into @id;select id;end;", 1064)
	// fetch not support delete
	tk.MustGetErrCode("create procedure poc_test1() begin  declare xx CURSOR for delete from t1;open xx; fetch xx into @id;select id;end;", 1064)
	// Undeclared CURSOR
	tk.MustGetErrCode("create procedure poc_test1() begin declare id int ; open xx; fetch xx into id;select id;end;", 1324)
	// Undeclared CURSOR
	tk.MustGetErrCode("create procedure poc_test1() begin  declare id int ; fetch xx into id;select id;end;", 1324)
	// Undeclared CURSOR
	tk.MustGetErrCode("create procedure poc_test1() begin  declare id int ; close xx;select id;end;", 1324)
	tk.MustExec("create procedure poc_test1() begin declare id int ; declare xx CURSOR for select 1; open xx; fetch xx into id;select id;close xx;end;")
	tk.MustExec("call poc_test1")
	require.Equal(t, len(tk.Res), 1)
	tk.Res[0].Check(testkit.Rows("1"))
	tk.ClearProcedureRes()
	tk.MustExec("create procedure poc_test2() begin declare id int ; declare xx CURSOR for select 1; open xx; open xx;fetch xx into id;select id;close xx;end;")
	tk.MustGetErrCode("call poc_test2", 1325)
	tk.MustExec("create procedure poc_test3() begin declare id int ; declare xx CURSOR for select 1; fetch xx into id;select id;close xx;end;")
	tk.MustGetErrCode("call poc_test3", 1326)
	tk.MustExec("create procedure poc_test4() begin declare id int ; declare xx CURSOR for select 1;close xx;end;")
	tk.MustGetErrCode("call poc_test4", 1326)
	tk.MustExec("create procedure poc_test5() begin declare id int ; declare xx CURSOR for select 1;open xx;close xx;close xx;end;")
	tk.MustGetErrCode("call poc_test5", 1326)
	tk.MustExec("create procedure poc_test6() begin declare id int ; declare xx CURSOR for select 1;open xx;close xx;fetch xx into id;end;")
	tk.MustGetErrCode("call poc_test6", 1326)
	tk.MustExec("CREATE TABLE `user` ( `id` int(11) NOT NULL, `username` VARCHAR(30) DEFAULT NULL, `password` VARCHAR(30) DEFAULT NULL, " +
		"`age` int(11) NOT NULL, `sex` int(11) NOT NULL, PRIMARY KEY (`id`), KEY `username` (`username`) ) ENGINE=InnoDB;")
	tk.MustExec("CREATE TABLE `user_score` ( `id` int(11) NOT NULL, `subject` int(11) NOT NULL, `user_id` int(11) NOT NULL, " +
		"`score` int(11) NOT NULL, PRIMARY KEY (`id`) ) ENGINE=InnoDB;")
	tk.MustExec("CREATE TABLE `user_address` ( `id` int(11) NOT NULL, `user_id` int(11) NOT NULL, `address` VARCHAR(30) DEFAULT NULL, " +
		"PRIMARY KEY (`id`), KEY `address` (`address`) ) ENGINE=InnoDB;")
	tk.MustExec(`create procedure insert_data(i int, s_i int)  begin
		declare x int default 0;
		while x < i do
		insert into user values(x, CONCAT("username-", x),CONCAT("password-", x),FLOOR( 15 ),Mod(x,2));
		insert into user_score values(s_i, 1, x, FLOOR( 40 + x * 100));
		set s_i=s_i+1;
		insert into user_score values(s_i, 2, x, FLOOR( 40 + x * 100));
		set s_i=s_i+1;
		insert into user_score values(s_i, 3, x, FLOOR( 40 + x * 100));
		set s_i=s_i+1;
		insert into user_score values(s_i, 4, x, FLOOR( 40 + x * 100));
		set s_i=s_i+1;
		insert into user_score values(s_i, 5, x, FLOOR( 40 + x * 100));
		set s_i=s_i+1;
		insert into user_address values(x, x, CONCAT("useraddress-", x));
		set x=x+1;
		end while;  end;`)
	tk.MustExec("call insert_data(100,0)")
	tk.MustQuery("select count(*) from user").Check(testkit.Rows("100"))
	tk.MustQuery("select count(*) from user_score").Check(testkit.Rows("500"))
	tk.MustQuery("select count(*) from user_address").Check(testkit.Rows("100"))
	tk.ClearProcedureRes()
	// test cursor
	tk.MustExec(`create procedure check_data(id int) begin declare name1,name2 varchar(30);declare val,age1,sex1,num int default 0; declare s1  CURSOR for select * from user;
		open s1;
		while num < id do fetch s1 into val,name1,name2,age1,sex1;
		if (name1 != CONCAT("username-", num))||(name2 != CONCAT("password-", num))||(age1 != 15) || (num != val) then
		select "no equeal"; end if;
		set num = num + 1;end while;
		close s1;end;`)
	tk.MustExec("call check_data(10)")
	require.Equal(t, len(tk.Res), 0)
	tk.MustExec("call check_data(100)")
	require.Equal(t, len(tk.Res), 0)
	tk.MustGetErrCode("call check_data(200)", 1329)
	require.Equal(t, len(tk.Res), 0)

	// test more cursor
	tk.MustExec(`create procedure check_data2(id int)
	    begin
		declare name1,name2,addressInfo varchar(30);
		declare val,age1,sex1,num,id1,id2 int default 0;
		declare s1  CURSOR for select * from user;
		declare s2  CURSOR for select * from user_address;
		open s1;open s2;
		while num < id do
		fetch s1 into val,name1,name2,age1,sex1;
		if (name1 != CONCAT("username-", num))||(name2 != CONCAT("password-", num))||(age1 != 15) || (num != val) then
		select "no equeal"; end if;
		fetch s2 into id1,id2,addressInfo;
		if (addressInfo != CONCAT("useraddress-", num))||(id1 != num )||(id2 != num ) then
		select "no equeal"; end if;
		set num = num + 1;end while;
		close s1;end;`)
	tk.MustExec("call check_data2(10)")
	require.Equal(t, len(tk.Res), 0)
	tk.MustExec("call check_data2(100)")
	require.Equal(t, len(tk.Res), 0)
	tk.MustGetErrCode("call check_data2(200)", 1329)
	require.Equal(t, len(tk.Res), 0)

	// test cover cursor
	tk.MustExec(`create procedure check_data3(id int)
		begin
		declare name1,name2,addressInfo varchar(30);
		declare val,age1,sex1,num,id1,id2 int default 0;
		declare s1  CURSOR for select * from user;
		open s1;
		begin
		declare s1  CURSOR for select * from user_address;
		open s1;
		while num < id do
		fetch s1 into id1,id2,addressInfo;
		if (addressInfo != CONCAT("useraddress-", num))||(id1 != num )||(id2 != num ) then
		select "no equeal"; end if;
		set num = num + 1;end while;
		close s1 ;end;
		set num = 0;
		while num < id do
		fetch s1 into val,name1,name2,age1,sex1;
		if (name1 != CONCAT("username-", num))||(name2 != CONCAT("password-", num))||(age1 != 15) || (num != val) then
		select "no equeal"; end if;
		set num = num + 1;end while;
		close s1;end;`)
	tk.MustExec("call check_data3(10)")
	require.Equal(t, len(tk.Res), 0)
	tk.MustExec("call check_data3(100)")
	require.Equal(t, len(tk.Res), 0)
	tk.MustGetErrCode("call check_data3(200)", 1329)
	require.Equal(t, len(tk.Res), 0)

	tk.MustExec("truncate table user")
	tk.MustExec("truncate table user_score")
	tk.MustExec("truncate table user_address")
	tk.MustExec("call insert_data(20,20)")
	// test cursor complex SQL
	tk.MustExec(`create procedure comSelect()
		begin
		declare name1,name2 varchar(30);
		declare id,id1,id2 int default 0;
		declare s1  CURSOR for select a.id,a.username,a.password,a.age,a.sex from user a where a.id > 10 and a.id < 50 order by a.id;
		open s1;
		while 1 do
		fetch s1 into id,name1,name2,id1,id2;
		select  id,name1,name2,id1,id2;
		end while;
		end;`)
	tk.MustGetErrCode("call comSelect", 1329)
	require.Equal(t, len(tk.Res), 9)
	tk.Res[0].Check(testkit.Rows("11 username-11 password-11 15 1"))
	tk.Res[1].Check(testkit.Rows("12 username-12 password-12 15 0"))
	tk.Res[2].Check(testkit.Rows("13 username-13 password-13 15 1"))
	tk.Res[3].Check(testkit.Rows("14 username-14 password-14 15 0"))
	tk.Res[4].Check(testkit.Rows("15 username-15 password-15 15 1"))
	tk.Res[5].Check(testkit.Rows("16 username-16 password-16 15 0"))
	tk.Res[6].Check(testkit.Rows("17 username-17 password-17 15 1"))
	tk.Res[7].Check(testkit.Rows("18 username-18 password-18 15 0"))
	tk.Res[8].Check(testkit.Rows("19 username-19 password-19 15 1"))
	tk.ClearProcedureRes()
	tk.MustExec(`create procedure comSelect1()
		begin
		declare id1,id2,id3,id4,id5 int default 0;
		declare id6 decimal(10,4) default 0;
		declare s1  CURSOR for select us.subject,count(us.user_id),sum(us.score),avg(us.score),max(us.score),min(us.score) from user_score us where us.score > 90 group by us.subject order by us.subject;
		open s1;
		while 1 do
		fetch s1 into id1,id2,id3,id6,id4,id5;
		select  id1,id2,id3,id6,id4,id5;
		end while;
		end;`)
	tk.MustGetErrCode("call comSelect1", 1329)
	require.Equal(t, len(tk.Res), 5)
	tk.Res[0].Check(testkit.Rows("1 19 19760 1040.0000 1940 140"))
	tk.Res[1].Check(testkit.Rows("2 19 19760 1040.0000 1940 140"))
	tk.Res[2].Check(testkit.Rows("3 19 19760 1040.0000 1940 140"))
	tk.Res[3].Check(testkit.Rows("4 19 19760 1040.0000 1940 140"))
	tk.Res[4].Check(testkit.Rows("5 19 19760 1040.0000 1940 140"))
	tk.ClearProcedureRes()
	tk.MustExec(`create procedure comSelect2()
		begin
		declare id1,id2,id3,id4,id5 int default 0;
		declare s1  CURSOR for select *,rank() over (partition by subject order by score desc) as ranking from user_score order by id;
		open s1;
		while 1 do
		fetch s1 into id1,id2,id3,id4,id5;
		select  id1,id2,id3,id4,id5;
		end while;
		end;`)
	tk.MustGetErrCode("call comSelect2", 1329)
	require.Equal(t, len(tk.Res), 100)
	tk.Res[99].Check(testkit.Rows("119 5 19 1940 1"))
	tk.ClearProcedureRes()
	tk.MustExec(`create procedure comSelect3()
		begin
		declare id1,id2,id3,id4,id5,id7,id8,id9 int default 0;
		declare id6 decimal(10,4) default 0;
		declare s1  CURSOR for select us.*,sum(us.score) over (order by us.id) as current_sum,        avg(us.score) over (order by us.id) as current_avg,
		count(us.score) over (order by us.id) as current_count,        max(us.score) over (order by us.id) as current_max,        min(us.score) over (order by us.id) as current_min from user_score us order by id;
		open s1;
		while 1 do
		fetch s1 into id1,id2,id3,id4,id5,id6,id7,id8,id9;
		select  id1,id2,id3,id4,id5,id6,id7,id8,id9;
		end while;
		end;`)
	tk.MustGetErrCode("call comSelect3", 1329)
	require.Equal(t, len(tk.Res), 100)
	tk.Res[99].Check(testkit.Rows("119 5 19 1940 99000 990.0000 100 1940 40"))
	tk.ClearProcedureRes()
	tk.MustExec(`create procedure comSelect4()
		begin
		declare id1,id2,id3,id4,id5,id7,id8,id9 int default 0;
		declare id6 decimal(10,4) default 0;
		declare id10,id11,id12 varchar(30);
		declare s1  CURSOR for select us.*,sum(us.score) over (order by us.id) as current_sum,        avg(us.score) over (order by us.id) as current_avg,        count(us.score) over (order by us.id) as current_count,        max(us.score) over (order by us.id) as current_max,        min(us.score) over (order by us.id) as current_min,        u.username ,ua.address,CONCAT(u.username, "-" ,ua.address) as userinfo        from user_score us left join user u on u.id = us.user_id
		left join user_address ua on ua.id = us.user_id order by us.id;
		open s1;
		while 1 do
		fetch s1 into id1,id2,id3,id4,id5,id6,id7,id8,id9,id10,id11,id12;
		select  id1,id2,id3,id4,id5,id6,id7,id8,id9,id10,id11,id12;
		end while;
		end;`)
	tk.MustGetErrCode("call comSelect4", 1329)
	require.Equal(t, len(tk.Res), 100)
	tk.Res[99].Check(testkit.Rows("119 5 19 1940 99000 990.0000 100 1940 40 username-19 useraddress-19 username-19-useraddress-19"))
	tk.ClearProcedureRes()
	tk.MustExec(`create procedure comSelect5()
		begin
		declare id1,id5 int default 0;
		declare id2,id3,id4 varchar(30);
		declare s1  CURSOR for SELECT DISTINCT us.user_id,u.username ,ua.address,CONCAT(u.username, "-" ,ua.address) as userinfo,
		sum(us.score) from user_score us left join user u on u.id = us.user_id         left join user_address ua on ua.id = us.user_id group by us.user_id,u.username order by us.user_id;
		open s1;
		while 1 do
		fetch s1 into id1,id2,id3,id4,id5;
		select  id1,id2,id3,id4,id5;
		end while;
		end;`)
	tk.MustGetErrCode("call comSelect5", 1329)
	require.Equal(t, len(tk.Res), 20)
	tk.Res[19].Check(testkit.Rows("19 username-19 useraddress-19 username-19-useraddress-19 9700"))
	tk.ClearProcedureRes()
	tk.MustExec(`create procedure comSelect6()
	begin
	declare id1,id2,id3,id5,id6 int default 0;
	declare id4 varchar(30);
	declare s1  CURSOR for select a.subject,a.id,a.user_id,u.username, a.score,a.rownum from ( select id,user_id,subject,score,row_number() over (order by score desc) as rownum from user_score) as a
	left join user u on a.user_id = u.id inner join user_score as b on a.id=b.id where a.rownum<=10 order by a.rownum ;
	open s1;
	while 1 do
	fetch s1 into id1,id2,id3,id4,id5,id6;
	select  id1,id2,id3,id4,id5,id6;
	end while;
	end;`)
	tk.MustGetErrCode("call comSelect6", 1329)
	require.Equal(t, len(tk.Res), 10)
	tk.Res[9].Check(testkit.Rows("2 111 18 username-18 1840 10"))
	tk.ClearProcedureRes()
	tk.MustExec(`create procedure comSelect7()
	begin
	declare id1,id2,id3,id5,id6 int default 0;
	declare id4 varchar(30);
	declare s1  CURSOR for select a.subject,a.id,a.score,a.rownum from (         select id,subject,score,row_number()
	over (partition by subject order by score desc) as rownum from user_score) as a         inner join user_score as b on a.id=b.id where a.rownum<=10 order by a.subject,a.id ;
	open s1;
	while 1 do
	fetch s1 into id1,id2,id3,id4;
	select  id1,id2,id3,id4;
	end while;
	end;`)
	tk.MustGetErrCode("call comSelect7", 1329)
	require.Equal(t, len(tk.Res), 50)
	tk.Res[49].Check(testkit.Rows("5 119 1940 1"))
	tk.ClearProcedureRes()
	tk.MustExec(`create procedure comSelect8()
	begin
	declare id1,id2,id3,id4,id5,id8,id9,id10,id11,id17 int default 0;
	declare id6,id7,id12,id13,id14,id15 varchar(30);
	declare id16 decimal(10,4) default 0;
	declare s1  CURSOR for select *,u.username,ua.address,CONCAT(u.username, "-" ,ua.address) as userinfo,
	avg(us.score) over (order by us.id rows 2 preceding) as current_avg,         sum(score) over (order by us.id rows 2 preceding) as current_sum from user_score us
	left join user u on u.id = us.user_id left join user_address ua on ua.id = us.user_id order by u.id ;
	open s1;
	while 1 do
	fetch s1 into id1,id2,id3,id4,id5,id6,id7,id8,id9,id10,id11,id12,id13,id14,id15,id16,id17;
	select  id1,id2,id3,id4,id5,id6,id7,id8,id9,id10,id11,id12,id13,id14,id15,id16,id17;
	end while;
	end;`)
	tk.MustGetErrCode("call comSelect8", 1329)
	require.Equal(t, len(tk.Res), 100)
	tk.Res[99].Check(testkit.Rows("119 5 19 1940 19 username-19 password-19 15 1 19 19 useraddress-19 username-19 useraddress-19 username-19-useraddress-19 1940.0000 5820"))
	tk.ClearProcedureRes()
	tk.MustExec(`create procedure comSelect9()
	begin
	declare id1,id4,id5 int default 0;
	declare id2,id3 varchar(30);
	declare s1  CURSOR for select a.id,a.username,a.password,a.age,a.sex from user a where a.id in
	(select user_id from user_score where score > 90) order by a.id;
	open s1;
	while 1 do
	fetch s1 into id1,id2,id3,id4,id5;
	select  id1,id2,id3,id4,id5;
	end while;
	end;`)
	tk.MustGetErrCode("call comSelect9", 1329)
	require.Equal(t, len(tk.Res), 19)
	tk.Res[18].Check(testkit.Rows("19 username-19 password-19 15 1"))
	tk.ClearProcedureRes()
	tk.MustExec(`create procedure comSelect10()
	begin
	declare id1,id3,id4 int default 0;
	declare id2 varchar(30);
	declare s1  CURSOR for select us.user_id,u.username,us.subject,us.score from user_score us left join user u on u.id = us.user_id
	where us.score > 90 group by us.user_id,us.subject,us.score order by us.user_id,u.username,us.subject,us.score;
	open s1;
	while 1 do
	fetch s1 into id1,id2,id3,id4;
	select  id1,id2,id3,id4;
	end while;
	end;`)
	tk.MustGetErrCode("call comSelect10", 1329)
	require.Equal(t, len(tk.Res), 95)
	tk.Res[94].Check(testkit.Rows("19 username-19 5 1940"))
	tk.ClearProcedureRes()
	tk.MustExec(`create procedure comSelect11()
	begin
	declare id1,id4,id5 int default 0;
	declare id2,id3,id6,id7 varchar(30);
	declare s1  CURSOR for select a.id,a.username,a.password,a.age,a.sex,ad.address,CONCAT(a.username, "-" ,ad.address) as userinfo from user a          left join user_address ad
	on a.id = ad.user_id where a.id > 10 and a.id < 50 order by a.id;
	open s1;
	while 1 do
	fetch s1 into id1,id2,id3,id4,id5,id6,id7;
	select  id1,id2,id3,id4,id5,id6,id7;
	end while;
	end;`)
	tk.MustGetErrCode("call comSelect11", 1329)
	require.Equal(t, len(tk.Res), 9)
	tk.Res[8].Check(testkit.Rows("19 username-19 password-19 15 1 useraddress-19 username-19-useraddress-19"))
	tk.ClearProcedureRes()
	tk.MustExec(`create procedure comSelect12()
	begin
	declare id1,id4,id5 int default 0;
	declare id2,id3,id6,id7 varchar(30);
	declare s1  CURSOR for select a.id,a.username,a.password,a.age,a.sex,ad.score from user a right join user_score ad on a.id = ad.user_id
	where a.id > 10 and a.id < 50 order by a.id,a.username,a.password,a.age,a.sex,ad.score;
	open s1;
	while 1 do
	fetch s1 into id1,id2,id3,id4,id5,id6;
	select  id1,id2,id3,id4,id5,id6;
	end while;
	end;`)
	tk.MustGetErrCode("call comSelect12", 1329)
	require.Equal(t, len(tk.Res), 45)
	tk.Res[44].Check(testkit.Rows("19 username-19 password-19 15 1 1940"))
	tk.ClearProcedureRes()
	tk.MustExec(`create procedure comSelect13()
	begin
	declare id1,id4,id5 int default 0;
	declare id2,id3,id6,id7 varchar(30);
	declare s1  CURSOR for select a.id,a.username,a.password,a.age,a.sex,ad.score from user a right join user_score ad on a.id = ad.user_id
	where a.id > 10 and a.id < 50 order by a.id,a.username,a.password,a.age,a.sex,ad.score;
	open s1;
	while 1 do
	fetch s1 into id1,id2,id3,id4,id5,id6;
	select  id1,id2,id3,id4,id5,id6;
	end while;
	end;`)
	tk.MustGetErrCode("call comSelect13", 1329)
	require.Equal(t, len(tk.Res), 45)
	tk.Res[44].Check(testkit.Rows("19 username-19 password-19 15 1 1940"))
	tk.ClearProcedureRes()
	tk.MustExec(`create procedure comSelect14()
	begin
	declare id1,id4,id5 int default 0;
	declare id2,id3,id6,id7 varchar(30);
	declare s1  CURSOR for select a.id,a.username,a.password,a.age,a.sex,ad.score from user a
	left join user_score ad on a.id = ad.user_id where a.id in (select user_id from user_score where score > 90 and score < 99 )
	union select a.id,a.username,a.password,a.age,a.sex,ad.score from user a
	left join user_score ad on a.id = ad.user_id
	where a.id in (select user_id from user_score where score > 30 and score < 70 );
	open s1;
	while 1 do
	fetch s1 into id1,id2,id3,id4,id5,id6;
	select  id1,id2,id3,id4,id5,id6;
	end while;
	end;`)
	tk.MustGetErrCode("call comSelect14", 1329)
	require.Equal(t, len(tk.Res), 1)
	tk.Res[0].Check(testkit.Rows("0 username-0 password-0 15 0 40"))
	tk.ClearProcedureRes()

	tk.MustExec("truncate table user")
	tk.MustExec("truncate table user_score")
	tk.MustExec("truncate table user_address")
	tk.MustExec("call insert_data(100,0)")
	// variables in fetch
	tk.MustExec(`create procedure variableInSelect(info int)
	begin
	declare id1 int default 0;
	declare s1  CURSOR for select id from user where id < info;
	set info = 60;
	open s1;
	while 1 do
	fetch s1 into id1;
	set info = 90;
	select  id1;
	end while;
	end;`)
	tk.MustGetErrCode("call variableInSelect(60)", 1329)
	require.Equal(t, len(tk.Res), 60)
	tk.ClearProcedureRes()

	tk.MustExec(`create procedure variableInSelect1(info int)
	begin
	declare id1,id2 int default 0;
	declare s1  CURSOR for select id from user where id < info;
	set info = 60;
	open s1;
	while 1 do
	fetch s1 into id1,id2;
	set info = 90;
	select  id1;
	end while;
	end;`)
	tk.MustGetErrCode("call variableInSelect1(60)", 1328)
	require.Equal(t, len(tk.Res), 0)
}

func TestErrorControl(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.InProcedure()
	tk.MustExec("use test")
	tk.MustGetErrCode("create procedure select_error() begin  declare exit handler for 1146 select 2; declare id1 int default 0; end", 1337)
	tk.MustGetErrCode("create procedure select_error() begin  declare exit handler for 1146 select 2; declare s1  CURSOR for select id from user where id < info;end", 1338)
	tk.MustGetErrCode("create procedure select_error() begin  declare exit handler for 1146,2134,1146 select 2;end", 1413)
	tk.MustGetErrCode("create procedure select_error() begin  declare exit handler for 1146 select 2;  declare continue handler for 1146 select 2;end", 1413)
	tk.MustGetErrCode("create procedure select_error() begin  declare exit handler for not found,2134,not found select 2;end", 1413)
	tk.MustGetErrCode("create procedure select_error() begin  declare exit handler for not found select 2;  declare continue handler for not found select 2;end", 1413)

	tk.MustExec("CREATE TABLE `user` ( `id` int(11) NOT NULL, `username` VARCHAR(30) DEFAULT NULL, `password` VARCHAR(30) DEFAULT NULL, " +
		"`age` int(11) NOT NULL, `sex` int(11) NOT NULL, PRIMARY KEY (`id`), KEY `username` (`username`) ) ENGINE=InnoDB;")
	tk.MustExec("CREATE TABLE `user_score` ( `id` int(11) NOT NULL, `subject` int(11) NOT NULL, `user_id` int(11) NOT NULL, " +
		"`score` int(11) NOT NULL, PRIMARY KEY (`id`) ) ENGINE=InnoDB;")
	tk.MustExec("CREATE TABLE `user_address` ( `id` int(11) NOT NULL, `user_id` int(11) NOT NULL, `address` VARCHAR(30) DEFAULT NULL, " +
		"PRIMARY KEY (`id`), KEY `address` (`address`) ) ENGINE=InnoDB;")
	tk.MustExec(`create procedure insert_data(i int, s_i int)  begin
		declare x int default 0;
		while x < i do
		insert into user values(x, CONCAT("username-", x),CONCAT("password-", x),FLOOR( 15 ),Mod(x,2));
		insert into user_score values(s_i, 1, x, FLOOR( 40 + x * 100));
		set s_i=s_i+1;
		insert into user_score values(s_i, 2, x, FLOOR( 40 + x * 100));
		set s_i=s_i+1;
		insert into user_score values(s_i, 3, x, FLOOR( 40 + x * 100));
		set s_i=s_i+1;
		insert into user_score values(s_i, 4, x, FLOOR( 40 + x * 100));
		set s_i=s_i+1;
		insert into user_score values(s_i, 5, x, FLOOR( 40 + x * 100));
		set s_i=s_i+1;
		insert into user_address values(x, x, CONCAT("useraddress-", x));
		set x=x+1;
		end while;  end;`)
	tk.MustExec("call insert_data(100,0)")
	tk.MustExec(`create procedure select_error() begin select * from user1;select 1;end`)
	tk.MustGetErrCode("call select_error", 1146)
	// ignore 1146
	tk.MustExec(`create procedure select_error1() begin declare exit handler for 1146 select 2;select * from user1;select 1;end`)
	tk.MustExec("call select_error1")
	require.Equal(t, len(tk.Res), 1)
	tk.Res[0].Check(testkit.Rows("2"))
	tk.ClearProcedureRes()

	tk.MustExec(`create procedure select_error2() begin declare continue handler for 1146 select 2;select * from user1;select 1;end`)
	tk.MustExec("call select_error2")
	require.Equal(t, len(tk.Res), 2)
	tk.Res[0].Check(testkit.Rows("2"))
	tk.Res[1].Check(testkit.Rows("1"))
	tk.ClearProcedureRes()
	// SQLEXCEPTION
	tk.MustExec(`create procedure select_error3() begin declare continue handler for SQLEXCEPTION select 2;select * from user1;select 1;end`)
	tk.MustExec("call select_error3")
	require.Equal(t, len(tk.Res), 2)
	tk.Res[0].Check(testkit.Rows("2"))
	tk.Res[1].Check(testkit.Rows("1"))
	tk.ClearProcedureRes()
	// error cannot loop
	tk.MustExec(`create procedure select_error4() begin declare continue handler for SQLEXCEPTION select * from user1;select * from user1;select 1;end`)
	tk.MustGetErrCode("call select_error4", 1146)
	tk.ClearProcedureRes()
	// error control cover
	tk.MustExec(`create procedure select_error5() begin declare continue handler for SQLEXCEPTION select 1;
	begin
	declare exit handler for SQLEXCEPTION select 2;
	select * from user1;
	end;select 1;end`)
	tk.MustExec("call select_error5")
	require.Equal(t, 2, len(tk.Res))
	tk.Res[0].Check(testkit.Rows("2"))
	tk.Res[1].Check(testkit.Rows("1"))
	tk.ClearProcedureRes()

	// error control cover and recover
	tk.MustExec(`create procedure select_error6() begin declare exit handler for SQLEXCEPTION select 1;
	begin
	declare continue handler for SQLEXCEPTION select 2;
	select * from user1;
	end;select 1;
	select * from user1;
	end`)
	tk.MustExec("call select_error6")
	require.Equal(t, len(tk.Res), 3)
	tk.Res[0].Check(testkit.Rows("2"))
	tk.Res[1].Check(testkit.Rows("1"))
	tk.Res[2].Check(testkit.Rows("1"))
	tk.ClearProcedureRes()

	// mismatch error control
	tk.MustExec(`create procedure select_error7() begin declare continue handler for 1147 select 2;select * from user1;select 1;end`)
	tk.MustGetErrCode("call select_error7", 1146)

	// multierror control
	tk.MustExec(`create procedure select_error8() begin declare continue handler for 1146,1054 select 2;select * from user1;select id1 from user;select 1;end`)
	tk.MustExec("call select_error8")
	require.Equal(t, len(tk.Res), 3)
	tk.Res[0].Check(testkit.Rows("2"))
	tk.Res[1].Check(testkit.Rows("2"))
	tk.Res[2].Check(testkit.Rows("1"))
	tk.ClearProcedureRes()

	// SQLSTATE check
	tk.MustExec(`create procedure select_error9() begin declare continue handler for SQLSTATE '02000' select 2;select * from user1;select 1;end`)
	tk.MustGetErrCode("call select_error9", 1146)
	tk.MustGetErrCode(`create procedure handle_error() begin declare continue handler for SQLSTATE '00000' select 2;select 1;end`, 1407)
	tk.MustGetErrCode(`create procedure handle_error() begin declare continue handler for SQLSTATE '020000' select 2;select 1;end`, 1407)
	tk.MustGetErrCode(`create procedure handle_error() begin declare continue handler for SQLSTATE 'a2000' select 2;select 1;end`, 1407)
	tk.MustGetErrCode(`create procedure handle_error() begin declare continue handler for SQLSTATE '!2000' select 2;select 1;end`, 1407)

	// exit block
	tk.MustExec(`create procedure select_error10()
	begin declare exit handler for SQLEXCEPTION select 1;
	begin
	declare exit handler for SQLEXCEPTION select 2;
	select * from user1;
	select 3;
	end;
	select 1;
	end`)
	tk.MustExec("call select_error10")
	require.Equal(t, len(tk.Res), 2)
	tk.Res[0].Check(testkit.Rows("2"))
	tk.Res[1].Check(testkit.Rows("1"))
	tk.ClearProcedureRes()
	// exit block
	tk.MustExec(`create procedure select_error11()
		begin declare exit handler for SQLEXCEPTION select 1;
		begin
		select * from user1;
		select 3;
		end;
		select 1;
		end`)
	tk.MustExec("call select_error11")
	require.Equal(t, len(tk.Res), 1)
	tk.Res[0].Check(testkit.Rows("1"))
	tk.ClearProcedureRes()
	// warning handle
	tk.MustExec(`set sql_mode  = ""`)
	tk.MustExec("create table t3 (a smallint primary key)")
	tk.MustExec(`create procedure h_ew()
	begin
	  declare continue handler for 1264
		select 'Outer (bad)' as 'h_ew';

	  begin
		declare continue handler for sqlwarning
		  select 'Inner (good)' as 'h_ew';

		insert into t3 values (123456789012);
	  end;
	  delete from t3;
	  insert into t3 values (1);
	end`)
	tk.MustExec("call h_ew")
	require.Equal(t, len(tk.Res), 1)
	tk.Res[0].Check(testkit.Rows("Inner (good)"))
	tk.ClearProcedureRes()

	// more local to where the condition occurs takes precedence.
	tk.MustExec(`CREATE PROCEDURE p2() BEGIN      DECLARE CONTINUE HANDLER FOR 1146       SELECT 'SQLSTATE handler was activated' AS msg;   BEGIN      DECLARE CONTINUE HANDLER FOR SQLEXCEPTION
	 SELECT 'SQLEXCEPTION handler was activated' AS msg;      select * from  test.t;    END; END;`)
	tk.MustExec("call p2")
	tk.Res[0].Check(testkit.Rows("SQLEXCEPTION handler was activated"))
	tk.ClearProcedureRes()

	// not found
	tk.MustExec(`create procedure variableInSelect(info int)
	begin
	declare id1,done int default 0;
	declare s1  CURSOR for select id from user where id < info;
	declare continue handler for not found set done = 0;
	set done = 1;
	open s1;
	while done do
	fetch s1 into id1;
	set info = 90;
	select  id1;
	end while;
	end;`)
	tk.MustExec("call variableInSelect(10)")
	require.Equal(t, len(tk.Res), 11)
	tk.ClearProcedureRes()

	tk.MustExec(`create procedure variableInSelect1(info int)
	begin
	declare id1,done int default 0;
	declare s1  CURSOR for select id from user where id < info;
	declare continue handler for SQLSTATE '02000' set done = 0;
	set done = 1;
	open s1;
	while done do
	fetch s1 into id1;
	set info = 90;
	select  id1;
	end while;
	end;`)
	tk.MustExec("call variableInSelect1(10)")
	require.Equal(t, len(tk.Res), 11)
	tk.ClearProcedureRes()

	tk.MustExec("create table t1 (id int)")
	tk.MustExec("insert into t1 value(1)")
	//error control warning as error
	tk.MustExec(`CREATE PROCEDURE warnings1() BEGIN      DECLARE CONTINUE HANDLER FOR 1292       SELECT 'outer handler was activated' AS msg;
	BEGIN      DECLARE CONTINUE HANDLER FOR 1649      SELECT 'inner handler was activated' AS msg;
	select format(1,02,'des') from t1  where id = '1s';    END; END;`)
	tk.MustExec("call warnings1")
	tk.Res[0].Check(testkit.Rows("1.00"))
	tk.Res[1].Check(testkit.Rows("inner handler was activated"))
	tk.ClearProcedureRes()

	tk.MustExec(`CREATE PROCEDURE warnings2() BEGIN      DECLARE CONTINUE HANDLER FOR 1649       SELECT 'outer handler was activated' AS msg;
		BEGIN      DECLARE CONTINUE HANDLER FOR 1292      SELECT 'inner handler was activated' AS msg;
		select format(1,02,'des') from t1  where id = '1s';    END; END;`)
	tk.MustExec("call warnings2")
	tk.Res[0].Check(testkit.Rows("1.00"))
	tk.Res[1].Check(testkit.Rows("outer handler was activated"))
	tk.ClearProcedureRes()

	tk.MustExec(`CREATE PROCEDURE warnings3() BEGIN      DECLARE CONTINUE HANDLER FOR 1649       SELECT 'outer handler was activated' AS msg;
		BEGIN      DECLARE CONTINUE HANDLER FOR sqlwarning      SELECT 'inner handler was activated' AS msg;
		select format(1,02,'des') from t1  where id = '1s';    END; END;`)
	tk.MustExec("call warnings3")
	tk.Res[0].Check(testkit.Rows("1.00"))
	tk.Res[1].Check(testkit.Rows("inner handler was activated"))
	tk.ClearProcedureRes()
	tk.MustExec(`CREATE PROCEDURE warnings4() BEGIN      DECLARE CONTINUE HANDLER FOR sqlWarning       SELECT 'outer handler was activated' AS msg;
		BEGIN      DECLARE CONTINUE HANDLER FOR 1292      SELECT 'inner handler was activated' AS msg;
		select format(1,02,'des') from t1  where id = '1s';    END; END;`)
	tk.MustExec("call warnings4")
	tk.Res[0].Check(testkit.Rows("1.00"))
	tk.Res[1].Check(testkit.Rows("outer handler was activated"))
	tk.ClearProcedureRes()
	tk.MustExec(`create procedure select_error16() begin declare continue handler for SQLEXCEPTION begin select * from user1; end;select * from user1;select 1;end`)
	tk.MustGetErrCode("call select_error16", 1146)
	tk.MustExec(`drop procedure select_error16`)
	tk.MustExec(`create procedure select_error16() begin  declare continue handler for SQLEXCEPTION select 1; begin declare continue handler for SQLEXCEPTION begin select * from user1; end;select * from user1;select 1;end;end;`)
	tk.MustExec("call select_error16")
	require.Equal(t, 2, len(tk.Res))
	tk.Res[0].Check(testkit.Rows("1"))
	tk.Res[1].Check(testkit.Rows("1"))
	tk.ClearProcedureRes()

	tk.MustExec(`create procedure select_error17() begin  declare continue handler for SQLEXCEPTION select 1; begin declare exit handler for SQLEXCEPTION begin select * from user1; end;select * from user1;select 3;end;select 2;end;`)
	tk.MustExec("call select_error17")
	require.Equal(t, 2, len(tk.Res))
	tk.Res[0].Check(testkit.Rows("1"))
	tk.Res[1].Check(testkit.Rows("2"))
	tk.ClearProcedureRes()

	tk.MustExec(`create procedure select_error18() begin  declare continue handler for SQLEXCEPTION select 1; begin declare continue handler for SQLEXCEPTION begin select * from user1; end;select * from user1;select 3;end;select 2;end;`)
	tk.MustExec("call select_error18")
	require.Equal(t, 3, len(tk.Res))
	tk.Res[0].Check(testkit.Rows("1"))
	tk.Res[1].Check(testkit.Rows("3"))
	tk.Res[2].Check(testkit.Rows("2"))
	tk.ClearProcedureRes()
}

func TestCreateLabel(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.InProcedure()
	tk.MustExec("use test")
	// error
	tk.MustGetErrCode("create procedure t1() t1: begin end t2;", 1310)
	tk.MustGetErrCode("create procedure t1() t1: begin t1: begin end; end t1;", 1309)
	tk.MustGetErrCode("create procedure t1() t1: if id = 1 then  begin end; end if t1;", 1064)
	// label begin end
	tk.MustExec("create procedure t1() t1: begin end")
	tk.MustExec("create procedure t2() t1: begin end t1")
	tk.MustExec("create procedure t3() t1: begin t2: begin end; end t1")
	tk.MustExec("create procedure t4() t1: begin t2: begin end t2; end t1")
	// label loop
	tk.MustExec("create procedure t5(id int) t1: while id < 1 do   select 1 ; end while t1")
	tk.MustExec("create procedure t6(id int) t1: while id < 1 do   select 1 ; end while ")
	tk.MustExec("create procedure t7(id int) t1: repeat select 1 ; UNTIL id < 1   end repeat t1")
	tk.MustExec("create procedure t8(id int) t1: repeat select 1 ; UNTIL id < 1   end repeat")
	tk.MustExec("create procedure t9(id int) t1: repeat t2: while id < 1 do   select 1 ; end while; UNTIL id < 1   end repeat")

	//label loop begin
	tk.MustExec("create procedure t10(id int) begin t1: while id < 1 do   select 1 ; end while t1; end")
	tk.MustExec("create procedure t11(id int) t2: begin t1: while id < 1 do   select 1 ; end while t1; end")
	tk.MustExec("create procedure t12(id int) t1: repeat t2: begin end ; UNTIL id < 1   end repeat t1")

	//label mix test
	tk.MustExec("create procedure t17(id int) begin t1: while id < 1 do   t2: begin select 1 ; t3:repeat select 1 ; UNTIL id < 1   end repeat; end; end while t1; end")
	tk.MustExec("create procedure t18(id int) t5: begin t1: while id < 1 do   t2: begin select 1 ; t3:repeat select 1 ; UNTIL id < 1   end repeat; end; end while t1; end")
	tk.MustExec("create procedure t19(id int) t5: begin declare continue handler for 1111 t1: while id < 1 do   t2: begin select 1 ; t3:repeat select 1 ; UNTIL id < 1   end repeat; end; end while t1; begin select 2; end; end")

	// label handler
	tk.MustExec("create procedure t13(id int) begin declare continue handler for 1111 t1:begin select 1;end; t1: while id < 1 do   select 1 ; end while t1; end")
	tk.MustExec("create procedure t14(id int) begin declare continue handler for 1111 t1:begin t2: begin select 1;end;end; t1: while id < 1 do   select 1 ; end while t1; end")
	tk.MustExec("create procedure t15(id int) begin declare continue handler for 1111 t1:while id >1 do t2: begin select 1;end;end while; t1: while id < 1 do   select 1 ; end while t1; end")
	tk.MustExec("create procedure t16(id int) begin declare continue handler for 1111 while id >1 do t2: begin select 1;end;end while; t1: while id < 1 do   select 1 ; end while t1; end")
}

func TestLeaveIterateLabel(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.InProcedure()
	tk.MustExec("use test")
	tk.MustExec("create procedure t1(id int) t1: while id > 1 do  leave t1 ; end while t1")
	tk.MustExec("call t1(2)")
	tk.MustGetErrCode("create procedure t2(id int) t1:begin if id < 10 then leave t1; else ITERATE t1; end if; end t1", 1308)
	tk.MustGetErrCode("create procedure t2(id int) t1:begin declare continue handler for 1111 while id >1 do leave t1; begin select 1;end;end while;  if id < 10 then leave t1;end if; end t1", 1308)
	tk.MustGetErrCode("create procedure t2(id int) t1:while id < 1 do begin declare continue handler for 1111 while id >1 do ITERATE t1; begin select 1;end;end while;  if id < 10 then leave t1; end if;end; end while", 1308)
	tk.MustExec("create procedure t2(id int) t1: begin select id; leave t1; set id = id + 1; select id; end")
	tk.MustExec("call t2(2)")
	require.Equal(t, 1, len(tk.Res))
	tk.Res[0].Check(testkit.Rows("2"))
	tk.ClearProcedureRes()

	tk.MustExec("create procedure t3(id int) t1: begin declare id2 int default 2; t2:while id2 < 5 do select id2; if id < 10 then set id = 10; leave t1; end if;set id2 = id2 + 1; end while;end")
	tk.MustExec("call t3(2)")
	require.Equal(t, 1, len(tk.Res))
	tk.Res[0].Check(testkit.Rows("2"))
	tk.ClearProcedureRes()

	tk.MustExec("create procedure t4(id int) t1: begin declare id2 int default 2; t2:while id2 < 5 do select id2; if id < 10 then set id = 10; leave t2; end if;set id2 = id2 + 1; end while;select id;end")
	tk.MustExec("call t4(2)")
	require.Equal(t, 2, len(tk.Res))
	tk.Res[0].Check(testkit.Rows("2"))
	tk.Res[1].Check(testkit.Rows("10"))
	tk.ClearProcedureRes()

	tk.MustExec("create procedure t5(id int) t1: begin declare id2 int default 2; t2:while id2 < 5 do select id2; if id < 10 then set id = 10; ITERATE t2; end if;set id2 = id2 + 1; end while;end")
	tk.MustExec("call t5(2)")
	require.Equal(t, 4, len(tk.Res))
	tk.Res[0].Check(testkit.Rows("2"))
	tk.Res[1].Check(testkit.Rows("2"))
	tk.Res[2].Check(testkit.Rows("3"))
	tk.Res[3].Check(testkit.Rows("4"))
	tk.ClearProcedureRes()

	tk.MustExec("create procedure t6(id int) t1: begin declare id2 int default 2; t2:while id2 < 5 do select id2; set id2 = id2 + 1;if id < 10 then set id = 10; ITERATE t2; end if; end while;end")
	tk.MustExec("call t6(2)")
	require.Equal(t, 3, len(tk.Res))
	tk.Res[0].Check(testkit.Rows("2"))
	tk.Res[1].Check(testkit.Rows("3"))
	tk.Res[2].Check(testkit.Rows("4"))
	tk.ClearProcedureRes()

	tk.MustExec("create procedure t7(id int) t1: begin declare id2 int default 2; t2:repeat select id2; set id2 = id2 + 1;if id < 10 then set id = 10; ITERATE t2; end if;until id2 > 5 end repeat;end")
	tk.MustExec("call t7(2)")
	require.Equal(t, 4, len(tk.Res))
	tk.Res[0].Check(testkit.Rows("2"))
	tk.Res[1].Check(testkit.Rows("3"))
	tk.Res[2].Check(testkit.Rows("4"))
	tk.Res[3].Check(testkit.Rows("5"))
	tk.ClearProcedureRes()

	tk.MustExec("create procedure t8(id int) t1: begin declare id2 int default 2; t2:repeat select id2; set id2 = id2 + 1;if id < 10 then set id = 10; leave t2; end if;until id2 > 5 end repeat; select 3;end")
	tk.MustExec("call t8(2)")
	require.Equal(t, 2, len(tk.Res))
	tk.Res[0].Check(testkit.Rows("2"))
	tk.Res[1].Check(testkit.Rows("3"))
	tk.ClearProcedureRes()

	tk.MustExec("create procedure t9(id int) t1: begin declare id2 int default 2; t2:repeat select id2; set id2 = id2 + 1;if id < 10 then set id = 10; leave t1; end if;until id2 > 5 end repeat; select 3;end")
	tk.MustExec("call t9(2)")
	require.Equal(t, 1, len(tk.Res))
	tk.Res[0].Check(testkit.Rows("2"))
	tk.ClearProcedureRes()

	tk.MustExec(`create procedure t10(id int)
	t1: begin declare id2 int default 2;
	t2:repeat select id2; set id2 = id2 + 1;
	t3: begin declare id4 int default 3;select id4; if id < 10 then set id = 10; leave t1; end if; end;until id2 > 5 end repeat;
	select 3;end`)
	tk.MustExec("call t10(2)")
	require.Equal(t, 2, len(tk.Res))
	tk.Res[0].Check(testkit.Rows("2"))
	tk.Res[1].Check(testkit.Rows("3"))
	tk.ClearProcedureRes()

	tk.MustExec(`create procedure t11(id int)
	t1: begin declare id2 int default 2;
	t2:repeat select id2; set id2 = id2 + 1;
	t3: begin declare id4 int default 3; set id4 = id4 + 1;select id4;
	if id < 10 then set id = 10; leave t3; end if;
	end;
	until id2 > 5 end repeat;
	select 3;end`)
	tk.MustExec("call t11(2)")
	require.Equal(t, 9, len(tk.Res))
	tk.Res[0].Check(testkit.Rows("2"))
	tk.Res[1].Check(testkit.Rows("4"))
	tk.Res[2].Check(testkit.Rows("3"))
	tk.Res[3].Check(testkit.Rows("4"))
	tk.Res[4].Check(testkit.Rows("4"))
	tk.Res[5].Check(testkit.Rows("4"))
	tk.Res[6].Check(testkit.Rows("5"))
	tk.Res[7].Check(testkit.Rows("4"))
	tk.Res[8].Check(testkit.Rows("3"))
	tk.ClearProcedureRes()

	tk.MustExec(`create procedure t12(id int)
	t1: begin declare id2 int default 2;
	t2:repeat select id2; set id2 = id2 + 1;
	t3: begin declare id4 int default 3;select id4; if id < 10 then set id = 10; leave t2; end if; end; until id2 > 5 end repeat;
	select 3;end`)
	tk.MustExec("call t12(2)")
	require.Equal(t, 3, len(tk.Res))
	tk.Res[0].Check(testkit.Rows("2"))
	tk.Res[1].Check(testkit.Rows("3"))
	tk.Res[2].Check(testkit.Rows("3"))
	tk.ClearProcedureRes()

}

func TestJumpError(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.InProcedure()
	tk.MustExec("use test")
	tk.MustExec(`create procedure tt2()
	begin
	declare id int default 1;
	declare continue handler for SQLEXCEPTION select 3;
	if id in (select id from ttt1 )
	then select 1;
	else select 2; end if;
	select 4;
	end;`)
	tk.MustExec("call tt2")
	require.Equal(t, 2, len(tk.Res))
	tk.Res[0].Check(testkit.Rows("3"))
	tk.Res[1].Check(testkit.Rows("4"))
	tk.ClearProcedureRes()

	tk.MustExec(`create procedure tt3()
	begin
	declare id int default 1;
	declare continue handler for SQLEXCEPTION select 3;
	while id in (select id from ttt1 ) do
	select 1;
	select 2; end while;
	select 4;
	end;`)
	tk.MustExec("call tt3")
	require.Equal(t, 2, len(tk.Res))
	tk.Res[0].Check(testkit.Rows("3"))
	tk.Res[1].Check(testkit.Rows("4"))
	tk.ClearProcedureRes()
	tk.MustExec(`create procedure tt4()
	begin
	declare id int default 1;
	declare continue handler for SQLEXCEPTION select 3;
	if id in (select id from ttt1 )
	then select 1;
	elseif id in (select id from ttt1 ) then select 5;
	else select 2; end if;
	select 4;
	end;`)
	tk.MustExec("call tt4")
	require.Equal(t, 2, len(tk.Res))
	tk.Res[0].Check(testkit.Rows("3"))
	tk.Res[1].Check(testkit.Rows("4"))
	tk.ClearProcedureRes()

	tk.MustExec(`create procedure tt5()
	begin
	declare id int default 1;
	declare continue handler for SQLEXCEPTION select 3;
	if id in (select id from ttt1 )
	then select 1;
	elseif 1 then select 5;
	else select 2; end if;
	select 4;
	end;`)
	tk.MustExec("call tt5")
	require.Equal(t, 2, len(tk.Res))
	tk.Res[0].Check(testkit.Rows("3"))
	tk.Res[1].Check(testkit.Rows("4"))
	tk.ClearProcedureRes()

	tk.MustExec(`create procedure tt6()
	begin
	declare id int default 1;
	declare continue handler for SQLEXCEPTION select 3;
	begin
	declare continue handler for SQLEXCEPTION select 5;
	while id in (select id from ttt1 ) do
	select 1;
	select 2; end while;
	select 4;
	end;
	end;`)
	tk.MustExec("call tt6")
	require.Equal(t, 2, len(tk.Res))
	tk.Res[0].Check(testkit.Rows("5"))
	tk.Res[1].Check(testkit.Rows("4"))
	tk.ClearProcedureRes()

	tk.MustExec(`create procedure tt7()
	begin
	declare id int default 1;
	declare continue handler for SQLEXCEPTION select 3;
	begin
	declare continue handler for SQLEXCEPTION begin select 5; while id in (select id from ttt1 ) do
	select 1;
	select 2; end while; end;
	while id in (select id from ttt1 ) do
	select 1;
	select 2; end while;
	select 4;
	end;
	end;`)
	tk.MustExec("call tt7")
	require.Equal(t, 3, len(tk.Res))
	tk.Res[0].Check(testkit.Rows("5"))
	tk.Res[1].Check(testkit.Rows("3"))
	tk.Res[2].Check(testkit.Rows("4"))
	tk.ClearProcedureRes()
}

func TestCaseWhenThen(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.InProcedure()
	tk.MustExec("use test")
	tk.MustExec(`create procedure t1(id int) begin
	case id when 1 then select 1;
	when 2 then select 2;
	when 3 then select 3;
	end case;end`)
	tk.MustExec("call t1(1)")
	require.Equal(t, len(tk.Res), 1)
	tk.Res[0].Check(testkit.Rows("1"))
	tk.ClearProcedureRes()
	tk.MustExec("call t1(2)")
	require.Equal(t, len(tk.Res), 1)
	tk.Res[0].Check(testkit.Rows("2"))
	tk.ClearProcedureRes()
	tk.MustExec("call t1(3)")
	require.Equal(t, len(tk.Res), 1)
	tk.Res[0].Check(testkit.Rows("3"))
	tk.ClearProcedureRes()
	tk.ExecToErr("call t1(4)", 1339)
	require.Equal(t, len(tk.Res), 0)
	tk.ClearProcedureRes()

	tk.MustExec(`create procedure t2(id int) begin
	case id when 1 then select 1;
	when 2 then select 2;
	when 3 then select 3;
	else select 4;
	end case;end`)
	tk.MustExec("call t2(-1)")
	require.Equal(t, len(tk.Res), 1)
	tk.Res[0].Check(testkit.Rows("4"))
	tk.ClearProcedureRes()

	tk.MustExec(`create procedure t3(id int) begin
	case when id = 1 then select 1;
	when id = 2 then select 2;
	when id = 3 then select 3;
	else select 4;
	end case;end`)
	tk.MustExec("call t3(-1)")
	require.Equal(t, len(tk.Res), 1)
	tk.Res[0].Check(testkit.Rows("4"))
	tk.ClearProcedureRes()

	tk.MustExec("call t3(1)")
	require.Equal(t, len(tk.Res), 1)
	tk.Res[0].Check(testkit.Rows("1"))
	tk.ClearProcedureRes()

	tk.MustExec("call t3(2)")
	require.Equal(t, len(tk.Res), 1)
	tk.Res[0].Check(testkit.Rows("2"))
	tk.ClearProcedureRes()

	tk.MustExec("call t3(3)")
	require.Equal(t, len(tk.Res), 1)
	tk.Res[0].Check(testkit.Rows("3"))
	tk.ClearProcedureRes()

	tk.MustExec(`create procedure t4(id int) begin
	case when id = 1 then select 1;
	when id = 2 then select 2;
	when id = 3 then select 3;
	end case;end`)
	tk.ExecToErr("call t4(-1)", 1339)
	tk.ClearProcedureRes()

	tk.MustExec(`create procedure t5(id int) begin
	declare continue HANDLER for SQLEXCEPTION select 4;
	case when id = 1 then select 1;
	when id = 2 then select 2;
	when id = 3 then select 3;
	end case;
	select 5;
	end`)
	tk.MustExec("call t5(-1)")
	require.Equal(t, len(tk.Res), 2)
	tk.Res[0].Check(testkit.Rows("4"))
	tk.Res[1].Check(testkit.Rows("5"))
	tk.ClearProcedureRes()

	tk.MustExec("call t5(1)")
	require.Equal(t, len(tk.Res), 2)
	tk.Res[0].Check(testkit.Rows("1"))
	tk.Res[1].Check(testkit.Rows("5"))
	tk.ClearProcedureRes()

	tk.MustExec(`create procedure t6(id int) begin
	declare continue HANDLER for 1339 select 4;
	case when id = 1 then select 1;
	when id = 2 then select 2;
	when id = 3 then select 3;
	end case;
	select 5;
	end`)
	tk.MustExec("call t6(-1)")
	require.Equal(t, len(tk.Res), 2)
	tk.Res[0].Check(testkit.Rows("4"))
	tk.Res[1].Check(testkit.Rows("5"))
	tk.ClearProcedureRes()

	tk.MustExec("call t6(1)")
	require.Equal(t, len(tk.Res), 2)
	tk.Res[0].Check(testkit.Rows("1"))
	tk.Res[1].Check(testkit.Rows("5"))
	tk.ClearProcedureRes()

	tk.MustExec(`create procedure t7(id int) begin
	declare exit HANDLER for 1339 select 4;
	case when id = 1 then select 1;
	when id = 2 then select 2;
	when id = 3 then select 3;
	end case;
	select 5;
	end`)
	tk.MustExec("call t7(-1)")
	require.Equal(t, len(tk.Res), 1)
	tk.Res[0].Check(testkit.Rows("4"))
	tk.ClearProcedureRes()

	tk.MustExec(`create procedure t8(id int) begin
	declare exit HANDLER for 1339 select 4;
	case when id = 1 then select 1;
	select 10;
	when id = 2 then select 2;
	select 11;
	when id = 3 then select 3;
	select 12;
	end case;
	select 5;
	end`)

	tk.MustExec("call t8(-1)")
	require.Equal(t, len(tk.Res), 1)
	tk.Res[0].Check(testkit.Rows("4"))
	tk.ClearProcedureRes()

	tk.MustExec("call t8(1)")
	require.Equal(t, len(tk.Res), 3)
	tk.Res[0].Check(testkit.Rows("1"))
	tk.Res[1].Check(testkit.Rows("10"))
	tk.Res[2].Check(testkit.Rows("5"))
	tk.ClearProcedureRes()

	tk.MustExec("call t8(2)")
	require.Equal(t, len(tk.Res), 3)
	tk.Res[0].Check(testkit.Rows("2"))
	tk.Res[1].Check(testkit.Rows("11"))
	tk.Res[2].Check(testkit.Rows("5"))
	tk.ClearProcedureRes()

	// nested tests
	tk.MustExec(`create procedure t9(id int) begin
	declare exit HANDLER for 1339 select 4;
	case when id = 1 then select 1;
	begin
	declare id int default 123;
	select id;
	end;
	when id = 2 then select 2;
	while id < 10 do
	select id;
	set id = id + 1;
	end while;
	when id = 3 then select 3;
	select 12;
	else
	if id > 10 then select 10;
	elseif id = 10 then select 111;
	end if;
	end case;
	select 5;
	end`)

	tk.MustExec("call t9(1)")
	require.Equal(t, len(tk.Res), 3)
	tk.Res[0].Check(testkit.Rows("1"))
	tk.Res[1].Check(testkit.Rows("123"))
	tk.Res[2].Check(testkit.Rows("5"))
	tk.ClearProcedureRes()

	tk.MustExec("call t9(2)")
	require.Equal(t, len(tk.Res), 10)
	tk.Res[0].Check(testkit.Rows("2"))
	tk.Res[1].Check(testkit.Rows("2"))
	tk.Res[2].Check(testkit.Rows("3"))
	tk.Res[3].Check(testkit.Rows("4"))
	tk.Res[4].Check(testkit.Rows("5"))
	tk.Res[5].Check(testkit.Rows("6"))
	tk.Res[6].Check(testkit.Rows("7"))
	tk.Res[7].Check(testkit.Rows("8"))
	tk.Res[8].Check(testkit.Rows("9"))
	tk.Res[9].Check(testkit.Rows("5"))
	tk.ClearProcedureRes()

	tk.MustExec("call t9(3)")
	require.Equal(t, len(tk.Res), 3)
	tk.Res[0].Check(testkit.Rows("3"))
	tk.Res[1].Check(testkit.Rows("12"))
	tk.Res[2].Check(testkit.Rows("5"))
	tk.ClearProcedureRes()

	tk.MustExec("call t9(4)")
	require.Equal(t, len(tk.Res), 1)
	tk.Res[0].Check(testkit.Rows("5"))
	tk.ClearProcedureRes()

	tk.MustExec("call t9(10)")
	require.Equal(t, len(tk.Res), 2)
	tk.Res[0].Check(testkit.Rows("111"))
	tk.Res[1].Check(testkit.Rows("5"))
	tk.ClearProcedureRes()

	tk.MustExec("call t9(15)")
	require.Equal(t, len(tk.Res), 2)
	tk.Res[0].Check(testkit.Rows("10"))
	tk.Res[1].Check(testkit.Rows("5"))
	tk.ClearProcedureRes()

}

func TestProcedureComment(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.InProcedure()
	tk.MustExec("use test")
	tk.MustExec("set timestamp = 1675499582")
	tk.MustExec("create procedure t1() comment '1' begin select 1; end;")
	tk.MustExec("update mysql.routines set created ='2023-02-09 19:10:30', last_altered = '2023-02-09 19:10:30'")
	tk.MustQuery("show create procedure t1").Check(testkit.Rows("t1 ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION  CREATE PROCEDURE `t1`()\nCOMMENT '1' \nbegin select 1; end utf8mb4 utf8mb4_bin utf8mb4_bin"))
	tk.MustQuery("show procedure status").Check(testkit.Rows("test t1 PROCEDURE  2023-02-09 19:10:30.000000 2023-02-09 19:10:30.000000 DEFINER 1 utf8mb4 utf8mb4_bin utf8mb4_bin"))
	tk.MustExec("alter procedure t1 comment '111' ;")
	tk.MustExec("update mysql.routines set created ='2023-02-09 19:10:30', last_altered = '2023-02-09 19:10:30'")
	tk.MustQuery("show create procedure t1").Check(testkit.Rows("t1 ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION  CREATE PROCEDURE `t1`()\nCOMMENT '111' \nbegin select 1; end utf8mb4 utf8mb4_bin utf8mb4_bin"))
	tk.MustQuery("show procedure status").Check(testkit.Rows("test t1 PROCEDURE  2023-02-09 19:10:30.000000 2023-02-09 19:10:30.000000 DEFINER 111 utf8mb4 utf8mb4_bin utf8mb4_bin"))
	tk.MustExec("alter procedure t1 comment '112' comment '113' comment '114';")
	tk.MustExec("update mysql.routines set created ='2023-02-09 19:10:30', last_altered = '2023-02-09 19:10:30'")
	tk.MustQuery("show create procedure t1").Check(testkit.Rows("t1 ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION  CREATE PROCEDURE `t1`()\nCOMMENT '114' \nbegin select 1; end utf8mb4 utf8mb4_bin utf8mb4_bin"))
	tk.MustQuery("show procedure status").Check(testkit.Rows("test t1 PROCEDURE  2023-02-09 19:10:30.000000 2023-02-09 19:10:30.000000 DEFINER 114 utf8mb4 utf8mb4_bin utf8mb4_bin"))
	tk.MustQuery("select routine_comment from information_schema.routines").Check(testkit.Rows("114"))
}

func TestProcedureTranscation(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.InProcedure()
	tk.MustExec("use test")
	tk.MustExec("create table t1 (id int key,id2 int);")
	tk.MustExec("begin")
	tk.MustExec("insert into t1 value(1,1)")
	tk.MustExec("create procedure t1(id int) begin insert into t1 values(id,id); insert into t1 values(id+1,id+1);select * from t1;end;")
	tk.MustQuery("select * from t1").Check(testkit.Rows("1 1"))
	tk.MustExec("rollback")
	//should implicit commit
	tk.MustQuery("select * from t1").Check(testkit.Rows("1 1"))
	tk.MustExec("begin")
	tk.MustExec("call t1(2)")
	require.Equal(t, len(tk.Res), 1)
	tk.Res[0].Check(testkit.Rows("1 1", "2 2", "3 3"))
	tk.ClearProcedureRes()
	tk.MustQuery("select * from t1").Check(testkit.Rows("1 1", "2 2", "3 3"))
	// can rollback
	tk.MustExec("rollback")
	tk.MustQuery("select * from t1").Check(testkit.Rows("1 1"))
	//procedure can contains commit
	tk.MustExec("create procedure t2(id int) begin insert into t1 values(id,id); insert into t1 values(id+1,id+1);select * from t1;commit;end;")
	tk.MustExec("begin")
	tk.MustExec("call t2(2)")
	require.Equal(t, len(tk.Res), 1)
	tk.Res[0].Check(testkit.Rows("1 1", "2 2", "3 3"))
	tk.ClearProcedureRes()
	tk.MustQuery("select * from t1").Check(testkit.Rows("1 1", "2 2", "3 3"))
	// can not rollback
	tk.MustExec("rollback")
	tk.MustQuery("select * from t1").Check(testkit.Rows("1 1", "2 2", "3 3"))
	tk.MustExec("truncate table t1")
	tk.MustExec("begin")
	tk.MustExec("insert into t1 value(1,1)")
	tk.MustExec("alter procedure t1 comment \"1\"")
	tk.MustExec("rollback")
	tk.MustQuery("select * from t1").Check(testkit.Rows("1 1"))
	tk.MustExec("begin")
	tk.MustExec("insert into t1 value(2,2)")
	tk.MustExec("drop procedure t1 ")
	tk.MustExec("rollback")
	tk.MustQuery("select * from t1").Check(testkit.Rows("1 1", "2 2"))
	tk.MustExec("truncate table t1")
	// Supports internal existence transactions
	tk.MustExec("create procedure t3(id int) begin insert into t1 values(id,id); rollback;start transaction;insert into t1 values(id+1,id+1);insert into t1 values(id+2,id+2);select * from t1;commit;end;")
	tk.MustExec("set autocommit = 0;")
	tk.MustExec("insert into t1 value(1,1)")
	tk.MustExec("call t3(2)")
	require.Equal(t, len(tk.Res), 1)
	tk.Res[0].Check(testkit.Rows("3 3", "4 4"))
	tk.ClearProcedureRes()
	tk.MustQuery("select * from t1").Check(testkit.Rows("3 3", "4 4"))
	tk.MustExec("truncate table t1")
	tk.MustExec("create procedure t4(id int) begin declare exit HANDLER for SQLEXCEPTION rollback; start transaction;insert into t1 values(id+1,id+1);" +
		"insert into t2 values(id+1,id+1);insert into t1 values(id+2,id+2);select * from t1;commit;end;")
	tk.MustExec("call t4(1)")
	require.Equal(t, len(tk.Res), 0)
	tk.MustQuery("select * from t1").Check(testkit.Rows())
	tk.MustExec("create procedure t5(id int) begin start transaction;insert into t1 values(id,id);" +
		"insert into t1 values(id+1,id+1);if id in (select id from t1) then commit; else rollback; end if;end;")
	tk.MustExec("call t5(1)")
	tk.MustExec("rollback")
	tk.MustQuery("select * from t1").Check(testkit.Rows("1 1", "2 2"))
}

func TestProcedureSummary(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.InProcedure()
	tk.MustExec("use test")
	tk.MustExec("CREATE TABLE `user` ( `id` int(11) NOT NULL, `username` VARCHAR(30) DEFAULT NULL, `password` VARCHAR(30) DEFAULT NULL, " +
		"`age` int(11) NOT NULL, `sex` int(11) NOT NULL, PRIMARY KEY (`id`), KEY `username` (`username`) ) ENGINE=InnoDB;")
	tk.MustExec("CREATE TABLE `user_score` ( `id` int(11) NOT NULL, `subject` int(11) NOT NULL, `user_id` int(11) NOT NULL, " +
		"`score` int(11) NOT NULL, PRIMARY KEY (`id`) ) ENGINE=InnoDB;")
	tk.MustExec("CREATE TABLE `user_address` ( `id` int(11) NOT NULL, `user_id` int(11) NOT NULL, `address` VARCHAR(30) DEFAULT NULL, " +
		"PRIMARY KEY (`id`), KEY `address` (`address`) ) ENGINE=InnoDB;")
	tk.MustExec(`create procedure insert_data(i int, s_i int)  begin
		declare x int default 0;
		while x < i do
		insert into user values(x, CONCAT("username-", x),CONCAT("password-", x),FLOOR( 15 ),Mod(x,2));
		insert into user_score values(s_i, 1, x, FLOOR( 40 + x * 100));
		set s_i=s_i+1;
		insert into user_score values(s_i, 2, x, FLOOR( 40 + x * 100));
		set s_i=s_i+1;
		insert into user_score values(s_i, 3, x, FLOOR( 40 + x * 100));
		set s_i=s_i+1;
		insert into user_score values(s_i, 4, x, FLOOR( 40 + x * 100));
		set s_i=s_i+1;
		insert into user_score values(s_i, 5, x, FLOOR( 40 + x * 100));
		set s_i=s_i+1;
		insert into user_address values(x, x, CONCAT("useraddress-", x));
		set x=x+1;
		end while;  end;`)
	tk.MustExec("call insert_data(10,0)")
	tk.MustQuery("select count(*)  from INFORMATION_SCHEMA.CLUSTER_STATEMENTS_SUMMARY where QUERY_SAMPLE_TEXT=\"\";").Check(testkit.Rows("0"))
}

func TestProcedurePrepareExec(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.InProcedure()
	tk.MustExec("use test")
	tk.MustExec("create table t1 (id int key)")
	tk.MustExec("create procedure tt1() begin declare id int default 1; insert into t1 values(id); prepare stmt from 'select * from t1';set id = id +1;insert into t1 values(id); select * from t1;end;")
	tk.MustExec("call tt1")
	require.Equal(t, 1, len(tk.Res))
	tk.Res[0].Check(testkit.Rows("1", "2"))
	tk.ClearProcedureRes()
	tk.MustQuery("execute stmt").Check(testkit.Rows("1", "2"))
	// id is table column name
	tk.MustExec("create procedure tt2() begin declare id int default 1; prepare stmt from 'select id from t1'; end;")
	tk.MustExec("call tt2")
	// can use outof procedure.
	tk.MustQuery("execute stmt").Check(testkit.Rows("1", "2"))
	// variable is invalid in prepare
	tk.MustExec("create procedure tt3() begin declare id1 int default 1; prepare stmt from 'select id1 from t1'; end;")
	tk.MustGetErrCode("call tt3", 1054)
	// inoutput is invalid in prepare
	tk.MustExec("create procedure tt4(id1 int) begin prepare stmt from 'select id1 from t1'; end;")
	tk.MustGetErrCode("call tt4(@a)", 1054)

	tk.MustExec("create procedure tt5(out id1 int) begin prepare stmt from 'select id1 from t1'; end;")
	tk.MustGetErrCode("call tt5(@a)", 1054)

	tk.MustExec("create procedure tt6(out id1 int) begin prepare stmt from 'select id1 from t1'; end;")
	tk.MustGetErrCode("call tt6(@a)", 1054)

	tk.MustExec("create procedure tt7(inout id1 int) begin prepare stmt from 'select id1 from t1'; end;")
	tk.MustGetErrCode("call tt7(@a)", 1054)
	tk.ClearProcedureRes()
	// support prepare
	tk.MustExec(`create procedure tt8(inout id1 int) begin prepare stmt from 'select * from t1 where id = ?';
	set @a = 1; execute stmt using @a; drop prepare stmt;
	end;`)
	tk.MustExec("call tt8(@a)")
	require.Equal(t, 1, len(tk.Res))
	tk.Res[0].Check(testkit.Rows("1"))
	tk.ClearProcedureRes()

	// execute stmt not support local variable
	tk.MustGetErrCode(`create procedure tt8(inout id1 int) begin prepare stmt from 'select * from t1 where id = ?';
	set @a = 1; execute stmt using id1; drop prepare stmt;
	end;`, 1064)

	//Tests that procedure flags are restored
	tk.MustExec(`create procedure tt9( id1 int) begin prepare stmt from 'select * from t1 where id = ?';
	set @a = 1; execute stmt using @a; drop prepare stmt;select id1;select * from t1 where id = id1;
	end;`)
	tk.MustExec("call tt9(1)")
	require.Equal(t, 3, len(tk.Res))
	tk.Res[0].Check(testkit.Rows("1"))
	tk.Res[1].Check(testkit.Rows("1"))
	tk.Res[2].Check(testkit.Rows("1"))
	tk.ClearProcedureRes()

	// prepare support alter
	tk.MustExec(`create procedure tt10( id1 int) begin prepare stmt from 'alter table t1 add id2 int';
	execute stmt; drop prepare stmt;select id1;select * from t1 where id = id1;
	end;`)
	tk.MustExec("call tt10(1)")
	require.Equal(t, 2, len(tk.Res))
	tk.Res[0].Check(testkit.Rows("1"))
	tk.Res[1].Check(testkit.Rows("1 <nil>"))
	tk.ClearProcedureRes()
}

func TestProcedureDefiner(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk2 := testkit.NewTestKit(t, store)
	tk.InProcedure()
	tk.MustExec("use test")
	tk2.MustExec("create user test;")
	tk2.MustExec("grant create routine on *.* to test")
	require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "test", Hostname: "127.0.0.1", AuthHostname: "%"}, nil, nil, nil))
	tk.MustExec("create procedure t1() begin select * from t1 ; end;")
	tk.MustExec("create definer='test'@'%' procedure t2() begin select * from t1 ; end;")
	tk.MustExec("create definer=CURRENT_USER procedure t3() begin select * from t1 ; end;")
	tk.MustExec("create definer=CURRENT_USER() procedure t4() begin select * from t1 ; end;")
	tk2.MustQuery("select definer from mysql.routines").Check(testkit.Rows("test@%", "test@%", "test@%", "test@%"))
	// need super
	tk.MustGetErrCode("create definer='root1'@'%' procedure t5() begin select * from t1 ; end;", 1227)
	tk2.MustExec("grant super on *.* to test")
	// not check user exists
	tk.MustExec("create definer='root1'@'%' procedure t5() begin select * from t1 ; end;")
	tk.MustExec("create definer='root'@'%' procedure t6() begin select * from t1 ; end;")
	tk2.MustQuery("select definer from mysql.routines order by definer").Check(testkit.Rows("root1@%", "root@%", "test@%", "test@%", "test@%", "test@%"))
	tk.MustExec("create definer='root' procedure t7() begin select * from t1 ; end;")
	tk2.MustQuery("select definer from mysql.routines order by definer").Check(testkit.Rows("root1@%", "root@%", "root@%", "test@%", "test@%", "test@%", "test@%"))

	tk.MustContainErrMsg("create or replace definer='root' procedure t8() begin select * from t1 ; end;", "[parser:1525]Incorrect OrReplace (Should be empty) value: 'OR REPLACE'")
	tk.MustContainErrMsg("create ALGORITHM = MERGE definer='root' procedure t8() begin select * from t1 ; end;", "[parser:1525]Incorrect ViewAlgorithm (Should be empty) value: 'MERGE'")
	tk.MustContainErrMsg("create ALGORITHM = TEMPTABLE definer='root' procedure t8() begin select * from t1 ; end;", "[parser:1525]Incorrect ViewAlgorithm (Should be empty) value: 'TEMPTABLE'")
	// name too long
	tk.MustGetErrCode("create definer='root11111111111111111111111111111111'@'%' procedure t8() begin select * from t1 ; end;", 1470)
	// unfixable problem
	tk.MustExec("create ALGORITHM = UNDEFINED definer='root' procedure t8() begin select * from t1 ; end;")
}

func TestProcedureProcedureVariable(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.InProcedure()
	tk.MustExec("use test")
	tk.MustExec(`create procedure varTest(time_zone int)  begin
		set @time_zone = 111;
		select time_zone;
		select @time_zone;
		 end;`)
	tk.MustExec("call varTest(21)")
	require.Equal(t, 2, len(tk.Res))
	tk.Res[0].Check(testkit.Rows("21"))
	tk.Res[1].Check(testkit.Rows("111"))
	tk.MustExec("set @time_zone = NULL;")
	tk.ClearProcedureRes()
	tk.MustExec(`create procedure varTest1(time_zone int)  begin
		set time_zone = 111;
		select time_zone;
		select @time_zone;
		 end;`)
	tk.MustExec("call varTest1(21)")
	require.Equal(t, 2, len(tk.Res))
	tk.Res[0].Check(testkit.Rows("111"))
	tk.Res[1].Check(testkit.Rows("<nil>"))
	tk.ClearProcedureRes()
}

func TestProcedureLimit(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.InProcedure()
	tk.MustExec("use test")
	tk.MustExec(`create procedure varTest(limitRead int)  begin
		declare limitWrite int ;
		set limitRead = 111,limitWrite = (select 1 limit limitRead) ;
	end;`)
	tk.MustGetErrMsg("call varTest(11)", "the limit parameter 'limitread' is not supported to be modified in the same sql")
	tk.MustExec(`create procedure varTest1(limitRead int)  begin
		declare limitWrite int ;
		set limitRead = (select 1 limit limitRead) ;
	end;`)
	tk.MustGetErrMsg("call varTest1(11)", "the limit parameter 'limitread' is not supported to be modified in the same sql")

	tk.MustExec(`create procedure varTest2(limitRead int)  begin
		declare limitWrite int ;
		set limitRead = (select 1 limit 1,limitRead) ;
	end;`)
	tk.MustGetErrMsg("call varTest2(11)", "the limit parameter 'limitread' is not supported to be modified in the same sql")
	tk.MustExec(`create procedure varTest3(limitRead int)  begin
		declare limitWrite int ;
		set limitRead = (select 1 limit limitRead,limitRead) ;
	end;`)
	tk.MustGetErrMsg("call varTest3(11)", "the limit parameter 'limitread' is not supported to be modified in the same sql")
	tk.MustExec("CREATE TABLE `user` ( `id` int(11) NOT NULL, `username` VARCHAR(30) DEFAULT NULL, `password` VARCHAR(30) DEFAULT NULL, " +
		"`age` int(11) NOT NULL, `sex` int(11) NOT NULL, PRIMARY KEY (`id`), KEY `username` (`username`) ) ENGINE=InnoDB;")
	tk.MustExec("CREATE TABLE `user_score` ( `id` int(11) NOT NULL, `subject` int(11) NOT NULL, `user_id` int(11) NOT NULL, " +
		"`score` int(11) NOT NULL, PRIMARY KEY (`id`) ) ENGINE=InnoDB;")
	tk.MustExec("CREATE TABLE `user_address` ( `id` int(11) NOT NULL, `user_id` int(11) NOT NULL, `address` VARCHAR(30) DEFAULT NULL, " +
		"PRIMARY KEY (`id`), KEY `address` (`address`) ) ENGINE=InnoDB;")
	tk.MustExec(`create procedure insert_data(i int, s_i int)  begin
		declare x int default 0;
		while x < i do
		insert into user values(x, CONCAT("username-", x),CONCAT("password-", x),FLOOR( 15 ),Mod(x,2));
		insert into user_score values(s_i, 1, x, FLOOR( 40 + x * 100));
		set s_i=s_i+1;
		insert into user_score values(s_i, 2, x, FLOOR( 40 + x * 100));
		set s_i=s_i+1;
		insert into user_score values(s_i, 3, x, FLOOR( 40 + x * 100));
		set s_i=s_i+1;
		insert into user_score values(s_i, 4, x, FLOOR( 40 + x * 100));
		set s_i=s_i+1;
		insert into user_score values(s_i, 5, x, FLOOR( 40 + x * 100));
		set s_i=s_i+1;
		insert into user_address values(x, x, CONCAT("useraddress-", x));
		set x=x+1;
		end while;  end;`)
	tk.MustExec("call insert_data(50,0)")
	tk.MustExec(`create procedure procedureLimit(num int) begin
	select * from user_score order by id limit num;
	end`)
	tk.MustExec("call procedureLimit(2)")
	tk.Res[0].Check(testkit.Rows("0 1 0 40", "1 2 0 40"))
	tk.ClearProcedureRes()
	// support convert from -1
	tk.MustExec("call procedureLimit(-1)")
	require.Equal(t, 250, len(tk.Res[0].Rows()))
	tk.ClearProcedureRes()
	tk.MustExec("call procedureLimit(10)")
	tk.Res[0].Check(testkit.Rows("0 1 0 40", "1 2 0 40", "2 3 0 40", "3 4 0 40", "4 5 0 40", "5 1 1 140", "6 2 1 140", "7 3 1 140", "8 4 1 140", "9 5 1 140"))
	tk.ClearProcedureRes()
	tk.MustExec(`create procedure procedureLimit1(num int) begin
	select * from user_score order by id limit 1,num;
	end`)
	tk.MustExec("call procedureLimit1(2)")
	tk.Res[0].Check(testkit.Rows("1 2 0 40", "2 3 0 40"))
	tk.ClearProcedureRes()
	tk.MustExec("call procedureLimit1(10)")
	tk.Res[0].Check(testkit.Rows("1 2 0 40", "2 3 0 40", "3 4 0 40", "4 5 0 40", "5 1 1 140", "6 2 1 140", "7 3 1 140", "8 4 1 140", "9 5 1 140", "10 1 2 240"))
	tk.ClearProcedureRes()

	tk.MustExec(`create procedure procedureLimit2(num1 int,num2 int) begin
	select * from user_score order by id limit num1,num2;
	end`)
	tk.MustExec("call procedureLimit2(1,2)")
	tk.Res[0].Check(testkit.Rows("1 2 0 40", "2 3 0 40"))
	tk.ClearProcedureRes()
	tk.MustExec("call procedureLimit2(2,2)")
	tk.Res[0].Check(testkit.Rows("2 3 0 40", "3 4 0 40"))
	tk.ClearProcedureRes()
	tk.MustExec("call procedureLimit2(1,10)")
	tk.Res[0].Check(testkit.Rows("1 2 0 40", "2 3 0 40", "3 4 0 40", "4 5 0 40", "5 1 1 140", "6 2 1 140", "7 3 1 140", "8 4 1 140", "9 5 1 140", "10 1 2 240"))
	tk.ClearProcedureRes()
	tk.MustExec("call procedureLimit2(10,10)")
	tk.Res[0].Check(testkit.Rows("10 1 2 240", "11 2 2 240", "12 3 2 240", "13 4 2 240", "14 5 2 240", "15 1 3 340", "16 2 3 340", "17 3 3 340", "18 4 3 340", "19 5 3 340"))
	tk.ClearProcedureRes()
	tk.MustGetErrMsg("select * from user_score limit id", "[planner:1327]Undeclared variable: id")
	tk.MustGetErrMsg("select * from user_score limit 1,id", "[planner:1327]Undeclared variable: id")
	tk.MustGetErrMsg("select * from user_score limit 1,@id", "[parser:1064]You have an error in your SQL syntax; check the manual that corresponds to your TiDB version for the right syntax to use line 1 column 36 near \"@id\" ")
	tk.MustGetErrMsg("select * from user_score limit id,@id", "[parser:1064]You have an error in your SQL syntax; check the manual that corresponds to your TiDB version for the right syntax to use line 1 column 37 near \"@id\" ")
}

func TestProcedurePriv(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk2 := testkit.NewTestKit(t, store)
	tk.InProcedure()
	tk2.InProcedure()
	tk.MustExec("use test")
	tk.MustExec("create database test1")
	tk.MustExec("create user test")
	tk.MustExec("create table t1(id int key)")
	// use user test@%
	require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "test", Hostname: "127.0.0.1", AuthHostname: "%"}, nil, nil, nil))
	// no permission to create procedure
	tk.MustGetErrCode("create procedure test.t1(id int) begin select * from t1;end", 1044)
	tk2.MustExec("grant create routine on test.* to test")
	// procedure can be created under the test database.
	tk2.MustExec("set global automatic_sp_privileges=OFF")
	tk.MustExec("create procedure test.t1(id int) begin select * from t1;end")
	tk.MustGetErrCode("create procedure test1.t1(id int) begin select * from t1;end", 1044)
	// no permission to alter procedure
	tk.MustGetErrCode("alter procedure test.t1 comment 'test'", 1370)
	tk2.MustExec("grant alter routine on test.* to test")
	// procedure can be altered under the test database.
	tk.MustExec("alter procedure test.t1 comment 'test'")
	// no permission to call
	tk.MustGetErrCode("call test.t1(1)", 1370)
	tk2.MustExec("grant execute on test.* to test")
	require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "test", Hostname: "127.0.0.1", AuthHostname: "%"}, nil, nil, nil))
	// procedure can be called under the test database.
	// no permission to read table t1, using test.
	tk.MustGetErrCode("call test.t1(1)", 1142)
	tk2.MustGetErrCode("call test.t1(1)", 1142)
	tk2.MustExec("grant select on test.* to test")
	require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "test", Hostname: "127.0.0.1", AuthHostname: "%"}, nil, nil, nil))
	// get full access
	tk.MustExec("call test.t1(1)")
	require.Equal(t, 1, len(tk.Res))
	tk.Res[0].Check(testkit.Rows())
	tk.ClearProcedureRes()
	tk2.MustExec("call test.t1(1)")
	require.Equal(t, 1, len(tk2.Res))
	tk2.Res[0].Check(testkit.Rows())
	tk2.ClearProcedureRes()
	tk2.MustExec("drop user test")
	tk.MustGetErrCode("call test.t1(1)", 1370)
	tk2.MustGetErrCode("call test.t1(1)", 1449)
	tk2.MustExec("create user test")
	// procedure level privilege.
	tk2.MustExec("grant execute on procedure test.t1 to test")
	require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "test", Hostname: "127.0.0.1", AuthHostname: "%"}, nil, nil, nil))
	tk.MustGetErrCode("call test.t1(1)", 1142)
	tk2.MustExec("grant select on test.t1 to test")
	require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "test", Hostname: "127.0.0.1", AuthHostname: "%"}, nil, nil, nil))
	tk2.MustExec("create procedure test.t2(id int) begin select * from t1;end")
	tk.MustExec("call test.t1(1)")
	require.Equal(t, 1, len(tk.Res))
	tk.Res[0].Check(testkit.Rows())
	tk.ClearProcedureRes()
	tk.MustGetErrCode("call test.t2(1)", 1370)
}

func TestProcedurePrivInheritance(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk2 := testkit.NewTestKit(t, store)
	tk.InProcedure()
	tk2.InProcedure()
	tk.MustExec("use test")
	tk.MustExec("create database test1")
	tk.MustExec("create user test")
	tk.MustExec("create table t1(id int key)")
	require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "test", Hostname: "127.0.0.1", AuthHostname: "%"}, nil, nil, nil))
	tk2.MustExec("grant create routine on test.* to test")
	tk.MustExec("create procedure test.t1(id int) begin select * from t1;end")
	//  the server automatically grants the EXECUTE and ALTER ROUTINE privileges to the creator of a stored routine
	tk.MustQuery("show grants").Check(testkit.Rows("GRANT USAGE ON *.* TO 'test'@'%'", "GRANT CREATE ROUTINE ON `test`.* TO 'test'@'%'",
		"GRANT ALTER ROUTINE,EXECUTE ON PROCEDURE `test`.`t1` TO 'test'@'%'"))
	tk2.MustExec("set global automatic_sp_privileges=OFF")
	tk.MustExec("create procedure test.t2(id int) begin select * from t1;end")
	tk.MustQuery("show grants").Check(testkit.Rows("GRANT USAGE ON *.* TO 'test'@'%'", "GRANT CREATE ROUTINE ON `test`.* TO 'test'@'%'",
		"GRANT ALTER ROUTINE,EXECUTE ON PROCEDURE `test`.`t1` TO 'test'@'%'"))
}

func TestProcedureSQLSecurity(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.InProcedure()
	tk2 := testkit.NewTestKit(t, store)
	tk2.InProcedure()
	tk.MustExec("use test")
	tk.MustExec("create table t1(id int key)")
	tk.MustExec("insert into t1 value(1)")
	tk.MustExec("create procedure test.t1(id int) sql security definer begin select * from t1;end")
	tk.MustQuery("select security_type from mysql.routines where name = 't1'").Check(testkit.Rows("DEFINER"))
	tk.MustExec("create procedure test.t2(id int) sql security INVOKER begin select * from t1;end")
	tk.MustQuery("select security_type from mysql.routines where name = 't2'").Check(testkit.Rows("INVOKER"))
	tk.MustExec("alter procedure test.t1 sql security INVOKER; ")
	tk.MustQuery("select security_type from mysql.routines where name = 't1'").Check(testkit.Rows("INVOKER"))
	// The SQL SECURITY characteristic can be DEFINER or INVOKER to specify the security context; that is,
	// whether the routine executes using the privileges of the account named in the routine DEFINER clause or the user who invokes it.
	tk.MustExec("drop procedure test.t1;")
	tk.MustExec("drop procedure test.t2;")
	tk.MustExec("create user test;")
	require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "127.0.0.1", AuthHostname: "%"}, nil, nil, nil))
	tk.MustExec("create procedure test.t1(id int) sql security definer begin select * from t1;end")
	tk.MustExec("set timestamp = 1688554595")
	tk.MustExec("create procedure test.t2(id int) sql security INVOKER begin select * from t1;end")
	tk.MustQuery("show create procedure test.t2").Check(testkit.Rows("t2 ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION  CREATE DEFINER=`root`@`%` PROCEDURE `t2`(id int)\nSQL SECURITY INVOKER \nbegin select * from t1;end utf8mb4 utf8mb4_bin utf8mb4_bin"))
	tk.MustExec("alter procedure t2 comment '2222'")
	tk.MustQuery("show create procedure test.t2").Check(testkit.Rows("t2 ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION  CREATE DEFINER=`root`@`%` PROCEDURE `t2`(id int)\nCOMMENT '2222' \nSQL SECURITY INVOKER \nbegin select * from t1;end utf8mb4 utf8mb4_bin utf8mb4_bin"))
	tk.MustExec("grant execute on *.* to test")
	require.NoError(t, tk2.Session().Auth(&auth.UserIdentity{Username: "test", Hostname: "127.0.0.1", AuthHostname: "%"}, nil, nil, nil))
	tk2.MustExec("call test.t1(1)")
	tk2.Res[0].Check(testkit.Rows("1"))
	tk2.MustGetErrCode("select * from  test.t1", 1142)
	tk2.MustGetErrCode("call test.t2(1)", 1142)
	tk2.MustGetErrCode("select * from  test.t1", 1142)
	tk.MustQuery("show grants ").Check(testkit.Rows("GRANT ALL PRIVILEGES ON *.* TO 'root'@'%' WITH GRANT OPTION"))
	tk.MustExec("grant all on *.* to test")
	tk2.MustExec("create procedure test.t3(id int) sql security definer begin select * from t1;end")
	tk.MustExec("drop user test")
	tk.MustGetErrCode("call test.t3(1)", 1449)
}

func TestProcedureLoop(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.InProcedure()
	tk.MustExec("use test")
	tk.MustExec("create table t1(id int key)")
	tk.MustExec(`CREATE PROCEDURE doiterate(p1 INT)
	BEGIN
 	label1: LOOP
    SET p1 = p1 + 1;
    IF p1 > 10 THEN
      Leave label1;
    END IF;
  	END LOOP label1;
  	SET @x = p1;
	END;
	`)
	tk.MustExec("call doiterate(0)")
	tk.MustQuery("select @x").Check(testkit.Rows("11"))
	tk.MustExec("set @x = null")
	tk.MustExec(`CREATE PROCEDURE testerror(p1 INT)
	BEGIN
	declare exit handler for SQLEXCEPTION begin select 3; end;
 	label1: LOOP
    SET p1 = p1 + 1;
    IF p1 > 10 THEN
	  select 4;
	  select * from t2;
	  select 1;
      Leave label1;
    END IF;
  	END LOOP label1;
  	SET @x = p1;
	END;
	`)
	tk.MustExec("call testerror(0)")
	require.Equal(t, len(tk.Res), 2)
	tk.Res[0].Check(testkit.Rows("4"))
	tk.Res[1].Check(testkit.Rows("3"))
	tk.ClearProcedureRes()
	tk.MustExec(`CREATE PROCEDURE testerror2(p1 INT)
	BEGIN
	declare continue handler for SQLEXCEPTION begin select 3; end;
 	label1: LOOP
    SET p1 = p1 + 1;
    IF p1 > 10 THEN
	  select 4;
	  select * from t2;
	  select 1;
      Leave label1;
    END IF;
  	END LOOP label1;
  	SET @x = p1;
	END;
	`)
	tk.MustExec("call testerror2(0)")
	require.Equal(t, len(tk.Res), 3)
	tk.Res[0].Check(testkit.Rows("4"))
	tk.Res[1].Check(testkit.Rows("3"))
	tk.Res[2].Check(testkit.Rows("1"))
	tk.ClearProcedureRes()
	tk.MustExec(`CREATE PROCEDURE doiterate1(p1 INT)
 	label1: LOOP
    SET p1 = p1 + 1;
    IF p1 > 10 THEN
	  SET @x = p1;
      Leave label1;
    END IF;
  	END LOOP label1;
	`)
	tk.MustExec("call doiterate1(0)")
	tk.MustQuery("select @x").Check(testkit.Rows("11"))
	tk.MustExec(`CREATE PROCEDURE doiterate2(p1 INT)
 	label1: LOOP
    SET p1 = p1 + 1;
    IF p1 < 10 THEN
	  select 1;
      iterate label1;
	ELSE
	 select 2;
	 leave label1;
    END IF;
  	END LOOP label1;
	`)
	tk.MustExec("call doiterate2(8)")
	require.Equal(t, len(tk.Res), 2)
	tk.Res[0].Check(testkit.Rows("1"))
	tk.Res[1].Check(testkit.Rows("2"))
	tk.ClearProcedureRes()
	tk.MustExec(`CREATE PROCEDURE doiterate3(p1 INT)
 	label2:begin
	label1:LOOP
    SET p1 = p1 + 1;
    IF p1 < 10 THEN
	  select 1;
      iterate label1;
	ELSE
	 select 2;
	 leave label2;
    END IF;
  	END LOOP label1;
	select 3;
	end
	`)
	tk.MustExec("call doiterate3(8)")
	require.Equal(t, len(tk.Res), 2)
	tk.Res[0].Check(testkit.Rows("1"))
	tk.Res[1].Check(testkit.Rows("2"))
	tk.ClearProcedureRes()
}

func TestProcedureVariable(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustQuery("show variables like '%tidb_enable_procedure%'").Check(testkit.Rows("tidb_enable_procedure OFF"))
	tk.MustQuery("show variables like '%tidb_enable_sp_param_substitute%'").Check(testkit.Rows("tidb_enable_sp_param_substitute OFF"))
	tk.MustExec("set GLOBAL tidb_enable_procedure = ON")
	tk.MustQuery("show variables like '%tidb_enable_procedure%'").Check(testkit.Rows("tidb_enable_procedure ON"))
	tk.MustQuery("show variables like '%tidb_enable_sp_param_substitute%'").Check(testkit.Rows("tidb_enable_sp_param_substitute ON"))
	tk.MustQuery("show global variables like '%tidb_enable_sp_param_substitute%'").Check(testkit.Rows("tidb_enable_sp_param_substitute ON"))
	tk.MustExec("set GLOBAL tidb_enable_procedure = OFF")
	tk.MustQuery("show variables like '%tidb_enable_procedure%'").Check(testkit.Rows("tidb_enable_procedure OFF"))
	tk.MustQuery("show variables like '%tidb_enable_sp_param_substitute%'").Check(testkit.Rows("tidb_enable_sp_param_substitute ON"))
	tk.MustQuery("show global variables like '%tidb_enable_sp_param_substitute%'").Check(testkit.Rows("tidb_enable_sp_param_substitute ON"))
	tk.MustExec("set GLOBAL tidb_enable_procedure = ON")
	tk.MustExec("set GLOBAL tidb_enable_sp_param_substitute = OFF")
	tk.MustQuery("show variables like '%tidb_enable_procedure%'").Check(testkit.Rows("tidb_enable_procedure ON"))
	tk.MustQuery("show variables like '%tidb_enable_sp_param_substitute%'").Check(testkit.Rows("tidb_enable_sp_param_substitute ON"))
	tk.MustQuery("show global variables like '%tidb_enable_sp_param_substitute%'").Check(testkit.Rows("tidb_enable_sp_param_substitute OFF"))
	tk.MustExec("set GLOBAL tidb_enable_sp_param_substitute = ON")
	tk.MustExec("set tidb_enable_sp_param_substitute = OFF")
	tk.MustQuery("show variables like '%tidb_enable_procedure%'").Check(testkit.Rows("tidb_enable_procedure ON"))
	tk.MustQuery("show variables like '%tidb_enable_sp_param_substitute%'").Check(testkit.Rows("tidb_enable_sp_param_substitute OFF"))
	tk.MustQuery("show global variables like '%tidb_enable_sp_param_substitute%'").Check(testkit.Rows("tidb_enable_sp_param_substitute ON"))
}

func TestProcedureDDL(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.InProcedure()
	tk.MustExec("use test")
	tk.MustExec("create procedure t1() begin create table t1 (id int);end;")
	tk.MustExec("create procedure t2() begin drop table t1;end;")
	tk.MustExec("call t1")
	tk.MustQuery("show create table t1").Check(testkit.Rows("t1 CREATE TABLE `t1` (\n  `id` int(11) DEFAULT NULL\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))
	tk.MustExec("call t2")
	tk.MustGetErrCode("show create table t1", 1146)
	tk.MustExec("create procedure t3() begin create TEMPORARY table t1 (id int);end;")
	tk.MustExec("create procedure t4() begin alter table t1 add index(id);end;")
	tk.MustExec("call t3")
	tk.MustQuery("show create table t1").Check(testkit.Rows("t1 CREATE TEMPORARY TABLE `t1` (\n  `id` int(11) DEFAULT NULL\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))
	tk.MustGetErrCode("call t4", 8200)
	tk.MustExec("call t2")
	tk.MustGetErrCode("show create table t1", 1146)
	tk.MustExec("call t1")
	tk.MustQuery("show create table t1").Check(testkit.Rows("t1 CREATE TABLE `t1` (\n  `id` int(11) DEFAULT NULL\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))
	tk.MustExec("call t4")
	tk.MustQuery("show create table t1").Check(testkit.Rows("t1 CREATE TABLE `t1` (\n  `id` int(11) DEFAULT NULL,\n  KEY `id` (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))
	tk.MustExec("call t2")
	tk.MustExec("create procedure t5() begin create table t1 (id int,id2 varchar(30),id3 text);alter table t1 add index(id2);drop table t1;end;")
	tk.MustExec("call t5")
	tk.MustExec("call t5")
	tk.MustExec("create procedure t6() begin create table t1 (id int,id2 varchar(30),id3 text);insert into t1 values (1,1,1),(2,2,2);alter table t1 add index(id2);insert into t1 values (1,1,1),(2,2,2);select * from t1;drop table t1;end;")
	tk.MustExec("call t6")
	require.Equal(t, len(tk.Res), 1)
	tk.Res[0].Check(testkit.Rows("1 1 1", "2 2 2", "1 1 1", "2 2 2"))
	tk.ClearProcedureRes()
	tk.MustExec("call t6")
	require.Equal(t, len(tk.Res), 1)
	tk.Res[0].Check(testkit.Rows("1 1 1", "2 2 2", "1 1 1", "2 2 2"))
	tk.ClearProcedureRes()
}
