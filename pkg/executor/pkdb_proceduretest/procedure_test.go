// Copyright 2023-2023 PingCAP, Inc.

package procedure_test

import (
	"fmt"
	"testing"

	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/util/memory"
	"github.com/stretchr/testify/require"
)

func TestRoutinePriv(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.InProcedure()
	tk.MustExec("create user test")
	tk.MustExec("create database test1")
	// only a routine can object_type == PROCEDURE
	tk.MustGetErrCode("grant create routine on procedure *.* to test", 1144)
	tk.MustGetErrCode("grant create routine on procedure test.* to test", 1144)
	tk2 := testkit.NewTestKit(t, store)
	tk2.InProcedure()
	require.NoError(t, tk2.Session().Auth(&auth.UserIdentity{Username: "test", Hostname: "127.0.0.1", AuthHostname: "%"}, nil, nil, nil))
	require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "127.0.0.1", AuthHostname: "%"}, nil, nil, nil))
	tk2.MustGetErrCode("create procedure test.t1(id int) sql security definer begin select * from t1;end", 1044)
	tk.MustExec("grant create routine on *.* to test")
	// all database can create procedure
	tk2.MustExec("create procedure test.t1(id int) sql security definer begin select * from t1;end")
	tk2.MustExec("create procedure test1.t1(id int) sql security definer begin select * from t1;end")
	tk.MustQuery("show grants for test").Check(testkit.Rows("GRANT CREATE ROUTINE ON *.* TO 'test'@'%'", "GRANT ALTER ROUTINE,EXECUTE ON PROCEDURE `test1`.`t1` TO 'test'@'%'",
		"GRANT ALTER ROUTINE,EXECUTE ON PROCEDURE `test`.`t1` TO 'test'@'%'"))
	// revoke success
	tk.MustExec("revoke all on *.* from test")
	tk2.MustGetErrCode("create procedure test.t1(id int) sql security definer begin select * from t1;end", 1044)
	tk2.MustGetErrCode("create procedure test1.t1(id int) sql security definer begin select * from t1;end", 1044)

	// database level test
	tk.MustExec("grant create routine on test.* to test")
	tk2.MustExec("create procedure test.t2(id int) sql security definer begin select * from t1;end")
	tk2.MustGetErrCode("create procedure test1.t2(id int) sql security definer begin select * from t1;end", 1044)
	// global level can not revoke db level
	tk.MustExec("revoke create routine on *.* from test")
	tk2.MustExec("create procedure test.t3(id int) sql security definer begin select * from t1;end")
	tk.MustQuery("show grants for test").Check(testkit.Rows("GRANT USAGE ON *.* TO 'test'@'%'", "GRANT CREATE ROUTINE ON `test`.* TO 'test'@'%'", "GRANT ALTER ROUTINE,EXECUTE ON PROCEDURE `test1`.`t1` TO 'test'@'%'", "GRANT ALTER ROUTINE,EXECUTE ON PROCEDURE `test`.`t1` TO 'test'@'%'",
		"GRANT ALTER ROUTINE,EXECUTE ON PROCEDURE `test`.`t2` TO 'test'@'%'", "GRANT ALTER ROUTINE,EXECUTE ON PROCEDURE `test`.`t3` TO 'test'@'%'"))
	tk.MustExec("revoke create routine on test.* from test")
	tk2.MustGetErrCode("create procedure test.t4(id int) sql security definer begin select * from t1;end", 1044)

	// drop database && create check priv
	tk.MustExec("drop user test")
	tk.MustExec("create user test")
	tk.MustExec("grant create routine on test.* to test")
	tk.MustExec("grant create routine on test1.* to test")
	tk.MustExec("drop database test")
	tk.MustExec("drop database test1")
	tk.MustExec("create database test")
	tk.MustExec("create database test1")
	tk.MustQuery("show procedure status").Check(testkit.Rows())
	tk2.MustExec("create procedure test.t1(id int) sql security definer begin end")
	tk2.MustExec("create procedure test.t2(id int) sql security definer begin end")
	tk2.MustExec("create procedure test1.t1(id int) sql security definer begin end")
	tk.MustExec("revoke create routine on test.* from test")
	tk2.MustExec("create procedure test1.t2(id int) sql security definer begin select * from t1;end")
	tk2.MustGetErrCode("create procedure test.t2(id int) sql security definer begin select * from t1;end", 1044)
	tk.MustExec("create procedure test1.t3(id int) sql security definer begin select * from t1;end")
	// can not drop procedure
	tk2.MustGetErrCode("drop procedure test1.t3", 1370)
	// test procedure level
	tk.MustGetErrCode("grant create routine on test.t1 to test", 1144)
	tk.MustGetErrCode("grant create routine on procedure test.t1 to test", 1144)

	tk.MustExec("drop user test")
	tk.MustExec("create user test")
	tk.MustExec("grant create routine on test.* to test")
	tk.MustExec("grant create routine on test1.* to test")
	// test call need execute priv
	tk2.MustGetErrCode("call test.t1", 1370)
	// test alter procedure need alter procedure priv
	tk2.MustGetErrCode("alter procedure test.t1 comment '123'", 1370)
	tk.MustExec("grant execute on *.* to test")
	tk2.MustExec("call test.t1(1)")
	tk2.MustExec("call test1.t1(1)")
	tk.MustExec("revoke execute on *.* from test")
	tk2.MustGetErrCode("call test.t1(1)", 1370)
	tk2.MustGetErrCode("call test1.t1(1)", 1370)
	// test db level
	tk.MustExec("grant execute on test.* to test")
	tk2.MustExec("call test.t1(1)")
	tk2.MustGetErrCode("call test1.t1(1)", 1370)
	tk.MustExec("revoke execute on test.* from test")
	tk2.MustGetErrCode("call test.t1(1)", 1370)
	// test procedure level
	tk.MustGetErrCode("grant execute on test.t1 to test", 1144)
	tk.MustExec("grant execute on procedure test.t1 to test")
	tk2.MustExec("call test.t1(1)")
	tk2.MustGetErrCode("call test.t2(1)", 1370)
	tk2.MustGetErrCode("call test1.t1(1)", 1370)
	tk.MustGetErrMsg("revoke execute on test.t1 from test", "There is no such grant defined for user 'test' on host '%' on table test.t1")
	tk.MustExec("revoke execute on procedure test.t1 from test")
	tk2.MustGetErrCode("call test.t1(1)", 1370)
	// alter procedure
	// global level
	tk.MustGetErrCode("grant alter routine on procedure *.* to test", 1144)
	tk2.MustGetErrCode("alter  procedure test.t1 COMMENT 'xxxx'", 1370)
	tk.MustExec("grant alter routine on *.* to test")
	tk2.MustExec("alter  procedure test.t1 COMMENT 'xxxx'")
	tk.MustQuery("select comment from mysql.routines where route_schema= 'test' and name = 't1' and type= 'PROCEDURE'").Check(testkit.Rows("xxxx"))
	tk2.MustExec("alter  procedure test.t2 COMMENT 'xxxx'")
	tk.MustQuery("select comment from mysql.routines where route_schema= 'test' and name = 't2' and type= 'PROCEDURE'").Check(testkit.Rows("xxxx"))
	tk2.MustExec("alter  procedure test1.t1 COMMENT 'xxxx'")
	tk.MustQuery("select comment from mysql.routines where route_schema= 'test1' and name = 't1' and type= 'PROCEDURE'").Check(testkit.Rows("xxxx"))
	tk.MustExec("revoke alter routine on *.* from test")
	tk2.MustGetErrCode("alter  procedure test.t1 COMMENT 'xxxx'", 1370)
	// db level
	tk.MustExec("grant alter routine on test.* to test")
	tk2.MustExec("alter  procedure test.t1 COMMENT 'xxxx1'")
	tk.MustQuery("select comment from mysql.routines where route_schema= 'test' and name = 't1' and type= 'PROCEDURE'").Check(testkit.Rows("xxxx1"))
	tk2.MustGetErrCode("alter  procedure test1.t1 COMMENT 'xxxx1'", 1370)
	tk.MustQuery("select comment from mysql.routines where route_schema= 'test' and name = 't2' and type= 'PROCEDURE'").Check(testkit.Rows("xxxx"))
	tk.MustExec("revoke alter routine on test.* from test")
	tk2.MustGetErrCode("alter  procedure test.t1 COMMENT 'xxxx2'", 1370)
	tk.MustQuery("select comment from mysql.routines where route_schema= 'test' and name = 't1' and type= 'PROCEDURE'").Check(testkit.Rows("xxxx1"))
	// procedure level
	tk.MustGetErrCode("grant alter routine on test.t1 to test", 1144)
	tk.MustExec("grant alter routine on procedure test.t1 to test")
	tk2.MustExec("alter  procedure test.t1 COMMENT 'xxxx2'")
	tk.MustQuery("select comment from mysql.routines where route_schema= 'test' and name = 't1' and type= 'PROCEDURE'").Check(testkit.Rows("xxxx2"))
	tk2.MustGetErrCode("alter  procedure test1.t1 COMMENT 'xxxx1'", 1370)
	tk2.MustGetErrCode("revoke alter routine on test.t1 from test", 8121)
	tk.MustExec("revoke alter routine on procedure test.t1 from test")
	tk2.MustGetErrCode("alter  procedure test.t1 COMMENT 'xxxx3'", 1370)
	tk.MustQuery("select comment from mysql.routines where route_schema= 'test' and name = 't1' and type= 'PROCEDURE'").Check(testkit.Rows("xxxx2"))
	// alter procedure can drop procedure
	tk.MustExec("grant alter routine on procedure test.t1 to test")
	tk2.MustExec("drop  procedure test.t1")
}

func TestRoutineRolePriv(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk2 := testkit.NewTestKit(t, store)
	tk.MustExec("create user test")
	tk2.InProcedure()
	// test role
	tk.MustExec("create role test2")
	tk.MustExec("grant create routine on *.* to test2")
	require.NoError(t, tk2.Session().Auth(&auth.UserIdentity{Username: "test", Hostname: "127.0.0.1", AuthHostname: "%"}, nil, nil, nil))
	require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "127.0.0.1", AuthHostname: "%"}, nil, nil, nil))
	tk2.MustGetErrCode("create procedure test.t1(id int) sql security definer begin end", 1044)
	tk.MustExec("grant test2 to test")
	tk.MustExec("SET DEFAULT ROLE 'test2' TO 'test';")
	require.NoError(t, tk2.Session().Auth(&auth.UserIdentity{Username: "test", Hostname: "127.0.0.1", AuthHostname: "%"}, nil, nil, nil))
	tk2.MustExec("create procedure test.t1(id int) sql security definer begin end")
	tk.MustExec("create procedure test.t2(id int) sql security definer begin end")
	tk2.MustGetErrCode("alter  procedure test.t2 COMMENT 'xxxx3'", 1370)
	tk2.MustGetErrCode("call test.t2(1)", 1370)
	// disable role
	tk2.MustExec("SET ROLE NONE;")
	tk2.MustGetErrCode("create procedure test.t3(id int) sql security definer begin end", 1044)
	tk.MustExec("create role test3")
	tk.MustGetErrCode("grant alter routine on test.t2 to test3", 1144)
	tk.MustExec("grant alter routine on procedure test.t2 to test3")
	tk.MustExec("grant test3 to test")
	// enable role
	tk2.MustExec("SET ROLE test3;")
	tk2.MustGetErrCode("alter  procedure test.t2 COMMENT 'xxxx3'", 1227)
	tk.MustQuery("select comment from mysql.routines where route_schema= 'test' and name = 't2' and type= 'PROCEDURE'").Check(testkit.Rows(""))
	tk2.MustGetErrCode("call test.t2(1)", 1370)
	// disable role
	tk2.MustExec("SET ROLE NONE;")
	tk2.MustGetErrCode("alter  procedure test.t2 COMMENT 'xxxx4'", 1370)
	tk.MustQuery("select comment from mysql.routines where route_schema= 'test' and name = 't2' and type= 'PROCEDURE'").Check(testkit.Rows(""))

	tk.MustExec("create role test4")
	tk.MustGetErrCode("grant execute on test.t2 to test4", 1144)
	tk.MustExec("grant execute on procedure test.t2 to test4")
	tk.MustExec("grant test4 to test")
	tk2.MustExec("SET ROLE test4;")
	tk2.MustExec("call test.t2(1)")
	tk2.MustExec("SET ROLE NONE;")
	tk2.MustGetErrCode("call test.t2(1)", 1370)
	// all active
	tk2.MustExec("SET ROLE ALL;")
	tk2.MustExec("create procedure test.t3(id int) sql security definer begin end")
	tk2.MustExec("call test.t2(1)")
	tk2.MustGetErrCode("drop procedure test.t2", 1227)
}

func TestCallPriv(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("create user test")
	tk.MustExec("grant create routine,execute on *.* to test")
	tk2 := testkit.NewTestKit(t, store)
	tk2.InProcedure()
	require.NoError(t, tk2.Session().Auth(&auth.UserIdentity{Username: "test", Hostname: "127.0.0.1", AuthHostname: "%"}, nil, nil, nil))
	require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "127.0.0.1", AuthHostname: "%"}, nil, nil, nil))
	tk.MustExec("use test")
	tk.MustExec("create table t1(id int key)")
	tk2.MustExec("create procedure test.t1()begin  select * from t1;end")
	tk.MustExec("create procedure test.t3()begin  select * from t1;end")
	tk2.MustExec("call test.t3")
	tk.MustExec("create procedure t2() SQL SECURITY INVOKER begin  select * from t1;end")
	tk2.MustGetErrCode("call test.t2", mysql.ErrTableaccessDenied)
	// test role
	tk.MustExec("create role test2")
	tk.MustExec("grant select on *.* to test2")
	tk.MustExec("grant test2 to test")
	tk2.MustExec("set ROLE test2")
	tk2.MustExec("call test.t2")
	tk2.MustExec("set ROLE None")
	tk2.MustGetErrCode("call test.t2", mysql.ErrTableaccessDenied)

	tk.MustExec("grant alter routine on *.* to test")
	// DEFINER is current user no need super
	tk2.MustExec("alter procedure test.t1 SQL SECURITY definer")
	tk.MustQuery("select security_type from mysql.routines where route_schema= 'test' and name = 't1' and type= 'PROCEDURE'").Check(testkit.Rows("DEFINER"))
	tk2.MustExec("alter procedure test.t1 SQL SECURITY INVOKER")
	tk.MustQuery("select security_type from mysql.routines where route_schema= 'test' and name = 't1' and type= 'PROCEDURE'").Check(testkit.Rows("INVOKER"))
	// DEFINER is not current user  need super
	tk2.MustGetErrCode("alter procedure test.t2 SQL SECURITY definer", 1227)
	tk2.MustGetErrCode("alter procedure test.t2 SQL SECURITY INVOKER", 1227)
	tk.MustExec("grant super on *.* to test")
	tk2.MustExec("alter procedure test.t2 SQL SECURITY definer")
	tk.MustQuery("select security_type from mysql.routines where route_schema= 'test' and name = 't2' and type= 'PROCEDURE'").Check(testkit.Rows("DEFINER"))
	tk2.MustExec("alter procedure test.t2 SQL SECURITY INVOKER")
	tk.MustQuery("select security_type from mysql.routines where route_schema= 'test' and name = 't2' and type= 'PROCEDURE'").Check(testkit.Rows("INVOKER"))
	// test role
	tk.MustExec("revoke super on *.* from test")
	tk2.MustGetErrCode("alter procedure test.t2 SQL SECURITY definer", 1227)
	tk.MustExec("create role super_user")
	tk.MustExec("grant super on *.* to super_user")
	tk.MustExec("grant super_user to test")
	tk2.MustExec("set role super_user")
	tk2.MustExec("alter procedure test.t2 SQL SECURITY definer")
	tk.MustQuery("select security_type from mysql.routines where route_schema= 'test' and name = 't2' and type= 'PROCEDURE'").Check(testkit.Rows("DEFINER"))
	tk2.MustExec("set role none")
	tk2.MustGetErrCode("alter procedure test.t2 SQL SECURITY definer", 1227)
	// drop is consistent with alter
	tk2.MustGetErrCode("drop procedure test.t2", 1227)
	tk2.MustExec("set role super_user")
	tk2.MustExec("drop procedure test.t2")
}

func TestProcedureFloatVar(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.InProcedure()
	tk.MustExec("use test")
	tk.MustExec("create procedure t1(id float) begin select id; end;")
	tk.MustExec("call t1(1.2)")
	require.Equal(t, len(tk.Res), 1)
	tk.Res[0].Check(testkit.Rows("1.2"))
	tk.ClearProcedureRes()
	tk.MustExec("call t1(1.21)")
	require.Equal(t, len(tk.Res), 1)
	tk.Res[0].Check(testkit.Rows("1.21"))
	tk.ClearProcedureRes()
	// max min test
	tk.MustExec("call t1(3.40282e+38)")
	require.Equal(t, len(tk.Res), 1)
	tk.Res[0].Check(testkit.Rows("340282000000000000000000000000000000000"))
	tk.ClearProcedureRes()
	tk.MustExec("call t1(-3.40282e+38)")
	require.Equal(t, len(tk.Res), 1)
	tk.Res[0].Check(testkit.Rows("-340282000000000000000000000000000000000"))
	tk.ClearProcedureRes()
	// udv test
	tk.MustExec("set @a = 1.2")
	tk.MustExec("call t1(@a)")
	require.Equal(t, len(tk.Res), 1)
	tk.Res[0].Check(testkit.Rows("1.2"))
	tk.ClearProcedureRes()
	tk.MustExec("set @a = 1.21")
	tk.MustExec("call t1(@a)")
	require.Equal(t, len(tk.Res), 1)
	tk.Res[0].Check(testkit.Rows("1.21"))
	tk.ClearProcedureRes()
	tk.MustExec("set @a = 3.40282e+38")
	tk.MustExec("call t1(@a)")
	require.Equal(t, len(tk.Res), 1)
	tk.Res[0].Check(testkit.Rows("340282000000000000000000000000000000000"))
	tk.ClearProcedureRes()
	tk.MustExec("set @a = -3.40282e+38")
	tk.MustExec("call t1(@a)")
	require.Equal(t, len(tk.Res), 1)
	tk.Res[0].Check(testkit.Rows("-340282000000000000000000000000000000000"))
	tk.ClearProcedureRes()
	tk.MustExec("set @a = 3.40283e+38")
	tk.MustGetErrCode("call t1(@a)", 1690)
	// overflow
	tk.MustGetErrCode("call t1(3.40283e+38)", 1690)
	tk.MustGetErrCode("call t1(-3.40283e+38)", 1690)
	// unsigned float
	tk.MustExec("create procedure t2(id float unsigned) begin select id; end;")
	tk.MustExec("call t2(1.2)")
	require.Equal(t, len(tk.Res), 1)
	tk.Res[0].Check(testkit.Rows("1.2"))
	tk.ClearProcedureRes()
	tk.MustExec("call t2(1.21)")
	require.Equal(t, len(tk.Res), 1)
	tk.Res[0].Check(testkit.Rows("1.21"))
	tk.ClearProcedureRes()
	// max min test
	tk.MustExec("call t2(3.40282e+38)")
	require.Equal(t, len(tk.Res), 1)
	tk.Res[0].Check(testkit.Rows("340282000000000000000000000000000000000"))
	tk.ClearProcedureRes()
	// overflow
	tk.MustGetErrCode("call t2(-1)", 1690)
	tk.MustGetErrCode("call t2(3.40283e+38)", 1690)
	// declare
	tk.MustExec("create procedure t3() begin declare id float; set id = @id1;  select id; end;")
	tk.MustExec("set @id1 = 1.2")
	tk.MustExec("call t3()")
	require.Equal(t, len(tk.Res), 1)
	tk.Res[0].Check(testkit.Rows("1.2"))
	tk.ClearProcedureRes()
	tk.MustExec("set @id1 = 1.21")
	tk.MustExec("call t3()")
	require.Equal(t, len(tk.Res), 1)
	tk.Res[0].Check(testkit.Rows("1.21"))
	tk.ClearProcedureRes()
	tk.MustExec("set @id1 = 3.40282e+38")
	tk.MustExec("call t3()")
	require.Equal(t, len(tk.Res), 1)
	tk.Res[0].Check(testkit.Rows("340282000000000000000000000000000000000"))
	tk.ClearProcedureRes()
	tk.MustExec("set @id1 = 3.40283e+38")
	tk.MustGetErrCode("call t3()", 1690)
	tk.MustExec("set @id1 = -1")
	tk.MustExec("call t3()")
	require.Equal(t, len(tk.Res), 1)
	tk.Res[0].Check(testkit.Rows("-1"))
	tk.ClearProcedureRes()

	tk.MustExec("create procedure t4() begin declare id float unsigned; set id = @id1;  select id; end;")
	tk.MustExec("set @id1 = 1.2")
	tk.MustExec("call t4()")
	require.Equal(t, len(tk.Res), 1)
	tk.Res[0].Check(testkit.Rows("1.2"))
	tk.ClearProcedureRes()
	tk.MustExec("set @id1 = 1.21")
	tk.MustExec("call t4()")
	require.Equal(t, len(tk.Res), 1)
	tk.Res[0].Check(testkit.Rows("1.21"))
	tk.ClearProcedureRes()
	tk.MustExec("set @id1 = 3.40282e+38")
	tk.MustExec("call t4()")
	require.Equal(t, len(tk.Res), 1)
	tk.Res[0].Check(testkit.Rows("340282000000000000000000000000000000000"))
	tk.ClearProcedureRes()
	tk.MustExec("set @id1 = 3.40283e+38")
	tk.MustGetErrCode("call t4()", 1690)

	tk.MustExec("create table t1(id float key)")
	tk.MustExec("create procedure t5(id float ) begin insert into t1 values(id); end;")
	tk.MustExec("call t5(1.2)")
	tk.MustQuery("select * from t1").Check(testkit.Rows("1.2"))
	tk.MustExec("call t5(1.21)")
	tk.MustQuery("select * from t1").Check(testkit.Rows("1.2", "1.21"))
	tk.MustExec("call t5(3.40282e+38)")
	tk.MustQuery("select * from t1").Check(testkit.Rows("1.2", "1.21", "340282000000000000000000000000000000000"))
	tk.MustGetErrCode("call t5(3.40283e+38)", 1690)
	tk.MustQuery("select * from t1").Check(testkit.Rows("1.2", "1.21", "340282000000000000000000000000000000000"))
	tk.MustExec("call t5(-3.40282e+38)")
	tk.MustQuery("select * from t1").Check(testkit.Rows("-340282000000000000000000000000000000000", "1.2", "1.21", "340282000000000000000000000000000000000"))
}

func TestProcedureSysVar(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.InProcedure()
	tk.MustExec("use test")
	tk.MustGetErrCode(`CREATE PROCEDURE p1()
	BEGIN
	DECLARE v INT DEFAULT 0;
	SET @@SESSION.v= 10;
	END`, 1193)
	tk.MustGetErrCode(`CREATE PROCEDURE p3()
	BEGIN
	DECLARE v INT DEFAULT 0;
	SELECT @@SESSION.v;
	END`, 1193)
	tk.MustGetErrCode(`CREATE PROCEDURE p4()
	BEGIN
	DECLARE v INT DEFAULT 0;
	SET @@GLOBAL.v= 10;
	END`, 1193)
	tk.MustGetErrCode(`CREATE PROCEDURE p6()
	BEGIN
	DECLARE v INT DEFAULT 0;
	SET @@v= 0;
	END`, 1193)
}

func TestProcedureCheckexists(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.InProcedure()
	tk.MustExec("use test")
	tk.MustExec("create procedure t1() select * from t1;")
	tk.MustExec("create procedure t2() begin select * from t1; end;")
	tk.MustExec("create procedure t3() t1:begin select * from t1; end;")
	tk.MustGetErrCode("call t1", 1146)
	tk.MustGetErrCode("call t2", 1146)
	tk.MustGetErrCode("call t3", 1146)
	tk.MustExec("create procedure t4() t1:loop select * from t1; end loop;")
	tk.MustGetErrCode("call t4", 1146)
}

func TestProcedureDisableDefault(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.InProcedure()
	tk.MustExec("use test")
	tk.MustExec("CREATE TABLE t1(a INT);")
	tk.MustGetErrCode("CREATE PROCEDURE p(p INT) SET p = DEFAULT", 1064)
	tk.MustGetErrCode(`CREATE PROCEDURE p()
	BEGIN
	  DECLARE v INT;
	  SET v = DEFAULT;
	END`, 1064)
	tk.MustGetErrCode(`CREATE PROCEDURE p()
	BEGIN
	  DECLARE v INT DEFAULT 1;
	  SET v = DEFAULT;
	END`, 1064)
	tk.MustGetErrCode(`CREATE PROCEDURE p()
	BEGIN
	  DECLARE v INT DEFAULT (SELECT * FROM t1);
	  SET v = DEFAULT;
	END`, 1064)
}

func TestProcedureCache(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.InProcedure()
	tk.MustExec("use test")
	tk.MustExec("CREATE TABLE t1(a INT);")
	// test case block
	tk.MustExec(`create procedure p1(id int) begin
	declare continue HANDLER for SQLEXCEPTION select 4;
	case when id = 1 then select 1;
	when id = 2 then select 2;
	when id = 3 then select 3;
	end case;
	select 5;
	end`)
	tk.MustExec("call p1(1)")
	require.Equal(t, 2, len(tk.Res))
	tk.Res[0].Check(testkit.Rows("1"))
	tk.Res[1].Check(testkit.Rows("5"))
	tk.ClearProcedureRes()
	tk.MustExec("call p1(1)")
	require.Equal(t, 2, len(tk.Res))
	tk.Res[0].Check(testkit.Rows("1"))
	tk.Res[1].Check(testkit.Rows("5"))
	tk.ClearProcedureRes()
	tk.MustExec("call p1(2)")
	require.Equal(t, 2, len(tk.Res))
	tk.Res[0].Check(testkit.Rows("2"))
	tk.Res[1].Check(testkit.Rows("5"))
	tk.ClearProcedureRes()
	tk.MustExec("call p1(3)")
	require.Equal(t, 2, len(tk.Res))
	tk.Res[0].Check(testkit.Rows("3"))
	tk.Res[1].Check(testkit.Rows("5"))
	tk.ClearProcedureRes()

	tk.MustExec("call p1(4)")
	require.Equal(t, 2, len(tk.Res))
	tk.Res[0].Check(testkit.Rows("4"))
	tk.Res[1].Check(testkit.Rows("5"))
	tk.ClearProcedureRes()
	tk.MustExec("call p1(4)")
	require.Equal(t, 2, len(tk.Res))
	tk.Res[0].Check(testkit.Rows("4"))
	tk.Res[1].Check(testkit.Rows("5"))
	tk.ClearProcedureRes()

	// test cache variable
	tk.MustExec(`create procedure p2(id int) begin
	declare id1 int default @a;
	select id1;
	set id1 = id ;
	begin
	declare id3 int default @b;
	select id1;
	select id3;
	end;
	end`)
	tk.MustExec("set @a = 4,@b = 5;")
	tk.MustExec("call p2(2)")
	require.Equal(t, 3, len(tk.Res))
	tk.Res[0].Check(testkit.Rows("4"))
	tk.Res[1].Check(testkit.Rows("2"))
	tk.Res[2].Check(testkit.Rows("5"))
	tk.ClearProcedureRes()

	tk.MustExec("call p2(3)")
	require.Equal(t, 3, len(tk.Res))
	tk.Res[0].Check(testkit.Rows("4"))
	tk.Res[1].Check(testkit.Rows("3"))
	tk.Res[2].Check(testkit.Rows("5"))
	tk.ClearProcedureRes()

	tk.MustExec("set @a = 14,@b = 15;")
	tk.MustExec("call p2(3)")
	require.Equal(t, 3, len(tk.Res))
	tk.Res[0].Check(testkit.Rows("14"))
	tk.Res[1].Check(testkit.Rows("3"))
	tk.Res[2].Check(testkit.Rows("15"))
	tk.ClearProcedureRes()

	tk.MustExec("set @a = 24,@b = 25;")
	tk.MustExec("call p2(13)")
	require.Equal(t, 3, len(tk.Res))
	tk.Res[0].Check(testkit.Rows("24"))
	tk.Res[1].Check(testkit.Rows("13"))
	tk.Res[2].Check(testkit.Rows("25"))
	tk.ClearProcedureRes()

	// out variable
	tk.MustExec(`create procedure p3(id int,out id2 int) begin
	declare id1 int default @a;
	set id1 = id ;
	begin
	declare id3 int default @b;
	set id2 = id1*2 + id3;
	end;
	end`)
	tk.MustExec("set @a = 1,@b=2;")
	tk.MustExec("call p3(1,@c)")
	tk.MustQuery("select @c").Check(testkit.Rows("4"))
	tk.MustExec("call p3(1,@c)")
	tk.MustQuery("select @c").Check(testkit.Rows("4"))
	tk.MustExec("call p3(4,@c)")
	tk.MustQuery("select @c").Check(testkit.Rows("10"))

	// test if block
	tk.MustExec(`create procedure p4(id int) begin
	declare id1 int default @a;
	begin
	if id > 10 then select 10;
	elseif id = 10 then select 111;
	end if;
	end;
	end`)
	tk.MustExec("call p4(1)")
	require.Equal(t, 0, len(tk.Res))
	tk.ClearProcedureRes()
	tk.MustExec("call p4(10)")
	require.Equal(t, 1, len(tk.Res))
	tk.Res[0].Check(testkit.Rows("111"))
	tk.ClearProcedureRes()
	tk.MustExec("call p4(15)")
	require.Equal(t, 1, len(tk.Res))
	tk.Res[0].Check(testkit.Rows("10"))
	tk.ClearProcedureRes()

	//test while block
	tk.MustExec(`create procedure p5()
	begin
	declare id int default 1;
	begin
	declare continue handler for SQLEXCEPTION select 3;
	while id < (select a from t1) do
	set id = id +1;
	end while;
	end;
	select id;
	end;`)
	tk.MustExec("call p5()")
	require.Equal(t, 1, len(tk.Res))
	tk.Res[0].Check(testkit.Rows("1"))
	tk.ClearProcedureRes()
	tk.MustExec("insert into t1 values(2)")
	tk.MustExec("call p5()")
	require.Equal(t, 1, len(tk.Res))
	tk.Res[0].Check(testkit.Rows("2"))
	tk.ClearProcedureRes()
	tk.MustExec("update t1 set a = 10")
	tk.MustExec("call p5()")
	require.Equal(t, 1, len(tk.Res))
	tk.Res[0].Check(testkit.Rows("10"))
	tk.ClearProcedureRes()
}

func TestProcedureReuseLimit(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.InProcedure()
	tk.MustExec("use test")
	// It's not allowed to execute a prepared statement in such a recursive manner
	tk.MustExec("set max_sp_recursion_depth = 100")
	tk.MustExec("create procedure  t1() begin execute stmt; end;")
	tk.MustExec("prepare stmt from 'call t1';")
	tk.MustGetErrCode("call t1", 1444)
	tk.MustExec("prepare stmt from 'select 1'")
	tk.MustExec("call t1")
	tk.Res[0].Check(testkit.Rows("1"))
	tk.ClearProcedureRes()
	tk.MustExec("create procedure  t2() begin execute stmt1; end;")
	tk.MustExec("prepare stmt1 from 'call t1';")
	tk.MustExec("prepare stmt from 'call t2'")
	tk.MustGetErrCode("call t1", 1444)
	tk.MustExec(`create procedure p1()
	begin
	  prepare stmt from 'select 1 A';
	  execute stmt;
	end `)
	tk.MustExec("prepare stmt from 'call p1()'")
	tk.MustGetErrCode("execute stmt", 1444)
	tk.MustGetErrCode("execute stmt", 1444)

	// Recursive limit
	tk.MustExec("set max_sp_recursion_depth = 0")
	tk.MustExec("prepare stmt from 'call t1';")
	tk.MustGetErrCode("call t1", 1456)
	tk.MustGetErrCode("call t1", 1456)
	tk.MustGetErrCode("call t1", 1456)
	tk.MustExec("prepare stmt1 from 'call t1';")
	tk.MustExec("prepare stmt from 'call t2'")
	tk.MustGetErrCode("call t1", 1456)
}

func TestProcedureDDLUnspportPrepare(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.InProcedure()
	tk.MustExec("use test")
	tk.MustGetErrCode("prepare stmt from 'create procedure  t1() begin execute stmt; end'", 1295)
	tk.MustGetErrCode("prepare stmt from 'alter procedure  t1 comment \"xxxx\"' ", 1295)
	tk.MustGetErrCode("prepare stmt from 'drop procedure  t1'", 1295)
}

func TestProcedureWarnings(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.InProcedure()
	tk.MustExec("use test")
	tk.MustExec(`create procedure p1() BEGIN
    SELECT CAST('6x' as unsigned integer);
    SHOW WARNINGS;
  END `)
	tk.MustExec("call p1")
	require.Equal(t, 2, len(tk.Res))
	tk.Res[0].Check(testkit.Rows("6"))
	tk.Res[1].Check(testkit.Rows("Warning 1292 Truncated incorrect INTEGER value: '6x'"))
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1292 Truncated incorrect INTEGER value: '6x'"))
	tk.ClearProcedureRes()
	// add warnings
	tk.MustExec(`create procedure p2() BEGIN
    SELECT CAST('6x' as unsigned integer);
    SHOW WARNINGS;
	SELECT CAST('7x' as unsigned integer);
    SHOW WARNINGS;
  END `)
	tk.MustExec("call p2")
	require.Equal(t, 4, len(tk.Res))
	tk.Res[0].Check(testkit.Rows("6"))
	tk.Res[1].Check(testkit.Rows("Warning 1292 Truncated incorrect INTEGER value: '6x'"))
	tk.Res[2].Check(testkit.Rows("7"))
	tk.Res[3].Check(testkit.Rows("Warning 1292 Truncated incorrect INTEGER value: '7x'"))
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1292 Truncated incorrect INTEGER value: '7x'"))
	tk.ClearProcedureRes()
	// The content of the error report is consistent with the error report
	tk.MustExec(`create procedure p3() BEGIN
    SELECT CAST('6x' as unsigned integer);
    SHOW WARNINGS;
	SELECT CAST('6x' as unsigned integer);
    SHOW WARNINGS;
  END `)
	tk.MustExec("call p3")
	require.Equal(t, 4, len(tk.Res))
	tk.Res[0].Check(testkit.Rows("6"))
	tk.Res[1].Check(testkit.Rows("Warning 1292 Truncated incorrect INTEGER value: '6x'"))
	tk.Res[2].Check(testkit.Rows("6"))
	tk.Res[3].Check(testkit.Rows("Warning 1292 Truncated incorrect INTEGER value: '6x'"))
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1292 Truncated incorrect INTEGER value: '6x'"))
	tk.ClearProcedureRes()
	// test err ==> warning
	tk.MustExec(`create procedure p4() BEGIN
	declare continue HANDLER for SQLEXCEPTION begin  SHOW WARNINGS;end;
    SELECT CAST('6x' as unsigned integer);
    SHOW WARNINGS;
	SELECT CAST('6x' as unsigned integer);
    SHOW WARNINGS;
	select * from t1;
  END `)
	tk.MustExec("call p4")
	require.Equal(t, 5, len(tk.Res))
	tk.Res[0].Check(testkit.Rows("6"))
	tk.Res[1].Check(testkit.Rows("Warning 1292 Truncated incorrect INTEGER value: '6x'"))
	tk.Res[2].Check(testkit.Rows("6"))
	tk.Res[3].Check(testkit.Rows("Warning 1292 Truncated incorrect INTEGER value: '6x'"))
	tk.Res[4].Check(testkit.Rows("Error 1146 Table 'test.t1' doesn't exist"))
	tk.MustQuery("show warnings").Check(testkit.Rows("Error 1146 Table 'test.t1' doesn't exist"))
	tk.ClearProcedureRes()
}

func checkAllTracker(sessionVars *variable.SessionVars, t *testing.T) {
	if len(sessionVars.MemTracker.GetChildrenForTest()) == 1 {
		require.Equal(t, sessionVars.MemTracker.GetChildrenForTest()[0].Label(), memory.LabelForMemDB)
	} else {
		require.Len(t, sessionVars.MemTracker.GetChildrenForTest(), 0)
	}
	require.Len(t, sessionVars.DiskTracker.GetChildrenForTest(), 0)
}

func TestProcedureFetchIntoShouldSpill(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.InProcedure()
	tk.MustExec("use test")
	tk.MustExec("create table t1(id1 int key,id2 int)")
	checkAllTracker(tk.Session().GetSessionVars(), t)
	tk.MustExec("create procedure p1(start int,num int ) begin declare id int default 0; while id < num do insert into t1 value (start+id,start+id); set id = id+1; end while;   end;")
	tk.MustExec(`create procedure select123() begin declare id,id3,done int default 0; 
	declare s1  CURSOR for select * from t1;
	declare continue handler for SQLSTATE '02000' set done = 0;
	set done = 1; 
	open s1;
	while done do
	fetch s1 into id,id3;
	select  id,id3;
	end while;
	close s1;
	end;`)
	checkAllTracker(tk.Session().GetSessionVars(), t)
	tk.MustExec("call p1(0,100)")
	checkAllTracker(tk.Session().GetSessionVars(), t)
	tk.MustExec("set global tidb_enable_tmp_storage_on_oom = ON")
	tk.MustExec("set global tidb_mem_oom_action = 'CANCEL'")
	defer tk.MustExec("set global tidb_mem_oom_action= DEFAULT")
	tk.MustExec(fmt.Sprintf("set tidb_mem_quota_query=%d", 1))
	tk.MustExec("call select123")
	require.Equal(t, 101, len(tk.Res))
	for i := 0; i < 100; i++ {
		tk.Res[i].Check(testkit.Rows(fmt.Sprintf("%d %d", i, i)))
	}
	tk.ClearProcedureRes()
	checkAllTracker(tk.Session().GetSessionVars(), t)
	tk.MustExec("create table t2(id1 int key,id2 char(30))")
	tk.MustExec("create procedure p2(start int,num int ) begin declare id int default 0; while id < num do insert into t2 value (start+id,CONCAT(\"ssssssd\",start + id)); set id = id+1; end while;   end;")
	tk.MustExec(`create procedure select124() begin declare id,done int default 0; 
	declare id3 char(30) default ""; 
	declare s1  CURSOR for select * from t2;
	declare continue handler for SQLSTATE '02000' set done = 0;
	set done = 1; 
	open s1;
	while done do
	fetch s1 into id,id3;
	select  id,id3;
	end while;
	close s1;
	end;`)
	tk.MustExec("set tidb_mem_quota_query= default")
	checkAllTracker(tk.Session().GetSessionVars(), t)
	tk.MustExec("call p2(0,100)")
	checkAllTracker(tk.Session().GetSessionVars(), t)
	tk.MustExec(fmt.Sprintf("set tidb_mem_quota_query=%d", 1))
	tk.MustExec("call select124")
	require.Equal(t, 101, len(tk.Res))
	for i := 0; i < 100; i++ {
		tk.Res[i].Check(testkit.Rows(fmt.Sprintf("%d ssssssd%d", i, i)))
	}
	tk.ClearProcedureRes()
	checkAllTracker(tk.Session().GetSessionVars(), t)
	tk.MustExec("set tidb_mem_quota_query= default")
	tk.MustExec(`create procedure select125() begin declare id,done,id4,id5 int default 0; 
	declare id3 char(30) default ""; 
	declare s1  CURSOR for select * from t2;
	declare s2  CURSOR for select * from t1;
	declare continue handler for SQLSTATE '02000' set done = 0;
	set done = 1; 
	open s1;
	open s2;
	while done do
	fetch s1 into id,id3;
	select  id,id3;
	fetch s2 into id4,id5;
	select id4,id5;
	end while;
	close s1;
	close s2;
	end;`)
	tk.MustExec(fmt.Sprintf("set tidb_mem_quota_query=%d", 1))
	tk.MustExec("call select125")
	require.Equal(t, 202, len(tk.Res))
	for i := 0; i < 100; i++ {
		tk.Res[2*i].Check(testkit.Rows(fmt.Sprintf("%d ssssssd%d", i, i)))
		tk.Res[2*i+1].Check(testkit.Rows(fmt.Sprintf("%d %d", i, i)))
	}
	tk.ClearProcedureRes()
	checkAllTracker(tk.Session().GetSessionVars(), t)
	tk.MustExec(`create procedure select126() begin declare id,done,id4,id5 int default 0; 
	declare id3 char(30) default ""; 
	declare s1  CURSOR for select * from t2;
	declare s2  CURSOR for select * from t1;
	declare continue handler for SQLSTATE '02000' set done = 0;
	set done = 1; 
	open s1;
	open s2;
	while done do
	fetch s1 into id,id3;
	select  id,id3;
	fetch s2 into id4,id5;
	select id4,id5;
	end while;
	close s1;
	end;`)
	tk.MustExec("call select126")
	require.Equal(t, 202, len(tk.Res))
	for i := 0; i < 100; i++ {
		tk.Res[2*i].Check(testkit.Rows(fmt.Sprintf("%d ssssssd%d", i, i)))
		tk.Res[2*i+1].Check(testkit.Rows(fmt.Sprintf("%d %d", i, i)))
	}
	tk.ClearProcedureRes()
	checkAllTracker(tk.Session().GetSessionVars(), t)

	tk.MustExec(`create procedure select127() 
	begin 
	declare id,done,id4,id5 int default 0; 
	declare id3 char(30) default ""; 
	declare s1  CURSOR for select * from t2;
	declare s2  CURSOR for select * from t1;
	declare continue handler for SQLSTATE '02000' set done = 0;
	set done = 1; 
	begin
	open s1;
	open s2;
	end;
	while done do
	fetch s1 into id,id3;
	select  id,id3;
	fetch s2 into id4,id5;
	select id4,id5;
	end while;
	close s1;
	end;`)
	tk.MustExec("call select127")
	require.Equal(t, 202, len(tk.Res))
	for i := 0; i < 100; i++ {
		tk.Res[2*i].Check(testkit.Rows(fmt.Sprintf("%d ssssssd%d", i, i)))
		tk.Res[2*i+1].Check(testkit.Rows(fmt.Sprintf("%d %d", i, i)))
	}
	tk.ClearProcedureRes()
	checkAllTracker(tk.Session().GetSessionVars(), t)

	// Cursor with the same name
	tk.MustExec(`create procedure select128() 
	begin 
	declare id,done,id4,id5 int default 0; 
	declare id3 char(30) default ""; 
	declare s1  CURSOR for select * from t2;
	declare s2  CURSOR for select * from t1;
	declare continue handler for SQLSTATE '02000' set done = 0;
	set done = 1; 
	begin
	open s1;
	open s2;
	end;
	begin
	declare s1  CURSOR for select * from t2;
	open s1;
	end;
	while done do
	fetch s1 into id,id3;
	select  id,id3;
	fetch s2 into id4,id5;
	select id4,id5;
	end while;
	close s1;
	end;`)
	tk.MustExec("call select128")
	require.Equal(t, 202, len(tk.Res))
	for i := 0; i < 100; i++ {
		tk.Res[2*i].Check(testkit.Rows(fmt.Sprintf("%d ssssssd%d", i, i)))
		tk.Res[2*i+1].Check(testkit.Rows(fmt.Sprintf("%d %d", i, i)))
	}
	tk.ClearProcedureRes()
	checkAllTracker(tk.Session().GetSessionVars(), t)

	tk.MustExec(`create procedure select129() 
	begin 
	declare id,done,id4,id5 int default 0; 
	declare id3 char(30) default ""; 
	declare s1  CURSOR for select * from t2 join t1 on t1.id1 = t2.id1; 
	declare continue handler for SQLSTATE '02000' set done = 0;
	set done = 1; 
	begin
	open s1;
	end;
	while done do
	fetch s1 into id,id3,id4,id5;
	select  id,id3,id4,id5;
	end while;
	close s1;
	end;`)
	tk.MustExec("call select129")
	require.Equal(t, 101, len(tk.Res))
	for i := 0; i < 100; i++ {
		tk.Res[i].Check(testkit.Rows(fmt.Sprintf("%d ssssssd%d %d %d", i, i, i, i)))
	}
	tk.ClearProcedureRes()
	checkAllTracker(tk.Session().GetSessionVars(), t)
}

func TestProcedureAgg(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.InProcedure()
	tk.MustExec("use test")
	tk.MustExec(`create table t1 (id1 int key,id2 int)`)
	tk.MustExec("create procedure p1(start int,num int ) begin declare id int default 0; while id < num do insert into t1 value (start+id,start+id); set id = id+1; end while;   end;")
	tk.MustExec("call p1(0,100)")
	tk.MustQuery("select count(*) from t1").Check(testkit.Rows("100"))
	// block
	tk.MustExec(`create procedure baseblock() 
	begin 
	select count(*) from t1;
	end;`)
	tk.MustExec("call baseblock")
	tk.ClearProcedureRes()
	// if block
	tk.MustExec(`create procedure ifblock() 
	begin 
	if (select count(*) from t1) then
	select "now";
	select count(*) from t1;
	end if;
	end;`)
	tk.MustExec("call ifblock")
	require.Equal(t, 2, len(tk.Res))
	tk.ClearProcedureRes()
	tk.MustExec(`create procedure ifblock1() 
	begin 
	if 0 then
	select count(*) from t1;
	elseif (select count(*) from t1) then
	select "now";
	select count(*) from t1;
	end if;
	end;`)
	tk.MustExec("call ifblock1")
	require.Equal(t, 2, len(tk.Res))
	tk.ClearProcedureRes()
	tk.MustExec(`create procedure ifblock2() 
	begin 
	if 0 then
	select count(*) from t1;
	else 
	select "now";
	select count(*) from t1;
	end if;
	end;`)
	tk.MustExec("call ifblock2")
	require.Equal(t, 2, len(tk.Res))
	tk.ClearProcedureRes()

	// case
	tk.MustExec(`create procedure caseblock1() 
	begin 
	case (select count(*) from t1)
	when 100 then select count(*) from t1;
	select "now";
	end case;
	end;`)
	tk.MustExec("call caseblock1")
	require.Equal(t, 2, len(tk.Res))
	tk.ClearProcedureRes()
	tk.MustExec(`create procedure caseblock2() 
	begin 
	case (select count(*) from t1)
	when 101 then select count(*) from t1;
	else 
	select "now";
	select count(*) from t1;
	end case;
	end;`)
	tk.MustExec("call caseblock2")
	require.Equal(t, 2, len(tk.Res))
	tk.ClearProcedureRes()
	tk.MustExec(`create procedure caseblock3() 
	begin 
	case 
	when (select count(*) from t1) then 
	select "now";
	select count(*) from t1;
	end case;
	end;`)
	tk.MustExec("call caseblock3")
	require.Equal(t, 2, len(tk.Res))
	tk.ClearProcedureRes()
	tk.MustExec(`create procedure caseblock4() 
	begin 
	case 
	when (select count(*) from t1) = 102 then select count(*) from t1;
	else 
	select "now";
	select count(*) from t1;
	end case;
	end;`)
	tk.MustExec("call caseblock4")
	require.Equal(t, 2, len(tk.Res))
	tk.ClearProcedureRes()

	//Loop
	tk.MustExec(`create procedure loopblock1() 
	begin 
	t1:loop 
    select count(*) from t1;
	leave t1;
	end loop;
	end;`)
	tk.MustExec("call loopblock1")
	require.Equal(t, 1, len(tk.Res))
	tk.ClearProcedureRes()

	// REPEAT
	tk.MustExec(`create procedure repeatblock1() 
	begin 
	t1:repeat 
    select count(*) from t1;
	until (select count(*) from t1) end repeat;
	end;`)
	tk.MustExec("call repeatblock1")
	require.Equal(t, 1, len(tk.Res))
	tk.ClearProcedureRes()

	// while
	tk.MustExec(`create procedure whilelock1() 
	begin 
	t1:while  (select count(*) from t1) do
    select count(*) from t1;
	leave t1;
	end while;
	end;`)
	tk.MustExec("call whilelock1")
	require.Equal(t, 1, len(tk.Res))
	tk.ClearProcedureRes()

	//cursor
	tk.MustExec(`create procedure cursorlock1() 
	begin 
	declare t1 cursor for select count(*) from t1;
	open t1;
	close t1;
	end;`)
	tk.MustExec("call cursorlock1")
	tk.ClearProcedureRes()

	//handler
	tk.MustExec(`create procedure handlerblock1() 
	begin 
	declare continue handler for SQLEXCEPTION begin select 'error';select count(*) from t1; end;
	select count(*) from t2;
	select count(*) from t1;
	end;`)
	tk.MustExec("call handlerblock1")
	require.Equal(t, 3, len(tk.Res))
	tk.ClearProcedureRes()

	// in
	tk.MustExec(`create procedure par(a int)begin select a; end;`)
	tk.MustExec("call par((select count(*)from t1))")
	require.Equal(t, 1, len(tk.Res))
	tk.Res[0].Check(testkit.Rows("100"))
	tk.ClearProcedureRes()
}

func TestProcedureAddCollate(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.InProcedure()
	tk.MustExec("use test")
	tk.MustExec("create table t1(id1 int key,id2 int)")
	checkAllTracker(tk.Session().GetSessionVars(), t)
	tk.MustExec("create procedure p1(start int,num int ) begin declare id int default 0; while id < num do insert into t1 value (start+id,start+id); set id = id+1; end while;end;")
	tk.MustExec("call p1(0,100)")
	tk.MustContainErrMsg("create Procedure t1(id int collate utf8mb4_bin) begin end", "This version of TiDB doesn't yet support 'COLLATE with no CHARACTER SET in SP parameters, RETURNS, DECLARE'")
	tk.MustContainErrMsg("create Procedure t1() begin declare id int collate utf8mb4_bin; end", "This version of TiDB doesn't yet support 'COLLATE with no CHARACTER SET in SP parameters, RETURNS, DECLARE'")
	// collate mismatches character set
	tk.MustContainErrMsg("create Procedure t1(id varchar(30) character set utf8 collate utf8mb4_bin) begin end", "COLLATION 'utf8mb4_bin' is not valid for CHARACTER SET 'utf8'")
	tk.MustContainErrMsg("create Procedure t1() begin declare id varchar(30) character set utf8 collate utf8mb4_bin; end", "COLLATION 'utf8mb4_bin' is not valid for CHARACTER SET 'utf8'")
	// vaild collate
	tk.MustExec("create procedure t1(id varchar(30) character set utf8mb4 collate utf8mb4_bin) begin declare id varchar(30) character set utf8mb4 collate utf8mb4_bin; end;")
	tk.MustQuery("show create procedure t1").Check(testkit.Rows("t1 ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION  CREATE PROCEDURE `t1`(id varchar(30) character set utf8mb4 collate utf8mb4_bin)\nbegin declare id varchar(30) character set utf8mb4 collate utf8mb4_bin; end utf8mb4 utf8mb4_bin utf8mb4_bin"))
	tk.MustExec("create procedure t2(id char(30) character set utf8mb4 collate utf8mb4_bin) begin declare id char(30) character set utf8mb4 collate utf8mb4_bin; end;")
	tk.MustQuery("show create procedure t2").Check(testkit.Rows("t2 ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION  CREATE PROCEDURE `t2`(id char(30) character set utf8mb4 collate utf8mb4_bin)\nbegin declare id char(30) character set utf8mb4 collate utf8mb4_bin; end utf8mb4 utf8mb4_bin utf8mb4_bin"))
	tk.MustExec("create procedure t3(id nchar(30) character set utf8mb4 collate utf8mb4_bin) begin declare id nchar(30) character set utf8mb4 collate utf8mb4_bin; end;")
	tk.MustQuery("show create procedure t3").Check(testkit.Rows("t3 ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION  CREATE PROCEDURE `t3`(id nchar(30) character set utf8mb4 collate utf8mb4_bin)\nbegin declare id nchar(30) character set utf8mb4 collate utf8mb4_bin; end utf8mb4 utf8mb4_bin utf8mb4_bin"))
	tk.MustExec("create procedure t4(id nchar(30) character set utf8mb4 collate utf8mb4_general_ci) begin declare id nchar(30) character set utf8mb4 collate utf8mb4_general_ci; end;")
	tk.MustQuery("show create procedure t4").Check(testkit.Rows("t4 ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION  CREATE PROCEDURE `t4`(id nchar(30) character set utf8mb4 collate utf8mb4_general_ci)\nbegin declare id nchar(30) character set utf8mb4 collate utf8mb4_general_ci; end utf8mb4 utf8mb4_bin utf8mb4_bin"))
	tk.MustExec(`create procedure t5(id1 char(10) character set utf8mb4 collate utf8mb4_general_ci,id2 char(10) character set utf8mb4 collate utf8mb4_general_ci)
	begin
	select id1=id2; 
	end`)
	tk.MustExec("call t5('a','A')")
	tk.Res[0].Check(testkit.Rows("1"))
	tk.ClearProcedureRes()
	tk.MustExec(`create procedure t6(id1 char(10) character set utf8mb4 collate utf8mb4_bin,id2 char(10) character set utf8mb4 collate utf8mb4_bin)
	begin
	select id1=id2; 
	end`)
	tk.MustExec("call t6('a','A')")
	tk.Res[0].Check(testkit.Rows("0"))
	tk.ClearProcedureRes()

	tk.MustExec(`create procedure t7()
	begin
	declare id1,id2 char(10) character set utf8mb4 collate utf8mb4_general_ci;
	set id1 = 'A',id2 = 'a';
	select id1=id2; 
	end`)
	tk.MustExec("call t7()")
	tk.Res[0].Check(testkit.Rows("1"))
	tk.ClearProcedureRes()
	tk.MustExec(`create procedure t8()
	begin
	declare id1,id2 char(10) character set utf8mb4 collate utf8mb4_bin;
	set id1 = 'A',id2 = 'a';
	select id1=id2; 
	end`)
	tk.MustExec("call t8()")
	tk.Res[0].Check(testkit.Rows("0"))
	tk.ClearProcedureRes()

	tk.MustExec(`create procedure t9(id2 char(10) character set utf8mb4 collate utf8mb4_general_ci)
	begin
	declare id1 char(10) character set utf8mb4 collate utf8mb4_general_ci;
	set id1 = 'A';
	select id1=id2; 
	end`)
	tk.MustExec("call t9('a')")
	tk.Res[0].Check(testkit.Rows("1"))
	tk.ClearProcedureRes()
	tk.MustExec(`create procedure t10(id2 char(10) character set utf8mb4 collate utf8mb4_bin)
	begin
	declare id1 char(10) character set utf8mb4 collate utf8mb4_bin;
	set id1 = 'A';
	select id1=id2; 
	end`)
	tk.MustExec("call t10('a')")
	tk.Res[0].Check(testkit.Rows("0"))
	tk.ClearProcedureRes()
}

func TestProcedureRecursiveCall(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustQuery("show variables like '%tidb_enable_sp_ast_cache%'").Check(testkit.Rows("tidb_enable_sp_ast_cache ON"))
	tk.InProcedure()
	procedureRecusrsiveCall(tk, t)
	tk.MustExec("drop database test")
	tk.MustExec("create database test")
	tk.MustExec("use test")
	tk.MustExec("set global tidb_enable_sp_ast_cache = OFF")
	tk.MustQuery("show variables like '%tidb_enable_sp_ast_cache%'").Check(testkit.Rows("tidb_enable_sp_ast_cache OFF"))
	procedureRecusrsiveCall(tk, t)
}

func procedureRecusrsiveCall(tk *testkit.TestKit, t *testing.T) {
	tk.MustExec("use test")
	tk.MustExec(`create table t1 (id1 int key,id2 int)`)
	tk.MustExec("create procedure p1(start int,num int ) begin declare id int default 0; while id < num do insert into t1 value (start+id,start+id); set id = id+1; end while;   end;")
	tk.MustExec("call p1(0,100)")
	// test in out param
	tk.MustExec(`create procedure t1(id int) begin call t2(id); select id;  end;`)
	tk.MustExec(`create procedure t2(out id int) begin set id = 3 ; end;`)
	tk.MustExec("call t1(1)")
	require.Equal(t, 1, len(tk.Res))
	tk.Res[0].Check(testkit.Rows("3"))
	tk.ClearProcedureRes()
	tk.MustExec("call t1(1)")
	require.Equal(t, 1, len(tk.Res))
	tk.Res[0].Check(testkit.Rows("3"))
	tk.ClearProcedureRes()
	tk.MustExec(`drop procedure t2;`)
	tk.MustExec(`create procedure t2(inout id int) begin set id = id+1 ; end;`)
	tk.MustExec("call t1(1)")
	require.Equal(t, 1, len(tk.Res))
	tk.Res[0].Check(testkit.Rows("2"))
	tk.ClearProcedureRes()
	tk.MustExec("call t1(3)")
	require.Equal(t, 1, len(tk.Res))
	tk.Res[0].Check(testkit.Rows("4"))
	tk.ClearProcedureRes()
	tk.MustExec(`drop procedure t2;`)
	tk.MustExec(`create procedure t2(id int) begin set id = id+1 ; end;`)
	tk.MustExec("call t1(1)")
	require.Equal(t, 1, len(tk.Res))
	tk.Res[0].Check(testkit.Rows("1"))
	tk.ClearProcedureRes()
	tk.MustExec("call t1(3)")
	require.Equal(t, 1, len(tk.Res))
	tk.Res[0].Check(testkit.Rows("3"))
	tk.ClearProcedureRes()
	tk.MustExec(`drop procedure t1;`)
	tk.MustExec(`drop procedure t2;`)
	tk.MustExec(`create procedure t2(in id int) begin set id = id+1 ; select id;end;`)
	tk.MustExec(`create procedure t1(id int) begin call t2(id+1); select id;  end;`)
	tk.MustExec("call t1(1)")
	require.Equal(t, 2, len(tk.Res))
	tk.Res[0].Check(testkit.Rows("3"))
	tk.Res[1].Check(testkit.Rows("1"))
	tk.ClearProcedureRes()
	tk.MustGetErrCode("call t1(a+1)", 1054)

	// test recursive
	tk.MustExec(`create procedure t3(id int) begin declare num int default 1 ; if num < id then  set id = id -1; call t3(id); select id; end if;  end;`)
	tk.MustExec("call t3(1)")
	require.Equal(t, 0, len(tk.Res))
	tk.ClearProcedureRes()
	tk.MustGetErrCode("call t3(2)", 1456)
	tk.MustExec("set max_sp_recursion_depth = 1")
	require.Equal(t, 0, len(tk.Res))
	tk.ClearProcedureRes()
	tk.MustExec("call t3(2)")
	require.Equal(t, 1, len(tk.Res))
	tk.ClearProcedureRes()
	tk.MustGetErrCode("call t3(3)", 1456)
	tk.MustExec("set max_sp_recursion_depth = 2")
	tk.MustExec("call t3(3)")
	require.Equal(t, 2, len(tk.Res))
	tk.ClearProcedureRes()
	tk.MustExec("set max_sp_recursion_depth = default")

	// test type conversion
	tk.MustExec(`create procedure t4(inout id varchar(30)) begin call t5(id);select id;end;`)
	tk.MustExec(`create procedure t5(id int) begin set id = id+1; select id;end;`)
	tk.MustExec(`set @a ='111';`)
	tk.MustExec(`call t4(@a)`)
	require.Equal(t, 2, len(tk.Res))
	tk.Res[0].Check(testkit.Rows("112"))
	tk.Res[1].Check(testkit.Rows("111"))
	tk.ClearProcedureRes()
	tk.MustQuery("select @a").Check(testkit.Rows("111"))
	tk.MustExec(`set @a ='111xx';`)
	tk.MustGetErrCode(`call t4(@a)`, 1292)
	tk.ClearProcedureRes()

	tk.MustExec(`set @a ='111.11';`)
	tk.MustExec(`call t4(@a)`)
	tk.Res[0].Check(testkit.Rows("112"))
	tk.Res[1].Check(testkit.Rows("111.11"))
	tk.ClearProcedureRes()

	tk.MustExec(`drop procedure t5;`)
	tk.MustExec(`create procedure t5(inout id datetime) begin set id = id+1; select id;end;`)
	tk.MustExec(`set @a ="2023-11-16 08:10:58";`)
	tk.MustExec(`call t4(@a)`)
	require.Equal(t, 2, len(tk.Res))
	tk.Res[0].Check(testkit.Rows("2023-11-16 08:10:59"))
	tk.Res[1].Check(testkit.Rows("2023-11-16 08:10:59"))
	tk.ClearProcedureRes()
	tk.MustExec(`set @a ="2023-11-16";`)
	tk.MustExec(`call t4(@a)`)
	require.Equal(t, 2, len(tk.Res))
	tk.Res[0].Check(testkit.Rows("2023-11-16 00:00:01"))
	tk.Res[1].Check(testkit.Rows("2023-11-16 00:00:01"))
	tk.ClearProcedureRes()
	tk.MustExec(`set @a ="2023-11-31";`)
	tk.MustGetErrCode(`call t4(@a)`, 1292)
	tk.ClearProcedureRes()

	// handler test
	tk.MustExec(`create procedure t6(id int) begin declare continue handler for SQLEXCEPTION select 'error1'; 
	call t7(id);select id; end; `)
	tk.MustExec(`create procedure t7(inout id datetime) begin declare continue handler for SQLEXCEPTION select 'error2'; set id = 12 ;select id;end;`)
	tk.MustExec(`call t6(1112)`)
	require.Equal(t, 3, len(tk.Res))
	tk.Res[0].Check(testkit.Rows("error2"))
	tk.Res[1].Check(testkit.Rows("<nil>"))
	tk.Res[2].Check(testkit.Rows("<nil>"))
	tk.ClearProcedureRes()
	tk.MustExec(`call t6(1112)`)
	require.Equal(t, 3, len(tk.Res))
	tk.Res[0].Check(testkit.Rows("error2"))
	tk.Res[1].Check(testkit.Rows("<nil>"))
	tk.Res[2].Check(testkit.Rows("<nil>"))
	tk.ClearProcedureRes()
	tk.MustExec(`drop procedure t7`)
	tk.MustExec(`create procedure t7(inout id datetime) begin  set id = 12 ;select id;end;`)
	tk.MustExec(`call t6(1111)`)
	require.Equal(t, 2, len(tk.Res))
	tk.Res[0].Check(testkit.Rows("error1"))
	tk.Res[1].Check(testkit.Rows("1111"))
	tk.ClearProcedureRes()

	tk.MustExec(`drop procedure t6`)
	tk.MustExec(`create procedure t6(id varchar(10)) 
	begin declare continue handler for SQLEXCEPTION select 'error1'; 
	call t7(id);select id; end; `)
	tk.MustExec(`call t6("111x2")`)
	require.Equal(t, 2, len(tk.Res))
	tk.Res[0].Check(testkit.Rows("error1"))
	tk.Res[1].Check(testkit.Rows("111x2"))
	tk.ClearProcedureRes()

	tk.MustExec(`call t6("111x2")`)
	require.Equal(t, 2, len(tk.Res))
	tk.Res[0].Check(testkit.Rows("error1"))
	tk.Res[1].Check(testkit.Rows("111x2"))
	tk.ClearProcedureRes()

	tk.MustExec(`call t6("111x3")`)
	require.Equal(t, 2, len(tk.Res))
	tk.Res[0].Check(testkit.Rows("error1"))
	tk.Res[1].Check(testkit.Rows("111x3"))
	tk.ClearProcedureRes()

	// cursor
	tk.MustExec(`create procedure t8() 
	begin
	declare id1,id2 int;
	declare s1 CURSOR FOR  select * from t1;
	open s1;
	call t9();
	fetch s1 into id1,id2;
	select id1,id2;
	close s1;
	end`)
	tk.MustExec(`create procedure t9() 
	begin
	declare id1,id2 int;
	declare s1 CURSOR FOR  select * from t1;
	select 't9';
	open s1;
	fetch s1 into id1,id2;
	select id1,id2;
	close s1;
	end`)
	tk.MustExec(`call t8()`)
	require.Equal(t, 3, len(tk.Res))
	tk.Res[0].Check(testkit.Rows("t9"))
	tk.Res[1].Check(testkit.Rows("0 0"))
	tk.Res[2].Check(testkit.Rows("0 0"))
	tk.ClearProcedureRes()

	// more conversion
	// Unable to read parent stored procedure variable
	tk.MustExec(`create procedure tt1() begin call tt2(a); end;`)
	tk.MustExec(`create procedure tt2(id int) begin select id; end;`)
	tk.MustExec(`create procedure tt3() begin declare id int default 0;call tt1(); end;`)
	tk.MustGetErrCode(`call tt3`, 1054)

	tk.MustExec(`drop procedure tt1`)
	tk.MustExec(`create procedure tt1(inout a int) begin call tt2(a); set a = 22; end;`)
	tk.MustExec(`create procedure tt4() begin declare id int default 0;call tt1(id); select id;end;`)
	tk.MustExec(`call tt4`)
	require.Equal(t, 2, len(tk.Res))
	tk.Res[0].Check(testkit.Rows("0"))
	tk.Res[1].Check(testkit.Rows("22"))
	tk.ClearProcedureRes()
}

func ProcedureViewCall(tk *testkit.TestKit, t *testing.T) {
	require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "localhost"}, nil, nil, nil))
	tk.MustExec("use test")
	tk.MustExec(`create table t1 (id int key)`)
	tk.MustExec(`insert into t1 values (1),(2),(3)`)
	tk.MustExec(`create view xx as select * from t1;`)
	tk.MustExec("create procedure t1() begin select * from xx; end;")
	tk.MustExec("call t1")
	tk.Res[0].Check(testkit.Rows("1", "2", "3"))
	tk.ClearProcedureRes()
	tk.MustExec("call t1")
	tk.Res[0].Check(testkit.Rows("1", "2", "3"))
	tk.ClearProcedureRes()
	tk.MustExec(`drop view xx ;`)
	tk.MustExec(`create table t2 (id char(22) key)`)
	tk.MustExec(`insert into t2 values ("a"),("b"),("c")`)
	tk.MustExec(`create view xx as select * from t2;`)
	tk.MustExec("call t1")
	tk.Res[0].Check(testkit.Rows("a", "b", "c"))
	tk.ClearProcedureRes()
	tk.MustExec("call t1")
	tk.Res[0].Check(testkit.Rows("a", "b", "c"))
	tk.ClearProcedureRes()
}

func TestProcedureViewCall(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustQuery("show variables like '%tidb_enable_sp_ast_cache%'").Check(testkit.Rows("tidb_enable_sp_ast_cache ON"))
	tk.InProcedure()
	ProcedureViewCall(tk, t)
	tk.MustExec("drop database test")
	tk.MustExec("create database test")
	tk.MustExec("use test")
	tk.MustExec("set global tidb_enable_sp_ast_cache = OFF")
	tk.MustQuery("show variables like '%tidb_enable_sp_ast_cache%'").Check(testkit.Rows("tidb_enable_sp_ast_cache OFF"))
	ProcedureViewCall(tk, t)
}

func TestProcedureHanderErrorSQL(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.InProcedure()
	tk.MustExec("use test")
	tk.MustExec(`create table t1 (id int key)`)
	tk.MustExec(`create procedure t2(id int) begin
	declare continue HANDLER for SQLEXCEPTION select @@sp_last_error_sql;
	insert into t2 value (id);
	insert into t2 value (id);
	end`)
	// can read insert.
	tk.MustExec("call t2(2)")
	require.Equal(t, 2, len(tk.Res))
	tk.Res[0].Check(testkit.Rows("[ERROR] execute SQL: insert into t2 value (id). [local variables[id:2]]"))
	tk.Res[1].Check(testkit.Rows("[ERROR] execute SQL: insert into t2 value (id). [local variables[id:2]]"))
	tk.ClearProcedureRes()
	tk.MustExec("call t2(3)")
	require.Equal(t, 2, len(tk.Res))
	tk.Res[0].Check(testkit.Rows("[ERROR] execute SQL: insert into t2 value (id). [local variables[id:3]]"))
	tk.Res[1].Check(testkit.Rows("[ERROR] execute SQL: insert into t2 value (id). [local variables[id:3]]"))
	tk.ClearProcedureRes()
	tk.MustExec(`create procedure t3(id int) begin
	declare continue HANDLER for SQLEXCEPTION select @@sp_last_error_sql;
	select count(*) from t2 where x > id;
	insert into t2 value (id);
	end`)
	// can read select
	tk.MustExec("call t3(3)")
	require.Equal(t, 2, len(tk.Res))
	tk.Res[0].Check(testkit.Rows("[ERROR] execute SQL: select count(*) from t2 where x > id. [local variables[id:3]]"))
	tk.Res[1].Check(testkit.Rows("[ERROR] execute SQL: insert into t2 value (id). [local variables[id:3]]"))
	tk.ClearProcedureRes()
	tk.MustExec("call t3(4)")
	require.Equal(t, 2, len(tk.Res))
	tk.Res[0].Check(testkit.Rows("[ERROR] execute SQL: select count(*) from t2 where x > id. [local variables[id:4]]"))
	tk.Res[1].Check(testkit.Rows("[ERROR] execute SQL: insert into t2 value (id). [local variables[id:4]]"))
	tk.ClearProcedureRes()

	tk.MustExec(`create procedure t4(id int) begin
	declare continue HANDLER for SQLEXCEPTION select @@sp_last_error_sql;
	delete from t2 where x > id;
	end`)
	// can read delete
	tk.MustExec("call t4(3)")
	require.Equal(t, 1, len(tk.Res))
	tk.Res[0].Check(testkit.Rows("[ERROR] execute SQL: delete from t2 where x > id. [local variables[id:3]]"))
	tk.ClearProcedureRes()
	tk.MustExec("call t4(4)")
	require.Equal(t, 1, len(tk.Res))
	tk.Res[0].Check(testkit.Rows("[ERROR] execute SQL: delete from t2 where x > id. [local variables[id:4]]"))
	tk.ClearProcedureRes()
	tk.MustQuery("select @@sp_last_error_sql").Check(testkit.Rows(""))

	tk.MustExec(`create procedure t14(id int) begin
	declare continue HANDLER for SQLEXCEPTION select @@sp_last_error_sql;
	select * from (select count(*) from t2) a;
	end`)
	tk.MustExec("call t14(3)")
	require.Equal(t, 1, len(tk.Res))
	tk.Res[0].Check(testkit.Rows("[ERROR] execute SQL: select * from (select count(*) from t2) a."))
	tk.ClearProcedureRes()
	tk.MustExec("call t4(4)")
	require.Equal(t, 1, len(tk.Res))
	tk.Res[0].Check(testkit.Rows("[ERROR] execute SQL: delete from t2 where x > id. [local variables[id:4]]"))
	tk.ClearProcedureRes()
	tk.MustQuery("select @@sp_last_error_sql").Check(testkit.Rows(""))

	// test cache warnings
	tk.MustExec(`create procedure t5(id char(10)) begin
	declare continue HANDLER for SQLWARNING select @@sp_last_error_sql;
	SELECT CAST(id as unsigned integer);
	end`)
	tk.MustExec("call t5('sswd')")
	require.Equal(t, 2, len(tk.Res))
	tk.Res[0].Check(testkit.Rows("0"))
	tk.Res[1].Check(testkit.Rows("[WARNING] execute SQL: SELECT CAST(id as unsigned integer). [local variables[id:sswd]]"))
	tk.ClearProcedureRes()
	tk.MustQuery("select @@sp_last_error_sql").Check(testkit.Rows(""))
	tk.MustExec(`create procedure t6(id char(10)) begin
	declare continue HANDLER for SQLWARNING select @@sp_last_error_sql;
	SELECT CAST(id as unsigned integer);
	select @@sp_last_error_sql;
	end`)
	tk.MustExec("call t6('sswd')")
	require.Equal(t, 3, len(tk.Res))
	tk.Res[0].Check(testkit.Rows("0"))
	tk.Res[1].Check(testkit.Rows("[WARNING] execute SQL: SELECT CAST(id as unsigned integer). [local variables[id:sswd]]"))
	tk.Res[2].Check(testkit.Rows("[WARNING] execute SQL: SELECT CAST(id as unsigned integer). [local variables[id:sswd]]"))
	tk.ClearProcedureRes()
	tk.MustQuery("select @@sp_last_error_sql").Check(testkit.Rows(""))
	tk.MustExec("call t6('中国')")
	require.Equal(t, 3, len(tk.Res))
	tk.Res[0].Check(testkit.Rows("0"))
	tk.Res[1].Check(testkit.Rows("[WARNING] execute SQL: SELECT CAST(id as unsigned integer). [local variables[id:中国]]"))
	tk.Res[2].Check(testkit.Rows("[WARNING] execute SQL: SELECT CAST(id as unsigned integer). [local variables[id:中国]]"))
	tk.ClearProcedureRes()
	tk.MustQuery("select @@sp_last_error_sql").Check(testkit.Rows(""))

	//error in handler
	tk.MustExec(`create procedure t7(id char(10)) begin
	declare continue HANDLER for SQLWARNING begin 
	declare continue HANDLER for SQLEXCEPTION select @@sp_last_error_sql;
	select @@sp_last_error_sql;
    select * from t2 where id = a;
	end;
	SELECT CAST(id as unsigned integer);
	select @@sp_last_error_sql;
	end`)
	tk.MustExec("call t7('顿顿')")
	require.Equal(t, 4, len(tk.Res))
	tk.Res[0].Check(testkit.Rows("0"))
	tk.Res[1].Check(testkit.Rows("[WARNING] execute SQL: SELECT CAST(id as unsigned integer). [local variables[id:顿顿]]"))
	tk.Res[2].Check(testkit.Rows("[ERROR] execute SQL: select * from t2 where id = a. [local variables[id:顿顿]]"))
	tk.Res[3].Check(testkit.Rows("[ERROR] execute SQL: select * from t2 where id = a. [local variables[id:顿顿]]"))
	tk.ClearProcedureRes()
	tk.MustQuery("select @@sp_last_error_sql").Check(testkit.Rows(""))

	// error in call
	tk.MustExec(`create procedure t8(id char(10)) begin
	declare continue HANDLER for SQLEXCEPTION select @@sp_last_error_sql;
	call t9(id);
	end`)
	tk.MustExec(`create procedure t9(id char(10))  begin begin
	declare continue HANDLER for SQLEXCEPTION select @@sp_last_error_sql;
	select * from t2 where id = a; 
	end;
	select * from t2 where id = a; 
	end`)
	tk.MustExec("call t8('顿顿')")
	require.Equal(t, 2, len(tk.Res))
	tk.Res[0].Check(testkit.Rows("[ERROR] execute SQL: select * from t2 where id = a. [local variables[id:顿顿]]"))
	tk.Res[1].Check(testkit.Rows("[ERROR] execute SQL: call t9(id). [local variables[id:顿顿]]"))
	tk.ClearProcedureRes()

	tk.MustExec(`create procedure t10(id char(10)) begin
	declare continue HANDLER for SQLEXCEPTION select @@sp_last_error_sql;
	select *,id from t2 where id = a;
	select * into id from t2 where id = a ;
	end`)
	tk.MustExec("call t10('xx')")
	require.Equal(t, 2, len(tk.Res))
	tk.Res[0].Check(testkit.Rows("[ERROR] execute SQL: select *,id from t2 where id = a. [local variables[id:xx,id:xx]]"))
	tk.Res[1].Check(testkit.Rows("[ERROR] execute SQL: select * into id from t2 where id = a. [local variables[id:xx]]"))
	tk.ClearProcedureRes()

	// test if
	tk.MustExec(`create procedure if1(id char(10)) begin
	declare continue HANDLER for SQLEXCEPTION select @@sp_last_error_sql;
	if (select count(*) from t2) then select 1;
	end if;
	end`)
	tk.MustExec("call if1('xx')")
	require.Equal(t, 1, len(tk.Res))
	tk.Res[0].Check(testkit.Rows("[ERROR] Using (select count(*) from t2) in if statements."))
	tk.ClearProcedureRes()

	tk.MustExec(`create procedure if2(id char(10)) begin
	declare continue HANDLER for SQLEXCEPTION select @@sp_last_error_sql;
	if 1 then 
	select count(*),id from t2;
	end if;
	end`)
	tk.MustExec("call if2('xx')")
	require.Equal(t, 1, len(tk.Res))
	tk.Res[0].Check(testkit.Rows("[ERROR] execute SQL: select count(*),id from t2. [local variables[id:xx]]"))
	tk.ClearProcedureRes()

	tk.MustExec(`create procedure if3(id char(10)) begin
	declare continue HANDLER for SQLEXCEPTION select @@sp_last_error_sql;
	if 0 then 
	select 1;
	else 
	select count(*),id from t2;
	end if;
	end`)
	tk.MustExec("call if3('xx')")
	require.Equal(t, 1, len(tk.Res))
	tk.Res[0].Check(testkit.Rows("[ERROR] execute SQL: select count(*),id from t2. [local variables[id:xx]]"))
	tk.ClearProcedureRes()

	// test case
	tk.MustExec(`create procedure case1(id char(10)) begin
	declare continue HANDLER for SQLEXCEPTION select @@sp_last_error_sql;
	case (select count(*) from t2) when 1 then 
	select 1;
	end case;
	end`)
	tk.MustExec("call case1('xx')")
	require.Equal(t, 1, len(tk.Res))
	tk.Res[0].Check(testkit.Rows("[ERROR] Using case_expr:(select count(*) from t2), when_expr:1 in case statements."))
	tk.ClearProcedureRes()

	tk.MustExec(`create procedure case2(id char(10)) begin
	declare continue HANDLER for SQLEXCEPTION select @@sp_last_error_sql;
	case 1 when (select count(*) from t2 where a = id) then 
	select 1;
	end case;
	end`)
	tk.MustExec("call case2('xx')")
	require.Equal(t, 1, len(tk.Res))
	tk.Res[0].Check(testkit.Rows("[ERROR] Using case_expr:1, when_expr:(select count(*) from t2 where a = id) in case statements. when local variables[id:xx]"))
	tk.ClearProcedureRes()

	tk.MustExec(`create procedure case3(id char(10)) begin
	declare continue HANDLER for SQLEXCEPTION select @@sp_last_error_sql;
	case 1 when 1 then 
	select count(*) from t2 where a = id;
	end case;
	end`)
	tk.MustExec("call case3('xx')")
	require.Equal(t, 1, len(tk.Res))
	tk.Res[0].Check(testkit.Rows("[ERROR] execute SQL: select count(*) from t2 where a = id. [local variables[id:xx]]"))
	tk.ClearProcedureRes()

	tk.MustExec(`create procedure case4(id char(10)) begin
	declare continue HANDLER for SQLEXCEPTION select @@sp_last_error_sql;
	case 1 when 2 then 
	select count(*) from t2 where a = id;
	end case;
	end`)
	tk.MustExec("call case4('xx')")
	require.Equal(t, 1, len(tk.Res))
	tk.Res[0].Check(testkit.Rows("[ERROR] Using case_expr:1 in case statements."))
	tk.ClearProcedureRes()

	tk.MustExec(`create procedure case5(id char(10)) begin
	declare continue HANDLER for SQLEXCEPTION select @@sp_last_error_sql;
	case when (select count(*) from t2 where a = id) then 
		select 2;
	end case;
	end`)
	tk.MustExec("call case5('xx')")
	require.Equal(t, 1, len(tk.Res))
	tk.Res[0].Check(testkit.Rows("[ERROR] Using search_expr:(select count(*) from t2 where a = id) in case statements. [local variables[id:xx]]"))
	tk.ClearProcedureRes()

	tk.MustExec(`create procedure case6(id char(10)) begin
	declare continue HANDLER for SQLEXCEPTION select @@sp_last_error_sql;
	case when 1 then 
	select count(*) from t2 where a = id;
	end case;
	end`)
	tk.MustExec("call case6('xx')")
	require.Equal(t, 1, len(tk.Res))
	tk.Res[0].Check(testkit.Rows("[ERROR] execute SQL: select count(*) from t2 where a = id. [local variables[id:xx]]"))
	tk.ClearProcedureRes()

	tk.MustExec(`create procedure case7(id char(10)) begin
	declare continue HANDLER for SQLEXCEPTION select @@sp_last_error_sql;
	case when 0 then 
	select count(*) from t2 where a = id;
	end case;
	end`)
	tk.MustExec("call case7('xx')")
	require.Equal(t, 1, len(tk.Res))
	tk.Res[0].Check(testkit.Rows("[ERROR] no case meet operating conditions in case statements."))
	tk.ClearProcedureRes()

	// default var
	tk.MustExec(`create procedure var1(id char(10)) begin
	declare continue HANDLER for SQLEXCEPTION select @@sp_last_error_sql;
	begin
	declare id varchar(30) default (select count(*) from t2 where a = id);
	end;
	end`)
	tk.MustExec("call var1('xx')")
	require.Equal(t, 1, len(tk.Res))
	tk.Res[0].Check(testkit.Rows("[ERROR] Initialize id using (select count(*) from t2 where a = id)"))
	tk.ClearProcedureRes()

	// open cursor
	tk.MustExec(`create procedure curs1(id char(10)) begin
	declare continue HANDLER for SQLEXCEPTION select @@sp_last_error_sql;
	begin
	declare id CURSOR FOR select count(*) from t2 where a = id;
	open id;
	end;
	end`)
	tk.MustExec("call curs1('xx')")
	require.Equal(t, 1, len(tk.Res))
	tk.Res[0].Check(testkit.Rows("[ERROR] open cursor id"))
	tk.ClearProcedureRes()

	// close cursor
	tk.MustExec(`create procedure curs2(id char(10)) begin
	declare continue HANDLER for SQLEXCEPTION select @@sp_last_error_sql;
	begin
	declare id CURSOR FOR select count(*) from t2 where a = id;
	close id;
	end;
	end`)
	tk.MustExec("call curs2('xx')")
	require.Equal(t, 1, len(tk.Res))
	tk.Res[0].Check(testkit.Rows("[ERROR] close cursor id"))
	tk.ClearProcedureRes()

	// fetch into
	tk.MustExec(`create procedure curs3(id char(10)) begin
	declare continue HANDLER for SQLEXCEPTION select @@sp_last_error_sql;
	begin
	declare id CURSOR FOR select 1;
	fetch id into id; 
	end;
	end`)
	tk.MustExec("call curs3('xx')")
	require.Equal(t, 1, len(tk.Res))
	tk.Res[0].Check(testkit.Rows("[ERROR] fetch id into id"))
	tk.ClearProcedureRes()

}
