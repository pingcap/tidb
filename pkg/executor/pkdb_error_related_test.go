package executor_test

import (
	"testing"

	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func TestSignalWarnings(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("signal SQLSTATE '01000'")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1642 Unhandled user-defined warning condition"))
	tk.MustContainErrMsg("signal SQLSTATE '00000'", "[planner:1407]Bad SQLSTATE: '00000'")
	tk.MustExec("signal SQLSTATE '01003'")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1642 Unhandled user-defined warning condition"))
	tk.MustExec("signal SQLSTATE '01000' set MYSQL_ERRNO = 1234")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1234 Unhandled user-defined warning condition"))
	tk.MustExec("signal SQLSTATE '01000' set MYSQL_ERRNO = 1234, MESSAGE_TEXT= 'xxx'")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1234 xxx"))
	tk.MustExec("set @as = '112234'")
	tk.MustExec("signal SQLSTATE '01000' set MYSQL_ERRNO = 1234, MESSAGE_TEXT= @as")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1234 112234"))
	tk.MustExec("signal SQLSTATE '01000' set MYSQL_ERRNO = 1234, MESSAGE_TEXT= @@mpp_version")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1234 UNSPECIFIED"))
	tk.MustContainErrMsg("signal SQLSTATE '01000' set MYSQL_ERRNO = 1234, MESSAGE_TEXT= a+b", "You have an error in your SQL syntax; check the manual that corresponds to your TiDB version for the right syntax")
	tk.MustContainErrMsg("signal SQLSTATE '01000' set MYSQL_ERRNO = 1234, MESSAGE_TEXT= (select 1)", "You have an error in your SQL syntax; check the manual that corresponds to your TiDB version for the right syntax")
	tk.MustContainErrMsg("signal SQLSTATE '01003' set MYSQL_ERRNO = 1234,MYSQL_ERRNO = 1234", "[planner:1641]Duplicate condition information item 'MYSQL_ERRNO'")
	tk.MustContainErrMsg("signal SQLSTATE '01003' set MYSQL_ERRNO = 1234,CLASS_ORIGIN = '1234xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx'", "[executor:1648]Data too long for condition item 'CLASS_ORIGIN'")
}

func TestSignalError(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustGetErrCode("signal SQLSTATE '02000'", 1643)
	tk.MustGetErrCode("signal SQLSTATE '03000'", 1644)
	tk.MustGetErrCode("signal SQLSTATE '13000'", 1644)
	tk.MustContainErrMsg("signal SQLSTATE '02000' set MESSAGE_TEXT= 'xwsfq' ", "xwsfq")
	tk.MustContainErrMsg(`signal SQLSTATE '02000' set MESSAGE_TEXT= 'xwsfq', MYSQL_ERRNO =  1234 ,COLUMN_NAME = 'xswasz',CONSTRAINT_NAME = 'xxxsas',CLASS_ORIGIN = 'xxxxx',SUBCLASS_ORIGIN= 'xxxxx',
	CONSTRAINT_CATALOG='xzzasa',CONSTRAINT_SCHEMA='xxxzsaa',CATALOG_NAME='xxzsaa',SCHEMA_NAME='xZas',TABLE_NAME='xZsa',CURSOR_NAME='xazass'`, "xwsfq")
}

func TestProcedureSignal(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.InProcedure()
	tk.MustExec("use test")
	tk.MustExec("create procedure t1() begin signal SQLSTATE '03000'; end;")
	tk.MustContainErrMsg("call t1", "Unhandled user-defined exception condition")
	tk.MustExec("create procedure t2(id int) begin declare EXIT HANDLER for 3456 select 'get error'; signal SQLSTATE '03000' set MYSQL_ERRNO = id; end;")
	tk.MustExec("call t2(3456)")
	require.Equal(t, len(tk.Res), 1)
	tk.Res[0].Check(testkit.Rows("get error"))
	tk.ClearProcedureRes()
	tk.MustContainErrMsg("call t2(3457)", "Unhandled user-defined exception condition")
	tk.MustExec("create procedure t3(id int) begin declare EXIT HANDLER for not found select 'get error'; signal SQLSTATE '02000' set MYSQL_ERRNO = id; end;")
	tk.MustExec("call t3(3456)")
	require.Equal(t, len(tk.Res), 1)
	tk.Res[0].Check(testkit.Rows("get error"))
	tk.ClearProcedureRes()
	tk.MustExec("create procedure t4(id int) begin declare EXIT HANDLER for not found select 'get error'; signal SQLSTATE '04000' set MYSQL_ERRNO = id, MESSAGE_TEXT = 'xxxxx'; end;")
	tk.MustContainErrMsg("call t4(3457)", "xxxxx")
	tk.ClearProcedureRes()
	tk.MustExec("create procedure t5(id int) begin declare EXIT HANDLER for SQLEXCEPTION select 'get error'; signal SQLSTATE '04000' set MYSQL_ERRNO = id, MESSAGE_TEXT = 'xxxxx'; end;")
	tk.MustExec("call t5(3456)")
	require.Equal(t, len(tk.Res), 1)
	tk.Res[0].Check(testkit.Rows("get error"))
	tk.ClearProcedureRes()
}

func TestProcedureSignalConLen(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set @a = concat(repeat('A', 64), 'X')")
	tk.MustContainErrMsg("signal SQLSTATE '01003' set CLASS_ORIGIN = @a", "[executor:1648]Data too long for condition item 'CLASS_ORIGIN'")
	tk.MustExec("set @b = concat(repeat('A', 128), 'X')")
	tk.MustContainErrMsg("signal SQLSTATE '02003' set MESSAGE_TEXT = @b", "[executor:1648]Data too long for condition item 'MESSAGE_TEXT'")
	tk.MustExec("set sql_mode=''")
	tk.MustExec("signal SQLSTATE '01003' set CLASS_ORIGIN = @a")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1647 Data truncated for condition item 'CLASS_ORIGIN'", "Warning 1642 Unhandled user-defined warning condition"))
	tk.MustContainErrMsg("signal SQLSTATE '02003' set MESSAGE_TEXT = @b", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1647 Data truncated for condition item 'MESSAGE_TEXT'",
		"Error 1643 AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"))
}

func TestGetDiagnostics(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustGetErrCode("get STACKED Diagnostics @a = ROW_COUNT", mysql.ErrGetStackedDaWithoutActiveHandler)
	tk.MustGetErrCode("get STACKED Diagnostics CONDITION 1 @a = MESSAGE_TEXT", mysql.ErrGetStackedDaWithoutActiveHandler)
	tk.MustGetErrCode("get  Diagnostics a = ROW_COUNT", mysql.ErrUnknownSystemVariable)
	tk.MustGetErrCode("get  Diagnostics CONDITION 1 a = MESSAGE_TEXT", mysql.ErrUnknownSystemVariable)
	tk.MustExec("create table t1 (id int )")
	tk.MustExec("insert into t1 value (1)")
	tk.MustExec("get  Diagnostics @a = ROW_COUNT ,@b = number")
	tk.MustQuery("select @a,@b").Check(testkit.Rows("1 0"))
	tk.MustQuery("select CAST('6x' as unsigned integer),CAST('6x' as unsigned integer),CAST('6x' as unsigned integer)")
	tk.MustExec("get  Diagnostics @a = ROW_COUNT ,@b = number")
	tk.MustExec("get  Diagnostics @a = ROW_COUNT ,@b = number")
	tk.MustExec("get  Diagnostics @a = ROW_COUNT ,@b = number")
	tk.MustQuery("select @a,@b").Check(testkit.Rows("-1 3"))
	tk.MustQuery("select CAST('6x' as unsigned integer),CAST('6x' as unsigned integer),CAST('6x' as unsigned integer)")
	tk.MustExec("get  Diagnostics CONDITION 1 @a = MESSAGE_TEXT ,@b = RETURNED_SQLSTATE,@c = MYSQL_ERRNO")
	tk.MustQuery("select @a,@b,@c").Check(testkit.Rows("Truncated incorrect INTEGER value: '6x' 22007 1292"))
	tk.MustExec("get  Diagnostics CONDITION \"1\" @a = MESSAGE_TEXT ,@b = RETURNED_SQLSTATE,@c = MYSQL_ERRNO")
	tk.MustQuery("select @a,@b,@c").Check(testkit.Rows("Truncated incorrect INTEGER value: '6x' 22007 1292"))
	tk.MustExec("get  Diagnostics CONDITION 0 @d = MESSAGE_TEXT ,@e = RETURNED_SQLSTATE,@f = MYSQL_ERRNO")
	tk.MustQuery("select @d,@e,@f").Check(testkit.Rows("<nil> <nil> <nil>"))
	tk.MustQuery("select CAST('6x' as unsigned integer),CAST('6x' as unsigned integer),CAST('6x' as unsigned integer)")
	tk.MustExec("get  Diagnostics CONDITION 1 @a = CLASS_ORIGIN ,@b = SUBCLASS_ORIGIN,@c = CONSTRAINT_CATALOG")
	tk.MustQuery("select @a,@b,@c").Check(testkit.Rows("ISO 9075 ISO 9075 "))
	tk.MustQuery("select CAST('6x' as unsigned integer)")
	tk.MustExec("get  Diagnostics CONDITION 2 @a = CLASS_ORIGIN ,@b = SUBCLASS_ORIGIN,@c = CONSTRAINT_CATALOG")
	tk.MustExec("get  Diagnostics CONDITION 3 @a = CLASS_ORIGIN ,@b = SUBCLASS_ORIGIN,@c = CONSTRAINT_CATALOG")
	tk.MustExec("get  Diagnostics CONDITION 1 @a = CLASS_ORIGIN ,@b = SUBCLASS_ORIGIN,@c =MYSQL_ERRNO, @d= MESSAGE_TEXT,@e = RETURNED_SQLSTATE")
	tk.MustExec("get  Diagnostics CONDITION 2 @a1 = CLASS_ORIGIN ,@b1 = SUBCLASS_ORIGIN,@c1 =MYSQL_ERRNO, @d1= MESSAGE_TEXT,@e1 = RETURNED_SQLSTATE")
	tk.MustQuery("select @a,@b,@c,@d,@e").Check(testkit.Rows("ISO 9075 ISO 9075 1292 Truncated incorrect INTEGER value: '6x' 22007"))
	tk.MustQuery("select @a1,@b1,@c1,@d1,@e1").Check(testkit.Rows("ISO 9075 ISO 9075 1758 Invalid condition number 35000"))
}

func TestSignalGetDiagnostics(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("signal SQLSTATE '01000'")
	tk.MustExec("get  Diagnostics CONDITION 1 @d = MESSAGE_TEXT ,@e = RETURNED_SQLSTATE,@f = MYSQL_ERRNO")
	tk.MustQuery("select @d,@e,@f").Check(testkit.Rows("Unhandled user-defined warning condition 01000 1642"))
	tk.MustExec("signal SQLSTATE '01000' set MESSAGE_TEXT = 'xxx',MYSQL_ERRNO=1233")
	tk.MustExec("get  Diagnostics CONDITION 1 @d = MESSAGE_TEXT ,@e = RETURNED_SQLSTATE,@f = MYSQL_ERRNO,@a=CLASS_ORIGIN")
	tk.MustQuery("select @d,@e,@f,@a").Check(testkit.Rows("xxx 01000 1233 "))
	tk.MustExec("signal SQLSTATE '01000' set MESSAGE_TEXT = 'xxx',MYSQL_ERRNO=1233,CLASS_ORIGIN='xxx',SUBCLASS_ORIGIN='xxa12', CONSTRAINT_CATALOG= 'ssa1',CONSTRAINT_SCHEMA='xa2wq',CONSTRAINT_NAME='xaw2',CATALOG_NAME='xaq',SCHEMA_NAME='xa2q',TABLE_NAME='xa2q',COLUMN_NAME='asawq',CURSOR_NAME='asea'")
	tk.MustExec("get  Diagnostics CONDITION 1 @a1 = MYSQL_ERRNO ,@a2 = RETURNED_SQLSTATE,@a3 = CLASS_ORIGIN,@a4 =SUBCLASS_ORIGIN,@a5 = CONSTRAINT_CATALOG,@a6= CONSTRAINT_SCHEMA,@a7 = CONSTRAINT_NAME, @a8 = CATALOG_NAME, @a9 = SCHEMA_NAME, @a10 = TABLE_NAME, @a11 = COLUMN_NAME, @a12 = CURSOR_NAME")
	tk.MustQuery("select @a1,@a2,@a3,@a4,@a5,@a6,@a7,@a8,@a9,@a10,@a11,@a12").Check(testkit.Rows("1233 01000 xxx xxa12 ssa1 xa2wq xaw2 xaq xa2q xa2q asawq asea"))
}

func TestGetDiagnosticsInProcedure(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.InProcedure()
	tk.MustExec("create procedure p() begin get STACKED Diagnostics @a = ROW_COUNT; end;")
	tk.MustGetErrCode("call p", mysql.ErrGetStackedDaWithoutActiveHandler)
	tk.MustExec("create procedure p1(a int) begin declare CONTINUE handler for SQLWARNING begin get STACKED Diagnostics a = ROW_COUNT, @a=number;  get current Diagnostics @c = ROW_COUNT, @d=number; select @a,a, @c,@d;end; SELECT CAST('6x' as unsigned integer);end;")
	tk.MustExec("call p1(1)")
	require.Equal(t, len(tk.Res), 2)
	tk.Res[0].Check(testkit.Rows("6"))
	tk.Res[1].Check(testkit.Rows("1 -1 -1 1"))
	tk.ClearProcedureRes()
	tk.MustExec("create procedure p2(a int) begin declare CONTINUE handler for SQLWARNING begin get STACKED Diagnostics CONDITION 1 a = MYSQL_ERRNO, @a = MESSAGE_TEXT, @b = RETURNED_SQLSTATE;  get current Diagnostics CONDITION 1 @a1 = MYSQL_ERRNO, @b1 = MESSAGE_TEXT, @c1 = RETURNED_SQLSTATE; select @a,a, @b,@a1,@b1,@c1;end; SELECT CAST('6x' as unsigned integer);end;")
	tk.MustExec("call p2(1)")
	require.Equal(t, len(tk.Res), 2)
	tk.Res[0].Check(testkit.Rows("6"))
	tk.Res[1].Check(testkit.Rows("Truncated incorrect INTEGER value: '6x' 1292 22007 1292 Truncated incorrect INTEGER value: '6x' 22007"))
	tk.ClearProcedureRes()
	tk.MustExec("create procedure p3(a int) begin declare exit handler for SQLWARNING begin get STACKED Diagnostics a = ROW_COUNT, @a=number;  get current Diagnostics @c = ROW_COUNT, @d=number; select @a,a, @c,@d;end; SELECT CAST('6x' as unsigned integer);end;")
	tk.MustExec("call p3(1)")
	require.Equal(t, len(tk.Res), 2)
	tk.Res[0].Check(testkit.Rows("6"))
	tk.Res[1].Check(testkit.Rows("1 -1 -1 1"))
	tk.ClearProcedureRes()
	tk.MustExec("create procedure p4(a int) begin declare exit handler for SQLWARNING begin get STACKED Diagnostics CONDITION 1 a = MYSQL_ERRNO, @a = MESSAGE_TEXT, @b = RETURNED_SQLSTATE;  get current Diagnostics CONDITION 1 @a1 = MYSQL_ERRNO, @b1 = MESSAGE_TEXT, @c1 = RETURNED_SQLSTATE; select @a,a, @b,@a1,@b1,@c1;end; SELECT CAST('6x' as unsigned integer);end;")
	tk.MustExec("call p4(1)")
	require.Equal(t, len(tk.Res), 2)
	tk.Res[0].Check(testkit.Rows("6"))
	tk.Res[1].Check(testkit.Rows("Truncated incorrect INTEGER value: '6x' 1292 22007 1292 Truncated incorrect INTEGER value: '6x' 22007"))
	tk.ClearProcedureRes()
	tk.MustExec(`create procedure p5(a int)
	 begin declare CONTINUE handler for SQLWARNING begin get STACKED Diagnostics CONDITION 1 a = MYSQL_ERRNO, @a = MESSAGE_TEXT, @b = RETURNED_SQLSTATE;  get current Diagnostics CONDITION 1 @a1 = MYSQL_ERRNO, @b1 = MESSAGE_TEXT, @c1 = RETURNED_SQLSTATE; select @a,a, @b,@a1,@b1,@c1;end; SELECT CAST('6x' as unsigned integer);get STACKED Diagnostics CONDITION 1 a = MYSQL_ERRNO, @a = MESSAGE_TEXT, @b = RETURNED_SQLSTATE;end;`)
	tk.MustGetErrCode("call p5(1)", mysql.ErrGetStackedDaWithoutActiveHandler)
	require.Equal(t, len(tk.Res), 2)
	tk.Res[0].Check(testkit.Rows("6"))
	tk.Res[1].Check(testkit.Rows("Truncated incorrect INTEGER value: '6x' 1292 22007 1292 Truncated incorrect INTEGER value: '6x' 22007"))
	tk.ClearProcedureRes()
	tk.MustExec(`create procedure p6(a int)
	 begin declare CONTINUE handler for SQLWARNING begin get STACKED Diagnostics CONDITION 1 a = MYSQL_ERRNO, @a = MESSAGE_TEXT, @b = RETURNED_SQLSTATE;  
	 get current Diagnostics CONDITION 1 @a1 = MYSQL_ERRNO, @b1 = MESSAGE_TEXT, @c1 = RETURNED_SQLSTATE; 
	 select @a,a, @b,@a1,@b1,@c1;
	 get current Diagnostics CONDITION 1 @a1 = MYSQL_ERRNO, @b1 = MESSAGE_TEXT, @c1 = RETURNED_SQLSTATE;
	 get STACKED Diagnostics CONDITION 1 a = MYSQL_ERRNO, @a = MESSAGE_TEXT, @b = RETURNED_SQLSTATE;
	 select @a,a, @b,@a1,@b1,@c1; end; 
	 SELECT CAST('6x' as unsigned integer);end;`)
	tk.MustExec("call p6(1)")
	require.Equal(t, len(tk.Res), 3)
	tk.Res[0].Check(testkit.Rows("6"))
	tk.Res[1].Check(testkit.Rows("Truncated incorrect INTEGER value: '6x' 1292 22007 1292 Truncated incorrect INTEGER value: '6x' 22007"))
	tk.Res[2].Check(testkit.Rows("Truncated incorrect INTEGER value: '6x' 1292 22007 1292 Truncated incorrect INTEGER value: '6x' 22007"))
	tk.ClearProcedureRes()
	tk.MustExec(`create procedure p7(a int)
	 begin declare CONTINUE handler for SQLWARNING begin 
	 	declare CONTINUE handler for SQLEXCEPTION begin 
		 get STACKED Diagnostics CONDITION 1 a = MYSQL_ERRNO, @a = MESSAGE_TEXT, @b = RETURNED_SQLSTATE;  
		 get current Diagnostics CONDITION 1 @a1 = MYSQL_ERRNO, @b1 = MESSAGE_TEXT, @c1 = RETURNED_SQLSTATE; 
		 select @a,a, @b,@a1,@b1,@c1;
		end;
	 get STACKED Diagnostics CONDITION 1 a = MYSQL_ERRNO, @a = MESSAGE_TEXT, @b = RETURNED_SQLSTATE;  
	 get current Diagnostics CONDITION 1 @a1 = MYSQL_ERRNO, @b1 = MESSAGE_TEXT, @c1 = RETURNED_SQLSTATE; 
	 select @a,a, @b,@a1,@b1,@c1;
	 signal SQLSTATE '05000' set MYSQL_ERRNO = 1234, MESSAGE_TEXT= 'xxxxxs';
	 get current Diagnostics CONDITION 1 @a1 = MYSQL_ERRNO, @b1 = MESSAGE_TEXT, @c1 = RETURNED_SQLSTATE;
	 get STACKED Diagnostics CONDITION 1 a = MYSQL_ERRNO, @a = MESSAGE_TEXT, @b = RETURNED_SQLSTATE;
	 select @a,a, @b,@a1,@b1,@c1; end; 
	 SELECT CAST('6x' as unsigned integer);end;`)
	tk.MustExec("call p7(1)")
	require.Equal(t, len(tk.Res), 4)
	tk.Res[0].Check(testkit.Rows("6"))
	tk.Res[1].Check(testkit.Rows("Truncated incorrect INTEGER value: '6x' 1292 22007 1292 Truncated incorrect INTEGER value: '6x' 22007"))
	tk.Res[2].Check(testkit.Rows("xxxxxs 1234 05000 1234 xxxxxs 05000"))
	tk.Res[3].Check(testkit.Rows("Truncated incorrect INTEGER value: '6x' 1292 22007 1234 xxxxxs 05000"))
	tk.ClearProcedureRes()
}
