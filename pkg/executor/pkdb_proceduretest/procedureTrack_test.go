package procedure_test

import (
	"testing"

	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func checkTracker(t *testing.T, vars *variable.SessionVars) {
	if len(vars.MemTracker.GetChildrenForTest()) == 1 {
		require.Equal(t, vars.MemTracker.GetChildrenForTest()[0].Label(), -28)
	} else {
		require.Len(t, vars.MemTracker.GetChildrenForTest(), 0)
	}
	require.Len(t, vars.DiskTracker.GetChildrenForTest(), 0)
}
func TestProcedureTrack(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.InProcedure()
	tk.MustExec("use test")
	tk.MustExec("create table t1(id1 int key,id2 int)")
	checkTracker(t, tk.Session().GetSessionVars())
	tk.MustExec("create procedure t1() begin end")
	checkTracker(t, tk.Session().GetSessionVars())
	tk.MustExec("call t1")
	tk.MustExec("create procedure t2(id1 int,id2 int) begin end")
	checkTracker(t, tk.Session().GetSessionVars())
	tk.MustExec("call t2(1,1)")
	checkTracker(t, tk.Session().GetSessionVars())
	tk.MustExec("create procedure p1(start int,num int ) begin declare id int default 0; while id < num do insert into t1 value (start+id,start+id); set id = id+1; end while;   end;")
	tk.MustExec("call p1(1,1000)")
	checkTracker(t, tk.Session().GetSessionVars())
	tk.MustExec(`create procedure select1(start int) begin 
	select * from t1;select * from t1 where id1 > start;select * from t1 where id2 > start;
	select * from t1 where id1 > start && id2 < start +2; 
	select * from (select * from t1 ) as a;select * from t1 union (select * from t1);
	select * from t1 join (select * from t1 where id1 > start) as a on t1.id2 = a.id2;
	end;`)
	tk.MustExec("call select1(3)")
	tk.ClearProcedureRes()
	checkTracker(t, tk.Session().GetSessionVars())
	tk.MustExec(`create procedure select2(start int) begin 
	declare id,id3,done int default 0; 
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
	checkTracker(t, tk.Session().GetSessionVars())
	tk.MustExec("call select2(3)")
	checkTracker(t, tk.Session().GetSessionVars())
	tk.ClearProcedureRes()

	tk.MustExec(`create procedure select3(start int) begin 
	if start > 10 then select * from t1 where id1 = 10;  elseif start < 100 then select * from t1 where id1 = 100; 
	else select * from t1;  end if;  end`)
	checkTracker(t, tk.Session().GetSessionVars())
	tk.MustExec("call select3(3)")
	checkTracker(t, tk.Session().GetSessionVars())
	tk.ClearProcedureRes()
	tk.MustExec("call select3(30)")
	checkTracker(t, tk.Session().GetSessionVars())
	tk.ClearProcedureRes()
	tk.MustExec("call select3(300)")
	checkTracker(t, tk.Session().GetSessionVars())
	tk.ClearProcedureRes()

	tk.MustExec(`create procedure select4(start int) begin 
	case start%4 
	when 0 then select * from t1 where id1%3 = 0;
	when 1 then select * from t1 where id1%3 = 1;
	else select * from t1;
	end case; end`)
	checkTracker(t, tk.Session().GetSessionVars())
	tk.MustExec("call select4(0)")
	checkTracker(t, tk.Session().GetSessionVars())
	tk.ClearProcedureRes()
	tk.MustExec("call select4(1)")
	checkTracker(t, tk.Session().GetSessionVars())
	tk.ClearProcedureRes()
	tk.MustExec("call select4(2)")
	checkTracker(t, tk.Session().GetSessionVars())
	tk.ClearProcedureRes()
	tk.MustExec("call select4(3)")
	checkTracker(t, tk.Session().GetSessionVars())
	tk.ClearProcedureRes()

	tk.MustExec(`create procedure select5(start int)  
	t1:begin 
		REPEAT 
		select * from t1;
		set start = start+1;
		if start = 20 then leave t1; end if;
		until start > 30 end repeat;
	end;`)
	checkTracker(t, tk.Session().GetSessionVars())
	tk.MustExec("call select5(3)")
	checkTracker(t, tk.Session().GetSessionVars())
	tk.ClearProcedureRes()
	tk.MustExec("call select5(19)")
	checkTracker(t, tk.Session().GetSessionVars())
	tk.ClearProcedureRes()
	tk.MustExec("call select5(21)")
	checkTracker(t, tk.Session().GetSessionVars())
	tk.ClearProcedureRes()

	tk.MustExec(`create procedure select6(start int)  
	t1:begin 
		t2:REPEAT 
		select * from t1;
		set start = start+1;
		if start = 20 then set start = 21; ITERATE t2; end if;
		until start > 30 end repeat;
	end;`)
	checkTracker(t, tk.Session().GetSessionVars())
	tk.MustExec("call select6(3)")
	checkTracker(t, tk.Session().GetSessionVars())
	tk.ClearProcedureRes()
	tk.MustExec("call select6(19)")
	checkTracker(t, tk.Session().GetSessionVars())
	tk.ClearProcedureRes()
	tk.MustExec("call select6(21)")
	checkTracker(t, tk.Session().GetSessionVars())
	tk.ClearProcedureRes()
}
