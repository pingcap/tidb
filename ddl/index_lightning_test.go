package ddl_test

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/testkit"
	"github.com/stretchr/testify/require"
)

func testLitAddIndex(tk *testkit.TestKit, t *testing.T, ctx sessionctx.Context, tblID int64, unique bool, indexName string, colName string, dom *domain.Domain) int64 {
	un := ""
	if unique {
		un = "unique"
	}
	sql := fmt.Sprintf("alter table t add %s index %s(%s)", un, indexName, colName)
	tk.MustExec(sql)

	idi, _ := strconv.Atoi(tk.MustQuery("admin show ddl jobs 1;").Rows()[0][0].(string))
	id := int64(idi)
	v := getSchemaVer(t, ctx)
	require.NoError(t, dom.Reload())
	tblInfo, exist := dom.InfoSchema().TableByID(tblID)
	require.True(t, exist)
	checkHistoryJobArgs(t, ctx, id, &historyJobArgs{ver: v, tbl: tblInfo.Meta()})
	return id
}

func TestEnableLightning(t *testing.T) {
	var options []ddl.Option
	store, _, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	ic := infoschema.NewCache(2)
	ic.Insert(infoschema.MockInfoSchemaWithSchemaVer(nil, 0), 0)
	options = append(options, ddl.WithInfoCache(ic))
	d := ddl.NewDDL(context.Background(), options...)
	err := d.Start(nil)
	
	// Check default value, Current is off
	allow := ddl.IsAllowFastDDL()
    require.Equal(t, false, allow)
    // Set illedge value
	err := tk.ExecToErr("set @@global.tidb_fast_ddl = abc")
	require.Error(t, err)
	allow = ddl.IsAllowFastDDL()
    require.Equal(t, false, allow)

	// set to on
	tk.MustExec("set @@global.tidb_fast_ddl = on")
	allow = ddl.IsAllowFastDDL()
	require.Equal(t, true, allow)
}

func TestAddIndexLit(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()
	ddl.SetWaitTimeWhenErrorOccurred(1 * time.Microsecond)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (c1 int primary key, c2 int, c3 int)")
	tk.MustExec("insert t values (1, 1, 1), (2, 2, 2), (3, 3, 1);")
	tk.MustExec("set @@global.tidb_fast_ddl = on")

	var tableID int64
	rs := tk.MustQuery("select TIDB_TABLE_ID from information_schema.tables where table_name='t' and table_schema='test';")
	tableIDi, _ := strconv.Atoi(rs.Rows()[0][0].(string))
	tableID = int64(tableIDi)

	// Non-unique secondary index
	jobID := testLitAddIndex(tk, t, testNewContext(store), tableID, false, "idx1", "c2", dom)
	testCheckJobDone(t, store, jobID, true)

	// Unique secondary index
	jobID = testLitAddIndex(tk, t, testNewContext(store), tableID, true, "idx2", "c2", dom)
	testCheckJobDone(t, store, jobID, true)

	// Unique duplicate key
	err := tk.ExecToErr("alter table t1 add index unique idx3(c3)")
	require.Error(t, err)
}