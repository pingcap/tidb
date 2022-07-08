// Copyright 2016 PingCAP, Inc.
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

package ddl_test

import (
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/pingcap/tidb/ddl"
	lit "github.com/pingcap/tidb/ddl/lightning"
	"github.com/pingcap/tidb/domain"
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
	lit.GlobalEnv.SetMinQuota()
	store, _, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
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
	lit.GlobalEnv.SetMinQuota()
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
