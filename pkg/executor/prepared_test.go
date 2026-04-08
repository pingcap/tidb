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

package executor_test

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/parser/terror"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/session/sessmgr"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testdata"
	"github.com/stretchr/testify/require"
)

func TestPlanCacheWithDifferentVariableTypes(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`set tidb_enable_prepared_plan_cache=1`)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1, t2")
	tk.MustExec("set @@tidb_enable_collect_execution_info=0;")
	tk.MustExec("create table t1(a varchar(20), b int, c float, key(b, a))")
	tk.MustExec("insert into t1 values('1',1,1.1),('2',2,222),('3',3,333)")
	tk.MustExec("create table t2(a varchar(20), b int, c float, key(b, a))")
	tk.MustExec("insert into t2 values('3',3,3.3),('2',2,222),('3',3,333)")

	var input []struct {
		PrepareStmt string
		Executes    []struct {
			Vars []struct {
				Name  string
				Value string
			}
			ExecuteSQL string
		}
	}
	var output []struct {
		PrepareStmt string
		Executes    []struct {
			SQL  string
			Vars []struct {
				Name  string
				Value string
			}
			Plan             []string
			LastPlanUseCache string
			Result           []string
		}
	}
	prepareMergeSuiteData.LoadTestCases(t, &input, &output)
	for i, tt := range input {
		tk.MustExec(tt.PrepareStmt)
		testdata.OnRecord(func() {
			output[i].PrepareStmt = tt.PrepareStmt
			output[i].Executes = make([]struct {
				SQL  string
				Vars []struct {
					Name  string
					Value string
				}
				Plan             []string
				LastPlanUseCache string
				Result           []string
			}, len(tt.Executes))
		})
		require.Equal(t, tt.PrepareStmt, output[i].PrepareStmt)
		for j, exec := range tt.Executes {
			for _, v := range exec.Vars {
				tk.MustExec(fmt.Sprintf(`set @%s = %s`, v.Name, v.Value))
			}
			res := tk.MustQuery(exec.ExecuteSQL)
			lastPlanUseCache := tk.MustQuery("select @@last_plan_from_cache").Rows()[0][0]
			tk.MustQuery(exec.ExecuteSQL)
			tkProcess := tk.Session().ShowProcess()
			ps := []*sessmgr.ProcessInfo{tkProcess}
			tk.Session().SetSessionManager(&testkit.MockSessionManager{PS: ps})
			plan := tk.MustQuery(fmt.Sprintf("explain for connection %d", tkProcess.ID))
			testdata.OnRecord(func() {
				output[i].Executes[j].SQL = exec.ExecuteSQL
				output[i].Executes[j].Plan = testdata.ConvertRowsToStrings(plan.Rows())
				output[i].Executes[j].Vars = exec.Vars
				output[i].Executes[j].LastPlanUseCache = lastPlanUseCache.(string)
				output[i].Executes[j].Result = testdata.ConvertRowsToStrings(res.Rows())
			})

			require.Equal(t, exec.ExecuteSQL, output[i].Executes[j].SQL)
			plan.Check(testkit.Rows(output[i].Executes[j].Plan...))
			require.Equal(t, exec.Vars, output[i].Executes[j].Vars)
			require.Equal(t, lastPlanUseCache.(string), output[i].Executes[j].LastPlanUseCache)
			res.Check(testkit.Rows(output[i].Executes[j].Result...))
		}
	}
}

func TestParameterPushDown(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`set tidb_enable_prepared_plan_cache=1`)
	tk.MustExec(`use test`)
	tk.MustExec(`drop table if exists t`)
	tk.MustExec(`create table t (a int, b int, c int, key(a))`)
	tk.MustExec(`insert into t values (1, 1, 1), (2, 2, 2), (3, 3, 3), (4, 4, 4), (5, 5, 5), (6, 6, 6)`)
	tk.MustExec("set @@tidb_enable_collect_execution_info=0;")
	tk.MustExec(`set @x1=1,@x5=5,@x10=10,@x20=20`)

	var input []struct {
		SQL string
	}
	var output []struct {
		Result    []string
		Plan      []string
		FromCache string
	}
	prepareMergeSuiteData.LoadTestCases(t, &input, &output)

	for i, tt := range input {
		if strings.HasPrefix(tt.SQL, "execute") {
			res := tk.MustQuery(tt.SQL).Sort()
			fromCache := tk.MustQuery("select @@last_plan_from_cache")
			tk.MustQuery(tt.SQL)
			tkProcess := tk.Session().ShowProcess()
			ps := []*sessmgr.ProcessInfo{tkProcess}
			tk.Session().SetSessionManager(&testkit.MockSessionManager{PS: ps})
			plan := tk.MustQuery(fmt.Sprintf("explain for connection %d", tkProcess.ID))

			testdata.OnRecord(func() {
				output[i].Result = testdata.ConvertRowsToStrings(res.Rows())
				output[i].Plan = testdata.ConvertRowsToStrings(plan.Rows())
				output[i].FromCache = fromCache.Rows()[0][0].(string)
			})

			res.Check(testkit.Rows(output[i].Result...))
			plan.Check(testkit.Rows(output[i].Plan...))
			require.Equal(t, fromCache.Rows()[0][0].(string), output[i].FromCache)
		} else {
			tk.MustExec(tt.SQL)
			testdata.OnRecord(func() {
				output[i].Result = nil
			})
		}
	}
}

func TestPrepareStmtAfterIsolationReadChange(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`set tidb_enable_prepared_plan_cache=0`)
	tk.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "localhost", CurrentUser: true, AuthUsername: "root", AuthHostname: "%"}, nil, []byte("012345678901234567890"), nil)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int)")
	tk.MustExec("set @@session.tidb_allow_tiflash_cop=ON")

	// create virtual tiflash replica.
	is := dom.InfoSchema()
	tbl, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t"))
	require.NoError(t, err)
	tbl.Meta().TiFlashReplica = &model.TiFlashReplicaInfo{
		Count:     1,
		Available: true,
	}

	tk.MustExec("set @@session.tidb_isolation_read_engines='tikv'")
	tk.MustExec("set @@tidb_enable_collect_execution_info=0;")
	tk.MustExec("prepare stmt from \"select * from t\"")
	tk.MustQuery("execute stmt")
	tkProcess := tk.Session().ShowProcess()
	ps := []*sessmgr.ProcessInfo{tkProcess}
	tk.Session().SetSessionManager(&testkit.MockSessionManager{PS: ps})
	rows := tk.MustQuery(fmt.Sprintf("explain for connection %d", tkProcess.ID)).Rows()
	require.Equal(t, "cop[tikv]", rows[len(rows)-1][2])

	tk.MustExec("set @@session.tidb_isolation_read_engines='tiflash'")
	// allowing mpp will generate mpp[tiflash] plan, the test framework will time out due to
	// "retry for TiFlash peer with region missing", so disable mpp mode to use cop mode instead.
	tk.MustExec("set @@session.tidb_allow_mpp=0")
	tk.MustExec("execute stmt")
	tkProcess = tk.Session().ShowProcess()
	ps = []*sessmgr.ProcessInfo{tkProcess}
	tk.Session().SetSessionManager(&testkit.MockSessionManager{PS: ps})
	rows = tk.MustQuery(fmt.Sprintf("explain for connection %d", tkProcess.ID)).Rows()
	require.Equal(t, rows[len(rows)-1][2], "cop[tiflash]")

	require.Equal(t, 1, len(tk.Session().GetSessionVars().PreparedStmts))
	require.Equal(t, "select * from `t`", tk.Session().GetSessionVars().PreparedStmts[1].(*plannercore.PlanCacheStmt).NormalizedSQL)
	require.Equal(t, "", tk.Session().GetSessionVars().PreparedStmts[1].(*plannercore.PlanCacheStmt).NormalizedPlan)
}

func TestMaxPreparedStmtCount(t *testing.T) {
	oldVal := atomic.LoadInt64(&variable.PreparedStmtCount)
	atomic.StoreInt64(&variable.PreparedStmtCount, 0)
	defer func() {
		atomic.StoreInt64(&variable.PreparedStmtCount, oldVal)
	}()
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set @@global.max_prepared_stmt_count = 2")
	tk.MustExec("prepare stmt1 from 'select ? as num from dual'")
	tk.MustExec("prepare stmt2 from 'select ? as num from dual'")
	err := tk.ExecToErr("prepare stmt3 from 'select ? as num from dual'")
	require.True(t, terror.ErrorEqual(err, variable.ErrMaxPreparedStmtCountReached))
}

func TestExecuteWithWrongType(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec(`set tidb_enable_prepared_plan_cache=1`)
	tk.MustExec("use test")
	tk.MustExec("CREATE TABLE t3 (c1 int, c2 decimal(32, 30))")

	tk.MustExec(`prepare p1 from "update t3 set c1 = 2 where c2 in (?, ?)"`)
	tk.MustExec(`set @i0 = 0.0, @i1 = 0.0`)
	tk.MustExec(`execute p1 using @i0, @i1`)
	tk.MustExec(`set @i0 = 0.0, @i1 = 'aa'`)
	tk.MustExecToErr(`execute p1 using @i0, @i1`)

	tk.MustExec(`prepare p2 from "update t3 set c1 = 2 where c2 in (?, ?)"`)
	tk.MustExec(`set @i0 = 0.0, @i1 = 'aa'`)
	tk.MustExecToErr(`execute p2 using @i0, @i1`)
	tk.MustExec(`set @i0 = 0.0, @i1 = 0.0`)
	tk.MustExec(`execute p2 using @i0, @i1`)
}

func TestIssue58870(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("set names GBK")
	tk.MustExec(`CREATE TABLE tsecurity  (
  security_id int(11) NOT NULL DEFAULT 0,
  mkt_id smallint(6) NOT NULL DEFAULT 0,
  security_code varchar(64) CHARACTER SET gbk COLLATE gbk_bin NOT NULL DEFAULT ' ',
  security_name varchar(128) CHARACTER SET gbk COLLATE gbk_bin NOT NULL DEFAULT ' ',
  PRIMARY KEY (security_id) USING BTREE
) ENGINE = InnoDB CHARACTER SET = gbk COLLATE = gbk_bin ROW_FORMAT = Compact;`)
	tk.MustExec("INSERT INTO tsecurity (security_id, security_code, mkt_id, security_name) VALUES (1, '1', 1 ,'\xB2\xE2')")
	tk.MustExec("PREPARE a FROM 'INSERT INTO tsecurity (security_id, security_code, mkt_id, security_name) VALUES (2, 2, 2 ,\"\xB2\xE2\")'")
	tk.MustExec("EXECUTE a")
	stmt, _, _, err := tk.Session().PrepareStmt("INSERT INTO tsecurity (security_id, security_code, mkt_id, security_name) VALUES (3, 3, 3 ,\"\xB2\xE2\")")
	require.Nil(t, err)
	rs, err := tk.Session().ExecutePreparedStmt(context.TODO(), stmt, nil)
	require.Nil(t, err)
	require.Nil(t, rs)
}
