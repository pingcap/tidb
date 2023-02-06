// Copyright 2023 PingCAP, Inc.
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
	"testing"

	"github.com/pingcap/tidb/ddl/internal/callback"
	"github.com/pingcap/tidb/errno"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/testkit"
	"github.com/stretchr/testify/require"
	atomicutil "go.uber.org/atomic"
)

func TestDropColumnWithCompositeIndex(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int, b int, c int, index i_ab(a, b), index i_a(a))")
	tk.MustExec("insert into t values(1, 1, 1), (2, 2, 2), (3, 3, 3)")
	tk.MustExec("alter table t drop column a")
	query := queryIndexOnTable("test", "t")
	tk.MustQuery(query).Check(testkit.Rows("i_ab YES"))
}

func TestDropColumnWithMultiCompositeIndex(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int, b int, c int, d int, index i_abc(a, b, c), index i_acd(a, c, d))")
	tk.MustExec("insert into t values(1, 1, 1, 1), (2, 2, 2, 2), (3, 3, 3, 3)")
	tk.MustExec("alter table t drop column a")
	query := queryIndexOnTable("test", "t")
	tk.MustQuery(query).Check(testkit.Rows("i_abc YES", "i_acd YES"))
	tk.MustQuery("select c, d from t where c > 2").Check(testkit.Rows("3 3"))
	tk.MustQuery("select b, c from t where c > 2").Check(testkit.Rows("3 3"))
}

func TestDropColumnWithUniqCompositeIndex(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1 (a int, b int, c int, unique index i_ab(a, b))")
	tk.MustExec("insert into t1 values(1, 1, 1), (2, 1, 2), (3, 1, 3)")
	tk.MustGetErrCode("alter table t1 drop column a", errno.ErrDupEntry)

	tk.MustExec("drop table if exists t2")
	tk.MustExec("create table t2 (a int, b int, c int, unique index i_ab(a, b))")
	tk.MustExec("insert into t2 values(1, 1, 1), (2, 1, 2), (3, 1, 3)")
	tk.MustExec("alter table t2 drop column b")
}

var cancelTestCases = []testCancelJob{
	{"alter table t_cidx drop column a", true, model.StateCreateIndexDeleteOnly, false, true, nil},
	{"alter table t_cidx drop column a", true, model.StateCreateIndexWriteOnly, false, true, nil},
	{"alter table t_cidx drop column a", true, model.StateWriteReorganization, false, true, nil},
	{"alter table t_cidx drop column a", false, model.StateDeleteOnly, false, true, nil},
}

func matchCancelState(t *testing.T, job *model.Job, cancelState interface{}, sql string) bool {
	switch v := cancelState.(type) {
	case model.SchemaState:
		if job.Type == model.ActionMultiSchemaChange {
			msg := fmt.Sprintf("unexpected multi-schema change(sql: %s, cancel state: %s)", sql, v)
			require.Failf(t, msg, "use []model.SchemaState as cancel states instead")
			return false
		}
		return job.SchemaState == v
	default:
		return false
	}
}

func TestCancelDropColumnWithMultiCompositeIndex(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tkCancel := testkit.NewTestKit(t, store)

	// Prepare schema
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t_cidx;")
	tk.MustExec("create table t_cidx(a int, b int, c int, index i_ab(a, b), index i_ac(a, c));")

	// Prepare data:
	for i := 0; i < 10; i++ {
		tk.MustExec(fmt.Sprintf("insert into t_cidx values(%d, %d, %d)", i, i*2, i*3))
	}

	tk = testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	hook := &callback.TestDDLCallback{Do: dom}
	i := atomicutil.NewInt64(0)
	cancel := atomicutil.NewBool(false)
	cancelResult := atomicutil.NewBool(false)
	cancelWhenReorgNotStart := atomicutil.NewBool(false)
	hookFunc := func(job *model.Job) {
		if matchCancelState(t, job, cancelTestCases[i.Load()].cancelState, cancelTestCases[i.Load()].sql) && !cancel.Load() {
			if !cancelWhenReorgNotStart.Load() && job.SchemaState == model.StateWriteReorganization && job.MayNeedReorg() && job.RowCount == 0 {
				return
			}
			rs := tkCancel.MustQuery(fmt.Sprintf("admin cancel ddl jobs %d", job.ID))
			cancelResult.Store(cancelSuccess(rs))
			cancel.Store(true)
		}
	}

	dom.DDL().SetHook(hook.Clone())
	restHook := func(h *callback.TestDDLCallback) {
		h.OnJobRunBeforeExported = nil
		h.OnJobUpdatedExported.Store(nil)
		dom.DDL().SetHook(h.Clone())
	}
	registHook := func(h *callback.TestDDLCallback, onJobRunBefore bool) {
		if onJobRunBefore {
			h.OnJobRunBeforeExported = hookFunc
		} else {
			h.OnJobUpdatedExported.Store(&hookFunc)
		}
		dom.DDL().SetHook(h.Clone())
	}

	for j, tc := range cancelTestCases {
		i.Store(int64(j))
		msg := fmt.Sprintf("sql: %s, state: %s", tc.sql, tc.cancelState)
		if tc.onJobBefore {
			restHook(hook)
			for _, prepareSQL := range tc.prepareSQL {
				tk.MustExec(prepareSQL)
			}
			cancel.Store(false)
			cancelWhenReorgNotStart.Store(true)
			registHook(hook, true)
			if tc.ok {
				tk.MustGetErrCode(tc.sql, errno.ErrCancelledDDLJob)
			} else {
				tk.MustExec(tc.sql)
			}
			if cancel.Load() {
				require.Equal(t, tc.ok, cancelResult.Load(), msg)
			}
		}
		if tc.onJobUpdate {
			restHook(hook)
			for _, prepareSQL := range tc.prepareSQL {
				tk.MustExec(prepareSQL)
			}
			cancel.Store(false)
			cancelWhenReorgNotStart.Store(false)
			registHook(hook, false)
			if tc.ok {
				tk.MustGetErrCode(tc.sql, errno.ErrCancelledDDLJob)
			} else {
				tk.MustExec(tc.sql)
			}
			if cancel.Load() {
				require.Equal(t, tc.ok, cancelResult.Load(), msg)
			}
		}
	}
}
