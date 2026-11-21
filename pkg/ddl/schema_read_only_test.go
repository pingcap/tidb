// Copyright 2026 PingCAP, Inc.
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
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/ddl"
	ddlutil "github.com/pingcap/tidb/pkg/ddl/util"
	"github.com/pingcap/tidb/pkg/executor"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/privilege"
	"github.com/pingcap/tidb/pkg/server"
	"github.com/pingcap/tidb/pkg/session"
	"github.com/pingcap/tidb/pkg/session/syssession"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	ttlsession "github.com/pingcap/tidb/pkg/ttl/session"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/stretchr/testify/require"
)

func querySchemaReadOnlyTest(ctx context.Context, tk *testkit.TestKit, sql string) ([][]string, error) {
	rs, err := tk.ExecWithContext(ctx, sql)
	if err != nil {
		return nil, err
	}
	if rs == nil {
		return nil, fmt.Errorf("query returned no result set: %s", sql)
	}
	rows, err := session.ResultSetToStringSlice(ctx, tk.Session(), rs)
	return rows, errors.Join(err, rs.Close())
}

func waitSchemaReadOnlyJobs(t *testing.T, tk *testkit.TestKit, sql string, ready func([][]string) bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		rows, err := querySchemaReadOnlyTest(ctx, tk, sql)
		require.NoError(t, err)
		if ready(rows) {
			return
		}
		select {
		case <-ticker.C:
		case <-ctx.Done():
			t.Fatal("DDL job was not observed before the deadline")
		}
	}
}

func TestSchemaReadOnlyReviewRegressions(t *testing.T) {
	t.Run("CreateViewDraining", func(t *testing.T) {
		for _, mdl := range []bool{true, false} {
			for _, bypass := range []bool{false, true} {
				t.Run(fmt.Sprintf("mdl=%t/bypass=%t", mdl, bypass), func(t *testing.T) {
					if !mdl && kerneltype.IsNextGen() {
						t.Skip("MDL is always enabled in nextgen")
					}
					enableReadOnlyDDLFp(t)
					store, dom := testkit.CreateMockStoreAndDomain(t)
					sv := server.CreateMockServer(t, store)
					sv.SetDomain(dom)
					dom.InfoSyncer().SetSessionManager(sv)
					defer sv.Close()
					conn := server.CreateMockConn(t, sv)
					view := testkit.NewTestKitWithSession(t, store, conn.Context().Session)
					admin := testkit.NewTestKit(t, store)
					alter := testkit.NewTestKit(t, store)
					admin.MustExec("create database ro")
					admin.MustExec("create table ro.t(a int)")
					if !mdl {
						admin.MustExec("set global tidb_enable_metadata_lock = 0")
						defer admin.MustExec("set global tidb_enable_metadata_lock = 1")
					}
					if bypass {
						admin.MustExec("grant RESTRICTED_REPLICA_WRITER_ADMIN on *.* to 'root'@'%'")
						require.NoError(t, view.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil, nil))
					}
					view.MustExec("set autocommit = 0")
					view.MustExec("insert into ro.t values (1)")
					preprocessed := make(chan struct{})
					resume := make(chan struct{})
					release := sync.OnceFunc(func() { close(resume) })
					var txnID atomic.Uint64
					testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/executor/afterCreateViewPreprocess", func(ctx sessionctx.Context) {
						if ctx == view.Session() {
							txnID.Store(ctx.GetSessionVars().TxnCtx.StartTS)
							close(preprocessed)
							<-resume
						}
					})
					observed := make(chan struct{})
					var once sync.Once
					testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/checkUncommittedTxns", func(ids map[int64]struct{}) {
						if _, ok := ids[int64(txnID.Load())]; ok {
							once.Do(func() { close(observed) })
						}
					})
					ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
					defer cancel()
					var wg sync.WaitGroup
					defer func() { release(); conn.Close(); cancel(); wg.Wait() }()
					viewDone := make(chan error, 1)
					alterDone := make(chan error, 1)
					wg.Add(1)
					go func() {
						defer wg.Done()
						_, err := view.ExecWithContext(ctx, "create view ro.v as select * from ro.t")
						viewDone <- err
					}()
					select {
					case <-preprocessed:
					case <-ctx.Done():
						t.Fatal("CREATE VIEW did not finish preprocessing")
					}
					// DDL must already have committed the preceding user transaction.
					admin.MustQuery("select * from ro.t").Check(testkit.Rows("1"))
					wg.Add(1)
					go func() {
						defer wg.Done()
						_, err := alter.ExecWithContext(ctx, "alter database ro read only = 1")
						alterDone <- err
					}()
					select {
					case <-observed:
					case err := <-alterDone:
						t.Fatalf("ALTER returned before observing the CREATE VIEW transaction: %v", err)
					case <-ctx.Done():
						t.Fatal("ALTER did not observe the CREATE VIEW transaction")
					}
					// Clearing table registrations cannot release a transaction ID that
					// the transition already cached; preprocessing's transaction must end.
					release()
					select {
					case err := <-alterDone:
						require.NoError(t, err)
					case <-ctx.Done():
						t.Fatal("ALTER remained blocked by CREATE VIEW")
					}
					select {
					case err := <-viewDone:
						if bypass {
							require.NoError(t, err)
							admin.MustQuery("select * from ro.v").Check(testkit.Rows("1"))
						} else {
							require.ErrorContains(t, err, "[schema:3989]")
							admin.MustQuery("show full tables from ro").Check(testkit.Rows("t BASE TABLE"))
						}
					case <-ctx.Done():
						t.Fatal("CREATE VIEW remained blocked after the read-only transition")
					}
					admin.MustGetErrCode("insert into ro.t values (2)", 3989)
				})
			}
		}
	})
	t.Run("ImportModeTransition", func(t *testing.T) {
		enableReadOnlyDDLFp(t)
		store, dom := testkit.CreateMockStoreAndDomain(t)
		admin := testkit.NewTestKit(t, store)
		worker := testkit.NewTestKit(t, store)
		admin.MustExec("create database ro")
		admin.MustExec("create table ro.t(id int primary key)")
		db, ok := dom.InfoSchema().SchemaByName(ast.NewCIStr("ro"))
		require.True(t, ok)
		tbl, err := dom.InfoSchema().TableByName(context.Background(), db.Name, ast.NewCIStr("t"))
		require.NoError(t, err)
		// An already-submitted physical import is not part of read-only draining.
		require.NoError(t, ddl.AlterTableMode(dom.DDLExecutor(), worker.Session(), model.TableModeImport, db.ID, tbl.Meta().ID))
		admin.MustExec("alter database ro read only = 1")
		db, ok = dom.InfoSchema().SchemaByName(db.Name)
		require.True(t, ok)
		require.True(t, db.ReadOnly)
		tbl, err = dom.InfoSchema().TableByName(context.Background(), db.Name, ast.NewCIStr("t"))
		require.NoError(t, err)
		require.Equal(t, model.TableModeImport, tbl.Meta().Mode)
		// Completion must still release import mode after the database becomes read-only.
		require.NoError(t, ddl.AlterTableMode(dom.DDLExecutor(), worker.Session(), model.TableModeNormal, db.ID, tbl.Meta().ID))
		admin.MustQuery("select * from ro.t").Check(testkit.Rows())
		admin.MustGetErrCode("import into ro.t from ''", 3989)
		admin.MustGetErrCode("insert into ro.t values(1)", 3989)
	})
	t.Run("SequenceWriteDraining", func(t *testing.T) {
		for _, sql := range []string{
			"select nextval(ro.s)", "select setval(ro.s, 100)",
			"select nextval(ro.s) from ro.t where id = 1", "select setval(ro.s, 100) from ro.t where id = 1",
		} {
			t.Run(sql, func(t *testing.T) {
				enableReadOnlyDDLFp(t)
				store := testkit.CreateMockStore(t)
				writer := testkit.NewTestKit(t, store)
				alter := testkit.NewTestKit(t, store)
				alter.MustExec("create database ro")
				alter.MustExec("create sequence ro.s nocache")
				alter.MustExec("create table ro.t(id int primary key)")
				alter.MustExec("insert into ro.t values (1)")
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				checked := make(chan struct{})
				resume := make(chan struct{})
				release := sync.OnceFunc(func() { close(resume) })
				var paused atomic.Bool
				testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/expression/sessionexpr/afterSequenceReadOnlyCheck", func() {
					if paused.CompareAndSwap(false, true) {
						close(checked)
						<-resume
					}
				})
				var wg sync.WaitGroup
				defer func() { release(); wg.Wait() }()
				writeDone := make(chan error, 1)
				alterDone := make(chan error, 1)
				wg.Add(1)
				go func() {
					defer wg.Done()
					_, err := querySchemaReadOnlyTest(ctx, writer, sql)
					writeDone <- err
				}()
				select {
				case <-checked:
				case <-time.After(5 * time.Second):
					t.Fatal("sequence write did not pass the read-only check")
				}
				waiting := make(chan struct{}, 1)
				testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/checkUncommittedTxns", func(ids map[int64]struct{}) {
					if len(ids) > 0 {
						select {
						case waiting <- struct{}{}:
						default:
						}
					}
				})
				wg.Add(1)
				go func() { defer wg.Done(); alterDone <- alter.ExecToErr("alter database ro read only = 1") }()
				select {
				case <-waiting:
				case err := <-alterDone:
					t.Fatalf("transition finished before independent sequence allocation: %v", err)
				case <-time.After(5 * time.Second):
					t.Fatal("transition did not wait for the sequence write")
				}
				select {
				case err := <-alterDone:
					t.Fatalf("transition finished while the sequence write was paused: %v", err)
				default:
				}
				release()
				require.NoError(t, <-writeDone)
				require.NoError(t, <-alterDone)
				require.ErrorContains(t, writer.QueryToErr(sql), "[schema:3989]")
				alter.MustExec("alter database ro read only = 0")
				want := "2"
				if strings.Contains(sql, "setval") {
					want = "101"
				}
				alter.MustQuery("select nextval(ro.s)").Check(testkit.Rows(want))
			})
		}
	})
	t.Run("SequenceParallelProjection", func(t *testing.T) {
		store := testkit.CreateMockStore(t)
		tk := testkit.NewTestKit(t, store)
		tk.MustExec("create database ro")
		tk.MustExec("create sequence ro.s")
		tk.MustExec("set tidb_max_chunk_size = 32")
		tk.MustExec("set tidb_projection_concurrency = 4")
		// Constant inputs need no KV reader to activate a transaction before evaluation.
		inputs := make([]string, 128)
		for i := range inputs {
			inputs[i] = fmt.Sprintf("select %d as n", i)
		}
		rows := tk.MustQuery("select nextval(ro.s) from (" + strings.Join(inputs, " union all ") + ") d").Rows()
		require.Len(t, rows, len(inputs))
		tk.MustQuery("select nextval(ro.s)").Check(testkit.Rows("129"))
	})
	t.Run("SequenceLateActivation", func(t *testing.T) {
		enableReadOnlyDDLFp(t)
		store := testkit.CreateMockStore(t)
		writer := testkit.NewTestKit(t, store)
		alter := testkit.NewTestKit(t, store)
		alter.MustExec("create database ro")
		alter.MustExec("create sequence ro.s nocache")
		started := make(chan struct{})
		resume := make(chan struct{})
		release := sync.OnceFunc(func() { close(resume) })
		var once sync.Once
		testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/sessiontxn/isolation/beforeActivateTxnForSchemaReadOnly", func(ctx sessionctx.Context) {
			if ctx == writer.Session() {
				once.Do(func() { close(started); <-resume })
			}
		})
		done := make(chan error, 1)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := querySchemaReadOnlyTest(ctx, writer, "select nextval(ro.s)")
			done <- err
		}()
		defer func() { release(); wg.Wait() }()
		select {
		case <-started:
		case <-time.After(5 * time.Second):
			t.Fatal("sequence operation did not activate its registered session transaction")
		}
		alterDone := make(chan error, 1)
		wg.Add(1)
		go func() { defer wg.Done(); alterDone <- alter.ExecToErr("alter database ro read only = 1") }()
		select {
		case err := <-alterDone:
			require.NoError(t, err)
		case <-time.After(5 * time.Second):
			release()
			wg.Wait()
			t.Fatal("ALTER waited for the paused writer")
		}
		release()
		require.ErrorContains(t, <-done, "read only")
		alter.MustExec("alter database ro read only = 0")
		alter.MustQuery("select nextval(ro.s)").Check(testkit.Rows("1"))
	})
	for _, name := range []string{"CTEOnlyOF", "DerivedReadSource"} {
		t.Run("LockQueryBlockTargets/"+name, func(t *testing.T) {
			enableReadOnlyDDLFp(t)
			store := testkit.CreateMockStore(t)
			tk := testkit.NewTestKit(t, store)
			tk.MustExec("create database ro")
			tk.MustExec("create database rw")
			tk.MustExec("create table ro.t(id int primary key)")
			tk.MustExec("create table rw.t(id int primary key)")
			tk.MustExec("insert into ro.t values (1)")
			tk.MustExec("insert into rw.t values (1)")
			tk.MustExec("alter database ro read only = 1")
			tk.MustExec("begin pessimistic")
			defer tk.MustExec("rollback")
			if name == "CTEOnlyOF" {
				tk.MustGetErrCode("with c as (select 1 as k) select t.id from ro.t as t cross join c for update of c", 3989)
			} else {
				tk.MustQuery("select n.id from rw.t n join (select id from ro.t) d on n.id = d.id for update").Check(testkit.Rows("1"))
				tk.MustQuery("select * from (select id from ro.t) d for update").Check(testkit.Rows("1"))
				tk.MustGetErrCode("select n.id from rw.t n join (select id from ro.t for update) d on n.id = d.id for update", 3989)
				tk.MustGetErrCode("select n.id from ro.t n join (select id from rw.t) d on n.id = d.id for update", 3989)
			}
		})
	}
	t.Run("PrecheckReadRetry", func(t *testing.T) {
		enableReadOnlyDDLFp(t)
		store := testkit.CreateMockStore(t)
		tk := testkit.NewTestKit(t, store)
		tk.MustExec("create database ro")
		var reads atomic.Int32
		testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/afterSchemaReadOnlyPrecheckRead", func(job *model.Job, err *error) {
			if job.SchemaName == "ro" && job.Type == model.ActionCreateTable && reads.Add(1) == 1 {
				*err = errors.New("temporary schema precheck read failure")
			}
		})
		tk.MustExec("create table ro.t(id int)")
		require.GreaterOrEqual(t, reads.Load(), int32(2))
		tk.MustQuery("show tables from ro").Check(testkit.Rows("t"))
	})
	t.Run("PreparedWriterPrivilege", func(t *testing.T) {
		for _, cache := range []bool{false, true} {
			for _, role := range []bool{false, true} {
				t.Run(fmt.Sprintf("cache=%t/role=%t", cache, role), func(t *testing.T) {
					enableReadOnlyDDLFp(t)
					store := testkit.CreateMockStore(t)
					admin := testkit.NewTestKit(t, store)
					writer := testkit.NewTestKit(t, store)
					admin.MustExec("create database ro")
					admin.MustExec("create table ro.t(id int primary key)")
					admin.MustExec("create user 'writer'@'%'")
					admin.MustExec("grant insert on ro.* to 'writer'@'%'")
					grantee := "'writer'@'%'"
					if role {
						admin.MustExec("create role 'replica_writer'")
						admin.MustExec("grant 'replica_writer' to 'writer'@'%'")
						grantee = "'replica_writer'"
					}
					admin.MustExec("grant RESTRICTED_REPLICA_WRITER_ADMIN on *.* to " + grantee)
					require.NoError(t, writer.Session().Auth(&auth.UserIdentity{Username: "writer", Hostname: "%"}, nil, nil, nil))
					if role {
						writer.MustExec("set role 'replica_writer'")
					}
					writer.MustExec(fmt.Sprintf("set tidb_enable_prepared_plan_cache = %t", cache))
					admin.MustExec("alter database ro read only = 1")
					writer.MustExec("prepare ins from 'insert into ro.t values (?)'")
					writer.MustExec("prepare explain_write from 'explain insert into ro.t values (99)'")
					writer.MustExec("set @id = 1")
					writer.MustExec("execute ins using @id")
					writer.MustExec("set @id = 2")
					writer.MustExec("execute ins using @id")
					require.Equal(t, cache, writer.Session().GetSessionVars().FoundInPlanCache)
					if role {
						writer.MustExec("set role none")
					} else {
						admin.MustExec("revoke RESTRICTED_REPLICA_WRITER_ADMIN on *.* from " + grantee)
					}
					writer.MustExec("set @id = 3")
					writer.MustGetErrCode("execute ins using @id", 3989)
					writer.MustGetErrCode("execute explain_write", 3989)
					admin.MustQuery("select * from ro.t").Check(testkit.Rows("1", "2"))
				})
			}
		}
	})
	t.Run("PreparedReadOnlyErrorCode", func(t *testing.T) {
		for _, cache := range []bool{false, true} {
			t.Run(fmt.Sprintf("cache=%t", cache), func(t *testing.T) {
				enableReadOnlyDDLFp(t)
				store := testkit.CreateMockStore(t)
				tk := testkit.NewTestKit(t, store)
				tk.MustExec(fmt.Sprintf("set tidb_enable_prepared_plan_cache = %t", cache))
				tk.MustExec("create database ro")
				tk.MustExec("create table ro.t(id int primary key, v int)")
				statements := []string{
					"insert into ro.t values (?, 1)",
					"update ro.t set v = 2 where id = ?",
					"delete from ro.t where id = ?",
					"select * from ro.t where id = ? for update",
					"truncate table ro.t",
					"drop database ro",
					"alter database ro charset gbk",
					"create table ro.new_table(id int)",
					"drop table ro.t",
					"alter table ro.t add column extra int",
				}
				for i, sql := range statements {
					tk.MustExec(fmt.Sprintf("prepare stmt%d from '%s'", i, sql))
				}
				// Warm the INSERT plan; the other statements have only been prepared.
				for _, id := range []int{1, 2} {
					tk.MustExec(fmt.Sprintf("set @id = %d", id))
					tk.MustExec("execute stmt0 using @id")
				}
				require.Equal(t, cache, tk.Session().GetSessionVars().FoundInPlanCache)
				tk.MustExec("set @id = 3")
				tk.MustExec("alter database ro read only = 1")
				for i := range statements {
					execute := fmt.Sprintf("execute stmt%d", i)
					if i < 4 {
						execute += " using @id"
					}
					tk.MustGetErrCode(execute, 3989)
				}
				tk.MustQuery("select * from ro.t order by id").Check(testkit.Rows("1 1", "2 1"))
				tk.MustExec("alter database ro read only = 0")
				tk.MustExec("execute stmt0 using @id")
				tk.MustQuery("select * from ro.t order by id").Check(testkit.Rows("1 1", "2 1", "3 1"))
				// Unrelated column and schema errors must retain their original codes.
				tk.MustExec("prepare select_stmt from 'select v from ro.t'")
				tk.MustExec("alter table ro.t drop column v")
				tk.MustGetErrCode("execute select_stmt", 1054)
				tk.MustExec("drop table ro.t")
				tk.MustGetErrCode("execute select_stmt", 8113)
			})
		}
	})
	t.Run("PreparedMultiDeleteUsesPrepareDB", func(t *testing.T) {
		enableReadOnlyDDLFp(t)
		store := testkit.CreateMockStore(t)
		admin := testkit.NewTestKit(t, store)
		writer := testkit.NewTestKit(t, store)
		admin.MustExec("create database ro")
		admin.MustExec("create table ro.t(id int primary key)")
		admin.MustExec("insert into ro.t values (1), (2), (3)")
		admin.MustExec("create user 'writer'@'%'")
		admin.MustExec("grant select, delete on ro.* to 'writer'@'%'")
		admin.MustExec("grant select on test.* to 'writer'@'%'")
		admin.MustExec("grant RESTRICTED_REPLICA_WRITER_ADMIN on *.* to 'writer'@'%'")
		admin.MustExec("alter database ro read only = 1")
		require.NoError(t, writer.Session().Auth(&auth.UserIdentity{Username: "writer", Hostname: "%"}, nil, nil, nil))
		writer.MustExec("use ro")
		writer.MustExec("prepare del from 'delete t from t where id = ?'")
		for _, id := range []int{1, 2} {
			writer.MustExec(fmt.Sprintf("set @id = %d", id))
			writer.MustExec("execute del using @id")
		}
		require.True(t, writer.Session().GetSessionVars().FoundInPlanCache)
		admin.MustExec("revoke RESTRICTED_REPLICA_WRITER_ADMIN on *.* from 'writer'@'%'")
		writer.MustExec("use test")
		writer.MustExec("set @id = 3")
		writer.MustGetErrCode("execute del using @id", 3989)
		admin.MustQuery("select * from ro.t").Check(testkit.Rows("3"))
	})
	t.Run("NativeMaterializedViewDDL", func(t *testing.T) {
		for _, tc := range []struct {
			name, sql string
			setup     []string
		}{
			{name: "CreateView", sql: "create materialized view ro.mv(a, cnt) as select a, count(1) from ro.t group by a", setup: []string{"create materialized view log on ro.t(a)"}},
			{name: "CreateLog", sql: "create materialized view log on ro.t(a)"},
			{name: "DropView", sql: "drop materialized view ro.mv", setup: []string{"create materialized view log on ro.t(a)", "create materialized view ro.mv(a, cnt) as select a, count(1) from ro.t group by a"}},
			{name: "DropLog", sql: "drop materialized view log on ro.t", setup: []string{"create materialized view log on ro.t(a)"}},
			{name: "AlterLog", sql: "alter materialized view log on ro.t add column (b)", setup: []string{"create materialized view log on ro.t(a)"}},
		} {
			t.Run(tc.name, func(t *testing.T) {
				enableReadOnlyDDLFp(t)
				store := testkit.CreateMockStore(t)
				tk := testkit.NewTestKit(t, store)
				tk.MustExec("set tidb_mview_enable = on")
				tk.MustExec("create database ro")
				tk.MustExec("create table ro.t(a int not null, b int)")
				for _, sql := range tc.setup {
					tk.MustExec(sql)
				}
				before := tk.MustQuery("show tables from ro").Rows()
				tk.MustExec("alter database ro read only = 1")
				tk.MustGetErrCode("prepare mv_ddl from '"+tc.sql+"'", 3989)
				tk.MustGetErrCode(tc.sql, 3989)
				require.Equal(t, before, tk.MustQuery("show tables from ro").Rows())
				if tc.name == "DropView" {
					tk.MustExec("grant RESTRICTED_REPLICA_WRITER_ADMIN on *.* to 'root'@'%'")
					require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil, nil))
					tk.MustExec(tc.sql)
				}
			})
		}
	})
	t.Run("ExplicitLockTargets", func(t *testing.T) {
		enableReadOnlyDDLFp(t)
		store := testkit.CreateMockStore(t)
		tk := testkit.NewTestKit(t, store)
		tk.MustExec("create database ro")
		tk.MustExec("create database rw")
		tk.MustExec("create table ro.t(id int primary key)")
		tk.MustExec("create table rw.t(id int primary key)")
		tk.MustExec("insert into ro.t values (1)")
		tk.MustExec("insert into rw.t values (1)")
		tk.MustExec("alter database ro read only = 1")
		tk.MustExec("begin pessimistic")
		defer tk.MustExec("rollback")
		tk.MustQuery("select a.id from rw.t a join ro.t b on a.id = b.id for update of a").Check(testkit.Rows("1"))
		tk.MustGetErrCode("select a.id from rw.t a join ro.t b on a.id = b.id for update of b", 3989)
		tk.MustExec("prepare lock_rw from 'select a.id from rw.t a join ro.t b on a.id = b.id for update of a'")
		tk.MustQuery("execute lock_rw").Check(testkit.Rows("1"))
	})
	t.Run("PendingStateName", func(t *testing.T) {
		require.Equal(t, "pending read only", model.StatePendingReadOnly.String())
	})
	t.Run("BatchTransactionPolling", func(t *testing.T) {
		enableReadOnlyDDLFp(t)
		store := testkit.CreateMockStore(t)
		alter := testkit.NewTestKit(t, store)
		alter.MustExec("create database ro")
		alter.MustExec("create table ro.t(id int primary key)")
		alter.MustExec("create table test.unrelated(id int)")
		unrelated := testkit.NewTestKit(t, store)
		unrelated.MustExec("begin optimistic")
		unrelated.MustQuery("select * from test.unrelated").Check(testkit.Rows())
		defer unrelated.MustExec("rollback")
		txns := make([]*testkit.TestKit, 4)
		for i := range txns {
			txns[i] = testkit.NewTestKit(t, store)
			txns[i].MustExec("begin optimistic")
			txns[i].MustExec(fmt.Sprintf("insert into ro.t values (%d)", i))
		}
		scanned := make(chan struct{})
		resume := make(chan struct{})
		release := sync.OnceFunc(func() { close(resume) })
		var once sync.Once
		var polls atomic.Int64
		var pollRows atomic.Int64
		testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/afterReadOnlyTxnQuery", func(_ sessionctx.Context, sql string, rows *[]chunk.Row) {
			if strings.HasPrefix(sql, "SELECT RELATED_TABLE_IDS") {
				once.Do(func() { close(scanned); <-resume })
			} else if strings.HasPrefix(sql, "SELECT ID ") {
				polls.Add(1)
				pollRows.Store(int64(len(*rows)))
			}
		})
		var wg sync.WaitGroup
		defer func() {
			release()
			for _, txn := range txns {
				txn.MustExec("rollback")
			}
			wg.Wait()
		}()
		done := make(chan error, 1)
		wg.Add(1)
		go func() { defer wg.Done(); done <- alter.ExecToErr("alter database ro read only = 1") }()
		select {
		case <-scanned:
		case <-time.After(5 * time.Second):
			t.Fatal("read-only transaction scan did not run")
		}
		for _, txn := range txns {
			txn.MustExec("commit")
		}
		release()
		require.NoError(t, <-done)
		require.EqualValues(t, 1, polls.Load(), "one poll must confirm all completed transactions")
		require.Zero(t, pollRows.Load(), "polling must not return unrelated active transactions")
	})
	t.Run("MDLOnLazyTransaction", func(t *testing.T) {
		enableReadOnlyDDLFp(t)
		store := testkit.CreateMockStore(t)
		writer := testkit.NewTestKit(t, store)
		alter := testkit.NewTestKit(t, store)
		alter.MustExec("create database ro")
		alter.MustExec("create table ro.t(a int)")
		planned := make(chan struct{})
		resume := make(chan struct{})
		release := sync.OnceFunc(func() { close(resume) })
		var paused atomic.Bool
		testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/sessiontxn/isolation/beforeActivateTxnForSchemaReadOnly", func(ctx sessionctx.Context) {
			if ctx != writer.Session() || !paused.CompareAndSwap(false, true) {
				return
			}
			close(planned)
			<-resume
		})
		var wg sync.WaitGroup
		defer func() { release(); wg.Wait() }()
		done := make(chan error, 1)
		wg.Add(1)
		go func() { defer wg.Done(); done <- writer.ExecToErr("insert into ro.t values (1)") }()
		select {
		case <-planned:
		case <-time.After(5 * time.Second):
			t.Fatal("INSERT did not reach transaction activation")
		}
		registered := false
		writer.Session().GetSessionVars().GetRelatedTableForMDL().Range(func(_, _ any) bool {
			registered = true
			return false
		})
		require.True(t, registered, "INSERT must register its table before the transition")
		alterDone := make(chan error, 1)
		wg.Add(1)
		go func() { defer wg.Done(); alterDone <- alter.ExecToErr("alter database ro read only = 1") }()
		select {
		case err := <-alterDone:
			require.NoError(t, err)
		case <-time.After(5 * time.Second):
			release()
			wg.Wait()
			t.Fatal("ALTER waited for the paused writer")
		}
		release()
		require.ErrorContains(t, <-done, "read only state has changed")
		alter.MustQuery("select * from ro.t").Check(testkit.Rows())
	})
	t.Run("QueuedDDL", func(t *testing.T) {
		for _, bypass := range []bool{false, true} {
			t.Run(fmt.Sprintf("bypass=%t", bypass), func(t *testing.T) {
				enableReadOnlyDDLFp(t)
				store := testkit.CreateMockStore(t)
				admin := testkit.NewTestKit(t, store)
				alter := testkit.NewTestKit(t, store)
				create := testkit.NewTestKit(t, store)
				admin.MustExec("create database ro")
				if bypass {
					admin.MustExec("grant RESTRICTED_REPLICA_WRITER_ADMIN on *.* to 'root'@'%'")
					require.NoError(t, create.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil, nil))
				}
				started := make(chan struct{})
				resume := make(chan struct{})
				release := sync.OnceFunc(func() { close(resume) })
				var paused atomic.Bool
				testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/beforeRunOneJobStep", func(job *model.Job) {
					if job.Type == model.ActionModifySchemaReadOnly && paused.CompareAndSwap(false, true) {
						close(started)
						<-resume
					}
				})
				var wg sync.WaitGroup
				defer func() { release(); wg.Wait() }()
				alterDone := make(chan error, 1)
				createDone := make(chan error, 1)
				wg.Add(1)
				go func() { defer wg.Done(); alterDone <- alter.ExecToErr("alter database ro read only = 1") }()
				select {
				case <-started:
				case <-time.After(5 * time.Second):
					t.Fatal("read-only job did not start")
				}
				wg.Add(1)
				go func() { defer wg.Done(); createDone <- create.ExecToErr("create table ro.late(a int)") }()
				waitSchemaReadOnlyJobs(t, admin, "admin show ddl jobs where db_name = 'ro' and job_type = 'create table'", func(rows [][]string) bool {
					return len(rows) != 0
				})
				release()
				require.NoError(t, <-alterDone)
				if bypass {
					require.NoError(t, <-createDone)
					admin.MustQuery("show tables from ro").Check(testkit.Rows("late"))
				} else {
					require.ErrorContains(t, <-createDone, "[schema:3989]")
					admin.MustQuery("show tables from ro").Check(testkit.Rows())
				}
			})
		}
	})
	t.Run("InternalTTLTransaction", func(t *testing.T) {
		enableReadOnlyDDLFp(t)
		store, dom := testkit.CreateMockStoreAndDomain(t)
		sv := server.CreateMockServer(t, store)
		sv.SetDomain(dom)
		dom.InfoSyncer().SetSessionManager(sv)
		defer sv.Close()
		admin := testkit.NewTestKit(t, store)
		alter := testkit.NewTestKit(t, store)
		admin.MustExec("set global tidb_ttl_job_enable = off")
		admin.MustExec("create database ro")
		admin.MustExec("create table ro.t(id int primary key, ts datetime) TTL = ts + interval 1 second")
		admin.MustExec("insert into ro.t values (1, now() - interval 1 day)")
		deleted := make(chan struct{})
		resume := make(chan struct{})
		release := sync.OnceFunc(func() { close(resume) })
		var txnID atomic.Uint64
		var wg sync.WaitGroup
		defer func() { release(); wg.Wait() }()
		ttlDone := make(chan error, 1)
		wg.Add(1)
		go func() {
			defer wg.Done()
			ttlDone <- dom.AdvancedSysSessionPool().WithSession(func(s *syssession.Session) error {
				return s.WithSessionContext(func(ctx sessionctx.Context) error {
					ttl := ttlsession.NewSession(ctx, s.AvoidReuse)
					return ttl.RunInTxn(context.Background(), func() error {
						if _, err := ttl.ExecuteSQL(context.Background(), "delete from ro.t where ts < now() - interval 1 second"); err != nil {
							return err
						}
						txnID.Store(ctx.GetSessionVars().TxnCtx.StartTS)
						close(deleted)
						<-resume
						return nil
					}, ttlsession.TxnModeOptimistic)
				})
			})
		}()
		select {
		case <-deleted:
		case err := <-ttlDone:
			t.Fatalf("TTL transaction failed before DELETE: %v", err)
		case <-time.After(5 * time.Second):
			t.Fatal("TTL DELETE did not finish")
		}
		require.Contains(t, sv.GetInternalSessionStartTSList(), txnID.Load())
		require.NoError(t, admin.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil, nil))
		admin.MustQuery(fmt.Sprintf("select id from information_schema.tidb_trx where id = %d", txnID.Load())).Check(testkit.Rows())
		scanned := make(chan bool, 1)
		var scanObserved atomic.Bool
		testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/afterReadOnlyTxnQuery", func(_ sessionctx.Context, sql string, rows *[]chunk.Row) {
			if !strings.HasPrefix(sql, "SELECT RELATED_TABLE_IDS") || !scanObserved.CompareAndSwap(false, true) {
				return
			}
			found := false
			for _, row := range *rows {
				found = found || uint64(row.GetInt64(1)) == txnID.Load()
			}
			scanned <- found
		})
		alterDone := make(chan error, 1)
		wg.Add(1)
		go func() { defer wg.Done(); alterDone <- alter.ExecToErr("alter database ro read only = 1") }()
		select {
		case found := <-scanned:
			require.True(t, found, "read-only transition omitted the uncommitted internal TTL DELETE")
		case <-time.After(5 * time.Second):
			t.Fatal("read-only transaction scan did not run")
		}
		select {
		case err := <-alterDone:
			t.Fatalf("read-only transition finished before TTL COMMIT: %v", err)
		default:
		}
		release()
		require.NoError(t, <-ttlDone)
		require.NoError(t, <-alterDone)
		admin.MustQuery("select * from ro.t").Check(testkit.Rows())
	})
}

func TestSchemaReadOnlyTransitions(t *testing.T) {
	t.Run("ConcurrentRequestsWait", func(t *testing.T) {
		enableReadOnlyDDLFp(t)
		store := testkit.CreateMockStore(t)
		txn := testkit.NewTestKit(t, store)
		first := testkit.NewTestKit(t, store)
		second := testkit.NewTestKit(t, store)
		txn.MustExec("create database ro")
		txn.MustExec("create table ro.t(a int)")
		txn.MustExec("begin optimistic")
		txn.MustExec("insert into ro.t values (1)")
		var observed atomic.Bool
		testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/checkUncommittedTxns", func(ids map[int64]struct{}) { observed.Store(len(ids) != 0) })
		firstDone := make(chan error, 1)
		secondDone := make(chan error, 1)
		var wg sync.WaitGroup
		defer func() { txn.MustExec("rollback"); wg.Wait() }()
		wg.Add(1)
		go func() { defer wg.Done(); firstDone <- first.ExecToErr("alter database ro read only = 1") }()
		require.Eventually(t, observed.Load, 5*time.Second, 10*time.Millisecond)
		submitted := make(chan struct{}, 1)
		testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/waitJobSubmitted", func() { submitted <- struct{}{} })
		wg.Add(1)
		go func() { defer wg.Done(); secondDone <- second.ExecToErr("alter database ro read only = 1") }()
		select {
		case <-submitted:
		case err := <-secondDone:
			t.Fatalf("second ALTER returned before submission: %v", err)
		case <-time.After(5 * time.Second):
			t.Fatal("second ALTER was not submitted")
		}
		select {
		case err := <-secondDone:
			t.Fatalf("duplicate ALTER returned before the existing transaction finished: %v", err)
		default:
		}
		txn.MustExec("commit")
		for _, done := range []chan error{firstDone, secondDone} {
			select {
			case err := <-done:
				require.NoError(t, err)
			case <-time.After(5 * time.Second):
				t.Fatal("ALTER did not finish after COMMIT")
			}
		}
	})
	t.Run("MDLOffExistingWrite", func(t *testing.T) {
		if kerneltype.IsNextGen() {
			t.Skip("MDL is always enabled in nextgen")
		}
		enableReadOnlyDDLFp(t)
		store := testkit.CreateMockStore(t)
		txn := testkit.NewTestKit(t, store)
		other := testkit.NewTestKit(t, store)
		txn.MustExec("create database ro")
		txn.MustExec("create table ro.t(a int)")
		txn.MustExec("set global tidb_enable_metadata_lock = 0")
		defer other.MustExec("set global tidb_enable_metadata_lock = 1")
		txn.MustExec("begin optimistic")
		txn.MustExec("insert into ro.t values (1)")
		defer txn.MustExec("rollback")
		txnID := txn.MustQuery("select @@tidb_current_ts").Rows()[0][0].(string)
		observed := make(chan struct{})
		var once sync.Once
		testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/checkUncommittedTxns", func(ids map[int64]struct{}) {
			for id := range ids {
				if fmt.Sprint(id) == txnID {
					once.Do(func() { close(observed) })
				}
			}
		})
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		done := make(chan error, 1)
		var wg sync.WaitGroup
		defer func() { txn.Session().RollbackTxn(context.Background()); cancel(); wg.Wait() }()
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := other.ExecWithContext(ctx, "alter database ro read only = 1")
			done <- err
		}()
		select {
		case <-observed:
		case err := <-done:
			t.Fatalf("ALTER returned before observing the existing write: %v", err)
		case <-ctx.Done():
			t.Fatal("ALTER did not observe the existing write before the deadline")
		}
		txn.MustExec("commit")
		select {
		case err := <-done:
			require.NoError(t, err)
		case <-ctx.Done():
			t.Fatal("ALTER did not finish after COMMIT")
		}
		other.MustQuery("select * from ro.t").Check(testkit.Rows("1"))
	})
	t.Run("MDLOffLateAccess", func(t *testing.T) {
		if kerneltype.IsNextGen() {
			t.Skip("MDL is always enabled in nextgen")
		}
		enableReadOnlyDDLFp(t)
		store := testkit.CreateMockStore(t)
		txn := testkit.NewTestKit(t, store)
		other := testkit.NewTestKit(t, store)
		txn.MustExec("create database ro")
		txn.MustExec("create table ro.t(a int)")
		txn.MustExec("create table test.anchor(a int)")
		txn.MustExec("set global tidb_enable_metadata_lock = 0")
		defer other.MustExec("set global tidb_enable_metadata_lock = 1")
		txn.MustExec("begin optimistic")
		txn.MustQuery("select * from test.anchor").Check(testkit.Rows())
		defer txn.MustExec("rollback")
		other.MustExec("alter database ro read only = 1")
		txn.MustGetErrMsg("insert into ro.t values (2)", "[domain:8028]public schema ro read only state has changed")
		txn.MustExec("rollback")
		other.MustQuery("select * from ro.t").Check(testkit.Rows())
	})
	t.Run("SequenceMutation", func(t *testing.T) {
		enableReadOnlyDDLFp(t)
		store := testkit.CreateMockStore(t)
		tk := testkit.NewTestKit(t, store)
		tk.MustExec("create database ro")
		tk.MustExec("create sequence ro.s nocache")
		tk.MustQuery("select nextval(ro.s)").Check(testkit.Rows("1"))
		tk.MustExec("alter database ro read only = 1")
		require.ErrorContains(t, tk.QueryToErr("select nextval(ro.s)"), "[schema:3989]")
		require.ErrorContains(t, tk.QueryToErr("select setval(ro.s, 100)"), "[schema:3989]")
		tk.MustQuery("select lastval(ro.s)").Check(testkit.Rows("1"))
	})
	t.Run("MDLOffLazyTransaction", func(t *testing.T) {
		if kerneltype.IsNextGen() {
			t.Skip("MDL is always enabled in nextgen")
		}
		enableReadOnlyDDLFp(t)
		store := testkit.CreateMockStore(t)
		writer := testkit.NewTestKit(t, store)
		alter := testkit.NewTestKit(t, store)
		alter.MustExec("create database ro")
		alter.MustExec("create table ro.t(a int)")
		alter.MustExec("set global tidb_enable_metadata_lock = 0")
		defer alter.MustExec("set global tidb_enable_metadata_lock = 1")
		planned := make(chan struct{})
		resume := make(chan struct{})
		release := sync.OnceFunc(func() { close(resume) })
		var paused atomic.Bool
		testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/planner/core/afterCheckSchemaReadOnlyWithoutMDL", func(db ast.CIStr) {
			if db.L == "ro" && paused.CompareAndSwap(false, true) {
				close(planned)
				<-resume
			}
		})
		done := make(chan error, 1)
		var wg sync.WaitGroup
		wg.Add(1)
		go func() { defer wg.Done(); done <- writer.ExecToErr("insert into ro.t values (1)") }()
		defer func() { release(); wg.Wait() }()
		select {
		case <-planned:
		case <-time.After(5 * time.Second):
			t.Fatal("write was not planned")
		}
		alterDone := make(chan error, 1)
		wg.Add(1)
		go func() { defer wg.Done(); alterDone <- alter.ExecToErr("alter database ro read only = 1") }()
		select {
		case err := <-alterDone:
			require.NoError(t, err)
		case <-time.After(5 * time.Second):
			release()
			wg.Wait()
			t.Fatal("ALTER waited for the paused writer")
		}
		release()
		require.ErrorContains(t, <-done, "read only state has changed")
		alter.MustQuery("select * from ro.t").Check(testkit.Rows())
	})
	t.Run("MaterializedViewAttributes", func(t *testing.T) {
		enableReadOnlyDDLFp(t)
		store, dom := testkit.CreateMockStoreAndDomain(t)
		tk := testkit.NewTestKit(t, store)
		tk.MustExec("set tidb_mview_enable = on")
		tk.MustExec("create database ro")
		tk.MustExec("use ro")
		tk.MustExec("create table t(a int not null)")
		tk.MustExec("create materialized view log on t(a)")
		tk.MustExec("create materialized view mv(a, cnt) as select a, count(1) from t group by a")
		tk.MustExec("alter database ro read only = 1")
		tk.MustGetErrCode("alter materialized view mv attributes='mview_alert_warning=5,mview_alert_overdue=10,mview_alert_refresh_failed=yes'", 3989)
		mv, err := dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("ro"), ast.NewCIStr("mv"))
		require.NoError(t, err)
		require.False(t, mv.Meta().MaterializedView.AlertRefreshFailed)
	})
}

func TestSchemaReadOnlyIncompleteTransactionScan(t *testing.T) {
	for _, polling := range []bool{false, true} {
		t.Run(fmt.Sprintf("polling=%t", polling), func(t *testing.T) {
			enableReadOnlyDDLFp(t)
			store := testkit.CreateMockStore(t)
			txn := testkit.NewTestKit(t, store)
			alter := testkit.NewTestKit(t, store)
			txn.MustExec("create database ro")
			txn.MustExec("create table ro.t(a int)")
			alter.MustExec("set global tidb_ddl_error_count_limit = 1")
			if polling {
				txn.MustExec("begin optimistic")
				txn.MustExec("insert into ro.t values (1)")
				defer txn.MustExec("rollback")
			}
			testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/afterReadOnlyTxnQuery", func(ctx sessionctx.Context, sql string, rows *[]chunk.Row) {
				isPolling := strings.HasPrefix(sql, "SELECT ID ")
				if isPolling == polling {
					*rows = nil
					ctx.GetSessionVars().StmtCtx.AppendWarning(errors.New("remote TiDB transaction scan unavailable"))
				}
			})
			err := alter.ExecToErr("alter database ro read only = 1")
			require.ErrorContains(t, err, "remote TiDB transaction scan unavailable")
			alter.MustQuery("show create database ro").Check(testkit.Rows("ro CREATE DATABASE `ro` /*!40100 DEFAULT CHARACTER SET utf8mb4 */"))
		})
	}
}

func TestSchemaReadOnlyRollbackAfterPanic(t *testing.T) {
	for _, readOnly := range []bool{false, true} {
		t.Run(fmt.Sprintf("readOnly=%t", readOnly), func(t *testing.T) {
			enableReadOnlyDDLFp(t)
			store, dom := testkit.CreateMockStoreAndDomain(t)
			tk := testkit.NewTestKit(t, store)
			tk.MustExec("create database ro")
			if !readOnly {
				tk.MustExec("alter database ro read only = 1")
			}
			var panicked atomic.Bool
			testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/beforeUpdateSchemaVersion", func(job *model.Job, _ *error) {
				if job.Type == model.ActionModifySchemaReadOnly && panicked.CompareAndSwap(false, true) {
					panic("read-only metadata was written before schema version update")
				}
			})
			readOnlyValue := 0
			if readOnly {
				readOnlyValue = 1
			}
			err := tk.ExecToErr(fmt.Sprintf("alter database ro read only = %d", readOnlyValue))
			require.Error(t, err)
			require.True(t, panicked.Load())
			db, ok := dom.InfoSchema().SchemaByName(ast.NewCIStr("ro"))
			require.True(t, ok)
			require.Equal(t, !readOnly, db.ReadOnly)
		})
	}
}

func TestSchemaReadOnlyReplicaWriterSequence(t *testing.T) {
	enableReadOnlyDDLFp(t)
	store := testkit.CreateMockStore(t)
	admin := testkit.NewTestKit(t, store)
	writer := testkit.NewTestKit(t, store)
	admin.MustExec("create database ro")
	admin.MustExec("create sequence ro.s nocache")
	admin.MustExec("create table ro.t(id int primary key)")
	admin.MustExec("insert into ro.t values (1)")
	admin.MustExec("create user 'repl_writer'@'%'")
	admin.MustExec("grant all on ro.* to 'repl_writer'@'%'")
	require.NoError(t, writer.Session().Auth(&auth.UserIdentity{Username: "repl_writer", Hostname: "%"}, nil, nil, nil))
	writer.MustExec("prepare seq_stmt from 'select nextval(ro.s)'")
	writer.MustExec("prepare seq_point_get from 'select nextval(ro.s) from ro.t where id = 1'")
	admin.MustExec("alter database ro read only = 1")
	require.ErrorContains(t, writer.QueryToErr("select nextval(ro.s)"), "[schema:3989]")
	require.ErrorContains(t, writer.QueryToErr("execute seq_stmt"), "[schema:3989]")
	require.ErrorContains(t, writer.QueryToErr("execute seq_point_get"), "[schema:3989]")
	admin.MustExec("grant RESTRICTED_REPLICA_WRITER_ADMIN on *.* to 'repl_writer'@'%'")
	writer.MustQuery("execute seq_stmt").Check(testkit.Rows("1"))
	writer.MustQuery("execute seq_point_get").Check(testkit.Rows("2"))
	writer.MustQuery("select setval(ro.s, 100)").Check(testkit.Rows("100"))
	writer.MustQuery("select nextval(ro.s)").Check(testkit.Rows("101"))
}

func TestSchemaReadOnlyRecoveryAndCancellation(t *testing.T) {
	t.Run("CancelQueuedDuplicate", func(t *testing.T) {
		enableReadOnlyDDLFp(t)
		store, dom := testkit.CreateMockStoreAndDomain(t)
		first := testkit.NewTestKit(t, store)
		second := testkit.NewTestKit(t, store)
		admin := testkit.NewTestKit(t, store)
		admin.MustExec("create database ro")
		firstStarted := make(chan struct{})
		releaseFirst := make(chan struct{})
		release := sync.OnceFunc(func() { close(releaseFirst) })
		var firstID atomic.Int64
		var wg sync.WaitGroup
		testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/beforeRunOneJobStep", func(job *model.Job) {
			if job.Type == model.ActionModifySchemaReadOnly && firstID.CompareAndSwap(0, job.ID) {
				close(firstStarted)
				<-releaseFirst
			}
		})
		t.Cleanup(func() { release(); wg.Wait() })
		firstDone := make(chan error, 1)
		secondDone := make(chan error, 1)
		wg.Add(1)
		go func() { defer wg.Done(); firstDone <- first.ExecToErr("alter database ro read only = 1") }()
		select {
		case <-firstStarted:
		case <-time.After(5 * time.Second):
			t.Fatal("first ALTER did not start")
		}
		wg.Add(1)
		go func() { defer wg.Done(); secondDone <- second.ExecToErr("alter database ro read only = 1") }()
		var secondID string
		waitSchemaReadOnlyJobs(t, admin, "admin show ddl jobs where db_name = 'ro' and job_type = 'modify schema read only'", func(rows [][]string) bool {
			for _, row := range rows {
				if fmt.Sprint(row[0]) != fmt.Sprint(firstID.Load()) {
					secondID = fmt.Sprint(row[0])
					return true
				}
			}
			return false
		})
		admin.MustQuery("admin cancel ddl jobs " + secondID).Check(testkit.Rows(secondID + " successful"))
		release()
		require.NoError(t, <-firstDone)
		require.ErrorContains(t, <-secondDone, "Cancelled DDL job")
		db, ok := dom.InfoSchema().SchemaByName(ast.NewCIStr("ro"))
		require.True(t, ok)
		require.True(t, db.ReadOnly)
	})
	t.Run("DroppedTableID", func(t *testing.T) {
		if kerneltype.IsNextGen() {
			t.Skip("MDL is always enabled in nextgen")
		}
		enableReadOnlyDDLFp(t)
		store := testkit.CreateMockStore(t)
		txn := testkit.NewTestKit(t, store)
		other := testkit.NewTestKit(t, store)
		txn.MustExec("create database ro")
		txn.MustExec("create table test.ghost(a int)")
		txn.MustExec("begin optimistic")
		txn.MustQuery("select * from test.ghost").Check(testkit.Rows())
		other.MustExec("set global tidb_enable_metadata_lock = 0")
		defer other.MustExec("set global tidb_enable_metadata_lock = 1")
		defer txn.MustExec("rollback")
		other.MustExec("drop table test.ghost")
		other.MustExec("set global tidb_ddl_error_count_limit = 1")
		other.MustExec("alter database ro read only = 1")
	})
	t.Run("RecoverTables", func(t *testing.T) {
		enableReadOnlyDDLFp(t)
		store := testkit.CreateMockStore(t)
		tk := testkit.NewTestKit(t, store)
		gcWasEnabled := ddlutil.IsEmulatorGCEnable()
		ddlutil.EmulatorGCDisable()
		defer func() {
			if gcWasEnabled {
				ddlutil.EmulatorGCEnable()
			}
		}()
		tk.MustExec(fmt.Sprintf("insert into mysql.tidb values ('tikv_gc_safe_point', '%s', '') on duplicate key update variable_value=values(variable_value)", time.Now().Add(-time.Hour).Format("20060102-15:04:05 -0700 MST")))
		tk.MustExec("create database ro")
		tk.MustExec("create table ro.a(id int)")
		tk.MustExec("create table ro.b(id int)")
		tk.MustExec("drop table ro.a, ro.b")
		jobs := tk.MustQuery("admin show ddl jobs where db_name = 'ro' and job_type = 'drop table'").Rows()
		var dropJobID string
		for _, job := range jobs {
			if job[2] == "b" {
				dropJobID = fmt.Sprint(job[0])
			}
		}
		require.NotEmpty(t, dropJobID)
		tk.MustExec("alter database ro read only = 1")
		tk.MustExec("use test")
		tk.MustGetErrCode("flashback table ro.a", 3989)
		tk.MustGetErrCode("recover table ro.b", 3989)
		tk.MustGetErrCode("recover table by job "+dropJobID, 3989)
		tk.MustQuery("select table_name from information_schema.tables where table_schema='ro' order by table_name").Check(testkit.Rows())
		tk.MustExec("grant RESTRICTED_REPLICA_WRITER_ADMIN on *.* to 'root'@'%'")
		require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil, nil))
		tk.MustExec("flashback table ro.a")
		tk.MustExec("recover table ro.b")
		tk.MustQuery("select table_name from information_schema.tables where table_schema='ro' order by table_name").Check(testkit.Rows("a", "b"))
	})
}

func TestSchemaReadOnlyReviewFixes(t *testing.T) {
	for _, release := range []string{"unlock", "disconnect", "cleanup"} {
		t.Run("ReleaseTableLock/"+release, func(t *testing.T) {
			enableReadOnlyDDLFp(t)
			store := testkit.CreateMockStore(t)
			admin := testkit.NewTestKit(t, store)
			writer := testkit.NewTestKit(t, store)
			admin.MustExec("create database ro")
			admin.MustExec("create table ro.t(a int)")
			admin.MustExec("create user locker")
			admin.MustExec("grant all on ro.* to locker")
			admin.MustExec("grant RESTRICTED_REPLICA_WRITER_ADMIN on *.* to locker")
			require.NoError(t, writer.Session().Auth(&auth.UserIdentity{Username: "locker", Hostname: "%"}, nil, nil, nil))
			admin.MustExec("alter database ro read only = 1")
			writer.MustExec("lock tables ro.t write")
			admin.MustExec("revoke RESTRICTED_REPLICA_WRITER_ADMIN on *.* from locker")
			switch release {
			case "unlock":
				writer.MustExec("unlock tables")
			case "disconnect":
				writer.Session().Close()
			case "cleanup":
				admin.MustExec("admin cleanup table lock ro.t")
			}
			checkTableLock(t, admin, "ro", "t", ast.TableLockNone)
			admin.MustQuery("select * from ro.t").Check(testkit.Rows())
		})
	}
	t.Run("BatchDeleteTransaction", func(t *testing.T) {
		enableReadOnlyDDLFp(t)
		store := testkit.CreateMockStore(t)
		writer, alter := testkit.NewTestKit(t, store), testkit.NewTestKit(t, store)
		writer.MustExec("create database ro")
		writer.MustExec("create table ro.t(a int primary key)")
		writer.MustExec("insert into ro.t values (1), (2)")
		writer.MustExec("set global tidb_enable_batch_dml = 1")
		defer writer.MustExec("set global tidb_enable_batch_dml = 0")
		writer.MustExec("set tidb_batch_delete = 1")
		writer.MustExec("set tidb_dml_batch_size = 1")
		var once sync.Once
		testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/executor/afterBatchDeleteNewTxn", func(sessionctx.Context) {
			once.Do(func() { alter.MustExec("alter database ro read only = 1") })
		})
		require.ErrorContains(t, writer.ExecToErr("delete from ro.t order by a"), "read only mode")
		writer.MustQuery("select * from ro.t").Check(testkit.Rows("2"))
	})
	for _, statement := range []string{"create temporary table ro.new_tmp(a int)", "drop temporary table ro.tmp", "truncate table ro.tmp"} {
		t.Run("LocalTemporaryDDL/"+statement, func(t *testing.T) {
			enableReadOnlyDDLFp(t)
			store := testkit.CreateMockStore(t)
			writer, alter := testkit.NewTestKit(t, store), testkit.NewTestKit(t, store)
			writer.MustExec("create database ro")
			writer.MustExec("create table test.anchor(a int)")
			writer.MustExec("create temporary table ro.tmp(a int)")
			writer.MustExec("insert into ro.tmp values (1)")
			writer.MustExec("begin")
			writer.MustQuery("select * from test.anchor").Check(testkit.Rows())
			alter.MustExec("alter database ro read only = 1")
			writer.MustGetErrCode(statement, 3989)
			writer.MustQuery("select * from ro.tmp").Check(testkit.Rows("1"))
			writer.MustExec("rollback")
		})
	}
	for _, statement := range []string{"replace into ro.p values (1)", "update ro.p set id = 2", "delete from ro.p", "insert into ro.p values (1) on duplicate key update id = 2"} {
		for _, childDB := range []string{"ro", "other"} {
			t.Run("ScopedForeignKey/"+childDB+"/"+statement, func(t *testing.T) {
				enableReadOnlyDDLFp(t)
				store := testkit.CreateMockStore(t)
				tk := testkit.NewTestKit(t, store)
				tk.MustExec("create database ro")
				tk.MustExec("create database other")
				tk.MustExec("create table ro.p(id int primary key)")
				tk.MustExec("create table " + childDB + ".c(id int, foreign key(id) references ro.p(id) on delete cascade on update cascade)")
				tk.MustExec("insert into ro.p values (1)")
				tk.MustExec("insert into " + childDB + ".c values (1)")
				tk.MustExec("alter database ro read only = 1")
				tk.MustExec("alter database other read only = 1")
				restore := privilege.WithInternalSchemaReadOnlyBypass(tk.Session(), "ro")
				defer restore()
				if childDB == "ro" {
					tk.MustExec(statement)
				} else {
					tk.MustGetErrCode(statement, 3989)
					tk.MustQuery("select * from other.c").Check(testkit.Rows("1"))
				}
			})
		}
	}
	t.Run("LegacyImportJob", func(t *testing.T) {
		enableReadOnlyDDLFp(t)
		store, dom := testkit.CreateMockStoreAndDomain(t)
		tk := testkit.NewTestKit(t, store)
		tk.MustExec("create database Ro")
		tk.MustExec("create table Ro.t(a int)")
		tbl, err := dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("ro"), ast.NewCIStr("t"))
		require.NoError(t, err)
		tk.MustExec("insert into mysql.tidb_import_jobs(table_schema,table_name,table_id,created_by,parameters,source_file_size,status,step) values ('ro','t',?,'root','{}',1,'pending','')", tbl.Meta().ID)
		tk.MustExec("alter database ro read only = 1")
		tk.MustGetErrCode("import into ro.t from ''", 3989)
		tk.MustExec("alter database ro read only = 0")
		tk.MustExec("update mysql.tidb_import_jobs set status = 'running', step = 'global-sorting'")
		tk.MustExec("alter database ro read only = 1")
	})
	t.Run("MixedVersion", func(t *testing.T) {
		enableReadOnlyDDLFp(t)
		store := testkit.CreateMockStore(t)
		tk := testkit.NewTestKit(t, store)
		tk.MustExec("create database ro")
		testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/domain/serverinfo/mockGetAllServerInfo", "return(`{\"new\":{\"schema_read_only\":true},\"old\":{}}`)")
		require.ErrorContains(t, tk.ExecToErr("alter database ro read only = 1"), "do not support database read-only")
		testfailpoint.Disable(t, "github.com/pingcap/tidb/pkg/domain/serverinfo/mockGetAllServerInfo")
		tk.MustExec("alter database ro read only = 1")
		tk.MustExec("alter database ro read only = 0")
	})
	t.Run("BatchTransaction", func(t *testing.T) {
		enableReadOnlyDDLFp(t)
		store := testkit.CreateMockStore(t)
		writer := testkit.NewTestKit(t, store)
		alter := testkit.NewTestKit(t, store)
		writer.MustExec("create database ro")
		writer.MustExec("create table ro.t(a int primary key)")
		writer.MustExec("set global tidb_enable_batch_dml = 1")
		defer writer.MustExec("set global tidb_enable_batch_dml = 0")
		writer.MustExec("set tidb_batch_insert = 1")
		writer.MustExec("set tidb_dml_batch_size = 1")
		var once sync.Once
		testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/executor/afterBatchInsertNewTxn", func(sessionctx.Context) {
			once.Do(func() { alter.MustExec("alter database ro read only = 1") })
		})
		require.ErrorContains(t, writer.ExecToErr("insert into ro.t values (1), (2)"), "read only mode")
		writer.MustQuery("select * from ro.t").Check(testkit.Rows("1"))
	})
	t.Run("TiFlashAvailability", func(t *testing.T) {
		enableReadOnlyDDLFp(t)
		testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/infoschema/mockTiFlashStoreCount", "return(true)")
		testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/ddl/forceSetTiFlashReplicaSkipColumnarStorageGate", "return(true)")
		store, dom := testkit.CreateMockStoreAndDomain(t)
		tk := testkit.NewTestKit(t, store)
		tk.MustExec("create database ro")
		tk.MustExec("create table ro.t(a int)")
		tk.MustExec("alter table ro.t set tiflash replica 1")
		tbl, err := dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("ro"), ast.NewCIStr("t"))
		require.NoError(t, err)
		tk.MustExec("alter database ro read only = 1")
		require.NoError(t, dom.DDLExecutor().UpdateTableReplicaInfo(tk.Session(), tbl.Meta().ID, true))
		tbl, err = dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("ro"), ast.NewCIStr("t"))
		require.NoError(t, err)
		require.True(t, tbl.Meta().TiFlashReplica.Available)
		tk.MustGetErrCode("alter table ro.t set tiflash replica 0", 3989)
	})
	t.Run("IndexMaintenance", func(t *testing.T) {
		enableReadOnlyDDLFp(t)
		store := testkit.CreateMockStore(t)
		tk := testkit.NewTestKit(t, store)
		tk.MustExec("create database ro")
		tk.MustExec("create table ro.t(a int, index idx(a))")
		tk.MustExec("insert into ro.t values (1)")
		tk.MustExec("alter database ro read only = 1")
		for _, sql := range []string{"admin recover index ro.t idx", "admin cleanup index ro.t idx"} {
			tk.MustGetErrCode(sql, 3989)
		}
		tk.MustExec("admin check table ro.t")
		tk.MustExec("grant RESTRICTED_REPLICA_WRITER_ADMIN on *.* to 'root'@'%'")
		require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil, nil))
		tk.MustQuery("admin recover index ro.t idx").Check(testkit.Rows("0 1"))
		tk.MustQuery("admin cleanup index ro.t idx").Check(testkit.Rows("0"))
	})
	for _, mdl := range []bool{false, true} {
		t.Run(fmt.Sprintf("LocalTemporaryTableMDL%v", mdl), func(t *testing.T) {
			if !mdl && kerneltype.IsNextGen() {
				t.Skip("NextGen always enables MDL")
			}
			enableReadOnlyDDLFp(t)
			store := testkit.CreateMockStore(t)
			writer := testkit.NewTestKit(t, store)
			alter := testkit.NewTestKit(t, store)
			if !mdl {
				alter.MustExec("set global tidb_enable_metadata_lock = 0")
				defer alter.MustExec("set global tidb_enable_metadata_lock = 1")
			}
			writer.MustExec("create database ro")
			writer.MustExec("create table test.anchor(a int)")
			writer.MustExec("create temporary table ro.tmp(a int)")
			writer.MustExec("insert into ro.tmp values (1)")
			writer.MustExec("begin optimistic")
			defer writer.MustExec("rollback")
			writer.MustQuery("select * from test.anchor").Check(testkit.Rows())
			alter.MustExec("alter database ro read only = 1")
			writer.Session().SetValue(executor.LoadDataReaderBuilderKey, executor.LoadDataReaderBuilder{
				Build: func(string) (io.ReadCloser, error) { return io.NopCloser(strings.NewReader("2\n")), nil },
				Wg:    &sync.WaitGroup{},
			})
			for _, sql := range []string{
				"load data local infile 'temporary.csv' into table ro.tmp",
				"insert into ro.tmp values (2)",
				"replace into ro.tmp values (2)",
				"insert into ro.tmp select a from test.anchor",
				"update ro.tmp set a = 2",
				"update ro.tmp x set x.a = 2",
				"update ro.tmp set ro.tmp.a = 2",
				"delete from ro.tmp",
				"delete x from ro.tmp x",
			} {
				writer.MustGetErrCode(sql, 3989)
			}
			writer.MustExec("update test.anchor a join ro.tmp t set a.a = t.a")
			writer.MustQuery("select * from ro.tmp").Check(testkit.Rows("1"))
		})
	}
	for _, method := range []string{"fast", "complete delta apply", "complete out of place"} {
		t.Run("Refresh_"+method, func(t *testing.T) {
			enableReadOnlyDDLFp(t)
			store := testkit.CreateMockStore(t)
			tk := testkit.NewTestKit(t, store)
			tk.MustExec("set tidb_mview_enable = on")
			tk.MustExec("create database ro")
			tk.MustExec("create table ro.t(a int not null)")
			tk.MustExec("insert into ro.t values (1)")
			tk.MustExec("create materialized view log on ro.t(a)")
			tk.MustExec("create materialized view ro.mv(a, cnt) as select a, count(1) from ro.t group by a")
			tk.MustExec("insert into ro.t values (2)")
			tk.MustExec("alter database ro read only = 1")
			sql := "refresh materialized view ro.mv " + method
			tk.MustGetErrCode(sql, 3989)
			tk.MustQuery("select * from ro.mv order by a").Check(testkit.Rows("1 1"))
			ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnAdmin)
			_, err := tk.Session().GetSQLExecutor().ExecuteInternal(ctx, sql)
			require.ErrorContains(t, err, "read only mode")
			tk.MustExec("grant RESTRICTED_REPLICA_WRITER_ADMIN on *.* to 'root'@'%'")
			require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil, nil))
			tk.MustExec("set autocommit = 0")
			tk.MustExec(sql)
			tk.MustQuery("select @@tidb_current_ts = 0").Check(testkit.Rows("1"))
			if method == "fast" {
				tk.MustExec("prepare refresh_stmt from '" + sql + "'")
				tk.MustExec("execute refresh_stmt")
				tk.MustQuery("select @@tidb_current_ts = 0").Check(testkit.Rows("1"))
				tk.MustExec("deallocate prepare refresh_stmt")
				tk.MustQuery(sql + " with profile")
				tk.MustQuery("select @@tidb_current_ts = 0").Check(testkit.Rows("1"))
			}
			tk.MustQuery("select * from ro.mv order by a").Check(testkit.Rows("1 1", "2 1"))
			tk.MustExec("rollback")
			tk.MustExec("set autocommit = 1")
		})
	}
	t.Run("RefreshTransitionDraining", func(t *testing.T) {
		enableReadOnlyDDLFp(t)
		store := testkit.CreateMockStore(t)
		writer := testkit.NewTestKit(t, store)
		alter := testkit.NewTestKit(t, store)
		writer.MustExec("set tidb_mview_enable = on")
		writer.MustExec("create database ro")
		writer.MustExec("create table ro.t(a int not null)")
		writer.MustExec("insert into ro.t values (1)")
		writer.MustExec("create materialized view log on ro.t(a)")
		writer.MustExec("create materialized view ro.mv(a, cnt) as select a, count(1) from ro.t group by a")
		writer.MustExec("insert into ro.t values (2)")
		writer.MustExec("set autocommit = 0")
		started, resume := make(chan struct{}), make(chan struct{})
		release := sync.OnceFunc(func() { close(resume) })
		var once sync.Once
		testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/executor/refreshMaterializedViewAfterAcquireAdvisoryLock", func() {
			once.Do(func() { close(started); <-resume })
		})
		var wg sync.WaitGroup
		defer func() { release(); wg.Wait() }()
		refreshDone, alterDone := make(chan error, 1), make(chan error, 1)
		wg.Add(1)
		go func() { defer wg.Done(); refreshDone <- writer.ExecToErr("refresh materialized view ro.mv fast") }()
		select {
		case <-started:
		case <-time.After(5 * time.Second):
			t.Fatal("refresh did not start")
		}
		var draining atomic.Bool
		testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/checkUncommittedTxns", func(ids map[int64]struct{}) {
			draining.Store(len(ids) > 0)
		})
		wg.Add(1)
		go func() { defer wg.Done(); alterDone <- alter.ExecToErr("alter database ro read only = 1") }()
		require.Eventually(t, draining.Load, 5*time.Second, 10*time.Millisecond)
		release()
		require.Error(t, <-refreshDone)
		require.NoError(t, <-alterDone)
		writer.MustQuery("select @@tidb_current_ts = 0").Check(testkit.Rows("1"))
		writer.MustQuery("select * from ro.mv order by a").Check(testkit.Rows("1 1"))
	})

	t.Run("PurgeEntry", func(t *testing.T) {
		enableReadOnlyDDLFp(t)
		store := testkit.CreateMockStore(t)
		tk := testkit.NewTestKit(t, store)
		tk.MustExec("set tidb_mview_enable = on")
		tk.MustExec("create database ro")
		tk.MustExec("create table ro.t(a int not null)")
		tk.MustExec("create materialized view log on ro.t(a)")
		tk.MustExec("insert into ro.t values (1)")
		tk.MustExec("alter database ro read only = 1")
		tk.MustGetErrCode("purge materialized view log on ro.t", 3989)
		tk.MustExec("grant RESTRICTED_REPLICA_WRITER_ADMIN on *.* to 'root'@'%'")
		require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil, nil))
		tk.MustExec("purge materialized view log on ro.t")
		tk.MustQuery("select count(*) from ro.`$mlog$t`").Check(testkit.Rows("0"))
		tk.MustExec("revoke RESTRICTED_REPLICA_WRITER_ADMIN on *.* from 'root'@'%'")
		tk.MustGetErrCode("purge materialized view log on ro.t", 3989)
	})
	t.Run("InternalAuthorizationScope", func(t *testing.T) {
		enableReadOnlyDDLFp(t)
		store := testkit.CreateMockStore(t)
		tk := testkit.NewTestKit(t, store)
		for _, name := range []string{"ro", "other"} {
			tk.MustExec("create database " + name)
			tk.MustExec("create table " + name + ".t(a int)")
			tk.MustExec("alter database " + name + " read only = 1")
		}
		restore := privilege.WithInternalSchemaReadOnlyBypass(tk.Session(), "ro")
		tk.MustExec("insert into ro.t values (1)")
		tk.MustGetErrCode("insert into other.t values (1)", 3989)
		restoreNested := privilege.WithInternalSchemaReadOnlyBypass(tk.Session(), "other")
		tk.MustExec("insert into other.t values (1)")
		restoreNested()
		tk.MustExec("insert into ro.t values (2)")
		restore()
		tk.MustGetErrCode("insert into ro.t values (3)", 3989)
	})
}
