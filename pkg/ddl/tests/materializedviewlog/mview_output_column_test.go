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

package materializedviewlog

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessiontxn"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/stretchr/testify/require"
)

func TestMaterializedViewOutputLogModifyColumn(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := newMViewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_output_type (a int not null, b int not null)")
	tk.MustExec("insert into t_output_type values (1, 10)")
	tk.MustExec("create materialized view log on t_output_type (a, b)")
	tk.MustExec("create materialized view mv_output_type (k, l, cnt) as select a, b, count(1) from t_output_type group by a, b")
	tk.MustExec("create materialized view log on mv_output_type (k, l)")
	tk.MustExec("alter table t_output_type modify column a bigint not null, modify column b bigint not null")
	tk.MustQuery("select table_name, column_name, data_type from information_schema.columns where table_schema='test' and table_name in ('t_output_type', '$mlog$t_output_type', 'mv_output_type', '$mlog$mv_output_type') and column_name in ('a', 'b', 'k', 'l') order by table_name, ordinal_position").Check(testkit.Rows(
		"$mlog$mv_output_type k bigint", "$mlog$mv_output_type l bigint", "$mlog$t_output_type a bigint", "$mlog$t_output_type b bigint", "mv_output_type k bigint", "mv_output_type l bigint", "t_output_type a bigint", "t_output_type b bigint",
	))
	tk.MustExec("insert into t_output_type values (2147483648, 2147483649)")
	tk.MustExec("refresh materialized view mv_output_type fast")
	tk.MustQuery("select k, l, cnt from mv_output_type order by k").Check(testkit.Rows("1 10 1", "2147483648 2147483649 1"))
	tk.MustQuery("select k, l from `$mlog$mv_output_type`").Check(testkit.Rows("2147483648 2147483649"))
}

const outputLogAlterSQL = "alter table t_output_batch modify column a bigint not null, modify column b bigint not null"

func buildOutputLogModifyJob(t *testing.T, tk *testkit.TestKit, is infoschema.InfoSchema) *ddl.JobWrapper {
	t.Helper()
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnDDL)
	require.NoError(t, sessiontxn.NewTxn(ctx, tk.Session()))
	stmt, err := parser.New().ParseOneStmt(outputLogAlterSQL, "", "")
	require.NoError(t, err)
	schema, ok := is.SchemaByName(ast.NewCIStr("test"))
	require.True(t, ok)
	base, err := is.TableByName(context.Background(), schema.Name, ast.NewCIStr("t_output_batch"))
	require.NoError(t, err)
	var jobW *ddl.JobWrapper
	specs := stmt.(*ast.AlterTableStmt).Specs
	subJobs := make([]*model.SubJob, 0, len(specs))
	for _, spec := range specs {
		columnJob, err := ddl.GetModifiableColumnJob(ctx, tk.Session(), is,
			ast.Ident{Schema: schema.Name, Name: base.Meta().Name}, spec.NewColumns[0].Name.Name, schema, base, spec)
		require.NoError(t, err)
		if jobW == nil {
			jobW = columnJob
		}
		subJobs = append(subJobs, &model.SubJob{Type: columnJob.Type, JobArgs: columnJob.JobArgs,
			Revertible: true, ReorgTp: columnJob.ReorgMeta.ReorgTp, InvolvingSchemaInfo: columnJob.InvolvingSchemaInfo})
	}
	jobW.Type = model.ActionMultiSchemaChange
	jobW.MultiSchemaInfo = &model.MultiSchemaInfo{Revertible: true, SubJobs: subJobs}
	return ddl.NewJobWrapperWithArgs(jobW.Job, &model.EmptyArgs{}, false)
}

func TestMaterializedViewOutputLogModifyColumnRuntimeDependency(t *testing.T) {
	for _, cacheSize := range []int64{0, 1 << 30} {
		t.Run(fmt.Sprintf("schema_cache_size=%d", cacheSize), func(t *testing.T) {
			store, dom := testkit.CreateMockStoreAndDomain(t)
			tk := newMViewTestKit(t, store)
			originalCacheSize := tk.MustQuery("select @@global.tidb_schema_cache_size").Rows()[0][0]
			defer tk.MustExec(fmt.Sprintf("set @@global.tidb_schema_cache_size=%v", originalCacheSize))
			tk.MustExec(fmt.Sprintf("set @@global.tidb_schema_cache_size=%d", cacheSize))
			tk.MustExec("use test")
			tk.MustExec("create table t_output_batch (a int not null, b int not null)")
			tk.MustExec("create materialized view log on t_output_batch (a, b)")
			tk.MustExec("create materialized view mv_output_known (k, l, cnt) as select a, b, count(1) from t_output_batch group by a, b")
			tk.MustExec("create materialized view log on mv_output_known (k, l)")
			tkAlter := newMViewTestKit(t, store)
			tkAlter.MustExec("use test")
			jobW := buildOutputLogModifyJob(t, tkAlter, dom.InfoSchema())
			// This sibling MV is absent from the submitted ALTER's scheduling set.
			tk.MustExec("create materialized view mv_output_late (k, l, cnt) as select a, b, count(1) from t_output_batch group by a, b")
			for _, info := range jobW.GetInvolvingSchemaInfo() {
				require.NotEqual(t, "mv_output_late", info.Table)
			}
			boundary := make(chan struct{})
			resume := make(chan struct{})
			var paused, released atomic.Bool
			var alterJobID atomic.Int64
			release := func() {
				if released.CompareAndSwap(false, true) {
					close(resume)
				}
			}
			testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/beforeTransitOneJobStep", func(job *model.JobW) {
				if job.Type != model.ActionMultiSchemaChange || job.TableID != jobW.TableID || !job.MultiSchemaInfo.Revertible || job.IsRollingback() {
					return
				}
				for _, sub := range job.MultiSchemaInfo.SubJobs {
					if sub.Revertible || sub.IsFinished() {
						return
					}
				}
				if paused.CompareAndSwap(false, true) {
					alterJobID.Store(job.ID)
					close(boundary)
					<-resume
				}
			})
			done := make(chan error, 1)
			finished := false
			defer func() {
				release()
				if !finished {
					select {
					case <-done:
					case <-time.After(15 * time.Second):
						t.Error("timeout cleaning up ALTER")
					}
				}
			}()
			go func() {
				err := sessiontxn.NewTxn(context.Background(), tkAlter.Session())
				if err == nil {
					tkAlter.Session().SetValue(sessionctx.QueryString, outputLogAlterSQL)
					err = dom.DDLExecutor().(ddl.ExecutorForTest).DoDDLJobWrapper(tkAlter.Session(), jobW)
				}
				done <- err
			}()
			select {
			case <-boundary:
			case <-time.After(15 * time.Second):
				t.Fatal("ALTER did not reach its batch boundary")
			}
			// CREATE MLog can run because the late MV is outside the submitted set.
			tk.MustExec("create materialized view log on mv_output_late (k, l)")
			names := []string{"$mlog$t_output_batch", "mv_output_known", "$mlog$mv_output_known", "mv_output_late", "$mlog$mv_output_late"}
			var affectedIDs []int64
			for _, name := range names {
				tbl, err := dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr(name))
				require.NoError(t, err)
				affectedIDs = append(affectedIDs, tbl.Meta().ID) // Prime V2 caches before applying the diff.
			}
			release()
			select {
			case err := <-done:
				finished = true
				require.NoError(t, err)
			case <-time.After(20 * time.Second):
				t.Fatal("ALTER did not finish")
			}
			historyJob, err := ddl.GetHistoryJobByID(tk.Session(), alterJobID.Load())
			require.NoError(t, err)
			require.NoError(t, kv.RunInNewTxn(context.Background(), store, false, func(_ context.Context, txn kv.Transaction) error {
				diff, err := meta.NewMutator(txn).GetSchemaDiff(historyJob.BinlogInfo.SchemaVersion)
				require.NoError(t, err)
				require.NotNil(t, diff)
				var got []int64
				for _, opt := range diff.AffectedOpts {
					got = append(got, opt.TableID)
				}
				require.ElementsMatch(t, affectedIDs, got)
				return nil
			}))
			tk.MustQuery("select distinct data_type from information_schema.columns where table_schema='test' and table_name in ('t_output_batch', '$mlog$t_output_batch', 'mv_output_known', '$mlog$mv_output_known', 'mv_output_late', '$mlog$mv_output_late') and column_name in ('a', 'b', 'k', 'l')").Check(testkit.Rows("bigint"))
		})
	}
}

func TestMaterializedViewOutputLogModifyColumnBatchRollback(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := newMViewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_output_batch (a int not null, b int not null)")
	tk.MustExec("create materialized view log on t_output_batch (a, b)")
	tk.MustExec("create materialized view mv_output_batch (k, l, cnt) as select a, b, count(1) from t_output_batch group by a, b")
	tk.MustExec("create materialized view log on mv_output_batch (k, l)")
	schema, ok := dom.InfoSchema().SchemaByName(ast.NewCIStr("test"))
	require.True(t, ok)
	names := []string{"t_output_batch", "$mlog$t_output_batch", "mv_output_batch", "$mlog$mv_output_batch"}
	tableIDs := make([]int64, 0, len(names))
	for _, name := range names {
		tbl, err := dom.InfoSchema().TableByName(context.Background(), schema.Name, ast.NewCIStr(name))
		require.NoError(t, err)
		tableIDs = append(tableIDs, tbl.Meta().ID)
	}
	var injected, observed atomic.Bool
	var failedJobID atomic.Int64
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/beforeBatchedMultiSchemaParentJobUpdate", func(parent, proxy *model.Job) {
		if parent.TableID == tableIDs[0] && proxy.Type == model.ActionModifyColumn && injected.CompareAndSwap(false, true) {
			// The first sub-job has staged parent/MV/MLog metadata. Fail the second.
			failedJobID.Store(parent.ID)
			testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/ddl/mockUpdateVersionAndTableInfoErr", "1*return(1)")
		}
	})
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/afterRollbackTxnForMustRollbackTxnOnError", func(job *model.Job) {
		if job.Type == model.ActionMultiSchemaChange && job.ID == failedJobID.Load() {
			observed.Store(true)
		}
	})
	require.ErrorContains(t, tk.ExecToErr(outputLogAlterSQL), "mock update version and tableInfo error")
	require.True(t, injected.Load())
	require.True(t, observed.Load(), "the failed batch must discard its whole transaction")
	// Check durable metadata after the failed ALTER has completed. Reading from a
	// separate transaction inside the rollback hook cannot see staged mutations.
	require.NoError(t, kv.RunInNewTxn(context.Background(), store, false, func(_ context.Context, txn kv.Transaction) error {
		m := meta.NewMutator(txn)
		for _, id := range tableIDs {
			info, err := m.GetTable(schema.ID, id)
			if err != nil {
				return err
			}
			require.NotNil(t, info, "table ID %d", id)
			for _, col := range info.Columns {
				switch col.Name.L {
				case "a", "b", "k", "l":
					require.Equal(t, mysql.TypeLong, col.GetType(), "%s.%s", info.Name, col.Name)
				}
			}
		}
		return nil
	}))
	historyJob, err := ddl.GetHistoryJobByID(tk.Session(), failedJobID.Load())
	require.NoError(t, err)
	require.Equal(t, model.JobStateRollbackDone, historyJob.State)
	require.NotNil(t, historyJob.MultiSchemaInfo)
	require.True(t, historyJob.MultiSchemaInfo.Revertible)
	require.Len(t, historyJob.MultiSchemaInfo.SubJobs, 2)
	// The restored sub-jobs cancel from SchemaStateNone without publishing changes.
	for i, sub := range historyJob.MultiSchemaInfo.SubJobs {
		require.Equal(t, model.JobStateCancelled, sub.State, "sub-job %d", i)
		require.Equal(t, model.StateNone, sub.SchemaState, "sub-job %d", i)
	}
	tk.MustQuery("select distinct data_type from information_schema.columns where table_schema='test' and column_name in ('a', 'b', 'k', 'l')").Check(testkit.Rows("int"))
	tk.MustExec(outputLogAlterSQL)
	tk.MustQuery("select distinct data_type from information_schema.columns where table_schema='test' and column_name in ('a', 'b', 'k', 'l')").Check(testkit.Rows("bigint"))
}
