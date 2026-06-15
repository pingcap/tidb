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
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/errno"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
)

func TestAlterTableAddNonclusteredAutoIncrementPrimaryKeyDMLDuringReorg(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk2 := testkit.NewTestKit(t, store)
	tk2.MustExec("use test")

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (v int)")
	tk.MustExec("insert into t values (10), (20), (30)")

	// Insert a few rows after the auto_increment column is in WriteReorganization state.
	// These rows won't be covered by the backfill snapshot and must be filled by DML itself.
	var once sync.Once
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/onAddColumnStateWriteReorg", func() {
		once.Do(func() {
			tk.MustExec("insert into t(v) values (40), (50)")
		})
	})

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		tk2.MustExec("alter table t " +
			"add column id bigint not null auto_increment, " +
			"add primary key (id) nonclustered")
	}()
	wg.Wait()

	tk.MustQuery("select count(*), count(distinct id), sum(id is null) from t").
		Check(testkit.Rows("5 5 0"))
	tk.MustQuery("select count(distinct id), sum(id=0), sum(id is null) from t where v in (40, 50)").
		Check(testkit.Rows("2 0 0"))
}

func TestAlterTableAddNonclusteredAutoIncrementPrimaryKeyOutOfRange(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (v int)")
	for i := 0; i < 200; i++ {
		tk.MustExec(fmt.Sprintf("insert into t values (%d)", i))
	}

	// Backfilling an AUTO_INCREMENT column should fail if the generated IDs are out of range
	// (instead of silently truncating and later failing with duplicate keys).
	sql := "alter table t add column id tinyint not null auto_increment, add primary key (id) nonclustered"
	err := tk.ExecToErr(sql)
	if err == nil {
		t.Fatalf("expected error, got nil")
	}
	originErr := errors.Cause(err)
	var code int
	switch v := originErr.(type) {
	case *terror.Error:
		code = int(v.Code())
	case *terror.TiDBError:
		code = int(v.MYSQLERRNO)
	default:
		t.Fatalf("unexpected error type %T: %v", originErr, err)
	}
	// Depending on statement flags/SQL mode, TiDB may report either an out-of-range cast error
	// or ErrAutoincReadFailed (our explicit guard against truncation duplicates).
	if code != errno.ErrAutoincReadFailed && code != errno.ErrWarnDataOutOfRange && code != errno.ErrDataOutOfRange {
		t.Fatalf("unexpected error code, got=%d err=%v", code, err)
	}
}

func TestCancelAlterTableAddNonclusteredAutoIncrementPrimaryKey(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tkCancel := testkit.NewTestKit(t, store)
	tkCancel.MustExec("use test")

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (v int)")
	tk.MustExec("set @@tidb_ddl_reorg_batch_size = 8")
	tk.MustExec("set @@tidb_ddl_reorg_worker_cnt = 1")
	// Make the DDL take long enough for cancel to reliably kick in.
	for i := 0; i < 200; i++ {
		tk.MustExec(fmt.Sprintf("insert into t values (%d)", i))
	}

	// Slow down the backfill workers so the cancel request has time to land.
	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/ddl/mockBackfillSlow", "return")
	// Ensure `runReorgJob` yields during backfill so the cancel hook can observe the running stage.
	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/ddl/updateProgressIntervalInMs", "return(50)")

	var cancelled atomic.Bool
	var lastCancelMsg atomic.Value
	var lastJobStr atomic.Value
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/beforeRunOneJobStep", func(job *model.Job) {
		lastJobStr.Store(job.String())
		if cancelled.Load() {
			return
		}
		if job.State != model.JobStateRunning {
			return
		}
		if job.Type != model.ActionMultiSchemaChange || job.MultiSchemaInfo == nil {
			return
		}
		q := strings.ToLower(job.Query)
		if !strings.Contains(q, "alter table") || !strings.Contains(q, "add column") || !strings.Contains(q, "auto_increment") {
			return
		}

		rs := tkCancel.MustQuery(fmt.Sprintf("admin cancel ddl jobs %d", job.ID))
		if len(rs.Rows()) > 0 {
			msg := rs.Rows()[0][1].(string)
			lastCancelMsg.Store(msg)
			if strings.Contains(msg, "success") {
				cancelled.Store(true)
			}
		}
	})

	sql := "alter table t " +
		"add column id bigint not null auto_increment, " +
		"add primary key (id) nonclustered"
	err := tk.ExecToErr(sql)
	if err == nil {
		t.Fatalf("expected ErrCancelledDDLJob, got nil; lastJob=%v lastCancelMsg=%v", lastJobStr.Load(), lastCancelMsg.Load())
	}
	originErr := errors.Cause(err)
	switch v := originErr.(type) {
	case *terror.Error:
		if int(v.Code()) != errno.ErrCancelledDDLJob {
			t.Fatalf("unexpected error code, want=%d got=%d, err=%v, lastJob=%v lastCancelMsg=%v", errno.ErrCancelledDDLJob, v.Code(), err, lastJobStr.Load(), lastCancelMsg.Load())
		}
	case *terror.TiDBError:
		if int(v.MYSQLERRNO) != errno.ErrCancelledDDLJob {
			t.Fatalf("unexpected error code, want=%d got=%d, err=%v, lastJob=%v lastCancelMsg=%v", errno.ErrCancelledDDLJob, v.MYSQLERRNO, err, lastJobStr.Load(), lastCancelMsg.Load())
		}
	default:
		t.Fatalf("unexpected error type %T: %v; lastJob=%v lastCancelMsg=%v", originErr, err, lastJobStr.Load(), lastCancelMsg.Load())
	}

	// DDL should rollback cleanly: the new column and PK should not be visible.
	tk.MustQuery("show create table t").CheckNotContain("`id`")
	tk.MustQuery("show create table t").CheckNotContain("PRIMARY KEY")
}
