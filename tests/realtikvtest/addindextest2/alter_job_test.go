// Copyright 2025 PingCAP, Inc.
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

package addindextest

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/ddl/ingest"
	"github.com/pingcap/tidb/pkg/dxf/framework/proto"
	"github.com/pingcap/tidb/pkg/dxf/framework/testutil"
	"github.com/pingcap/tidb/pkg/dxf/operator"
	"github.com/pingcap/tidb/pkg/lightning/backend/local"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/pingcap/tidb/tests/realtikvtest"
	"github.com/stretchr/testify/require"
)

func TestAlterThreadRightAfterJobFinish(t *testing.T) {
	if kerneltype.IsNextGen() {
		t.Skip("DXF is always enabled on nextgen")
	}
	store := realtikvtest.CreateMockStoreAndSetup(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set global tidb_enable_dist_task=0;")
	t.Cleanup(func() {
		tk.MustExec("set global tidb_enable_dist_task=1;")
	})
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (c1 int primary key, c2 int)")
	tk.MustExec("insert t values (1, 1), (2, 2), (3, 3);")
	var updated bool
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/checkJobCancelled", func(job *model.Job) {
		if !updated && job.Type == model.ActionAddIndex && job.SchemaState == model.StateWriteReorganization {
			updated = true
			tk2 := testkit.NewTestKit(t, store)
			tk2.MustExec(fmt.Sprintf("admin alter ddl jobs %d thread = 1", job.ID))
		}
	})
	var pipeClosed atomic.Bool
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/afterPipeLineClose", func(*operator.AsyncPipeline) {
		pipeClosed.Store(true)
		time.Sleep(5 * time.Second)
	})
	var onUpdateJobParam bool
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/onUpdateJobParam", func() {
		if !onUpdateJobParam {
			onUpdateJobParam = true
			for !pipeClosed.Load() {
				time.Sleep(100 * time.Millisecond)
			}
		}
	})
	tk.MustExec("alter table t add index idx(c2)")
	require.True(t, updated)
	require.True(t, pipeClosed.Load())
}

func TestAlterJobOnDXF(t *testing.T) {
	if kerneltype.IsNextGen() {
		t.Skip("resource params are calculated automatically on nextgen for add-index, we don't support alter them")
	}
	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/util/cpu/mockNumCpu", `return(16)`)
	testutil.ReduceCheckInterval(t)
	store := realtikvtest.CreateMockStoreAndSetup(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("drop database if exists test;")
	tk.MustExec("create database test;")
	tk.MustExec("use test;")
	if kerneltype.IsClassic() {
		tk.MustExec(`set global tidb_enable_dist_task=1;`)
	}
	tk.MustExec("create table t1(a bigint auto_random primary key);")
	for range 16 {
		tk.MustExec("insert into t1 values (), (), (), ()")
	}
	tk.MustExec("split table t1 between (3) and (8646911284551352360) regions 50;")
	tk.MustExec("set @@tidb_ddl_reorg_worker_cnt = 1")
	tk.MustExec("set @@tidb_ddl_reorg_batch_size = 32")
	tk.MustExec(`set @@global.tidb_cloud_storage_uri = ""`)
	if kerneltype.IsClassic() {
		tk.MustExec("set global tidb_ddl_reorg_max_write_speed = 16")
		t.Cleanup(func() {
			tk.MustExec("set global tidb_ddl_reorg_max_write_speed = 0")
		})
	}
	var pipeClosed bool
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/afterPipeLineClose", func(pipe *operator.AsyncPipeline) {
		pipeClosed = true
		reader, writer := pipe.GetReaderAndWriter()
		require.EqualValues(t, 4, reader.GetWorkerPoolSize())
		require.EqualValues(t, 6, writer.GetWorkerPoolSize())
	})
	var finishedSubtasks int
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/mockDMLExecutionAddIndexSubTaskFinish", func(be *local.Backend) {
		finishedSubtasks++
		require.EqualValues(t, 1024, be.GetWriteSpeedLimit())
	})
	var modified atomic.Bool
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/dxf/framework/taskexecutor/afterDetectAndHandleParamModify", func(_ proto.Step) {
		modified.Store(true)
	})
	var once sync.Once
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/scanRecordExec", func(reorgMeta *model.DDLReorgMeta) {
		once.Do(func() {
			tk1 := testkit.NewTestKit(t, store)
			rows := tk1.MustQuery("select job_id from mysql.tidb_ddl_job").Rows()
			require.Len(t, rows, 1)
			tk1.MustExec(fmt.Sprintf("admin alter ddl jobs %s thread = 8, batch_size = 256, max_write_speed=1024", rows[0][0]))
			require.Eventually(t, func() bool {
				return modified.Load()
			}, 20*time.Second, 100*time.Millisecond)
			require.Equal(t, 256, reorgMeta.GetBatchSize())
		})
	})
	tk.MustExec("alter table t1 add index idx(a);")
	require.True(t, pipeClosed)
	require.EqualValues(t, 1, finishedSubtasks)
	require.True(t, modified.Load())
	tk.MustExec("admin check index t1 idx;")
}

const issue65958ReproChildEnv = "TIDB_ISSUE_65958_REPRO_CHILD"

// TestIssue65958ReproCleanupCrashOnCancelDistTask reproduces issue #65958:
// canceling an add-index dist-task while local backend is still importing, then
// the tmp_ddl cleanup loop deletes the job directory, and Pebble later fatals on
// missing `00000x.sst`.
func TestIssue65958ReproCleanupCrashOnCancelDistTask(t *testing.T) {
	if kerneltype.IsNextGen() {
		t.Skip("nextgen always uses DXF; repro currently targeted at classic kernel")
	}
	if os.Getenv(issue65958ReproChildEnv) == "1" {
		testIssue65958ReproChild(t)
		return
	}

	cmd := exec.Command(os.Args[0], "-test.run=^TestIssue65958ReproCleanupCrashOnCancelDistTask$")
	cmd.Env = append(os.Environ(), issue65958ReproChildEnv+"=1")
	out, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("expected child process to crash (issue #65958 repro), but it exited normally:\n%s", out)
	}

	outStr := string(out)
	if !strings.Contains(outStr, ".sst:") ||
		!strings.Contains(outStr, "orig err: open") ||
		!strings.Contains(outStr, "list err: open") ||
		!strings.Contains(outStr, "no such file or directory") {
		t.Fatalf("child exited non-zero, but output does not look like a missing SST fatal: %v\n%s", err, out)
	}
}

func testIssue65958ReproChild(t *testing.T) {
	tmpDir := t.TempDir()
	restore := config.RestoreFunc()
	t.Cleanup(restore)

	// Speed up the tmp_ddl cleanup loop so we don't need to wait ~1 minute.
	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/ddl/mockCleanUpTempDirLoopInterval", "return(\"200ms\")")
	// Must set TempDir before bootstrap. The DDL background loop uses this at init.
	config.UpdateGlobal(func(conf *config.Config) {
		conf.TempDir = tmpDir
	})

	store, _ := realtikvtest.CreateMockStoreAndDomainAndSetup(t, realtikvtest.WithAllocPort(true))
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("create database if not exists test")
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a bigint primary key, b bigint)")

	// Need enough rows to generate on-disk Pebble SSTables.
	for i := 0; i < 2000; i += 1000 {
		vals := make([]string, 0, 1000)
		for j := 0; j < 1000 && i+j < 2000; j++ {
			id := i + j
			vals = append(vals, fmt.Sprintf("(%d,%d)", id, id))
		}
		tk.MustExec("insert into t values " + strings.Join(vals, ","))
	}

	var jobID atomic.Int64
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/beforeRunOneJobStep", func(job *model.Job) {
		if job.Type != model.ActionAddIndex || job.TableName != "t" || job.SchemaState != model.StateWriteReorganization {
			return
		}
		jobID.Store(job.ID)
	})

	readyForImport := make(chan struct{}, 1)
	resumeImport := make(chan struct{})
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/lightning/backend/local/ReadyForImportEngine", func() {
		select {
		case readyForImport <- struct{}{}:
		default:
		}
		<-resumeImport
	})

	alterDone := make(chan error, 1)
	go func() {
		rs, err := tk.Exec("alter table t add index idx(b)")
		if rs != nil {
			_ = rs.Close()
		}
		alterDone <- err
	}()

	<-readyForImport
	for jobID.Load() == 0 {
		time.Sleep(50 * time.Millisecond)
	}
	id := jobID.Load()

	tk2 := testkit.NewTestKit(t, store)
	tk2.MustExec(fmt.Sprintf("admin cancel ddl jobs %d", id))

	jobDir := filepath.Join(ingest.GetIngestTempDataDir(), strconv.FormatInt(id, 10))
	for {
		_, err := os.Stat(jobDir)
		if err != nil && os.IsNotExist(err) {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	close(resumeImport)
	err := <-alterDone
	require.ErrorContains(t, err, "Cancelled DDL job")
}
