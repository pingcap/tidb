package addindextest_test

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/tests/realtikvtest"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/oracle"
	"go.uber.org/zap"
)

func TestCreateNonUniqueIndexDis(t *testing.T) {
	store, dom := realtikvtest.CreateMockStoreAndDomainAndSetup(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("drop database if exists addindex;")
	tk.MustExec("create database addindex;")
	tk.MustExec("use addindex;")
	tk.MustExec(`set global tidb_ddl_enable_fast_reorg=on;`)

	// Prepare data.
	tk.MustExec("create table t(a bigint auto_random primary key) partition by hash(a) partitions 8;")
	for i := 0; i < 1000; i++ {
		tk.MustExec("insert into t values ();")
	}
	var minVal, maxVal string
	rs := tk.MustQuery("select min(a) from t;")
	minVal = rs.Rows()[0][0].(string)
	rs = tk.MustQuery("select max(a) from t;")
	maxVal = rs.Rows()[0][0].(string)
	tk.MustExec(fmt.Sprintf("split table t between (%s) and (%s) regions 50;", minVal, maxVal))

	hook := newTestCallBack(t, dom)
	var jobMeta *model.Job
	getJobCh := make(chan struct{})
	waitBackfillCh := make(chan struct{})
	hook.OnJobRunBeforeExported = func(job *model.Job) {
		if job.SchemaState == model.StateWriteOnly {
			jobMeta = job
			getJobCh <- struct{}{}
			<-waitBackfillCh
			job.SchemaState = model.StatePublic
			job.State = model.JobStateDone
		}
	}

	dom.DDL().SetHook(hook)

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		tk.Exec("alter table t add index idx(a);")
		wg.Done()
	}()

	<-getJobCh

	jobMeta.SnapshotVer = oracle.GoTimeToTS(time.Now())

	tblMeta, ok := dom.InfoSchema().TableByID(jobMeta.TableID)
	require.True(t, ok)
	idxMeta := tblMeta.Meta().FindIndexByName("idx")
	require.NotNil(t, idxMeta)
	pids := make([]int64, 0)
	for _, p := range tblMeta.Meta().Partition.Definitions {
		pids = append(pids, p.ID)
	}

	gmeta := ddl.BackfillGlobalMeta{
		Job:        *jobMeta.Clone(),
		EleID:      idxMeta.ID,
		EleTypeKey: meta.IndexElementKey,
	}
	gmetaByte, err := json.Marshal(&gmeta)
	require.NoError(t, err)

	smetas := make([]ddl.BackfillSubTaskMeta, 0)
	for _, pid := range pids {
		smetas = append(smetas, ddl.BackfillSubTaskMeta{
			PhysicalTableID: pid,
		})
	}

	scheduler, err := ddl.NewBackfillSchedulerHandle(gmetaByte, 0)
	require.NoError(t, err)
	err = scheduler.InitSubtaskExecEnv(context.Background())
	require.NoError(t, err)

	logutil.BgLogger().Info("begin to exec subtask", zap.Any("len", len(smetas)))
	for _, p := range smetas {
		pmeta, err := json.Marshal(p)
		require.NoError(t, err)
		minimalTasks, _ := scheduler.SplitSubtask(pmeta)

		subtaskExecutor := make([]ddl.BackFillSubtaskExecutor, 0)
		for _, task := range minimalTasks {
			subtaskExecutor = append(subtaskExecutor, ddl.BackFillSubtaskExecutor{Task: task})
		}
		var wg sync.WaitGroup
		wg.Add(len(subtaskExecutor))
		for _, executor := range subtaskExecutor {
			e := executor
			go func(e ddl.BackFillSubtaskExecutor) {
				defer wg.Done()
				err := e.Run(context.Background())
				require.NoError(t, err)
			}(e)
		}
		wg.Wait()
	}
	logutil.BgLogger().Warn("subTask exec finished")
	err = scheduler.CleanupSubtaskExecEnv(context.Background())
	require.NoError(t, err)

	tblMeta.Meta().Indices[0].State = model.StatePublic

	logutil.BgLogger().Info("begin to admin check table")
	tk2 := testkit.NewTestKit(t, store)
	tk2.MustExec("admin check index addindex.t idx;")
	logutil.BgLogger().Info("admin check table done")

	waitBackfillCh <- struct{}{}

	wg.Wait()

	time.Sleep(3 * time.Second)
}
