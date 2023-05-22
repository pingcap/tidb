// Copyright 2022 PingCAP, Inc.
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
	"fmt"
	"math/rand"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/ddl/internal/callback"
	"github.com/pingcap/tidb/errno"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/sqlexec"
	"github.com/stretchr/testify/require"
	atomicutil "go.uber.org/atomic"
)

type TestTableUser struct {
	id          int64
	user        string
	name        string
	age         int
	province    string
	city        string
	phone       string
	createdTime time.Time
	updatedTime time.Time
}

func generateString(letterRunes []rune, length int) (string, error) {
	b := make([]rune, length)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return string(b), nil
}

func generateName(length int) (string, error) {
	var letterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ ")
	return generateString(letterRunes, length)
}

func generatePhone(length int) (string, error) {
	var numberRunes = []rune("0123456789")
	return generateString(numberRunes, length)
}

func (tu *TestTableUser) generateAttributes() (err error) {
	tu.user, err = generateName(rand.Intn(127))
	if err != nil {
		return err
	}
	tu.name, err = generateName(rand.Intn(127))
	if err != nil {
		return err
	}

	tu.age = rand.Intn(100)

	tu.province, err = generateName(rand.Intn(32))
	if err != nil {
		return err
	}
	tu.city, err = generateName(rand.Intn(32))
	if err != nil {
		return err
	}
	tu.phone, err = generatePhone(14)
	if err != nil {
		return err
	}
	tu.createdTime = time.Now()
	tu.updatedTime = time.Now()

	return nil
}

func (tu *TestTableUser) insertStmt() string {
	return fmt.Sprintf("INSERT INTO t_user(user, name, age, province, city, phone, created_time, updated_time) VALUES ('%s', '%s', %d, '%s', '%s', '%s', '%s', '%s')",
		tu.user, tu.name, tu.age, tu.province, tu.city, tu.phone, tu.createdTime, tu.updatedTime)
}

type testPauseAndResumeJob struct {
	sql         string
	ok          bool
	jobState    interface{} // model.SchemaState | []model.SchemaState
	onJobBefore bool
	onJobUpdate bool
	prepareSQL  []string
}

var allPauseJobTestCase = []testPauseAndResumeJob{
	// Add primary key
	{"alter table t_user add primary key idx_id (id)", true, model.StateNone, true, false, nil},
	{"alter table t_user add primary key idx_id (id)", true, model.StateDeleteOnly, true, true, nil},
	{"alter table t_user add primary key idx_id (id)", true, model.StateWriteOnly, true, true, nil},
	{"alter table t_user add primary key idx_id (id)", true, model.StateWriteReorganization, true, true, nil},
	{"alter table t_user add primary key idx_id (id)", false, model.StatePublic, false, true, nil},

	// Drop primary key
	{"alter table t_user drop primary key", true, model.StatePublic, true, false, nil},
	{"alter table t_user drop primary key", false, model.StateWriteOnly, true, false, nil},
	{"alter table t_user drop primary key", false, model.StateWriteOnly, true, false, []string{"alter table t_user add primary key idx_id (id)"}},
	{"alter table t_user drop primary key", false, model.StateDeleteOnly, true, false, []string{"alter table t_user add primary key idx_id (id)"}},
	{"alter table t_user drop primary key", false, model.StateDeleteOnly, false, true, []string{"alter table t_user add primary key idx_id (id)"}},

	// Add unique key
	{"alter table t_user add unique index idx_name (id)", true, model.StateNone, true, false, nil},
	{"alter table t_user add unique index idx_name (id)", true, model.StateDeleteOnly, true, true, nil},
	{"alter table t_user add unique index idx_name (id)", true, model.StateWriteOnly, true, true, nil},
	{"alter table t_user add unique index idx_name (id)", true, model.StateWriteReorganization, true, true, nil},
	{"alter table t_user add unique index idx_name (id)", false, model.StatePublic, false, true, nil},

	{"alter table t_user add index idx_phone (phone)", true, model.StateNone, true, false, nil},
	{"alter table t_user add index idx_phone (phone)", true, model.StateDeleteOnly, true, true, nil},
	{"alter table t_user add index idx_phone (phone)", true, model.StateWriteOnly, true, true, nil},
	{"alter table t_user add index idx_phone (phone)", false, model.StatePublic, false, true, nil},

	// Add column.
	{"alter table t_user add column c4 bigint", true, model.StateNone, true, false, nil},
	{"alter table t_user add column c4 bigint", true, model.StateDeleteOnly, true, true, nil},
	{"alter table t_user add column c4 bigint", true, model.StateWriteOnly, true, true, nil},
	{"alter table t_user add column c4 bigint", true, model.StateWriteReorganization, true, true, nil},
	{"alter table t_user add column c4 bigint", false, model.StatePublic, false, true, nil},

	// Create table.
	{"create table test_create_table(a int)", true, model.StateNone, true, false, nil},
	{"create table test_create_table(a int)", false, model.StatePublic, false, true, nil},

	// Drop table.
	{"drop table test_create_table", true, model.StatePublic, true, false, nil},
	{"drop table test_create_table", false, model.StateWriteOnly, true, true, []string{"create table if not exists test_create_table(a int)"}},
	{"drop table test_create_table", false, model.StateDeleteOnly, true, true, []string{"create table if not exists test_create_table(a int)"}},
	{"drop table test_create_table", false, model.StateNone, false, true, []string{"create table if not exists test_create_table(a int)"}},

	// Create schema.
	{"create database test_create_db", true, model.StateNone, true, false, nil},
	{"create database test_create_db", false, model.StatePublic, false, true, nil},

	// Drop schema.
	{"drop database test_create_db", true, model.StatePublic, true, false, nil},
	{"drop database test_create_db", false, model.StateWriteOnly, true, true, []string{"create database if not exists test_create_db"}},
	{"drop database test_create_db", false, model.StateDeleteOnly, true, true, []string{"create database if not exists test_create_db"}},
	{"drop database test_create_db", false, model.StateNone, false, true, []string{"create database if not exists test_create_db"}},

	// Drop column.
	{"alter table t_user drop column c3", true, model.StatePublic, true, false, []string{"alter table t_user drop column if exists c3", "alter table t_user add column c3 bigint"}},
	{"alter table t_user drop column c3", false, model.StateDeleteOnly, true, false, []string{"alter table t_user drop column if exists c3", "alter table t_user add column c3 bigint"}},
	{"alter table t_user drop column c3", false, model.StateDeleteOnly, false, true, []string{"alter table t_user drop column if exists c3", "alter table t_user add column c3 bigint"}},
	{"alter table t_user drop column c3", false, model.StateWriteOnly, true, true, []string{"alter table t_user drop column if exists c3", "alter table t_user add column c3 bigint"}},
	{"alter table t_user drop column c3", false, model.StateDeleteReorganization, true, true, []string{"alter table t_user drop column if exists c3", "alter table t_user add column c3 bigint"}},
	{"alter table t_user drop column c3", false, model.StateNone, false, true, []string{"alter table t_user drop column if exists c3", "alter table t_user add column c3 bigint"}},

	// Drop column with index.
	{"alter table t_user drop column c3", true, model.StatePublic, true, false, []string{"alter table t_user drop column if exists c3", "alter table t_user add column c3 bigint", "alter table t_user add index idx_c3(c3)"}},
	{"alter table t_user drop column c3", false, model.StateDeleteOnly, true, false, []string{"alter table t_user drop column if exists c3", "alter table t_user add column c3 bigint", "alter table t_user add index idx_c3(c3)"}},
	{"alter table t_user drop column c3", false, model.StateDeleteOnly, false, true, []string{"alter table t_user drop column if exists c3", "alter table t_user add column c3 bigint", "alter table t_user add index idx_c3(c3)"}},
	{"alter table t_user drop column c3", false, model.StateWriteOnly, true, true, []string{"alter table t_user drop column if exists c3", "alter table t_user add column c3 bigint", "alter table t_user add index idx_c3(c3)"}},
	{"alter table t_user drop column c3", false, model.StateDeleteReorganization, true, true, []string{"alter table t_user drop column if exists c3", "alter table t_user add column c3 bigint", "alter table t_user add index idx_c3(c3)"}},
	{"alter table t_user drop column c3", false, model.StateNone, false, true, []string{"alter table t_user drop column if exists c3", "alter table t_user add column c3 bigint", "alter table t_user add index idx_c3(c3)"}},

	// Modify column, no reorg.
	{"alter table t_user modify column c3 mediumint", true, model.StateNone, true, false, []string{"alter table t_user drop column if exists c3", "alter table t_user add column c3 bigint"}},
	{"alter table t_user modify column c3 int", false, model.StatePublic, false, true, []string{"alter table t_user drop column if exists c3", "alter table t_user add column c3 bigint"}},

	// Modify column, reorg.
	{"alter table t_user modify column c3 char(10)", true, model.StateNone, true, false, []string{"alter table t_user drop column if exists c3", "alter table t_user add column c3 bigint"}},
	{"alter table t_user modify column c3 char(10)", true, model.StateDeleteOnly, true, true, []string{"alter table t_user drop column if exists c3", "alter table t_user add column c3 bigint"}},
	{"alter table t_user modify column c3 char(10)", true, model.StateWriteOnly, true, true, []string{"alter table t_user drop column if exists c3", "alter table t_user add column c3 bigint"}},
	{"alter table t_user modify column c3 char(10)", true, model.StateWriteReorganization, true, true, []string{"alter table t_user drop column if exists c3", "alter table t_user add column c3 bigint"}},
	{"alter table t_user modify column c3 char(10)", false, model.StatePublic, false, true, []string{"alter table t_user drop column if exists c3", "alter table t_user add column c3 bigint"}},
}

func isCommandSuccess(rs *testkit.Result) bool {
	return strings.Contains(rs.Rows()[0][1].(string), "success")
}

func TestPauseAndResumeMain(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomainWithSchemaLease(t, 100*time.Millisecond)
	tk := testkit.NewTestKit(t, store)
	tkCommand := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec(`CREATE TABLE if not exists t_user (
        id int(11) NOT NULL AUTO_INCREMENT,
        user varchar(128) NOT NULL,
        name varchar(128) NOT NULL,
        age int(11) NOT NULL,
        province varchar(32) NOT NULL DEFAULT '',
        city varchar(32) NOT NULL DEFAULT '',
        phone varchar(16) NOT NULL DEFAULT '',
        created_time datetime NOT NULL,
        updated_time datetime NOT NULL
      ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;`)

	idx := 0
	rowCount := 1000
	tu := &TestTableUser{}
	for idx < rowCount {
		_ = tu.generateAttributes()
		tk.MustExec(tu.insertStmt())

		idx += 1
	}

	logger := logutil.BgLogger()
	ddl.ReorgWaitTimeout = 10 * time.Millisecond
	tk.MustExec("set @@global.tidb_ddl_reorg_batch_size = 2")
	tk.MustExec("set @@global.tidb_ddl_reorg_worker_cnt = 1")
	tk = testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	hook := &callback.TestDDLCallback{Do: dom}
	i := atomicutil.NewInt64(0)

	isPaused := atomicutil.NewBool(false)
	pauseWhenReorgNotStart := atomicutil.NewBool(false)
	isCancelled := atomicutil.NewBool(false)
	cancelWhenReorgNotStart := atomicutil.NewBool(false)
	commandHook := func(job *model.Job) {
		logger.Info("allPauseJobTestCase commandHook: " + job.String())
		if testMatchCancelState(t, job, allPauseJobTestCase[i.Load()].jobState, allPauseJobTestCase[i.Load()].sql) && !isPaused.Load() {
			logger.Info("allPauseJobTestCase commandHook: pass the check")
			if !pauseWhenReorgNotStart.Load() && job.SchemaState == model.StateWriteReorganization && job.MayNeedReorg() && job.RowCount == 0 {
				logger.Info("allPauseJobTestCase commandHook: reorg, return")
				return
			}
			rs := tkCommand.MustQuery(fmt.Sprintf("admin pause ddl jobs %d", job.ID))
			logger.Info("allPauseJobTestCase commandHook: " + rs.Rows()[0][1].(string))
			isPaused.Store(isCommandSuccess(rs))
			time.Sleep(1 * time.Second)

			rs = tkCommand.MustQuery(fmt.Sprintf("admin cancel ddl jobs %d", job.ID))
			logger.Info("allPauseJobTestCase cancelHook: " + rs.Rows()[0][1].(string))
			isCancelled.Store(isCommandSuccess(rs))
		}
	}

	dom.DDL().SetHook(hook.Clone())

	restHook := func(h *callback.TestDDLCallback) {
		h.OnJobRunBeforeExported = nil
		h.OnJobRunAfterExported = nil
		dom.DDL().SetHook(h.Clone())
	}

	registHook := func(h *callback.TestDDLCallback) {
		h.OnJobRunBeforeExported = commandHook
		dom.DDL().SetHook(h.Clone())
	}

	for idx, tc := range allPauseJobTestCase {
		i.Store(int64(idx))
		msg := fmt.Sprintf("sql: %s, state: %s", tc.sql, tc.jobState)

		logger.Info("allPauseJobTestCase: " + msg)

		restHook(hook)
		for _, prepareSQL := range tc.prepareSQL {
			logger.Info("Prepare SQL:" + prepareSQL)
			tk.MustExec(prepareSQL)
		}

		isPaused.Store(false)
		isCancelled.Store(false)
		pauseWhenReorgNotStart.Store(false)
		cancelWhenReorgNotStart.Store(false)
		registHook(hook)

		if tc.ok {
			tk.MustGetErrCode(tc.sql, errno.ErrCancelledDDLJob)
			require.Equal(t, tc.ok, isPaused.Load(), msg)
			require.Equal(t, tc.ok, isCancelled.Load(), msg)
		} else {
			tk.MustExec(tc.sql)
		}

		// TODO: should add some check on Job during reorganization
	}
}

func TestPauseJobWriteConflict(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomainWithSchemaLease(t, dbTestLease)

	tk1 := testkit.NewTestKit(t, store)
	tk2 := testkit.NewTestKit(t, store)

	tk1.MustExec("use test")

	tk1.MustExec("create table t(id int)")

	var jobID int64
	var pauseErr error
	var pauseRS []sqlexec.RecordSet
	hook := &callback.TestDDLCallback{Do: dom}
	d := dom.DDL()
	originalHook := d.GetHook()
	d.SetHook(hook)
	defer d.SetHook(originalHook)

	// Test when pause cannot be retried and adding index succeeds.
	hook.OnJobRunBeforeExported = func(job *model.Job) {
		if job.Type == model.ActionAddIndex && job.State == model.JobStateRunning && job.SchemaState == model.StateWriteReorganization {
			require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/ddl/mockFailedCommandOnConcurencyDDL", `return(true)`))
			defer func() {
				require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/ddl/mockFailedCommandOnConcurencyDDL"))
			}()

			jobID = job.ID
			stmt := fmt.Sprintf("admin pause ddl jobs %d", jobID)
			pauseRS, pauseErr = tk2.Session().Execute(context.Background(), stmt)
		}
	}
	tk1.MustExec("alter table t add index (id)")
	require.EqualError(t, pauseErr, "mock failed admin command on ddl jobs")

	var cancelRS []sqlexec.RecordSet
	var cancelErr error
	hook.OnJobRunBeforeExported = func(job *model.Job) {
		if job.Type == model.ActionAddIndex && job.State == model.JobStateRunning && job.SchemaState == model.StateWriteReorganization {
			jobID = job.ID
			stmt := fmt.Sprintf("admin pause ddl jobs %d", jobID)
			pauseRS, pauseErr = tk2.Session().Execute(context.Background(), stmt)

			time.Sleep(5 * time.Second)
			stmt = fmt.Sprintf("admin cancel ddl jobs %d", jobID)
			cancelRS, cancelErr = tk2.Session().Execute(context.Background(), stmt)
		}
	}
	tk1.MustGetErrCode("alter table t add index (id)", errno.ErrCancelledDDLJob)
	require.NoError(t, pauseErr)
	require.NoError(t, cancelErr)
	result := tk2.ResultSetToResultWithCtx(context.Background(), pauseRS[0], "pause ddl job successfully")
	result.Check(testkit.Rows(fmt.Sprintf("%d successful", jobID)))
	result = tk2.ResultSetToResultWithCtx(context.Background(), cancelRS[0], "cancel ddl job successfully")
	result.Check(testkit.Rows(fmt.Sprintf("%d successful", jobID)))
}

func TestPauseJobNegative(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomainWithSchemaLease(t, dbTestLease)

	tk1 := testkit.NewTestKit(t, store)
	tk2 := testkit.NewTestKit(t, store)

	tk1.MustExec("use test")
	tk1.MustExec("create table t(id int)")

	d := dom.DDL()

	var jobID int64
	var pauseErr error
	var jobErrs []error

	hook := &callback.TestDDLCallback{Do: dom}
	originalHook := d.GetHook()
	defer d.SetHook(originalHook)
	d.SetHook(hook)
	// Test when pause cannot be retried and adding index succeeds.
	hook.OnJobRunBeforeExported = func(job *model.Job) {
		if job.Type == model.ActionAddIndex && job.State == model.JobStateRunning &&
			job.SchemaState == model.StateWriteReorganization {
			require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/ddl/mockCommitFailedOnDDLCommand", `return(true)`))
			defer func() {
				require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/ddl/mockCommitFailedOnDDLCommand"))
			}()
			jobID = job.ID
			jobErrs, pauseErr = ddl.PauseJobs(tk2.Session(), []int64{jobID})
		}
	}
	tk1.MustExec("alter table t add index (id)")
	require.EqualError(t, pauseErr, "mock commit failed on admin command on ddl jobs")
	require.Len(t, jobErrs, 1)
}

func TestResumeJobPositive(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomainWithSchemaLease(t, dbTestLease)

	tk1 := testkit.NewTestKit(t, store)
	tk2 := testkit.NewTestKit(t, store)

	tk1.MustExec("use test")
	tk1.MustExec("create table t(id int)")

	d := dom.DDL()

	var jobID int64
	var pauseErr error

	hook := &callback.TestDDLCallback{Do: dom}
	originalHook := d.GetHook()
	defer d.SetHook(originalHook)
	d.SetHook(hook)

	var pauseRS []sqlexec.RecordSet
	var resumeRS []sqlexec.RecordSet
	var resumeErr error
	// Test when pause cannot be retried and adding index succeeds.
	hook.OnJobRunBeforeExported = func(job *model.Job) {
		if job.Type == model.ActionAddIndex && job.State == model.JobStateRunning &&
			job.SchemaState == model.StateWriteReorganization {
			jobID = job.ID
			stmt := fmt.Sprintf("admin pause ddl jobs %d", jobID)
			pauseRS, pauseErr = tk2.Session().Execute(context.Background(), stmt)
		}
	}
	// Test when pause cannot be retried and adding index succeeds.
	hook.OnJobRunAfterExported = func(job *model.Job) {
		if job.Type == model.ActionAddIndex && job.State == model.JobStatePaused {
			time.Sleep(5 * time.Second)
			stmt := fmt.Sprintf("admin resume ddl jobs %d", jobID)
			resumeRS, resumeErr = tk2.Session().Execute(context.Background(), stmt)
		}
	}

	tk1.MustExec("alter table t add index (id)")

	require.NoError(t, pauseErr)
	result := tk2.ResultSetToResultWithCtx(context.Background(), pauseRS[0], "pause ddl job successfully")
	result.Check(testkit.Rows(fmt.Sprintf("%d successful", jobID)))

	require.NoError(t, resumeErr)
	result = tk2.ResultSetToResultWithCtx(context.Background(), resumeRS[0], "resume ddl job successfully")
	result.Check(testkit.Rows(fmt.Sprintf("%d successful", jobID)))
}
