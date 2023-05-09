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
	"fmt"
	"math/rand"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/ddl/internal/callback"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/testkit"
	"github.com/stretchr/testify/require"
	atomicutil "go.uber.org/atomic"
)

type testPauseAndResumeJob struct {
	sql         string
	ok          bool
	jobState    interface{} // model.SchemaState | []model.SchemaState
	onJobBefore bool
	onJobUpdate bool
	prepareSQL  []string
}

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
	{"alter table t_user add unique index idx_name (user)", true, model.StateNone, true, false, nil},
	{"alter table t_user add unique index idx_name (user)", true, model.StateDeleteOnly, true, true, nil},
	{"alter table t_user add unique index idx_name (user)", true, model.StateWriteOnly, true, true, nil},
	{"alter table t_user add unique index idx_name (user)", true, model.StateWriteReorganization, true, true, nil},
	{"alter table t_user add unique index idx_name (user)", false, model.StatePublic, false, true, nil},

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
	{"alter table t_user drop column c3", true, model.StatePublic, true, false, nil},
	{"alter table t_user drop column c3", false, model.StateDeleteOnly, true, false, nil},
	{"alter table t_user drop column c3", false, model.StateDeleteOnly, false, true, []string{"alter table t_user add column c3 bigint"}},
	{"alter table t_user drop column c3", false, model.StateWriteOnly, true, true, []string{"alter table t_user add column c3 bigint"}},
	{"alter table t_user drop column c3", false, model.StateDeleteReorganization, true, true, []string{"alter table t_user add column c3 bigint"}},
	{"alter table t_user drop column c3", false, model.StateNone, false, true, []string{"alter table t_user add column c3 bigint"}},

	// Drop column with index.
	{"alter table t_user drop column c3", true, model.StatePublic, true, false, []string{"alter table t_user add column c3 bigint", "alter table t_user add index idx_c3(c3)"}},
	{"alter table t_user drop column c3", false, model.StateDeleteOnly, true, false, nil},
	{"alter table t_user drop column c3", false, model.StateDeleteOnly, false, true, []string{"alter table t_user add column c3 bigint", "alter table t_user add index idx_c3(c3)"}},
	{"alter table t_user drop column c3", false, model.StateWriteOnly, true, true, []string{"alter table t_user add column c3 bigint", "alter table t_user add index idx_c3(c3)"}},
	{"alter table t_user drop column c3", false, model.StateDeleteReorganization, true, true, []string{"alter table t_user add column c3 bigint", "alter table t_user add index idx_c3(c3)"}},
	{"alter table t_user drop column c3", false, model.StateNone, false, true, []string{"alter table t_user add column c3 bigint", "alter table t_user add index idx_c3(c3)"}},

	// Modify column, no reorg.
	{"alter table t_user modify column c11 mediumint", true, model.StateNone, true, false, nil},
	{"alter table t_user modify column c11 int", false, model.StatePublic, false, true, nil},

	// Modify column, reorg.
	{"alter table t_user modify column c11 char(10)", true, model.StateNone, true, false, nil},
	{"alter table t_user modify column c11 char(10)", true, model.StateDeleteOnly, true, true, nil},
	{"alter table t_user modify column c11 char(10)", true, model.StateWriteOnly, true, true, nil},
	{"alter table t_user modify column c11 char(10)", true, model.StateWriteReorganization, true, true, nil},
	{"alter table t_user modify column c11 char(10)", false, model.StatePublic, false, true, nil},
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
        updated_time datetime NOT NULL,
        PRIMARY KEY (id)
      ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;`)

	idx := 0
	rowCount := 100000
	tu := &TestTableUser{}
	for idx < rowCount {
		_ = tu.generateAttributes()
		tk.MustExec(tu.insertStmt())

		idx += 1
	}

	ddl.ReorgWaitTimeout = 10 * time.Millisecond
	tk.MustExec("set @@global.tidb_ddl_reorg_batch_size = 2")
	tk.MustExec("set @@global.tidb_ddl_reorg_worker_cnt = 1")
	tk = testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/ddl/mockBackfillSlow", "return"))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/ddl/mockBackfillSlow"))
	}()

	hook := &callback.TestDDLCallback{Do: dom}
	i := atomicutil.NewInt64(0)

	isPaused := atomicutil.NewBool(false)
	pauseWhenReorgNotStart := atomicutil.NewBool(false)
	pauseHook := func(job *model.Job) {
		if testMatchCancelState(t, job, allPauseJobTestCase[i.Load()].jobState, allPauseJobTestCase[i.Load()].sql) && !isPaused.Load() {
			if !pauseWhenReorgNotStart.Load() && job.SchemaState == model.StateWriteReorganization && job.MayNeedReorg() && job.RowCount == 0 {
				return
			}
			rs := tkCommand.MustQuery(fmt.Sprintf("admin pause ddl jobs %d", job.ID))
			isPaused.Store(isCommandSuccess(rs))
			time.Sleep(1 * time.Second)
		}
	}

	isCancelled := atomicutil.NewBool(false)
	cancelWhenReorgNotStart := atomicutil.NewBool(false)
	cancelHook := func(job *model.Job) {
		if testMatchCancelState(t, job, allPauseJobTestCase[i.Load()].jobState, allPauseJobTestCase[i.Load()].sql) && !isCancelled.Load() {
			if !cancelWhenReorgNotStart.Load() && job.SchemaState == model.StateWriteReorganization && job.MayNeedReorg() && job.RowCount == 0 {
				return
			}
			rs := tkCommand.MustQuery(fmt.Sprintf("admin cancel ddl jobs %d", job.ID))
			isCancelled.Store(isCommandSuccess(rs))
		}
	}

	dom.DDL().SetHook(hook.Clone())

	restHook := func(h *callback.TestDDLCallback) {
		h.OnJobRunBeforeExported = nil
		h.OnJobRunAfterExported = nil
		h.OnJobUpdatedExported.Store(nil)
		dom.DDL().SetHook(h.Clone())
	}

	registHook := func(h *callback.TestDDLCallback) {
		h.OnJobRunBeforeExported = pauseHook
		h.OnJobUpdatedExported.Store(&cancelHook)
		dom.DDL().SetHook(h.Clone())
	}

	for idx, tc := range allPauseJobTestCase {
		i.Store(int64(idx))
		msg := fmt.Sprintf("sql: %s, state: %s", tc.sql, tc.jobState)

		restHook(hook)
		for _, prepareSQL := range tc.prepareSQL {
			tk.MustExec(prepareSQL)
		}

		isPaused.Store(false)
		isCancelled.Store(false)
		pauseWhenReorgNotStart.Store(true)
		registHook(hook)

		tk.MustExec(tc.sql)

		if tc.ok {
			require.Equal(t, tc.ok, isPaused.Load(), msg)
			require.Equal(t, tc.ok, isCancelled.Load(), msg)
		}

		// TODO: should add some check on Job during reorganization
	}
}
