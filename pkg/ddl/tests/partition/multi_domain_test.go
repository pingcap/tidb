// Copyright 2024 PingCAP, Inc.
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

package partition

import (
	"fmt"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/ddl/util/callback"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/session"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestMultiSchemaReorganizePK(t *testing.T) {
	createSQL := `create table t (c1 INT primary key, c2 CHAR(255), c3 CHAR(255), c4 CHAR(255), c5 CHAR(255)) partition by range (c1) (partition p1 values less than (200), partition pMax values less than (maxvalue))`
	i := 1
	initFn := func(tkO *testkit.TestKit) {
		tkO.MustExec(fmt.Sprintf(`insert into t values (%d,'%s','Original',%d,%d)`, i, "init O", 4185725186-i, 7483634197-i))
		i++
		tkO.MustExec(fmt.Sprintf(`insert into t values (%d,'%s','Original',%d,%d)`, i, "init O", 4185725186-i, 7483634197-i))
		i++
	}
	alterSQL := `alter table t reorganize partition p1 into (partition p0 values less than (100), partition p1 values less than (200))`
	loopFn := func(tkO, tkNO *testkit.TestKit) {
		res := tkO.MustQuery(`select schema_state from information_schema.DDL_JOBS where table_name = 't' order by job_id desc limit 1`)
		schemaState := res.Rows()[0][0].(string)
		tkO.MustExec(fmt.Sprintf(`insert into t values (%d,'%s','Original',%d,%d)`, i, schemaState+" O", 4185725186-i, 7483634197-i))
		i++
		tkNO.MustExec(fmt.Sprintf(`insert into t values (%d,'%s','Original',%d,%d)`, i, schemaState+" NO", 4185725186-i, 7483634197-i))
		i++
	}
	postFn := func(tkO *testkit.TestKit) {
		require.Equal(t, int(6*2+1), i)
		tkO.MustQuery(`select c1,c2 from t`).Sort().Check(testkit.Rows(""+
			"1 init O",
			"10 delete reorganization NO",
			"11 none O",
			"12 none NO",
			"2 init O",
			"3 delete only O",
			"4 delete only NO",
			"5 write only O",
			"6 write only NO",
			"7 write reorganization O",
			"8 write reorganization NO",
			"9 delete reorganization O"))
	}
	runMultiSchemaTest(t, createSQL, alterSQL, initFn, postFn, loopFn, false)
	i = 1
	postFn = func(tkO *testkit.TestKit) {
		tkO.MustQuery(`select c1,c2,c3 from t`).Sort().Check(testkit.Rows(""+
			"1 init O updated",
			"10 delete reorganization NO Original",
			"11 none O Original",
			"12 none NO Original",
			"2 init O updated",
			"3 delete only O updated",
			"4 delete only NO updated",
			"5 write only O updated",
			"6 write only NO updated",
			"7 write reorganization O Original",
			"8 write reorganization NO Original",
			"9 delete reorganization O Original"))
	}
	runMultiSchemaTest(t, createSQL, alterSQL, initFn, postFn, loopFn, true)
}

func TestMultiSchemaReorganizeNoPK(t *testing.T) {
	createSQL := `create table t (c1 INT, c2 CHAR(255), c3 CHAR(255), c4 CHAR(255), c5 CHAR(255)) partition by range (c1) (partition p1 values less than (200), partition pMax values less than (maxvalue))`
	i := 1
	initFn := func(tkO *testkit.TestKit) {
		tkO.MustExec(fmt.Sprintf(`insert into t values (%d,'%s','Original',repeat('%d', 25),repeat('%d', 25))`, i, "init O", 4185725186-i, 7483634197-i))
		i++
		tkO.MustExec(fmt.Sprintf(`insert into t values (%d,'%s','Original',repeat('%d', 25),repeat('%d', 25))`, i, "init O", 4185725186-i, 7483634197-i))
		i++
	}
	alterSQL := `alter table t reorganize partition p1 into (partition p0 values less than (100), partition p1 values less than (200))`
	loopFn := func(tkO, tkNO *testkit.TestKit) {
		res := tkO.MustQuery(`select schema_state from information_schema.DDL_JOBS where table_name = 't' order by job_id desc limit 1`)
		schemaState := res.Rows()[0][0].(string)
		tkO.MustExec(fmt.Sprintf(`insert into t values (%d,'%s','Original',repeat('%d', 25),repeat('%d', 25))`, i, schemaState+" O", 4185725186-i, 7483634197-i))
		i++
		tkNO.MustExec(fmt.Sprintf(`insert into t values (%d,'%s','Original',repeat('%d', 25),repeat('%d', 25))`, i, schemaState+" NO", 4185725186-i, 7483634197-i))
		i++
	}
	postFn := func(tkO *testkit.TestKit) {
		require.Equal(t, int(6*2+1), i)
		tkO.MustQuery(`select c1,_tidb_rowid,c2 from t`).Sort().Check(testkit.Rows(""+
			"1 60001 init O",
			"10 30004 delete reorganization NO",
			"11 7 none O",
			"12 30005 none NO",
			"2 60002 init O",
			"3 60003 delete only O",
			"4 60004 delete only NO",
			"5 4 write only O",
			// Before, there were a DUPLICATE ROW here!!!
			//"5 60004 write only O",
			"6 60005 write only NO",
			"7 5 write reorganization O",
			"8 30003 write reorganization NO",
			"9 6 delete reorganization O"))
	}
	runMultiSchemaTest(t, createSQL, alterSQL, initFn, postFn, loopFn, false)
	i = 1
	postFn = func(tkO *testkit.TestKit) {
		require.Equal(t, int(6*2+1), i)
		tkO.MustQuery(`select c1,_tidb_rowid,c2,c3 from t`).Sort().Check(testkit.Rows(""+
			"1 1 init O updated",
			"10 30004 delete reorganization NO Original",
			"11 7 none O Original",
			"12 30005 none NO Original",
			"2 2 init O updated",
			"3 3 delete only O updated",
			"4 30001 delete only NO updated",
			"5 4 write only O updated",
			"6 30002 write only NO updated",
			"7 5 write reorganization O Original",
			"8 30003 write reorganization NO Original",
			"9 6 delete reorganization O Original"))
	}
	runMultiSchemaTest(t, createSQL, alterSQL, initFn, postFn, loopFn, true)
}

func runMultiSchemaTest(t *testing.T, createSQL, alterSQL string, initFn, postFn func(*testkit.TestKit), loopFn func(tO, tNO *testkit.TestKit), injectUpdate bool) {
	distCtx := testkit.NewDistExecutionContextWithLease(t, 2, 15*time.Second)
	store := distCtx.Store
	domOwner := distCtx.GetDomain(0)
	domNonOwner := distCtx.GetDomain(1)
	defer func() {
		domOwner.Close()
		domNonOwner.Close()
		store.Close()
	}()

	if !domOwner.DDL().OwnerManager().IsOwner() {
		domOwner, domNonOwner = domNonOwner, domOwner
	}

	seOwner, err := session.CreateSessionWithDomain(store, domOwner)
	require.NoError(t, err)
	seNonOwner, err := session.CreateSessionWithDomain(store, domNonOwner)
	require.NoError(t, err)

	tkDDLOwner := testkit.NewTestKitWithSession(t, store, seOwner)
	tkDDLOwner.MustExec(`use test`)
	tkDDLOwner.MustExec(`set @@global.tidb_ddl_reorg_worker_cnt=1`)
	tkO := testkit.NewTestKitWithSession(t, store, seOwner)
	tkO.MustExec(`use test`)
	tkNO := testkit.NewTestKitWithSession(t, store, seNonOwner)
	tkNO.MustExec(`use test`)

	tkDDLOwner.MustExec(createSQL)
	domOwner.Reload()
	domNonOwner.Reload()
	initFn(tkO)
	verStart := domNonOwner.InfoSchema().SchemaMetaVersion()
	hookChan := make(chan struct{})
	hookFunc := func(job *model.Job) {
		hookChan <- struct{}{}
		logutil.BgLogger().Info("XXXXXXXXXXX Hook now waiting", zap.String("job.State", job.State.String()), zap.String("job.SchemaStage", job.SchemaState.String()))
		<-hookChan
		logutil.BgLogger().Info("XXXXXXXXXXX Hook released", zap.String("job.State", job.State.String()), zap.String("job.SchemaStage", job.SchemaState.String()))
	}
	hook := &callback.TestDDLCallback{Do: domOwner}
	hook.OnJobRunAfterExported = hookFunc
	domOwner.DDL().SetHook(hook)
	alterChan := make(chan struct{})
	go func() {
		tkDDLOwner.MustExec(alterSQL)
		logutil.BgLogger().Info("XXXXXXXXXXX drop partition done!")
		alterChan <- struct{}{}
	}()
	if injectUpdate {
		// This can be used for testing concurrent writes during backfill.
		// It tested OK, since the backfill will fail and retry where it will get the fresh values and succeed.
		require.NoError(t, failpoint.EnableCall("github.com/pingcap/tidb/pkg/ddl/PartitionBackfillData", func(rows []*ddl.RowVal) {
			if len(rows) > 0 {
				tkO.MustExec(`update t set c3 = "updated"`)
			}
		}))
		defer failpoint.Disable("github.com/pingcap/tidb/pkg/ddl/PartitionBackfillData")
	}

	// Skip the first state, since we want to compare before vs after in the loop
	<-hookChan
	hookChan <- struct{}{}
	verCurr := verStart + 1
	i := 0
	for {
		// Waiting for the next State change to be done (i.e. blocking the state after)
		releaseHook := true
		for {
			select {
			case <-hookChan:
			case <-alterChan:
				releaseHook = false
				logutil.BgLogger().Info("XXXXXXXXXXX release hook")
				break
			}
			domOwner.Reload()
			if domNonOwner.InfoSchema().SchemaMetaVersion() == domOwner.InfoSchema().SchemaMetaVersion() {
				// looping over reorganize data/indexes
				hookChan <- struct{}{}
				continue
			}
			break
		}
		//tk.t.Logf("RefreshSession rand seed: %d", seed)
		logutil.BgLogger().Info("XXXXXXXXXXX states loop", zap.Int64("verCurr", verCurr), zap.Int64("NonOwner ver", domNonOwner.InfoSchema().SchemaMetaVersion()), zap.Int64("Owner ver", domOwner.InfoSchema().SchemaMetaVersion()))
		domOwner.Reload()
		require.Equal(t, verCurr-1, domNonOwner.InfoSchema().SchemaMetaVersion())
		require.Equal(t, verCurr, domOwner.InfoSchema().SchemaMetaVersion())
		loopFn(tkO, tkNO)
		domNonOwner.Reload()
		verCurr++
		i++
		if releaseHook {
			// Continue to next state
			hookChan <- struct{}{}
		} else {
			// Alter done!
			break
		}
	}
	logutil.BgLogger().Info("XXXXXXXXXXX states loop done")
	postFn(tkO)
}
