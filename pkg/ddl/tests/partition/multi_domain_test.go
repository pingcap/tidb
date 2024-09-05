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
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/session"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestMultiSchemaDropPartition(t *testing.T) {
	testkit.SkipIfFailpointDisabled(t)
	createSQL := `create table t (a int primary key, b varchar(255)) partition by range (a) (partition p0 values less than (100), partition p1 values less than (200))`
	initFn := func(tkO *testkit.TestKit) {
		tkO.MustExec(`insert into t values (1,1)`)
	}
	alterSQL := `alter table t drop partition p0`
	loopFn := func(tkO, tkNO *testkit.TestKit) {
		res := tkO.MustQuery(`select schema_state from information_schema.DDL_JOBS where table_name = 't' order by job_id desc limit 1`)
		schemaState := res.Rows()[0][0].(string)
		switch schemaState {
		case "delete only":
			tkO.MustExec(`insert into t values (1,1)`)
			// PK violation!!!
			tkNO.MustQuery(`select * from t`).Sort().Check(testkit.Rows("1 1", "1 1"))
			// Original row
			tkNO.MustQuery(`select * from t partition (p0)`).Sort().Check(testkit.Rows("1 1"))
			// This is not consistent with the view of the partition definitions!
			tkNO.MustQuery(`select * from t partition (p1)`).Sort().Check(testkit.Rows("1 1"))
			tkNO.MustQuery(`show create table t`).Check(testkit.Rows("" +
				"t CREATE TABLE `t` (\n" +
				"  `a` int(11) NOT NULL,\n" +
				"  `b` varchar(255) DEFAULT NULL,\n" +
				"  PRIMARY KEY (`a`) /*T![clustered_index] CLUSTERED */\n" +
				") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin\n" +
				"PARTITION BY RANGE (`a`)\n" +
				"(PARTITION `p0` VALUES LESS THAN (100),\n" +
				" PARTITION `p1` VALUES LESS THAN (200))"))
		case "delete reorganization":
			// just to not fail :)
		case "none":
			// just to not fail :)
		default:
			require.Failf(t, "unhandled schema state '%s'", schemaState)
		}
	}
	runMultiSchemaTest(t, createSQL, alterSQL, initFn, func(kit *testkit.TestKit) {}, loopFn)
}

func runMultiSchemaTest(t *testing.T, createSQL, alterSQL string, initFn, postFn func(*testkit.TestKit), loopFn func(tO, tNO *testkit.TestKit)) {
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
	tkDDLOwner.MustExec(`set @@global.tidb_enable_global_index = 1`)
	tkDDLOwner.MustExec(`set @@session.tidb_enable_global_index = 1`)
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
	failpoint.EnableCall("github.com/pingcap/tidb/pkg/ddl/onJobRunAfter", hookFunc)
	defer failpoint.Disable("github.com/pingcap/tidb/pkg/ddl/onJobRunAfter")
	alterChan := make(chan struct{})
	go func() {
		tkDDLOwner.MustExec(alterSQL)
		logutil.BgLogger().Info("XXXXXXXXXXX drop partition done!")
		alterChan <- struct{}{}
	}()
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
