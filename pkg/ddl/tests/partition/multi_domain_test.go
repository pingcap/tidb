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
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/session"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// TestMultiSchemaDropUniqueIndex to show behavior when
// dropping a unique index
func TestMultiSchemaDropUniqueIndex(t *testing.T) {
	testkit.SkipIfFailpointDisabled(t)
	createSQL := `create table t (a int primary key, b varchar(255), unique key uk_b (b))`
	initFn := func(tkO *testkit.TestKit) {
		tkO.MustExec(`insert into t values (1,1),(2,2),(3,3),(4,4),(5,5),(6,6),(7,7),(8,8),(9,9)`)
	}
	alterSQL := `alter table t drop index uk_b`
	loopFn := func(tkO, tkNO *testkit.TestKit) {
		res := tkO.MustQuery(`select schema_state from information_schema.DDL_JOBS where table_name = 't' order by job_id desc limit 1`)
		schemaState := res.Rows()[0][0].(string)
		switch schemaState {
		case "write only":
			tkNO.MustQuery(`show create table t`).Check(testkit.Rows("" +
				"t CREATE TABLE `t` (\n" +
				"  `a` int(11) NOT NULL,\n" +
				"  `b` varchar(255) DEFAULT NULL,\n" +
				"  PRIMARY KEY (`a`) /*T![clustered_index] CLUSTERED */,\n" +
				"  UNIQUE KEY `uk_b` (`b`)\n" +
				") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))
			tkO.MustQuery(`show create table t`).Check(testkit.Rows("" +
				"t CREATE TABLE `t` (\n" +
				"  `a` int(11) NOT NULL,\n" +
				"  `b` varchar(255) DEFAULT NULL,\n" +
				"  PRIMARY KEY (`a`) /*T![clustered_index] CLUSTERED */\n" +
				") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))
			tkO.MustContainErrMsg(`insert into t values (10,1)`, "[kv:1062]Duplicate entry '1' for key 't.uk_b'")
			tkNO.MustContainErrMsg(`insert into t values (10,1)`, "[kv:1062]Duplicate entry '1' for key 't.uk_b'")
		case "delete only":
			// Delete only from the uk_b unique index, cannot have errors
			tkO.MustExec(`insert into t values (10,1)`)
			tkO.MustExec(`insert into t values (11,11)`)
			tkO.MustExec(`delete from t where a = 2`)
			// Write only for uk_b, we cannot find anything through the index or read from the index, but still gives duplicate keys on insert/updates
			// So we already have two duplicates of b = 1, but only one in the unique index uk_a, so here we cannot insert any.
			tkNO.MustContainErrMsg(`insert into t values (12,1)`, "[kv:1062]Duplicate entry '1' for key 't.uk_b'")
			tkNO.MustContainErrMsg(`update t set b = 1 where a = 9`, "[kv:1062]Duplicate entry '1' for key 't.uk_b'")
			// Deleted from the index!
			tkNO.MustExec(`insert into t values (13,2)`)
			tkNO.MustContainErrMsg(`insert into t values (14,3)`, "[kv:1062]Duplicate entry '3' for key 't.uk_b'")
			// b = 11 never written to the index!
			tkNO.MustExec(`insert into t values (15,11)`)
			tkNO.MustQuery(`show create table t`).Check(testkit.Rows("" +
				"t CREATE TABLE `t` (\n" +
				"  `a` int(11) NOT NULL,\n" +
				"  `b` varchar(255) DEFAULT NULL,\n" +
				"  PRIMARY KEY (`a`) /*T![clustered_index] CLUSTERED */\n" +
				") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))
		case "delete reorganization":
			tkO.MustQuery(`show create table t`).Check(testkit.Rows("" +
				"t CREATE TABLE `t` (\n" +
				"  `a` int(11) NOT NULL,\n" +
				"  `b` varchar(255) DEFAULT NULL,\n" +
				"  PRIMARY KEY (`a`) /*T![clustered_index] CLUSTERED */\n" +
				") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))
		case "none":
			tkO.MustQuery(`show create table t`).Check(testkit.Rows("" +
				"t CREATE TABLE `t` (\n" +
				"  `a` int(11) NOT NULL,\n" +
				"  `b` varchar(255) DEFAULT NULL,\n" +
				"  PRIMARY KEY (`a`) /*T![clustered_index] CLUSTERED */\n" +
				") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))
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

// TODO: Test that TRUNCATE PARTITION without global index is a single state change!

// TestMultiSchemaTruncatePartitionWithGlobalIndex to show behavior when
// truncating a partition with a global index
func TestMultiSchemaTruncatePartitionWithGlobalIndex(t *testing.T) {
	testkit.SkipIfFailpointDisabled(t)
	// TODO: Also test non-clustered tables, PK as global key, non-int PK, multi-column PK
	createSQL := `create table t (a int primary key, b varchar(255), c varchar(255) default 'Filler', unique key uk_b (b) global) partition by hash (a) partitions 2`
	initFn := func(tkO *testkit.TestKit) {
		tkO.MustExec(`insert into t (a,b) values (1,1),(2,2),(3,3),(4,4),(51,51),(53,53),(55,55)`)
	}
	alterSQL := `alter table t truncate partition p1`
	loopFn := func(tkO, tkNO *testkit.TestKit) {
		res := tkO.MustQuery(`select schema_state from information_schema.DDL_JOBS where table_name = 't' order by job_id desc limit 1`)
		schemaState := res.Rows()[0][0].(string)
		// 1-20 Original rows in the original partition
		// 21-20 Inserted ... TODO: Describe and update test accordingly...
		switch schemaState {
		case "delete only":
			// tkNO is seeing state None, so still can access the dropped partition
			// tkO is seeing state delete only, so cannot see the dropped partition,
			// but must still double write to the global indexes.

			// Still in Schema version before ALTER, not affected
			tkNO.MustContainErrMsg(`insert into t values (1,1,"Duplicate key")`, "[kv:1062]Duplicate entry '1' for key 't.uk_b'")
			// OK with duplicate key, but otherwise it should allow any insert!
			tkO.MustContainErrMsg(`insert into t values (1,1,"Duplicate key")`, "[kv:1062]Duplicate entry '1' for key 't.uk_b'")
			tkNO.MustExec(`insert into t values (5,5,"OK")`)
			tkNO.MustExec(`insert into t values (7,7,"OK")`)
			tkO.MustContainErrMsg(`insert into t values (5,5,"Duplicate key")`, "[kv:1062]Duplicate entry '5' for key 't.uk_b'")
			tkO.MustContainErrMsg(`insert into t values (6,7,"Duplicate key")`, "[kv:1062]Duplicate entry '7' for key 't.uk_b'")
			tkO.MustExec(`insert into t values (9,9,"OK")`)
			tkNO.MustContainErrMsg(`insert into t values (8,9,"Duplicate key")`, "[kv:1062]Duplicate entry '9' for key 't.uk_b'")
			tkNO.MustQuery(`select count(*) from t where b = 9`).Check(testkit.Rows("1"))
			tkNO.MustQuery(`select b from t where b = 9`).Check(testkit.Rows("9"))
			// TODO: How to handle this inconsistency?
			tkNO.MustQuery(`select * from t where b = 9`).Check(testkit.Rows())
			// OK, since not found!
			tkNO.MustExec(`update t set a = 2 where b = 9`)
			require.Equal(t, uint64(0), tkNO.Session().GetSessionVars().LastFoundRows)
			tkO.MustQuery(`select count(*) from t where b = 7`).Check(testkit.Rows("0"))
			tkO.MustExec(`update t set a = 2 where b = 7`)
			require.Equal(t, uint64(0), tkNO.Session().GetSessionVars().LastFoundRows)
			tkNO.MustQuery(`select count(*) from t where a = 7`).Check(testkit.Rows("1"))
			tkNO.MustQuery(`select * from t where a = 7`).Check(testkit.Rows("7 7 OK"))
			tkNO.MustExec(`update t set b = 10 where a = 7`)
			require.Equal(t, uint64(1), tkNO.Session().GetSessionVars().LastFoundRows)
			tkNO.MustExec(`update t set b = 7 where a = 7`)
			require.Equal(t, uint64(1), tkNO.Session().GetSessionVars().LastFoundRows)
			tkO.MustExec(`update t set b = 9 where a = 9`)
			require.Equal(t, uint64(1), tkNO.Session().GetSessionVars().LastFoundRows)
			// TODO: Add tests for delete as well as index lookup and table scan!
		case "delete reorganization":
			// tkNO is seeing state delete only, so cannot see the dropped partition,
			// but must still double write to the global indexes.
			// tkO is seeing state delete reorganization, so cannot see the dropped partition,
			// and should ignore the dropped partitions entries in the Global Indexes!
			tkO.MustExec(`insert into t values (1,1,"OK")`)
			tkO.MustContainErrMsg(`insert into t values (1,1,"Duplicate")`, "[kv:1062]Duplicate entry '1' for key 't.uk_b'")
			tkO.MustContainErrMsg(`insert into t values (3,1,"Duplicate")`, "[kv:1062]Duplicate entry '1' for key 't.uk_b'")
			// b = 7 was inserted into the dropped partition, OK to delete
			tkO.MustExec(`insert into t values (10,7,"OK")`)
			tkNO.MustExec(`insert into t values (11,11,"OK")`)
			tkNO.MustContainErrMsg(`insert into t values (12,9,"Duplicate key")`, "[kv:1062]Duplicate entry '9' for key 't.uk_b'")
			tkNO.MustContainErrMsg(`insert into t values (9,9,"Duplicate key")`, "[kv:1062]Duplicate entry '9' for key 't.uk_b'")
			tkNO.MustContainErrMsg(`insert into t values (11,11,"Duplicate key")`, "[kv:1062]Duplicate entry '11' for key 't.uk_b'")
			tkO.MustExec(`insert into t values (13,13,"OK")`)
			tkO.MustContainErrMsg(`insert into t values (14,13,"Duplicate key")`, "[kv:1062]Duplicate entry '13' for key 't.uk_b'")
			tkNO.MustContainErrMsg(`update t set b = 51 where a = 11`, "[kv:1062]Duplicate entry '51' for key 't.uk_b'")
			tkNO.MustExec(`update t set a = 51 where b = 11`)
			tkO.MustExec(`update t set a = 53 where b = 13`)
		case "none":
			tkNO.MustExec(`insert into t values (5,5,"OK")`)
			tkO.MustContainErrMsg(`insert into t values (5,5,"Duplicate key")`, "[kv:1062]Duplicate entry '5' for key 't.uk_b'")
			tkNO.MustExec(`insert into t values (15,15,"OK")`)
			tkO.MustExec(`insert into t values (17,17,"OK")`)
		default:
			require.Failf(t, "unhandled schema state '%s'", schemaState)
		}
	}
	runMultiSchemaTest(t, createSQL, alterSQL, initFn, func(kit *testkit.TestKit) {}, loopFn)
}
