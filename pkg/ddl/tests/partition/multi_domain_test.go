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
	"strconv"
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

	ddlJobsSQL := `admin show ddl jobs where db_name = 'test' and table_name = 't' and job_type = 'drop index'`

	seOwner, err := session.CreateSessionWithDomain(store, domOwner)
	require.NoError(t, err)
	seNonOwner, err := session.CreateSessionWithDomain(store, domNonOwner)
	require.NoError(t, err)

	tkOwner := testkit.NewTestKitWithSession(t, store, seOwner)
	tkNonOwner := testkit.NewTestKitWithSession(t, store, seNonOwner)
	tkDDL := testkit.NewTestKitWithSession(t, store, seOwner)
	tkOwner.MustExec(`use test`)
	tkNonOwner.MustExec(`use test`)
	tkDDL.MustExec(`use test`)
	tkDDL.MustExec(`create table t (a int primary key, b varchar(255), unique key uk_b (b))`)
	domOwner.Reload()
	domNonOwner.Reload()
	verStart := domOwner.InfoSchema().SchemaMetaVersion()
	alterChan := make(chan struct{})

	// Add some rows to see if we can have duplicates even when we don't see the index
	tkOwner.MustExec(`insert into t values (1,1),(2,2),(3,3),(4,4),(5,5),(6,6),(7,7),(8,8),(9,9)`)

	// Is it possible to only do this on a single DOM?
	hookFunc := func(job *model.Job) {
		alterChan <- struct{}{}
		<-alterChan
	}
	failpoint.EnableCall("github.com/pingcap/tidb/pkg/ddl/onJobRunBefore", hookFunc)
	defer failpoint.Disable("github.com/pingcap/tidb/pkg/ddl/onJobRunBefore")
	go func() {
		tkDDL.MustExec(`alter table t drop index uk_b`)
		alterChan <- struct{}{}
	}()
	// Wait for the first state change to begin
	<-alterChan
	alterChan <- struct{}{}
	// Doing the first State change
	stateChange := int64(1)
	<-alterChan
	// Waiting before running the second State change
	verCurr := domOwner.InfoSchema().SchemaMetaVersion()
	require.Equal(t, stateChange, verCurr-verStart)
	require.Equal(t, verStart, domNonOwner.InfoSchema().SchemaMetaVersion())
	tkNonOwner.MustQuery(`show create table t`).Check(testkit.Rows("" +
		"t CREATE TABLE `t` (\n" +
		"  `a` int(11) NOT NULL,\n" +
		"  `b` varchar(255) DEFAULT NULL,\n" +
		"  PRIMARY KEY (`a`) /*T![clustered_index] CLUSTERED */,\n" +
		"  UNIQUE KEY `uk_b` (`b`)\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))
	domNonOwner.Reload()
	require.Equal(t, verCurr, domNonOwner.InfoSchema().SchemaMetaVersion())
	tkNonOwner.MustQuery(`show create table t`).Check(testkit.Rows("" +
		"t CREATE TABLE `t` (\n" +
		"  `a` int(11) NOT NULL,\n" +
		"  `b` varchar(255) DEFAULT NULL,\n" +
		"  PRIMARY KEY (`a`) /*T![clustered_index] CLUSTERED */\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))
	tkNonOwner.MustQuery(ddlJobsSQL).CheckAt([]int{4, 11}, [][]any{{"write only", "running"}})
	// So even if the session don't see the unique index, it is
	// in effect for writing!
	tkOwner.MustContainErrMsg(`insert into t values (10,1)`, "[kv:1062]Duplicate entry '1' for key 't.uk_b'")
	tkNonOwner.MustContainErrMsg(`insert into t values (10,1)`, "[kv:1062]Duplicate entry '1' for key 't.uk_b'")
	alterChan <- struct{}{}
	// doing second State change
	stateChange++
	<-alterChan
	// Waiting before running the third State change
	verCurr = domOwner.InfoSchema().SchemaMetaVersion()
	require.Equal(t, stateChange, verCurr-verStart)
	tkNonOwner.MustQuery(ddlJobsSQL).CheckAt([]int{4, 11}, [][]any{{"delete only", "running"}})
	// Delete only from the uk_b unique index, cannot have errors
	tkOwner.MustExec(`insert into t values (10,1)`)
	tkOwner.MustExec(`insert into t values (11,11)`)
	tkOwner.MustExec(`delete from t where a = 2`)
	// Write only for uk_b, we cannot find anything through the index or read from the index, but still gives duplicate keys on insert/updates
	// So we already have two duplicates of b = 1, but only one in the unique index uk_a, so here we cannot insert any.
	tkNonOwner.MustContainErrMsg(`insert into t values (12,1)`, "[kv:1062]Duplicate entry '1' for key 't.uk_b'")
	tkNonOwner.MustContainErrMsg(`update t set b = 1 where a = 9`, "[kv:1062]Duplicate entry '1' for key 't.uk_b'")
	// Deleted from the index!
	tkNonOwner.MustExec(`insert into t values (13,2)`)
	tkNonOwner.MustContainErrMsg(`insert into t values (14,3)`, "[kv:1062]Duplicate entry '3' for key 't.uk_b'")
	// b = 11 never written to the index!
	tkNonOwner.MustExec(`insert into t values (15,11)`)
	domNonOwner.Reload()
	require.Equal(t, verCurr, domOwner.InfoSchema().SchemaMetaVersion())
	tkNonOwner.MustQuery(`show create table t`).Check(testkit.Rows("" +
		"t CREATE TABLE `t` (\n" +
		"  `a` int(11) NOT NULL,\n" +
		"  `b` varchar(255) DEFAULT NULL,\n" +
		"  PRIMARY KEY (`a`) /*T![clustered_index] CLUSTERED */\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))
	alterChan <- struct{}{}
	<-alterChan
	stateChange++
	verCurr = domOwner.InfoSchema().SchemaMetaVersion()
	require.Equal(t, stateChange, verCurr-verStart)
	domNonOwner.Reload()
	require.Equal(t, verCurr, domNonOwner.InfoSchema().SchemaMetaVersion())
	tkNonOwner.MustQuery(`show create table t`).Check(testkit.Rows("" +
		"t CREATE TABLE `t` (\n" +
		"  `a` int(11) NOT NULL,\n" +
		"  `b` varchar(255) DEFAULT NULL,\n" +
		"  PRIMARY KEY (`a`) /*T![clustered_index] CLUSTERED */\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))
	tkNonOwner.MustQuery(ddlJobsSQL).CheckAt([]int{4, 11}, [][]any{{"delete reorganization", "running"}})
	alterChan <- struct{}{}
	<-alterChan
	stateChange++
	verCurr = domOwner.InfoSchema().SchemaMetaVersion()
	require.Equal(t, stateChange, verCurr-verStart)
	domNonOwner.Reload()
	require.Equal(t, verCurr, domNonOwner.InfoSchema().SchemaMetaVersion())
	tkNonOwner.MustQuery(`show create table t`).Check(testkit.Rows("" +
		"t CREATE TABLE `t` (\n" +
		"  `a` int(11) NOT NULL,\n" +
		"  `b` varchar(255) DEFAULT NULL,\n" +
		"  PRIMARY KEY (`a`) /*T![clustered_index] CLUSTERED */\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))
	tkNonOwner.MustQuery(ddlJobsSQL).CheckAt([]int{4, 11}, [][]any{{"none", "synced"}})
}

func TestMultiSchemaVerPartitionBy(t *testing.T) {
	testkit.SkipIfFailpointDisabled(t)
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

	ddlJobsSQL := `admin show ddl jobs where db_name = 'test' and table_name = 't' and job_type = 'alter table partition by'`

	seOwner, err := session.CreateSessionWithDomain(store, domOwner)
	require.NoError(t, err)
	seNonOwner, err := session.CreateSessionWithDomain(store, domNonOwner)
	require.NoError(t, err)

	tkOwner := testkit.NewTestKitWithSession(t, store, seOwner)
	tkNonOwner := testkit.NewTestKitWithSession(t, store, seNonOwner)
	tkDDL := testkit.NewTestKitWithSession(t, store, seOwner)
	tkOwner.MustExec(`use test`)
	tkNonOwner.MustExec(`use test`)
	tkDDL.MustExec(`use test`)
	tkDDL.MustExec(`create table t (a int primary key, b varchar(255))`)
	domOwner.Reload()
	domNonOwner.Reload()
	verStart := domOwner.InfoSchema().SchemaMetaVersion()
	alterChan := make(chan struct{})

	// Is it possible to only do this on a single DOM?
	hookFunc := func(job *model.Job) {
		alterChan <- struct{}{}
		<-alterChan
	}
	failpoint.EnableCall("github.com/pingcap/tidb/pkg/ddl/onJobRunBefore", hookFunc)
	defer failpoint.Disable("github.com/pingcap/tidb/pkg/ddl/onJobRunBefore")
	go func() {
		tkDDL.MustExec(`alter table t partition by hash(a) partitions 3`)
		alterChan <- struct{}{}
	}()
	// Wait for the first state change to begin
	<-alterChan
	alterChan <- struct{}{}
	// Doing the first State change
	stateChange := int64(1)
	<-alterChan
	// Waiting before running the second State change
	verCurr := domOwner.InfoSchema().SchemaMetaVersion()
	require.Equal(t, stateChange, verCurr-verStart)
	require.Equal(t, verStart, domNonOwner.InfoSchema().SchemaMetaVersion())
	tkNonOwner.MustQuery(`show create table t`).Check(testkit.Rows("" +
		"t CREATE TABLE `t` (\n" +
		"  `a` int(11) NOT NULL,\n" +
		"  `b` varchar(255) DEFAULT NULL,\n" +
		"  PRIMARY KEY (`a`) /*T![clustered_index] CLUSTERED */\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))
	domNonOwner.Reload()
	require.Equal(t, verCurr, domNonOwner.InfoSchema().SchemaMetaVersion())
	tkNonOwner.MustQuery(`show create table t`).Check(testkit.Rows("" +
		"t CREATE TABLE `t` (\n" +
		"  `a` int(11) NOT NULL,\n" +
		"  `b` varchar(255) DEFAULT NULL,\n" +
		"  PRIMARY KEY (`a`) /*T![clustered_index] CLUSTERED */\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin\n" +
		"PARTITION BY NONE ()\n" +
		"(PARTITION `pFullTable` COMMENT 'Intermediate partition during ALTER TABLE ... PARTITION BY ...')"))
	tkNonOwner.MustQuery(ddlJobsSQL).CheckAt([]int{4, 11}, [][]any{{"delete only", "running"}})
	alterChan <- struct{}{}
	// doing second State change
	stateChange++
	<-alterChan
	// Waiting before running the third State change
	verCurr = domOwner.InfoSchema().SchemaMetaVersion()
	require.Equal(t, stateChange, verCurr-verStart)
	domNonOwner.Reload()
	require.Equal(t, verCurr, domOwner.InfoSchema().SchemaMetaVersion())
	tkNonOwner.MustQuery(`show create table t`).Check(testkit.Rows("" +
		"t CREATE TABLE `t` (\n" +
		"  `a` int(11) NOT NULL,\n" +
		"  `b` varchar(255) DEFAULT NULL,\n" +
		"  PRIMARY KEY (`a`) /*T![clustered_index] CLUSTERED */\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin\n" +
		"PARTITION BY NONE ()\n" +
		"(PARTITION `pFullTable` COMMENT 'Intermediate partition during ALTER TABLE ... PARTITION BY ...')"))
	tkNonOwner.MustQuery(ddlJobsSQL).CheckAt([]int{4, 11}, [][]any{{"write only", "running"}})
	alterChan <- struct{}{}
	<-alterChan
	stateChange++
	verCurr = domOwner.InfoSchema().SchemaMetaVersion()
	require.Equal(t, stateChange, verCurr-verStart)
	domNonOwner.Reload()
	require.Equal(t, verCurr, domNonOwner.InfoSchema().SchemaMetaVersion())
	tkNonOwner.MustQuery(`show create table t`).Check(testkit.Rows("" +
		"t CREATE TABLE `t` (\n" +
		"  `a` int(11) NOT NULL,\n" +
		"  `b` varchar(255) DEFAULT NULL,\n" +
		"  PRIMARY KEY (`a`) /*T![clustered_index] CLUSTERED */\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin\n" +
		"PARTITION BY NONE ()\n" +
		"(PARTITION `pFullTable` COMMENT 'Intermediate partition during ALTER TABLE ... PARTITION BY ...')"))
	tkNonOwner.MustQuery(ddlJobsSQL).CheckAt([]int{4, 11}, [][]any{{"write reorganization", "running"}})
	alterChan <- struct{}{}
	<-alterChan
	stateChange++
	verCurr = domOwner.InfoSchema().SchemaMetaVersion()
	require.Equal(t, stateChange, verCurr-verStart)
	domNonOwner.Reload()
	require.Equal(t, verCurr, domNonOwner.InfoSchema().SchemaMetaVersion())
	tkNonOwner.MustQuery(`show create table t`).Check(testkit.Rows("" +
		"t CREATE TABLE `t` (\n" +
		"  `a` int(11) NOT NULL,\n" +
		"  `b` varchar(255) DEFAULT NULL,\n" +
		"  PRIMARY KEY (`a`) /*T![clustered_index] CLUSTERED */\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin\n" +
		"PARTITION BY HASH (`a`) PARTITIONS 3"))
	tkNonOwner.MustQuery(ddlJobsSQL).CheckAt([]int{4, 11}, [][]any{{"delete reorganization", "running"}})
	alterChan <- struct{}{}
	<-alterChan
	// Alter done!
	stateChange++
	verCurr = domOwner.InfoSchema().SchemaMetaVersion()
	require.Equal(t, stateChange, verCurr-verStart)
	domNonOwner.Reload()
	require.Equal(t, verCurr, domNonOwner.InfoSchema().SchemaMetaVersion())
	tkNonOwner.MustQuery(`show create table t`).Check(testkit.Rows("" +
		"t CREATE TABLE `t` (\n" +
		"  `a` int(11) NOT NULL,\n" +
		"  `b` varchar(255) DEFAULT NULL,\n" +
		"  PRIMARY KEY (`a`) /*T![clustered_index] CLUSTERED */\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin\n" +
		"PARTITION BY HASH (`a`) PARTITIONS 3"))
	tkNonOwner.MustQuery(ddlJobsSQL).CheckAt([]int{4, 11}, [][]any{{"none", "synced"}})
}

func TestMultiSchemaVerAddPartition(t *testing.T) {
	testkit.SkipIfFailpointDisabled(t)
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
	tkA := testkit.NewTestKitWithSession(t, store, seOwner)
	tkA.MustExec(`use test`)
	tkB := testkit.NewTestKitWithSession(t, store, seNonOwner)
	tkB.MustExec(`use test`)
	tkDDLOwner.MustExec(`create table t (a int primary key nonclustered global, b varchar(255) charset utf8mb4 collate utf8mb4_0900_ai_ci) partition by range columns (b) (partition p0 values less than ("m"))`)
	domOwner.Reload()
	domNonOwner.Reload()
	verStart := domNonOwner.InfoSchema().SchemaMetaVersion()
	alterChan := make(chan struct{})
	hookFunc := func(job *model.Job) {
		alterChan <- struct{}{}
		<-alterChan
	}
	failpoint.EnableCall("github.com/pingcap/tidb/pkg/ddl/onJobRunBefore", hookFunc)
	defer failpoint.Disable("github.com/pingcap/tidb/pkg/ddl/onJobRunBefore")
	go func() {
		tkDDLOwner.MustExec(`alter table t add partition (partition p1 values less than ("p"))`)
		alterChan <- struct{}{}
	}()
	// Wait for the first state change to begin
	<-alterChan
	alterChan <- struct{}{}
	// Doing the first State change
	stateChange := int64(1)
	<-alterChan
	// Waiting before running the second State change
	verCurr := domOwner.InfoSchema().SchemaMetaVersion()
	require.Equal(t, stateChange, verCurr-verStart)
	require.Equal(t, verStart, domNonOwner.InfoSchema().SchemaMetaVersion())
	tkA.MustQuery(`show create table t`).Check(testkit.Rows("" +
		"t CREATE TABLE `t` (\n" +
		"  `a` int(11) NOT NULL,\n" +
		"  `b` varchar(255) COLLATE utf8mb4_0900_ai_ci DEFAULT NULL,\n" +
		"  PRIMARY KEY (`a`) /*T![clustered_index] NONCLUSTERED */ /*T![global_index] GLOBAL */\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin\n" +
		"PARTITION BY RANGE COLUMNS(`b`)\n" +
		"(PARTITION `p0` VALUES LESS THAN ('m'))"))
	tkB.MustQuery(`show create table t`).Check(testkit.Rows("" +
		"t CREATE TABLE `t` (\n" +
		"  `a` int(11) NOT NULL,\n" +
		"  `b` varchar(255) COLLATE utf8mb4_0900_ai_ci DEFAULT NULL,\n" +
		"  PRIMARY KEY (`a`) /*T![clustered_index] NONCLUSTERED */ /*T![global_index] GLOBAL */\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin\n" +
		"PARTITION BY RANGE COLUMNS(`b`)\n" +
		"(PARTITION `p0` VALUES LESS THAN ('m'))"))
	domNonOwner.Reload()
	require.Equal(t, verCurr, domNonOwner.InfoSchema().SchemaMetaVersion())
	tkB.MustQuery(`show create table t`).Check(testkit.Rows("" +
		"t CREATE TABLE `t` (\n" +
		"  `a` int(11) NOT NULL,\n" +
		"  `b` varchar(255) COLLATE utf8mb4_0900_ai_ci DEFAULT NULL,\n" +
		"  PRIMARY KEY (`a`) /*T![clustered_index] NONCLUSTERED */ /*T![global_index] GLOBAL */\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin\n" +
		"PARTITION BY RANGE COLUMNS(`b`)\n" +
		"(PARTITION `p0` VALUES LESS THAN ('m'))"))
	ddlJobsSQL := `admin show ddl jobs where db_name = 'test' and table_name = 't' and job_type = 'add partition'`
	tkB.MustQuery(ddlJobsSQL).CheckAt([]int{4, 11}, [][]any{{"replica only", "running"}})
	alterChan <- struct{}{}
	stateChange++
	// Alter is completed
	<-alterChan
	tkB.MustQuery(ddlJobsSQL).CheckAt([]int{4, 11}, [][]any{{"public", "synced"}})
	verCurr = domOwner.InfoSchema().SchemaMetaVersion()
	require.Equal(t, stateChange, verCurr-verStart)
	tkDDLOwner.MustExec(`insert into t values (1,"Matt"),(2,"Anne")`)
	tkB.MustQuery(`select /*+ USE_INDEX(t, PRIMARY) */ a from t`).Check(testkit.Rows("2"))
	tkB.MustQuery(`select * from t`).Check(testkit.Rows("2 Anne"))
	domNonOwner.Reload()
	require.Equal(t, verCurr, domNonOwner.InfoSchema().SchemaMetaVersion())
	tkB.MustQuery(`show create table t`).Check(testkit.Rows("" +
		"t CREATE TABLE `t` (\n" +
		"  `a` int(11) NOT NULL,\n" +
		"  `b` varchar(255) COLLATE utf8mb4_0900_ai_ci DEFAULT NULL,\n" +
		"  PRIMARY KEY (`a`) /*T![clustered_index] NONCLUSTERED */ /*T![global_index] GLOBAL */\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin\n" +
		"PARTITION BY RANGE COLUMNS(`b`)\n" +
		"(PARTITION `p0` VALUES LESS THAN ('m'),\n" +
		" PARTITION `p1` VALUES LESS THAN ('p'))"))
	tkB.MustQuery(ddlJobsSQL).CheckAt([]int{4, 11}, [][]any{{"public", "synced"}})
	tkB.MustQuery(`select /*+ USE_INDEX(t, PRIMARY) */ a from t`).Check(testkit.Rows("1", "2"))
	tkB.MustQuery(`select * from t`).Sort().Check(testkit.Rows("1 Matt", "2 Anne"))
}

func TestMultiSchemaVerDropPartitionWithGlobalIndex(t *testing.T) {
	testkit.SkipIfFailpointDisabled(t)
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

	states := []string{"start", "delete only", "delete reorganization", "done"}

	offsetP1 := 1000000
	tkDDLOwner.MustExec(`create table t (id int unsigned primary key nonclustered, b int, part varchar(10) not null, state int not null, history text, unique key uk_b (b) global) PARTITION BY RANGE (id) (PARTITION p0 VALUES LESS THAN (` + strconv.Itoa(offsetP1) + `), PARTITION p1 VALUES LESS THAN (2000000))`)
	domOwner.Reload()
	domNonOwner.Reload()
	dbRows := make(map[int][]string)
	lastRowIDp0 := 8
	for i := 1; i <= lastRowIDp0; i++ {
		dbRows[i] = []string{strconv.Itoa(i), strconv.Itoa(offsetP1 - i), "p0", "-1", ""}
	}
	lastRowIDp1 := 1000007
	for i := offsetP1; i <= lastRowIDp1; i++ {
		dbRows[i] = []string{strconv.Itoa(i), strconv.Itoa(offsetP1 - i), "p1", "-1", ""}
	}
	for i := range dbRows {
		a, b, c, d, e := dbRows[i][0], dbRows[i][1], dbRows[i][2], dbRows[i][3], dbRows[i][4]
		tkDDLOwner.MustExec(fmt.Sprintf(`insert into t values (%s, %s, '%s', %s, '%s')`, a, b, c, d, e))
	}
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
		tkDDLOwner.MustExec(`alter table t drop partition p0`)
		logutil.BgLogger().Info("XXXXXXXXXXX drop partition done!")
		alterChan <- struct{}{}
	}()
	// Skip the first state, since we want to compare before vs after in the loop
	<-hookChan
	hookChan <- struct{}{}
	verCurr := verStart + 1
	for i := range states[1:] {
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
		logutil.BgLogger().Info("XXXXXXXXXXX states loop", zap.String("prev state", states[i]), zap.String("curr state", states[i+1]), zap.Int64("verCurr", verCurr), zap.Int64("NonOwner ver", domNonOwner.InfoSchema().SchemaMetaVersion()), zap.Int64("Owner ver", domOwner.InfoSchema().SchemaMetaVersion()))
		domOwner.Reload()
		if domNonOwner.InfoSchema().SchemaMetaVersion() == domOwner.InfoSchema().SchemaMetaVersion() {
			logutil.BgLogger().Info("XXXXXXXXXXX states loop continue", zap.String("prev state", states[i]), zap.String("curr state", states[i+1]), zap.Int64("verCurr", verCurr), zap.Int64("NonOwner ver", domNonOwner.InfoSchema().SchemaMetaVersion()), zap.Int64("Owner ver", domOwner.InfoSchema().SchemaMetaVersion()))
			continue
		}
		if i >= 0 {
			rows := tkO.MustQuery(`select id from t`).Sort().Rows()
			logutil.BgLogger().Info("rows tkO Start", zap.Reflect("rows", rows))
			rows = tkNO.MustQuery(`select id from t`).Sort().Rows()
			logutil.BgLogger().Info("rows tkNO Start", zap.Reflect("rows", rows))
		}
		require.Equal(t, verCurr-1, domNonOwner.InfoSchema().SchemaMetaVersion())
		require.Equal(t, verCurr, domOwner.InfoSchema().SchemaMetaVersion())
		// TODO: Add initial rows, i.e. state -1
		// SchemaVer - 1: NO
		// SchemaVer:     O
		getIds := 8
		keys := make([]int, 0, getIds)
		for key := range dbRows {
			if key >= offsetP1 {
				continue
			}
			keys = append(keys, key)
			if len(keys) >= getIds {
				break
			}
		}
		require.Len(t, keys, getIds)
		// NO:
		lastRowIDForNO := lastRowIDp0
		lastRowIDp0 = step1(tkNO, dbRows, keys[:4], i, offsetP1, lastRowIDp0)
		if i >= 0 {
			rows := tkO.MustQuery(`select id from t`).Sort().Rows()
			logutil.BgLogger().Info("rows tkO step 1 tkNO", zap.Reflect("rows", rows))
			rows = tkNO.MustQuery(`select id from t`).Sort().Rows()
			logutil.BgLogger().Info("rows tkNO step 1 tkNO", zap.Reflect("rows", rows))
		}
		// O:
		lastRowIDForO := lastRowIDp0
		lastRowIDp0 = step1(tkO, dbRows, keys[4:], i, offsetP1, lastRowIDp0)
		if i >= 0 {
			rows := tkO.MustQuery(`select id from t`).Sort().Rows()
			logutil.BgLogger().Info("tkO tkO step 1", zap.Reflect("rows", rows))
			rows = tkNO.MustQuery(`select id from t`).Sort().Rows()
			logutil.BgLogger().Info("tkNO tkO step 1", zap.Reflect("rows", rows))
		}
		// NO:
		lastRowIDp0 = step2(tkNO, dbRows, keys[4:], i, offsetP1, lastRowIDForO+1, lastRowIDp0)
		if i >= 0 {
			rows := tkO.MustQuery(`select id from t`).Sort().Rows()
			logutil.BgLogger().Info("tkO tkNO step 2", zap.Reflect("rows", rows))
			rows = tkNO.MustQuery(`select id from t`).Sort().Rows()
			logutil.BgLogger().Info("tkNO tkNO step 2", zap.Reflect("rows", rows))
		}
		// O:
		lastRowIDp0 = step2(tkO, dbRows, keys[:4], i, offsetP1, lastRowIDForNO+1, lastRowIDp0)
		if i >= 0 {
			rows := tkO.MustQuery(`select id from t`).Sort().Rows()
			logutil.BgLogger().Info("tkO tkO step 2", zap.Reflect("rows", rows))
			rows = tkNO.MustQuery(`select * from t`).Sort().Rows()
			logutil.BgLogger().Info("tkNO tkO step 2", zap.Reflect("rows", rows))
		}

		keys = keys[:0]
		for key := range dbRows {
			if key < offsetP1 {
				continue
			}
			keys = append(keys, key)
			if len(keys) >= getIds {
				break
			}
		}
		require.Len(t, keys, getIds)
		// NO:
		lastRowIDForNO = lastRowIDp1
		lastRowIDp1 = step1(tkNO, dbRows, keys[:4], i, offsetP1, lastRowIDp1)
		if i >= 0 {
			rows := tkO.MustQuery(`select id from t`).Sort().Rows()
			logutil.BgLogger().Info("rows", zap.Reflect("rows", rows))
			rows = tkNO.MustQuery(`select id from t`).Sort().Rows()
			logutil.BgLogger().Info("rows", zap.Reflect("rows", rows))
		}
		// O:
		lastRowIDForO = lastRowIDp1
		lastRowIDp1 = step1(tkO, dbRows, keys[4:], i, offsetP1, lastRowIDp1)
		if i >= 0 {
			rows := tkO.MustQuery(`select id from t`).Sort().Rows()
			logutil.BgLogger().Info("rows", zap.Reflect("rows", rows))
			rows = tkNO.MustQuery(`select id from t`).Sort().Rows()
			logutil.BgLogger().Info("rows", zap.Reflect("rows", rows))
		}
		// NO:
		lastRowIDp1 = step2(tkNO, dbRows, keys[4:], i, offsetP1, lastRowIDForO+1, lastRowIDp1)
		if i >= 0 {
			rows := tkO.MustQuery(`select id from t`).Sort().Rows()
			logutil.BgLogger().Info("rows", zap.Reflect("rows", rows))
			rows = tkNO.MustQuery(`select id from t`).Sort().Rows()
			logutil.BgLogger().Info("rows", zap.Reflect("rows", rows))
		}
		// O:
		lastRowIDp1 = step2(tkO, dbRows, keys[:4], i, offsetP1, lastRowIDForNO+1, lastRowIDp1)
		if i >= 0 {
			rows := tkO.MustQuery(`select id from t`).Sort().Rows()
			logutil.BgLogger().Info("rows", zap.Reflect("rows", rows))
			rows = tkNO.MustQuery(`select id from t`).Sort().Rows()
			logutil.BgLogger().Info("rows", zap.Reflect("rows", rows))
		}

		domNonOwner.Reload()
		verCurr++
		// Continue to next state
		if releaseHook {
			hookChan <- struct{}{}
		}
	}
	logutil.BgLogger().Info("XXXXXXXXXXX states loop done")
	if verCurr > 0 {
		rows := tkO.MustQuery(`select * from t`).Sort().Rows()
		logutil.BgLogger().Info("rows", zap.Reflect("rows", rows))
		rows = tkNO.MustQuery(`select * from t`).Sort().Rows()
		logutil.BgLogger().Info("rows", zap.Reflect("rows", rows))
	}
	//tk1.MustQuery(`select * from t`).Sort().Check(testkit.Rows())
	// First iteration, which rows needs to exists 'before':
	// 3 to update, 1 to delete X 2 = 8 rows
	// then 3 new ones - 1 delete X 2 = 10 rows
	// 1 new, 2 delete X 2 = 8 rows => Can we just keep 8 rows?
	// Would flipping the order between stage changes matter for the test?
}

func step1(tk *testkit.TestKit, dbRows map[int][]string, keys []int, state int, offset int, lastRowID int) int {
	// update at least three row from 'before'
	for j := 0; j < 3; j++ {
		// TODO: Should the updates also affect the indexes?
		r := dbRows[keys[j]]
		hist := fmt.Sprintf(":Update(%d:%d:%d)", state, j, lastRowID)
		r[4] += hist
		tk.MustExec(fmt.Sprintf(`update t set history = concat(history,'%s') where id = %d`, hist, keys[j]))
		logutil.BgLogger().Info("update", zap.Int("keys[j]", keys[j]))
	}
	// delete at least one row from 'before'
	tk.MustExec(fmt.Sprintf(`delete from t where id = %d`, keys[3]))
	logutil.BgLogger().Info("delete", zap.Int("keys[3]", keys[3]))
	delete(dbRows, keys[3])
	// insert at least three rows (one to keep untouched, one to update, one to delete)
	lastRowID++
	dbRows[lastRowID] = []string{strconv.Itoa(lastRowID), strconv.Itoa(offset - lastRowID), "p0", strconv.Itoa(state), ""}
	a, b, c, d, e := dbRows[lastRowID][0], dbRows[lastRowID][1], dbRows[lastRowID][2], dbRows[lastRowID][3], dbRows[lastRowID][4]
	tk.MustExec(fmt.Sprintf(`insert into t values (%s, %s, '%s', %s, '%s')`, a, b, c, d, e))
	logutil.BgLogger().Info("insert", zap.Int("id", lastRowID))
	lastRowID++
	dbRows[lastRowID] = []string{strconv.Itoa(lastRowID), strconv.Itoa(offset - lastRowID), "p0", strconv.Itoa(state), ""}
	a, b, c, d, e = dbRows[lastRowID][0], dbRows[lastRowID][1], dbRows[lastRowID][2], dbRows[lastRowID][3], dbRows[lastRowID][4]
	tk.MustExec(fmt.Sprintf(`insert into t values (%s, %s, '%s', %s, '%s')`, a, b, c, d, e))
	logutil.BgLogger().Info("insert", zap.Int("id", lastRowID))
	lastRowID++
	dbRows[lastRowID] = []string{strconv.Itoa(lastRowID), strconv.Itoa(offset - lastRowID), "p0", strconv.Itoa(state), ""}
	a, b, c, d, e = dbRows[lastRowID][0], dbRows[lastRowID][1], dbRows[lastRowID][2], dbRows[lastRowID][3], dbRows[lastRowID][4]
	tk.MustExec(fmt.Sprintf(`insert into t values (%s, %s, '%s', %s, '%s')`, a, b, c, d, e))
	logutil.BgLogger().Info("insert", zap.Int("id", lastRowID))
	// Check for rows, including test insert and update for duplicate key!!!
	return lastRowID
}

func step2(tk *testkit.TestKit, dbRows map[int][]string, keys []int, state int, offset int, startID int, lastRowID int) int {
	// insert one row
	lastRowID++
	dbRows[lastRowID] = []string{strconv.Itoa(lastRowID), strconv.Itoa(offset - lastRowID), "p0", strconv.Itoa(state), ""}
	a, b, c, d, e := dbRows[lastRowID][0], dbRows[lastRowID][1], dbRows[lastRowID][2], dbRows[lastRowID][3], dbRows[lastRowID][4]
	tk.MustExec(fmt.Sprintf(`insert into t values (%s, %s, '%s', %s, '%s')`, a, b, c, d, e))
	logutil.BgLogger().Info("insert", zap.Int("id", lastRowID))
	// update one row that A inserted
	r := dbRows[startID]
	hist := fmt.Sprintf(":Update(%d:%d:%d:%d)", state, 4, startID, lastRowID)
	r[4] += hist
	tk.MustExec(fmt.Sprintf(`update t set history = concat(history,'%s') where id = %d`, hist, startID))
	logutil.BgLogger().Info("update", zap.Int("id", startID))
	startID++
	// update one row that A updated
	r = dbRows[keys[0]]
	hist = fmt.Sprintf(":Update(%d:%d:%d:%d)", state, 4, startID, lastRowID)
	r[4] += hist
	tk.MustExec(fmt.Sprintf(`update t set history = concat(history,'%s') where id = %d`, hist, keys[0]))
	logutil.BgLogger().Info("update", zap.Int("id keys[0]", keys[0]))
	// delete one row that A inserted
	tk.MustExec(fmt.Sprintf(`delete from t where id = %d`, startID))
	logutil.BgLogger().Info("delete", zap.Int("startID", startID))
	delete(dbRows, startID)
	startID++
	// delete one row that A updated
	tk.MustExec(fmt.Sprintf(`delete from t where id = %d`, keys[1]))
	logutil.BgLogger().Info("delete", zap.Int("keys[1]", keys[1]))
	delete(dbRows, keys[1])
	// Check for the row A deleted
	return lastRowID
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

func TestMultiSchemaVerDropPartitionWithGlobalIndexGeneric(t *testing.T) {
	testkit.SkipIfFailpointDisabled(t)
	createSQL := `CREATE TABLE t (a int, b int, c varchar(255), d datetime, e binary(16), primary key (a), key i_b (b), key i_cb (c,b), unique index ui_bd (b,d) global, unique index ui_e (e) global) PARTITION BY RANGE (a) (partition p0 values less than (1000000), partition p1 values less than (2000000), partition p2 values less than (3000000))`
	alterSQL := `alter table t drop partition p0`
	i := 1
	initFn := func(tkO *testkit.TestKit) {
		tkO.MustExec(`insert into t values (1,1,1,now(),uuid_to_bin(uuid()))`)
	}
	loopFn := func(tkO, tkNO *testkit.TestKit) {
		i++
		tkO.MustExec(`insert into t values (?,?,?,now(),uuid_to_bin(uuid()))`, i, i, i)
		i++
		tkNO.MustExec(`insert into t values (?,?,?,now(),uuid_to_bin(uuid()))`, i, i, i)
	}
	postFn := func(tkO *testkit.TestKit) {
		tkO.MustQuery(`select * from t`).CheckAt([]int{0, 1, 2}, testkit.Rows("2 2 2", "4 4 4", "5 5 5", "6 6 6", "7 7 7"))
	}
	runMultiSchemaTest(t, createSQL, alterSQL, initFn, postFn, loopFn)
}
