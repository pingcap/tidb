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

	"github.com/pingcap/tidb/pkg/ddl/util/callback"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/session"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func TestMultiSchemaVerPartitionBy(t *testing.T) {
	// Last domain will be the DDLOwner
	distCtx := testkit.NewDistExecutionContextWithLease(t, 2, 15*time.Second)
	store := distCtx.Store
	dom1 := distCtx.GetDomain(0)
	dom2 := distCtx.GetDomain(1)
	defer func() {
		dom1.Close()
		dom2.Close()
		store.Close()
	}()

	ddlJobsSQL := `admin show ddl jobs where db_name = 'test' and table_name = 't' and job_type = 'alter table partition by'`

	se1, err := session.CreateSessionWithDomain(store, dom1)
	require.NoError(t, err)
	se2, err := session.CreateSessionWithDomain(store, dom2)
	require.NoError(t, err)

	// Session on non DDL owner domain (~ TiDB Server)
	tk1 := testkit.NewTestKitWithSession(t, store, se1)
	// Session on DDL owner domain (~ TiDB Server), used for concurrent DDL
	tk2 := testkit.NewTestKitWithSession(t, store, se2)
	// Session on DDL owner domain (~ TiDB Server), used for queries
	tk3 := testkit.NewTestKitWithSession(t, store, se2)
	tk1.MustExec(`use test`)
	tk2.MustExec(`use test`)
	tk3.MustExec(`use test`)
	// The DDL Owner will be the first created domain, so use tk1.
	tk1.MustExec(`create table t (a int primary key, b varchar(255))`)
	dom1.Reload()
	dom2.Reload()
	verStart := dom1.InfoSchema().SchemaMetaVersion()
	alterChan := make(chan struct{})
	originHook := dom1.DDL().GetHook()
	hook := &callback.TestDDLCallback{Do: nil}
	hook.OnJobRunBeforeExported = func(job *model.Job) {
		alterChan <- struct{}{}
		<-alterChan
	}
	dom1.DDL().SetHook(hook)
	defer dom1.DDL().SetHook(originHook)
	go func() {
		tk1.MustExec(`alter table t partition by hash(a) partitions 3`)
		alterChan <- struct{}{}
	}()
	// Wait for the first state change to begin
	<-alterChan
	alterChan <- struct{}{}
	// Doing the first State change
	stateChange := int64(1)
	<-alterChan
	// Waiting before running the second State change
	verCurr := dom1.InfoSchema().SchemaMetaVersion()
	require.Equal(t, stateChange, verCurr-verStart)
	require.Equal(t, verStart, dom2.InfoSchema().SchemaMetaVersion())
	tk2.MustQuery(`show create table t`).Check(testkit.Rows("" +
		"t CREATE TABLE `t` (\n" +
		"  `a` int(11) NOT NULL,\n" +
		"  `b` varchar(255) DEFAULT NULL,\n" +
		"  PRIMARY KEY (`a`) /*T![clustered_index] CLUSTERED */\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))
	dom2.Reload()
	require.Equal(t, verCurr, dom2.InfoSchema().SchemaMetaVersion())
	tk2.MustQuery(`show create table t`).Check(testkit.Rows("" +
		"t CREATE TABLE `t` (\n" +
		"  `a` int(11) NOT NULL,\n" +
		"  `b` varchar(255) DEFAULT NULL,\n" +
		"  PRIMARY KEY (`a`) /*T![clustered_index] CLUSTERED */\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin\n" +
		"PARTITION BY NONE ()\n" +
		"(PARTITION `pFullTable` COMMENT 'Intermediate partition during ALTER TABLE ... PARTITION BY ...')"))
	tk2.MustQuery(ddlJobsSQL).CheckAt([]int{4, 11}, [][]any{{"delete only", "running"}})
	alterChan <- struct{}{}
	// doing second State change
	stateChange++
	<-alterChan
	// Waiting before running the third State change
	verCurr = dom1.InfoSchema().SchemaMetaVersion()
	require.Equal(t, stateChange, verCurr-verStart)
	dom2.Reload()
	require.Equal(t, verCurr, dom1.InfoSchema().SchemaMetaVersion())
	tk2.MustQuery(`show create table t`).Check(testkit.Rows("" +
		"t CREATE TABLE `t` (\n" +
		"  `a` int(11) NOT NULL,\n" +
		"  `b` varchar(255) DEFAULT NULL,\n" +
		"  PRIMARY KEY (`a`) /*T![clustered_index] CLUSTERED */\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin\n" +
		"PARTITION BY NONE ()\n" +
		"(PARTITION `pFullTable` COMMENT 'Intermediate partition during ALTER TABLE ... PARTITION BY ...')"))
	tk2.MustQuery(ddlJobsSQL).CheckAt([]int{4, 11}, [][]any{{"write only", "running"}})
	alterChan <- struct{}{}
	<-alterChan
	stateChange++
	verCurr = dom1.InfoSchema().SchemaMetaVersion()
	require.Equal(t, stateChange, verCurr-verStart)
	dom2.Reload()
	require.Equal(t, verCurr, dom2.InfoSchema().SchemaMetaVersion())
	tk2.MustQuery(`show create table t`).Check(testkit.Rows("" +
		"t CREATE TABLE `t` (\n" +
		"  `a` int(11) NOT NULL,\n" +
		"  `b` varchar(255) DEFAULT NULL,\n" +
		"  PRIMARY KEY (`a`) /*T![clustered_index] CLUSTERED */\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin\n" +
		"PARTITION BY NONE ()\n" +
		"(PARTITION `pFullTable` COMMENT 'Intermediate partition during ALTER TABLE ... PARTITION BY ...')"))
	tk2.MustQuery(ddlJobsSQL).CheckAt([]int{4, 11}, [][]any{{"write reorganization", "running"}})
	alterChan <- struct{}{}
	<-alterChan
	stateChange++
	verCurr = dom1.InfoSchema().SchemaMetaVersion()
	require.Equal(t, stateChange, verCurr-verStart)
	dom2.Reload()
	require.Equal(t, verCurr, dom2.InfoSchema().SchemaMetaVersion())
	tk2.MustQuery(`show create table t`).Check(testkit.Rows("" +
		"t CREATE TABLE `t` (\n" +
		"  `a` int(11) NOT NULL,\n" +
		"  `b` varchar(255) DEFAULT NULL,\n" +
		"  PRIMARY KEY (`a`) /*T![clustered_index] CLUSTERED */\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin\n" +
		"PARTITION BY HASH (`a`) PARTITIONS 3"))
	tk2.MustQuery(ddlJobsSQL).CheckAt([]int{4, 11}, [][]any{{"delete reorganization", "running"}})
	alterChan <- struct{}{}
	<-alterChan
	// Alter done!
	stateChange++
	verCurr = dom1.InfoSchema().SchemaMetaVersion()
	require.Equal(t, stateChange, verCurr-verStart)
	dom2.Reload()
	require.Equal(t, verCurr, dom2.InfoSchema().SchemaMetaVersion())
	tk2.MustQuery(`show create table t`).Check(testkit.Rows("" +
		"t CREATE TABLE `t` (\n" +
		"  `a` int(11) NOT NULL,\n" +
		"  `b` varchar(255) DEFAULT NULL,\n" +
		"  PRIMARY KEY (`a`) /*T![clustered_index] CLUSTERED */\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin\n" +
		"PARTITION BY HASH (`a`) PARTITIONS 3"))
	tk2.MustQuery(ddlJobsSQL).CheckAt([]int{4, 11}, [][]any{{"none", "synced"}})
}

func TestMultiSchemaVerAddPartition(t *testing.T) {
	// Last domain will be the DDLOwner
	distCtx := testkit.NewDistExecutionContextWithLease(t, 2, 15*time.Second)
	store := distCtx.Store
	dom1 := distCtx.GetDomain(0)
	dom2 := distCtx.GetDomain(1)
	defer func() {
		dom1.Close()
		dom2.Close()
		store.Close()
	}()

	ddlJobsSQL := `admin show ddl jobs where db_name = 'test' and table_name = 't' and job_type = 'add partition'`

	se1, err := session.CreateSessionWithDomain(store, dom1)
	require.NoError(t, err)
	se2, err := session.CreateSessionWithDomain(store, dom2)
	require.NoError(t, err)

	// Session on non DDL owner domain (~ TiDB Server)
	tk1 := testkit.NewTestKitWithSession(t, store, se1)
	tk1.MustExec(`use test`)
	tk1.MustExec(`set @@global.tidb_enable_global_index = 1`)
	tk1.MustExec(`set @@session.tidb_enable_global_index = 1`)
	// Session on DDL owner domain (~ TiDB Server), used for concurrent DDL
	tk2 := testkit.NewTestKitWithSession(t, store, se2)
	tk2.MustExec(`use test`)
	tk2.MustExec(`set @@session.tidb_enable_global_index = 1`)
	// Session on DDL owner domain (~ TiDB Server), used for queries
	tk3 := testkit.NewTestKitWithSession(t, store, se2)
	tk3.MustExec(`use test`)
	// The DDL Owner will be the last created domain, so use tk2.
	tk2.MustExec(`create table t (a int primary key nonclustered, b varchar(255) charset utf8mb4 collate utf8mb4_0900_ai_ci) partition by range columns (b) (partition p0 values less than ("m"))`)
	dom1.Reload()
	dom2.Reload()
	verStart := dom2.InfoSchema().SchemaMetaVersion()
	alterChan := make(chan struct{})
	originHook := dom2.DDL().GetHook()
	hook := &callback.TestDDLCallback{Do: nil}
	hook.OnJobRunBeforeExported = func(job *model.Job) {
		alterChan <- struct{}{}
		<-alterChan
	}
	dom2.DDL().SetHook(hook)
	defer dom2.DDL().SetHook(originHook)
	go func() {
		tk2.MustExec(`alter table t add partition (partition p1 values less than ("p"))`)
		alterChan <- struct{}{}
	}()
	// Wait for the first state change to begin
	<-alterChan
	alterChan <- struct{}{}
	// Doing the first State change
	stateChange := int64(1)
	<-alterChan
	// Waiting before running the second State change
	verCurr := dom2.InfoSchema().SchemaMetaVersion()
	require.Equal(t, stateChange, verCurr-verStart)
	require.Equal(t, verStart, dom1.InfoSchema().SchemaMetaVersion())
	tk1.MustQuery(`show create table t`).Check(testkit.Rows("" +
		"t CREATE TABLE `t` (\n" +
		"  `a` int(11) NOT NULL,\n" +
		"  `b` varchar(255) COLLATE utf8mb4_0900_ai_ci DEFAULT NULL,\n" +
		"  PRIMARY KEY (`a`) /*T![clustered_index] NONCLUSTERED */\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin\n" +
		"PARTITION BY RANGE COLUMNS(`b`)\n" +
		"(PARTITION `p0` VALUES LESS THAN ('m'))"))
	dom1.Reload()
	require.Equal(t, verCurr, dom1.InfoSchema().SchemaMetaVersion())
	tk1.MustQuery(`show create table t`).Check(testkit.Rows("" +
		"t CREATE TABLE `t` (\n" +
		"  `a` int(11) NOT NULL,\n" +
		"  `b` varchar(255) COLLATE utf8mb4_0900_ai_ci DEFAULT NULL,\n" +
		"  PRIMARY KEY (`a`) /*T![clustered_index] NONCLUSTERED */\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin\n" +
		"PARTITION BY RANGE COLUMNS(`b`)\n" +
		"(PARTITION `p0` VALUES LESS THAN ('m'))"))
	tk1.MustQuery(ddlJobsSQL).CheckAt([]int{4, 11}, [][]any{{"replica only", "running"}})
	alterChan <- struct{}{}
	stateChange++
	<-alterChan
	verCurr = dom2.InfoSchema().SchemaMetaVersion()
	require.Equal(t, stateChange, verCurr-verStart)
	tk3.MustExec(`insert into t values (1,"Matt"),(2,"Anne")`)
	tk1.MustQuery(`select /*+ USE_INDEX(t, PRIMARY) */ a from t`).Check(testkit.Rows("2"))
	tk1.MustQuery(`select * from t`).Check(testkit.Rows("2 Anne"))
	dom1.Reload()
	require.Equal(t, verCurr, dom1.InfoSchema().SchemaMetaVersion())
	tk1.MustQuery(`show create table t`).Check(testkit.Rows("" +
		"t CREATE TABLE `t` (\n" +
		"  `a` int(11) NOT NULL,\n" +
		"  `b` varchar(255) COLLATE utf8mb4_0900_ai_ci DEFAULT NULL,\n" +
		"  PRIMARY KEY (`a`) /*T![clustered_index] NONCLUSTERED */\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin\n" +
		"PARTITION BY RANGE COLUMNS(`b`)\n" +
		"(PARTITION `p0` VALUES LESS THAN ('m'),\n" +
		" PARTITION `p1` VALUES LESS THAN ('p'))"))
	tk1.MustQuery(ddlJobsSQL).CheckAt([]int{4, 11}, [][]any{{"public", "synced"}})
	tk1.MustQuery(`select /*+ USE_INDEX(t, PRIMARY) */ a from t`).Check(testkit.Rows("1", "2"))
	tk1.MustQuery(`select * from t`).Sort().Check(testkit.Rows("1 Matt", "2 Anne"))
}
