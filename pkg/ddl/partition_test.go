// Copyright 2020 PingCAP, Inc.
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
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestDropAndTruncatePartition(t *testing.T) {
	store, domain := testkit.CreateMockStoreAndDomainWithSchemaLease(t, testLease)

	dbInfo, err := testSchemaInfo(store, "test_partition")
	require.NoError(t, err)
	de := domain.DDLExecutor().(ddl.ExecutorForTest)
	testCreateSchema(t, testkit.NewTestKit(t, store).Session(), de, dbInfo)
	// generate 5 partition in tableInfo.
	tblInfo, partIDs := buildTableInfoWithPartition(t, store)
	ctx := testkit.NewTestKit(t, store).Session()
	testCreateTable(t, ctx, de, dbInfo, tblInfo)
	testDropPartition(t, ctx, de, dbInfo, tblInfo, []string{"p0", "p1"})

	newIDs, err := genGlobalIDs(store, 2)
	require.NoError(t, err)
	testTruncatePartition(t, ctx, de, dbInfo, tblInfo, []int64{partIDs[3], partIDs[4]}, newIDs)
}

func buildTableInfoWithPartition(t *testing.T, store kv.Storage) (*model.TableInfo, []int64) {
	tbl := &model.TableInfo{
		Name: ast.NewCIStr("t"),
	}
	tbl.MaxColumnID++
	col := &model.ColumnInfo{
		Name:      ast.NewCIStr("c"),
		Offset:    0,
		State:     model.StatePublic,
		FieldType: *types.NewFieldType(mysql.TypeLong),
		ID:        tbl.MaxColumnID,
	}
	genIDs, err := genGlobalIDs(store, 1)
	require.NoError(t, err)
	tbl.ID = genIDs[0]
	tbl.Columns = []*model.ColumnInfo{col}
	tbl.Charset = "utf8"
	tbl.Collate = "utf8_bin"

	partIDs, err := genGlobalIDs(store, 5)
	require.NoError(t, err)
	partInfo := &model.PartitionInfo{
		Type:   ast.PartitionTypeRange,
		Expr:   tbl.Columns[0].Name.L,
		Enable: true,
		Definitions: []model.PartitionDefinition{
			{
				ID:       partIDs[0],
				Name:     ast.NewCIStr("p0"),
				LessThan: []string{"100"},
			},
			{
				ID:       partIDs[1],
				Name:     ast.NewCIStr("p1"),
				LessThan: []string{"200"},
			},
			{
				ID:       partIDs[2],
				Name:     ast.NewCIStr("p2"),
				LessThan: []string{"300"},
			},
			{
				ID:       partIDs[3],
				Name:     ast.NewCIStr("p3"),
				LessThan: []string{"400"},
			},
			{
				ID:       partIDs[4],
				Name:     ast.NewCIStr("p4"),
				LessThan: []string{"500"},
			},
		},
	}
	tbl.Partition = partInfo
	return tbl, partIDs
}

func buildDropPartitionJob(dbInfo *model.DBInfo, tblInfo *model.TableInfo, partNames []string) (*model.Job, *model.TablePartitionArgs) {
	return &model.Job{
		Version:     model.GetJobVerInUse(),
		SchemaID:    dbInfo.ID,
		SchemaName:  dbInfo.Name.L,
		TableID:     tblInfo.ID,
		TableName:   tblInfo.Name.L,
		SchemaState: model.StatePublic,
		Type:        model.ActionDropTablePartition,
		BinlogInfo:  &model.HistoryInfo{},
	}, &model.TablePartitionArgs{PartNames: partNames}
}

func testDropPartition(t *testing.T, ctx sessionctx.Context, d ddl.ExecutorForTest, dbInfo *model.DBInfo, tblInfo *model.TableInfo, partNames []string) *model.Job {
	job, args := buildDropPartitionJob(dbInfo, tblInfo, partNames)
	ctx.SetValue(sessionctx.QueryString, "skip")
	err := d.DoDDLJobWrapper(ctx, ddl.NewJobWrapperWithArgs(job, args, true))
	require.NoError(t, err)
	v := getSchemaVer(t, ctx)
	checkHistoryJobArgs(t, ctx, job.ID, &historyJobArgs{ver: v, tbl: tblInfo})
	return job
}

func buildTruncatePartitionJob(dbInfo *model.DBInfo, tblInfo *model.TableInfo, pids []int64, newIDs []int64) (*model.Job, *model.TruncateTableArgs) {
	return &model.Job{
		Version:     model.GetJobVerInUse(),
		SchemaID:    dbInfo.ID,
		SchemaName:  dbInfo.Name.L,
		TableID:     tblInfo.ID,
		TableName:   tblInfo.Name.L,
		Type:        model.ActionTruncateTablePartition,
		SchemaState: model.StatePublic,
		BinlogInfo:  &model.HistoryInfo{},
	}, &model.TruncateTableArgs{OldPartitionIDs: pids, NewPartitionIDs: newIDs}
}

func testTruncatePartition(t *testing.T, ctx sessionctx.Context, d ddl.ExecutorForTest, dbInfo *model.DBInfo, tblInfo *model.TableInfo, pids []int64, newIDs []int64) *model.Job {
	job, args := buildTruncatePartitionJob(dbInfo, tblInfo, pids, newIDs)
	ctx.SetValue(sessionctx.QueryString, "skip")
	err := d.DoDDLJobWrapper(ctx, ddl.NewJobWrapperWithArgs(job, args, true))
	require.NoError(t, err)
	v := getSchemaVer(t, ctx)
	checkHistoryJobArgs(t, ctx, job.ID, &historyJobArgs{ver: v, tbl: tblInfo})
	return job
}

func TestReorganizePartitionRollback(t *testing.T) {
	// See issue: https://github.com/pingcap/tidb/issues/42448
	store, do := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("CREATE TABLE `t1` (\n" +
		"    `id` bigint(20) NOT NULL AUTO_INCREMENT,\n" +
		"    `k` int(11) NOT NULL DEFAULT '0',\n" +
		"    `c` char(120) NOT NULL DEFAULT '',\n" +
		"    `pad` char(60) NOT NULL DEFAULT '',\n" +
		"    PRIMARY KEY (`id`) /*T![clustered_index] CLUSTERED */,\n" +
		"    KEY `k_1` (`k`)\n" +
		"  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin\n" +
		"  PARTITION BY RANGE (`id`)\n" +
		"  (PARTITION `p0` VALUES LESS THAN (2000000),\n" +
		"   PARTITION `p1` VALUES LESS THAN (4000000),\n" +
		"   PARTITION `p2` VALUES LESS THAN (6000000),\n" +
		"   PARTITION `p3` VALUES LESS THAN (8000000),\n" +
		"   PARTITION `p4` VALUES LESS THAN (10000000),\n" +
		"   PARTITION `p5` VALUES LESS THAN (MAXVALUE))")
	tk.MustExec("insert into t1(k, c, pad) values (1, 'a', 'beijing'), (2, 'b', 'chengdu')")

	wait := make(chan struct{})
	defer close(wait)
	ddlDone := make(chan error)
	defer close(ddlDone)
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/afterRunOneJobStep", func(job *model.Job) {
		if job.Type == model.ActionReorganizePartition && job.SchemaState == model.StateWriteReorganization {
			<-wait
			<-wait
		}
	})

	go func() {
		tk2 := testkit.NewTestKit(t, store)
		tk2.MustExec("use test")
		err := tk2.ExecToErr("alter table t1 reorganize partition p0, p1, p2, p3, p4 into( partition pnew values less than (10000000))")
		ddlDone <- err
	}()

	jobID := ""

	// wait DDL job reaches hook and then cancel
	select {
	case wait <- struct{}{}:
		rows := tk.MustQuery("admin show ddl jobs where JOB_TYPE='alter table reorganize partition'").Rows()
		require.Equal(t, 1, len(rows))
		jobID = rows[0][0].(string)
		tk.MustExec("admin cancel ddl jobs " + jobID)
	case <-time.After(time.Minute):
		require.FailNow(t, "timeout")
	}

	// continue to run DDL
	select {
	case wait <- struct{}{}:
	case <-time.After(time.Minute):
		require.FailNow(t, "timeout")
	}

	// wait ddl done
	select {
	case err := <-ddlDone:
		require.Error(t, err)
	case <-time.After(time.Minute):
		require.FailNow(t, "wait ddl cancelled timeout")
	}

	// check job rollback finished
	rows := tk.MustQuery("admin show ddl jobs where JOB_ID=" + jobID).Rows()
	require.Equal(t, 1, len(rows))
	require.Equal(t, "rollback done", rows[0][len(rows[0])-2])

	// check table meta after rollback
	tk.MustQuery("show create table t1").Check(testkit.Rows("t1 CREATE TABLE `t1` (\n" +
		"  `id` bigint(20) NOT NULL AUTO_INCREMENT,\n" +
		"  `k` int(11) NOT NULL DEFAULT '0',\n" +
		"  `c` char(120) NOT NULL DEFAULT '',\n" +
		"  `pad` char(60) NOT NULL DEFAULT '',\n" +
		"  PRIMARY KEY (`id`) /*T![clustered_index] CLUSTERED */,\n" +
		"  KEY `k_1` (`k`)\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin AUTO_INCREMENT=5001\n" +
		"PARTITION BY RANGE (`id`)\n" +
		"(PARTITION `p0` VALUES LESS THAN (2000000),\n" +
		" PARTITION `p1` VALUES LESS THAN (4000000),\n" +
		" PARTITION `p2` VALUES LESS THAN (6000000),\n" +
		" PARTITION `p3` VALUES LESS THAN (8000000),\n" +
		" PARTITION `p4` VALUES LESS THAN (10000000),\n" +
		" PARTITION `p5` VALUES LESS THAN (MAXVALUE))"))
	tbl, err := do.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t1"))
	require.NoError(t, err)
	require.NotNil(t, tbl.Meta().Partition)
	require.Nil(t, tbl.Meta().Partition.AddingDefinitions)
	require.Nil(t, tbl.Meta().Partition.DroppingDefinitions)

	// test then add index should success
	tk.MustExec("alter table t1 add index idx_kc (k, c)")
}

func TestUpdateDuringAddColumn(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t1 (c1 int, c2 int) partition by hash (c1) partitions 16")
	tk.MustExec("insert t1 values (1, 1), (2, 2)")
	tk.MustExec("create table t2 (c1 int, c2 int) partition by hash (c1) partitions 16")
	tk.MustExec("insert t2 values (1, 3), (2, 5)")
	tk2 := testkit.NewTestKit(t, store)
	tk2.MustExec("use test")

	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/afterWaitSchemaSynced", func(job *model.Job) {
		if job.SchemaState == model.StateWriteOnly {
			tk2.MustExec("update t1, t2 set t1.c1 = 8, t2.c2 = 10 where t1.c2 = t2.c1")
			tk2.MustQuery("select * from t1").Sort().Check(testkit.Rows("8 1", "8 2"))
			tk2.MustQuery("select * from t2").Sort().Check(testkit.Rows("1 10", "2 10"))
		}
	})

	tk.MustExec("alter table t1 add column c3 bigint default 9")

	tk.MustQuery("select * from t1").Sort().Check(testkit.Rows("8 1 9", "8 2 9"))
}
