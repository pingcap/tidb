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
	"math"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/ddl/util/callback"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/errno"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/server"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/external"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

const tiflashReplicaLease = 600 * time.Millisecond

func TestSetTableFlashReplica(t *testing.T) {
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/infoschema/mockTiFlashStoreCount", `return(true)`))
	store := testkit.CreateMockStoreWithSchemaLease(t, tiflashReplicaLease)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_flash(a int, b int)")

	tbl := external.GetTableByName(t, tk, "test", "t_flash")
	require.Nil(t, tbl.Meta().TiFlashReplica)

	tk.MustExec("alter table t_flash set tiflash replica 2 location labels 'a','b';")
	tbl = external.GetTableByName(t, tk, "test", "t_flash")
	require.NotNil(t, tbl.Meta().TiFlashReplica)
	require.Equal(t, uint64(2), tbl.Meta().TiFlashReplica.Count)
	require.Equal(t, "a,b", strings.Join(tbl.Meta().TiFlashReplica.LocationLabels, ","))

	tk.MustExec("alter table t_flash set tiflash replica 0")
	tbl = external.GetTableByName(t, tk, "test", "t_flash")
	require.Nil(t, tbl.Meta().TiFlashReplica)

	// Test set tiflash replica for partition table.
	tk.MustExec("drop table if exists t_flash;")
	tk.MustExec("create table t_flash(a int, b int) partition by hash(a) partitions 3")
	tk.MustExec("alter table t_flash set tiflash replica 2 location labels 'a','b';")
	tbl = external.GetTableByName(t, tk, "test", "t_flash")
	require.NotNil(t, tbl.Meta().TiFlashReplica)
	require.Equal(t, uint64(2), tbl.Meta().TiFlashReplica.Count)
	require.Equal(t, "a,b", strings.Join(tbl.Meta().TiFlashReplica.LocationLabels, ","))

	// Use table ID as physical ID, mock for partition feature was not enabled.
	err := domain.GetDomain(tk.Session()).DDL().UpdateTableReplicaInfo(tk.Session(), tbl.Meta().ID, true)
	require.NoError(t, err)
	tbl = external.GetTableByName(t, tk, "test", "t_flash")
	require.NotNil(t, tbl.Meta().TiFlashReplica)
	require.True(t, tbl.Meta().TiFlashReplica.Available)
	require.Len(t, tbl.Meta().TiFlashReplica.AvailablePartitionIDs, 0)

	err = domain.GetDomain(tk.Session()).DDL().UpdateTableReplicaInfo(tk.Session(), tbl.Meta().ID, false)
	require.NoError(t, err)
	tbl = external.GetTableByName(t, tk, "test", "t_flash")
	require.False(t, tbl.Meta().TiFlashReplica.Available)

	// Mock for partition 0 replica was available.
	partition := tbl.Meta().Partition
	require.Len(t, partition.Definitions, 3)
	err = domain.GetDomain(tk.Session()).DDL().UpdateTableReplicaInfo(tk.Session(), partition.Definitions[0].ID, true)
	require.NoError(t, err)
	tbl = external.GetTableByName(t, tk, "test", "t_flash")
	require.False(t, tbl.Meta().TiFlashReplica.Available)
	require.Equal(t, []int64{partition.Definitions[0].ID}, tbl.Meta().TiFlashReplica.AvailablePartitionIDs)

	// Mock for partition 0 replica become unavailable.
	err = domain.GetDomain(tk.Session()).DDL().UpdateTableReplicaInfo(tk.Session(), partition.Definitions[0].ID, false)
	require.NoError(t, err)
	tbl = external.GetTableByName(t, tk, "test", "t_flash")
	require.False(t, tbl.Meta().TiFlashReplica.Available)
	require.Len(t, tbl.Meta().TiFlashReplica.AvailablePartitionIDs, 0)

	// Mock for partition 0, 1,2 replica was available.
	err = domain.GetDomain(tk.Session()).DDL().UpdateTableReplicaInfo(tk.Session(), partition.Definitions[0].ID, true)
	require.NoError(t, err)
	err = domain.GetDomain(tk.Session()).DDL().UpdateTableReplicaInfo(tk.Session(), partition.Definitions[1].ID, true)
	require.NoError(t, err)
	err = domain.GetDomain(tk.Session()).DDL().UpdateTableReplicaInfo(tk.Session(), partition.Definitions[2].ID, true)
	require.NoError(t, err)
	tbl = external.GetTableByName(t, tk, "test", "t_flash")
	require.True(t, tbl.Meta().TiFlashReplica.Available)
	require.Equal(t, []int64{partition.Definitions[0].ID, partition.Definitions[1].ID, partition.Definitions[2].ID}, tbl.Meta().TiFlashReplica.AvailablePartitionIDs)

	// Mock for partition 1 replica was unavailable.
	err = domain.GetDomain(tk.Session()).DDL().UpdateTableReplicaInfo(tk.Session(), partition.Definitions[1].ID, false)
	require.NoError(t, err)
	tbl = external.GetTableByName(t, tk, "test", "t_flash")
	require.Equal(t, false, tbl.Meta().TiFlashReplica.Available)
	require.Equal(t, []int64{partition.Definitions[0].ID, partition.Definitions[2].ID}, tbl.Meta().TiFlashReplica.AvailablePartitionIDs)

	// Test for update table replica with unknown table ID.
	err = domain.GetDomain(tk.Session()).DDL().UpdateTableReplicaInfo(tk.Session(), math.MaxInt64, false)
	require.EqualError(t, err, "[schema:1146]Table which ID = 9223372036854775807 does not exist.")

	// Test for FindTableByPartitionID.
	is := domain.GetDomain(tk.Session()).InfoSchema()
	tbl, dbInfo, _ := is.FindTableByPartitionID(partition.Definitions[0].ID)
	require.NotNil(t, tbl)
	require.NotNil(t, dbInfo)
	require.Equal(t, "t_flash", tbl.Meta().Name.L)
	tbl, dbInfo, _ = is.FindTableByPartitionID(tbl.Meta().ID)
	require.Nil(t, tbl)
	require.Nil(t, dbInfo)
	err = failpoint.Disable("github.com/pingcap/tidb/pkg/infoschema/mockTiFlashStoreCount")
	require.NoError(t, err)

	// Test for set replica count more than the tiflash store count.
	tk.MustExec("drop table if exists t_flash;")
	tk.MustExec("create table t_flash(a int, b int)")
	tk.MustGetErrMsg("alter table t_flash set tiflash replica 2 location labels 'a','b';", "the tiflash replica count: 2 should be less than the total tiflash server count: 0")
}

// setUpRPCService setup grpc server to handle cop request for test.
func setUpRPCService(t *testing.T, addr string, dom *domain.Domain, sm util.SessionManager) (*grpc.Server, string) {
	lis, err := net.Listen("tcp", addr)
	require.NoError(t, err)
	srv := server.NewRPCServer(config.GetGlobalConfig(), dom, sm)
	port := lis.Addr().(*net.TCPAddr).Port
	addr = fmt.Sprintf("127.0.0.1:%d", port)
	go func() {
		err = srv.Serve(lis)
		require.NoError(t, err)
	}()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.Status.StatusPort = uint(port)
	})
	return srv, addr
}

func TestInfoSchemaForTiFlashReplica(t *testing.T) {
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/infoschema/mockTiFlashStoreCount", `return(true)`))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/infoschema/mockTiFlashStoreCount"))
	}()

	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	rpcserver, _ := setUpRPCService(t, "127.0.0.1:0", domain.GetDomain(tk.Session()), nil)
	defer rpcserver.Stop()
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int, b int, index idx(a))")
	tk.MustExec("alter table t set tiflash replica 2 location labels 'a','b';")
	tk.MustQuery("select TABLE_SCHEMA,TABLE_NAME,REPLICA_COUNT,LOCATION_LABELS,AVAILABLE,PROGRESS from information_schema.tiflash_replica").Check(testkit.Rows("test t 2 a,b 0 0"))
	tbl, err := domain.GetDomain(tk.Session()).InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	tbl.Meta().TiFlashReplica.Available = true
	tk.MustQuery("select TABLE_SCHEMA,TABLE_NAME,REPLICA_COUNT,LOCATION_LABELS,AVAILABLE,PROGRESS from information_schema.tiflash_replica").Check(testkit.Rows("test t 2 a,b 1 0"))
}

func TestSetTiFlashReplicaForTemporaryTable(t *testing.T) {
	// test for tiflash replica
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/infoschema/mockTiFlashStoreCount", `return(true)`))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/infoschema/mockTiFlashStoreCount"))
	}()

	store := testkit.CreateMockStoreWithSchemaLease(t, tiflashReplicaLease)
	tk := testkit.NewTestKit(t, store)
	rpcserver, _ := setUpRPCService(t, "127.0.0.1:0", domain.GetDomain(tk.Session()), nil)
	defer rpcserver.Stop()
	tk.MustExec("use test")
	tk.MustExec("create global temporary table temp(id int) on commit delete rows")
	tk.MustExec("create temporary table temp2(id int)")
	tk.MustGetErrCode("alter table temp set tiflash replica 1", errno.ErrOptOnTemporaryTable)
	tk.MustGetErrCode("alter table temp2 set tiflash replica 1", errno.ErrUnsupportedDDLOperation)
	tk.MustExec("drop table temp, temp2")

	tk.MustExec("drop table if exists normal")
	tk.MustExec("create table normal(id int)")
	tk.MustExec("alter table normal set tiflash replica 1")
	tk.MustQuery("select REPLICA_COUNT from information_schema.tiflash_replica where table_schema='test' and table_name='normal'").Check(testkit.Rows("1"))
	tk.MustExec("create global temporary table temp like normal on commit delete rows")
	tk.MustQuery("select REPLICA_COUNT from information_schema.tiflash_replica where table_schema='test' and table_name='temp'").Check(testkit.Rows())
	tk.MustExec("drop table temp")
	tk.MustExec("create temporary table temp like normal")
	tk.MustQuery("select REPLICA_COUNT from information_schema.tiflash_replica where table_schema='test' and table_name='temp'").Check(testkit.Rows())
}

func TestSetTableFlashReplicaForSystemTable(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomainWithSchemaLease(t, tiflashReplicaLease)

	tk := testkit.NewTestKit(t, store)
	sysTables := make([]string, 0, 24)
	memOrSysDB := []string{"MySQL", "INFORMATION_SCHEMA", "PERFORMANCE_SCHEMA", "METRICS_SCHEMA", "SYS"}
	for _, db := range memOrSysDB {
		tk.MustExec("use " + db)
		tk.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil, nil)
		rows := tk.MustQuery("show tables").Rows()
		for i := 0; i < len(rows); i++ {
			sysTables = append(sysTables, rows[i][0].(string))
		}
		for _, one := range sysTables {
			_, err := tk.Exec(fmt.Sprintf("alter table `%s` set tiflash replica 1", one))
			if db == "MySQL" || db == "SYS" {
				tbl, err1 := dom.InfoSchema().TableByName(model.NewCIStr(db), model.NewCIStr(one))
				require.NoError(t, err1)
				if tbl.Meta().View != nil {
					require.ErrorIs(t, err, dbterror.ErrWrongObject)
				} else {
					require.Equal(t, "[ddl:8200]Unsupported ALTER TiFlash settings for system table and memory table", err.Error())
				}
			} else {
				require.Equal(t, fmt.Sprintf("[planner:1142]ALTER command denied to user 'root'@'%%' for table '%s'", strings.ToLower(one)), err.Error())
			}
		}
		sysTables = sysTables[:0]
	}
}

func TestSkipSchemaChecker(t *testing.T) {
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/infoschema/mockTiFlashStoreCount", `return(true)`))
	defer func() {
		err := failpoint.Disable("github.com/pingcap/tidb/pkg/infoschema/mockTiFlashStoreCount")
		require.NoError(t, err)
	}()

	store := testkit.CreateMockStoreWithSchemaLease(t, tiflashReplicaLease)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set global tidb_enable_metadata_lock=0")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1 (a int)")
	tk2 := testkit.NewTestKit(t, store)
	tk2.MustExec("use test")

	// Test skip schema checker for ActionSetTiFlashReplica.
	tk.MustExec("begin")
	tk.MustExec("insert into t1 set a=1;")
	tk2.MustExec("alter table t1 set tiflash replica 2 location labels 'a','b';")
	tk.MustExec("commit")

	// Test skip schema checker for ActionUpdateTiFlashReplicaStatus.
	tk.MustExec("begin")
	tk.MustExec("insert into t1 set a=1;")
	tb := external.GetTableByName(t, tk, "test", "t1")
	err := domain.GetDomain(tk.Session()).DDL().UpdateTableReplicaInfo(tk.Session(), tb.Meta().ID, true)
	require.NoError(t, err)
	tk.MustExec("commit")

	// Test can't skip schema checker.
	tk.MustExec("begin")
	tk.MustExec("insert into t1 set a=1;")
	tk2.MustExec("alter table t1 add column b int;")
	err = tk.ExecToErr("commit")
	require.True(t, terror.ErrorEqual(domain.ErrInfoSchemaChanged, err))
}

// TestCreateTableWithLike2 tests create table with like when refer table have non-public column/index.
func TestCreateTableWithLike2(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomainWithSchemaLease(t, tiflashReplicaLease)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t1 (a int, b int, c int, index idx1(c));")

	tbl1 := external.GetTableByName(t, tk, "test", "t1")
	doneCh := make(chan error, 2)
	hook := &callback.TestDDLCallback{Do: dom}
	var onceChecker sync.Map
	hook.OnJobRunBeforeExported = func(job *model.Job) {
		if job.Type != model.ActionAddColumn && job.Type != model.ActionDropColumn &&
			job.Type != model.ActionAddIndex && job.Type != model.ActionDropIndex {
			return
		}
		if job.TableID != tbl1.Meta().ID {
			return
		}

		if job.SchemaState == model.StateDeleteOnly {
			if _, ok := onceChecker.Load(job.ID); ok {
				return
			}

			onceChecker.Store(job.ID, true)
			go backgroundExec(store, "test", "create table t2 like t1", doneCh)
		}
	}
	originalHook := dom.DDL().GetHook()
	defer dom.DDL().SetHook(originalHook)
	dom.DDL().SetHook(hook)

	// create table when refer table add column
	tk.MustExec("alter table t1 add column d int")
	checkTbl2 := func() {
		err := <-doneCh
		require.NoError(t, err)
		tk.MustExec("alter table t2 add column e int")
		t2Info := external.GetTableByName(t, tk, "test", "t2")
		require.Equal(t, len(t2Info.Cols()), len(t2Info.Meta().Columns))
	}
	checkTbl2()

	// create table when refer table drop column
	tk.MustExec("drop table t2;")
	tk.MustExec("alter table t1 drop column b;")
	checkTbl2()

	// create table when refer table add index
	tk.MustExec("drop table t2;")
	tk.MustExec("alter table t1 add index idx2(a);")
	checkTbl2 = func() {
		err := <-doneCh
		require.NoError(t, err)
		tk.MustExec("alter table t2 add column e int")
		tbl2 := external.GetTableByName(t, tk, "test", "t2")
		require.Equal(t, len(tbl2.Cols()), len(tbl2.Meta().Columns))

		for i := 0; i < len(tbl2.Meta().Indices); i++ {
			require.Equal(t, model.StatePublic, tbl2.Meta().Indices[i].State)
		}
	}
	checkTbl2()

	// create table when refer table drop index.
	tk.MustExec("drop table t2;")
	tk.MustExec("alter table t1 drop index idx2;")
	checkTbl2()

	// Test for table has tiflash  replica.
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/infoschema/mockTiFlashStoreCount", `return(true)`))
	defer func() {
		err := failpoint.Disable("github.com/pingcap/tidb/pkg/infoschema/mockTiFlashStoreCount")
		require.NoError(t, err)
	}()

	dom.DDL().SetHook(originalHook)
	tk.MustExec("drop table if exists t1,t2;")
	tk.MustExec("create table t1 (a int) partition by hash(a) partitions 2;")
	tk.MustExec("alter table t1 set tiflash replica 3 location labels 'a','b';")
	t1 := external.GetTableByName(t, tk, "test", "t1")
	// Mock for all partitions replica was available.
	partition := t1.Meta().Partition
	require.Equal(t, 2, len(partition.Definitions))
	err := domain.GetDomain(tk.Session()).DDL().UpdateTableReplicaInfo(tk.Session(), partition.Definitions[0].ID, true)
	require.NoError(t, err)
	err = domain.GetDomain(tk.Session()).DDL().UpdateTableReplicaInfo(tk.Session(), partition.Definitions[1].ID, true)
	require.NoError(t, err)
	t1 = external.GetTableByName(t, tk, "test", "t1")
	require.NotNil(t, t1.Meta().TiFlashReplica)
	require.True(t, t1.Meta().TiFlashReplica.Available)
	require.Equal(t, []int64{partition.Definitions[0].ID, partition.Definitions[1].ID}, t1.Meta().TiFlashReplica.AvailablePartitionIDs)

	tk.MustExec("create table t2 like t1")
	t2 := external.GetTableByName(t, tk, "test", "t2")
	require.Equal(t, t1.Meta().TiFlashReplica.Count, t2.Meta().TiFlashReplica.Count)
	require.Equal(t, t1.Meta().TiFlashReplica.LocationLabels, t2.Meta().TiFlashReplica.LocationLabels)
	require.False(t, t2.Meta().TiFlashReplica.Available)
	require.Len(t, t2.Meta().TiFlashReplica.AvailablePartitionIDs, 0)
	// Test for not affecting the original table.
	t1 = external.GetTableByName(t, tk, "test", "t1")
	require.NotNil(t, t1.Meta().TiFlashReplica)
	require.True(t, t1.Meta().TiFlashReplica.Available)
	require.Equal(t, []int64{partition.Definitions[0].ID, partition.Definitions[1].ID}, t1.Meta().TiFlashReplica.AvailablePartitionIDs)
}

func TestTruncateTable2(t *testing.T) {
	store := testkit.CreateMockStoreWithSchemaLease(t, tiflashReplicaLease)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table truncate_table (c1 int, c2 int)")
	tk.MustExec("insert truncate_table values (1, 1), (2, 2)")
	is := domain.GetDomain(tk.Session()).InfoSchema()
	oldTblInfo, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("truncate_table"))
	require.NoError(t, err)
	oldTblID := oldTblInfo.Meta().ID

	tk.MustExec("truncate table truncate_table")

	tk.MustExec("insert truncate_table values (3, 3), (4, 4)")
	tk.MustQuery("select * from truncate_table").Check(testkit.Rows("3 3", "4 4"))

	is = domain.GetDomain(tk.Session()).InfoSchema()
	newTblInfo, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("truncate_table"))
	require.NoError(t, err)
	require.Greater(t, newTblInfo.Meta().ID, oldTblID)

	// Verify that the old table data has been deleted by background worker.
	tablePrefix := tablecodec.EncodeTablePrefix(oldTblID)
	hasOldTableData := true
	for i := 0; i < waitForCleanDataRound; i++ {
		ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnDDL)
		err = kv.RunInNewTxn(ctx, store, false, func(ctx context.Context, txn kv.Transaction) error {
			it, err1 := txn.Iter(tablePrefix, nil)
			if err1 != nil {
				return err1
			}
			if !it.Valid() {
				hasOldTableData = false
			} else {
				hasOldTableData = it.Key().HasPrefix(tablePrefix)
			}
			it.Close()
			return nil
		})
		require.NoError(t, err)
		if !hasOldTableData {
			break
		}
		time.Sleep(waitForCleanDataInterval)
	}
	require.False(t, hasOldTableData)

	// Test for truncate table should clear the tiflash available status.
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/infoschema/mockTiFlashStoreCount", `return(true)`))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/infoschema/mockTiFlashStoreCount"))
	}()

	tk.MustExec("drop table if exists t1;")
	tk.MustExec("create table t1 (a int);")
	tk.MustExec("alter table t1 set tiflash replica 3 location labels 'a','b';")
	t1 := external.GetTableByName(t, tk, "test", "t1")
	// Mock for table tiflash replica was available.
	err = domain.GetDomain(tk.Session()).DDL().UpdateTableReplicaInfo(tk.Session(), t1.Meta().ID, true)
	require.NoError(t, err)
	t1 = external.GetTableByName(t, tk, "test", "t1")
	require.NotNil(t, t1.Meta().TiFlashReplica)
	require.True(t, t1.Meta().TiFlashReplica.Available)

	tk.MustExec("truncate table t1")
	t2 := external.GetTableByName(t, tk, "test", "t1")
	require.Equal(t, t1.Meta().TiFlashReplica.Count, t2.Meta().TiFlashReplica.Count)
	require.Equal(t, t1.Meta().TiFlashReplica.LocationLabels, t2.Meta().TiFlashReplica.LocationLabels)
	require.False(t, t2.Meta().TiFlashReplica.Available)
	require.Len(t, t2.Meta().TiFlashReplica.AvailablePartitionIDs, 0)

	// Test for truncate partition should clear the tiflash available status.
	tk.MustExec("drop table if exists t1;")
	tk.MustExec("create table t1 (a int) partition by hash(a) partitions 2;")
	tk.MustExec("alter table t1 set tiflash replica 3 location labels 'a','b';")
	t1 = external.GetTableByName(t, tk, "test", "t1")
	// Mock for all partitions replica was available.
	partition := t1.Meta().Partition
	require.Equal(t, 2, len(partition.Definitions))
	err = domain.GetDomain(tk.Session()).DDL().UpdateTableReplicaInfo(tk.Session(), partition.Definitions[0].ID, true)
	require.NoError(t, err)
	err = domain.GetDomain(tk.Session()).DDL().UpdateTableReplicaInfo(tk.Session(), partition.Definitions[1].ID, true)
	require.NoError(t, err)
	t1 = external.GetTableByName(t, tk, "test", "t1")
	require.NotNil(t, t1.Meta().TiFlashReplica)
	require.True(t, t1.Meta().TiFlashReplica.Available)
	require.Equal(t, []int64{partition.Definitions[0].ID, partition.Definitions[1].ID}, t1.Meta().TiFlashReplica.AvailablePartitionIDs)

	tk.MustExec("alter table t1 truncate partition p0")
	t2 = external.GetTableByName(t, tk, "test", "t1")
	require.Equal(t, t1.Meta().TiFlashReplica.Count, t2.Meta().TiFlashReplica.Count)
	require.Equal(t, t1.Meta().TiFlashReplica.LocationLabels, t2.Meta().TiFlashReplica.LocationLabels)
	require.False(t, t2.Meta().TiFlashReplica.Available)
	require.Equal(t, []int64{partition.Definitions[1].ID}, t2.Meta().TiFlashReplica.AvailablePartitionIDs)
	// Test for truncate twice.
	tk.MustExec("alter table t1 truncate partition p0")
	t2 = external.GetTableByName(t, tk, "test", "t1")
	require.Equal(t, t1.Meta().TiFlashReplica.Count, t2.Meta().TiFlashReplica.Count)
	require.Equal(t, t1.Meta().TiFlashReplica.LocationLabels, t2.Meta().TiFlashReplica.LocationLabels)
	require.False(t, t2.Meta().TiFlashReplica.Available)
	require.Equal(t, []int64{partition.Definitions[1].ID}, t2.Meta().TiFlashReplica.AvailablePartitionIDs)
}
