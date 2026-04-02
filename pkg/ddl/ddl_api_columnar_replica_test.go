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

package ddl_test

import (
	"context"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/store/mockstore"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/util/sqlkiller"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

func TestAddIndexWithColReplicaByAlterTable(t *testing.T) {
	testAddIndexWithColReplica(t /* useAlterTable= */, true)
}

func TestAddIndexWithColReplicaByCreateIndex(t *testing.T) {
	testAddIndexWithColReplica(t /* useAlterTable= */, false)
}

func testAddIndexWithColReplica(t *testing.T, useAlterTable bool) {
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/ddl/MockCheckColumnarIndexProcess", `return(1)`))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/ddl/MockCheckColumnarIndexProcess"))
	}()

	// tiflash stores = 0
	store, _ := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustContainErrMsg(`create table t(
		c INT,
		PRIMARY KEY (c) ADD_COLUMNAR_REPLICA_ON_DEMAND
	);`, `ADD_COLUMNAR_REPLICA_ON_DEMAND can be only used in columnar index`)

	tk.MustExec(`create table t(c INT, v VECTOR(3))`)
	if useAlterTable {
		tk.MustContainErrMsg(`alter table t ADD INDEX (c) ADD_COLUMNAR_REPLICA_ON_DEMAND`, `ADD_COLUMNAR_REPLICA_ON_DEMAND can be only used in columnar index`)
	} else {
		tk.MustContainErrMsg(`CREATE INDEX vidx ON t (c) ADD_COLUMNAR_REPLICA_ON_DEMAND`, `ADD_COLUMNAR_REPLICA_ON_DEMAND can be only used in columnar index`)
	}

	replicas, err := infoschema.GetTiFlashStoreCount(tk.Session().GetStore())
	require.NoError(t, err)
	require.Equal(t, uint64(0), replicas)
	tk.MustContainErrMsg(`create table t2(
		v VECTOR(3),
		VECTOR INDEX ((VEC_L2_DISTANCE(v)))
	);`, `columnar store (TiFlash) must be deployed in the cluster in order to use columnar index`)
	if useAlterTable {
		tk.MustContainErrMsg(`alter table t ADD VECTOR INDEX ((VEC_L2_DISTANCE(v))) ADD_COLUMNAR_REPLICA_ON_DEMAND`, `columnar store (TiFlash) must be deployed in the cluster in order to use columnar index`)
	} else {
		tk.MustContainErrMsg(`CREATE VECTOR INDEX vidx ON t ((VEC_L2_DISTANCE(v))) ADD_COLUMNAR_REPLICA_ON_DEMAND`, `columnar store (TiFlash) must be deployed in the cluster in order to use columnar index`)
	}

	// tiflash stores = 2
	store, dom := testkit.CreateMockStoreAndDomainWithSchemaLease(t, tiflashReplicaLease, mockstore.WithMockTiFlash(2))
	tk = testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tiflash := infosync.NewMockTiFlash()
	infosync.SetMockTiFlash(tiflash)
	defer func() {
		tiflash.Lock()
		tiflash.StatusServer.Close()
		tiflash.Unlock()
	}()

	replicas, err = infoschema.GetTiFlashStoreCount(tk.Session().GetStore())
	require.NoError(t, err)
	require.Equal(t, uint64(2), replicas)

	tk.MustExec(`create table t(c INT, v VECTOR(3))`)
	tbl, err := dom.InfoSchema().TableByName(context.Background(), model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	require.Nil(t, tbl.Meta().TiFlashReplica)

	if useAlterTable {
		tk.MustContainErrMsg(`alter table t ADD INDEX (c) ADD_COLUMNAR_REPLICA_ON_DEMAND`, `ADD_COLUMNAR_REPLICA_ON_DEMAND can be only used in columnar index`)
	} else {
		tk.MustContainErrMsg(`CREATE INDEX vidx ON t (c) ADD_COLUMNAR_REPLICA_ON_DEMAND`, `ADD_COLUMNAR_REPLICA_ON_DEMAND can be only used in columnar index`)
	}

	if useAlterTable {
		tk.MustExec(`alter table t ADD VECTOR INDEX ((VEC_L2_DISTANCE(v))) ADD_COLUMNAR_REPLICA_ON_DEMAND`)
	} else {
		tk.MustExec(`CREATE VECTOR INDEX vector_index ON t ((VEC_L2_DISTANCE(v))) ADD_COLUMNAR_REPLICA_ON_DEMAND`)
	}

	// ADD_COLUMNAR_REPLICA_ON_DEMAND should not display in SHOW CREATE TABLE
	tk.MustQuery(`show create table t`).Check(testkit.Rows("t CREATE TABLE `t` (\n  `c` int(11) DEFAULT NULL,\n  `v` vector(3) DEFAULT NULL,\n  VECTOR INDEX `vector_index`((VEC_L2_DISTANCE(`v`)))\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))
	tbl, err = dom.InfoSchema().TableByName(context.Background(), model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	require.Equal(t, uint64(1), tbl.Meta().TiFlashReplica.Count)

	// existing replicas should not be removed
	tk.MustExec(`create table t3(c INT, v VECTOR(3))`)
	tk.MustExec(`alter table t3 set tiflash replica 2`)
	tbl, err = dom.InfoSchema().TableByName(context.Background(), model.NewCIStr("test"), model.NewCIStr("t3"))
	require.NoError(t, err)
	require.Equal(t, uint64(2), tbl.Meta().TiFlashReplica.Count)
	if useAlterTable {
		tk.MustExec(`alter table t3 add vector index ((VEC_L2_DISTANCE(v))) ADD_COLUMNAR_REPLICA_ON_DEMAND`)
	} else {
		tk.MustExec(`CREATE VECTOR INDEX vector_index ON t3 ((VEC_L2_DISTANCE(v))) ADD_COLUMNAR_REPLICA_ON_DEMAND`)
	}
	tbl, err = dom.InfoSchema().TableByName(context.Background(), model.NewCIStr("test"), model.NewCIStr("t3"))
	require.NoError(t, err)
	require.Equal(t, uint64(2), tbl.Meta().TiFlashReplica.Count)
}

func TestAddIndexWithColReplicaWaitByAlterTable(t *testing.T) {
	testAddIndexWithColReplicaWait(t /* useAlterTable= */, true)
}

func TestAddIndexWithColReplicaWaitByCreateIndex(t *testing.T) {
	testAddIndexWithColReplicaWait(t /* useAlterTable= */, false)
}

func testAddIndexWithColReplicaWait(t *testing.T, useAlterTable bool) {
	// This tests what happens when some special DDL happens during ADD_COLUMNAR_REPLICA_ON_DEMAND.

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/ddl/MockCheckColumnarIndexProcess", `return(1)`))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/ddl/MockCheckColumnarIndexProcess"))
	}()

	store, _ := testkit.CreateMockStoreAndDomainWithSchemaLease(t, tiflashReplicaLease, mockstore.WithMockTiFlash(2))
	tk := testkit.NewTestKit(t, store)
	tk2 := testkit.NewTestKit(t, store)
	tk3 := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk2.MustExec("use test")
	tk3.MustExec("use test")

	tiflash := infosync.NewMockTiFlash()
	infosync.SetMockTiFlash(tiflash)
	defer func() {
		tiflash.Lock()
		tiflash.StatusServer.Close()
		tiflash.Unlock()
	}()

	replicas, err := infoschema.GetTiFlashStoreCount(tk.Session().GetStore())
	require.NoError(t, err)
	require.Equal(t, uint64(2), replicas)

	// Keep column replica as not available
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/ddl/MockCheckColumnarReplicaAvailability", `return(0)`))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/ddl/MockCheckColumnarReplicaAvailability"))
	}()

	defer func() {
		ddl.TestBarrierCheckColumnarReplicaAvailability = nil
	}()

	// Test: table is dropped while waiting replica (Should fail)
	ddl.TestBarrierCheckColumnarReplicaAvailability = make(chan int, 1)
	tk.MustExec(`create table t(c INT, v VECTOR(3))`)
	g := errgroup.Group{}
	g.Go(func() error {
		if useAlterTable {
			tk2.MustContainErrMsg(`alter table t ADD VECTOR INDEX ((VEC_L2_DISTANCE(v))) ADD_COLUMNAR_REPLICA_ON_DEMAND`, `Table 'test.t' not found`)
		} else {
			tk2.MustContainErrMsg(`CREATE VECTOR INDEX vector_index ON t ((VEC_L2_DISTANCE(v))) ADD_COLUMNAR_REPLICA_ON_DEMAND`, `Table 'test.t' not found`)
		}
		return nil
	})
	<-ddl.TestBarrierCheckColumnarReplicaAvailability // Hold until ADD VECTOR INDEX is really waiting
	tk.MustExec(`drop table t`)
	g.Wait()

	// Test: table is renamed while waiting replica (Should success)
	ddl.TestBarrierCheckColumnarReplicaAvailability = make(chan int, 1)
	tk.MustExec(`create table t(c INT, v VECTOR(3))`)
	g = errgroup.Group{}
	g.Go(func() error {
		if useAlterTable {
			tk2.MustExec(`alter table t ADD VECTOR INDEX ((VEC_L2_DISTANCE(v))) ADD_COLUMNAR_REPLICA_ON_DEMAND`)
		} else {
			tk2.MustExec(`CREATE VECTOR INDEX vector_index ON t ((VEC_L2_DISTANCE(v))) ADD_COLUMNAR_REPLICA_ON_DEMAND`)
		}
		return nil
	})
	<-ddl.TestBarrierCheckColumnarReplicaAvailability // Hold until ADD VECTOR INDEX is really waiting
	tk.MustExec(`alter table t rename to t2`)
	// Continue the ADD VECTOR INDEX
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/ddl/MockCheckColumnarReplicaAvailability", `return(1)`))
	g.Wait()
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/ddl/MockCheckColumnarReplicaAvailability", `return(0)`))
	tk.MustExec(`drop table t2`)

	// Test: kill statement while waiting replica (Should fail)
	ddl.TestBarrierCheckColumnarReplicaAvailability = make(chan int, 1)
	tk.MustExec(`create table t(c INT, v VECTOR(3))`)
	g = errgroup.Group{}
	g.Go(func() error {
		if useAlterTable {
			tk2.MustContainErrMsg(`alter table t ADD VECTOR INDEX ((VEC_L2_DISTANCE(v))) ADD_COLUMNAR_REPLICA_ON_DEMAND`, `DDL job is cancelled`)
		} else {
			tk2.MustContainErrMsg(`CREATE VECTOR INDEX vector_index ON t ((VEC_L2_DISTANCE(v))) ADD_COLUMNAR_REPLICA_ON_DEMAND`, `DDL job is cancelled`)
		}
		return nil
	})
	<-ddl.TestBarrierCheckColumnarReplicaAvailability // Hold until ADD VECTOR INDEX is really waiting
	time.Sleep(time.Millisecond * 3000)
	sessVars := tk2.Session().GetSessionVars()
	sessVars.SQLKiller.SendKillSignal(sqlkiller.QueryInterrupted)
	g.Wait()
	tk.MustExec(`drop table t`)

	// Test: table replica is set back to 0 while waiting replica (Should fail)
	ddl.TestBarrierCheckColumnarReplicaAvailability = make(chan int, 1)
	tk.MustExec(`create table t(c INT, v VECTOR(3))`)
	g = errgroup.Group{}
	g.Go(func() error {
		if useAlterTable {
			tk3.MustContainErrMsg(`alter table t ADD VECTOR INDEX ((VEC_L2_DISTANCE(v))) ADD_COLUMNAR_REPLICA_ON_DEMAND`, `Columnar replica is not configured for table 'test.t'`)
		} else {
			tk3.MustContainErrMsg(`CREATE VECTOR INDEX vector_index ON t ((VEC_L2_DISTANCE(v))) ADD_COLUMNAR_REPLICA_ON_DEMAND`, `Columnar replica is not configured for table 'test.t'`)
		}
		return nil
	})
	<-ddl.TestBarrierCheckColumnarReplicaAvailability // Hold until ADD VECTOR INDEX is really waiting
	tk.MustExec(`alter table t set tiflash replica 0`)
	g.Wait()
	tk.MustExec(`drop table t`)
}

func TestCreateTableWithColReplica(t *testing.T) {
	// Columnar replica should be added implicitly when columnar index is used.

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/ddl/MockCheckColumnarReplicaAvailability", `return(1)`))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/ddl/MockCheckColumnarReplicaAvailability"))
	}()

	// tiflash stores = 0
	store, _ := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	replicas, err := infoschema.GetTiFlashStoreCount(tk.Session().GetStore())
	require.NoError(t, err)
	require.Equal(t, uint64(0), replicas)

	tk.MustContainErrMsg(`create table t(
		c INT,
		v VECTOR(3),
		VECTOR INDEX ((VEC_L2_DISTANCE(v)))
	);`, `columnar store (TiFlash) must be deployed in the cluster in order to use columnar index`)

	// tiflash stores = 2
	store, dom := testkit.CreateMockStoreAndDomainWithSchemaLease(t, tiflashReplicaLease, mockstore.WithMockTiFlash(2))
	tk = testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tiflash := infosync.NewMockTiFlash()
	infosync.SetMockTiFlash(tiflash)
	defer func() {
		tiflash.Lock()
		tiflash.StatusServer.Close()
		tiflash.Unlock()
	}()

	replicas, err = infoschema.GetTiFlashStoreCount(tk.Session().GetStore())
	require.NoError(t, err)
	require.Equal(t, uint64(2), replicas)

	tk.MustExec(`create table t(
		c INT,
		v VECTOR(3),
		VECTOR INDEX ((VEC_L2_DISTANCE(v)))
	);`)
	tbl, err := dom.InfoSchema().TableByName(context.Background(), model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	require.NotNil(t, tbl.Meta().TiFlashReplica)
	require.Equal(t, uint64(1), tbl.Meta().TiFlashReplica.Count)
	tk.MustExec(`drop table t`)

	// // tiflash stores = 2, minReplica = 2
	// minCount := config.GetGlobalConfig().TiFlashReplicas.MinCount
	// config.GetGlobalConfig().TiFlashReplicas.MinCount = 2
	// defer func() {
	// 	config.GetGlobalConfig().TiFlashReplicas.MinCount = minCount
	// }()

	// tk.MustExec(`create table t(
	// 	c INT,
	// 	v VECTOR(3),
	// 	VECTOR INDEX ((VEC_L2_DISTANCE(v)))
	// );`)
	// tbl, err = dom.InfoSchema().TableByName(context.Background(), model.NewCIStr("test"), model.NewCIStr("t"))
	// require.NoError(t, err)
	// require.NotNil(t, tbl.Meta().TiFlashReplica)
	// require.Equal(t, uint64(2), tbl.Meta().TiFlashReplica.Count)
	// tk.MustExec(`drop table t`)
}

func TestCreateTableWithColReplicaWait(t *testing.T) {
	// This tests what happens when some special DDL happens when creating a table with columnar replica.

	store, _ := testkit.CreateMockStoreAndDomainWithSchemaLease(t, tiflashReplicaLease, mockstore.WithMockTiFlash(2))
	tk := testkit.NewTestKit(t, store)
	tk2 := testkit.NewTestKit(t, store)
	tk3 := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk2.MustExec("use test")
	tk3.MustExec("use test")

	tiflash := infosync.NewMockTiFlash()
	infosync.SetMockTiFlash(tiflash)
	defer func() {
		tiflash.Lock()
		tiflash.StatusServer.Close()
		tiflash.Unlock()
	}()

	replicas, err := infoschema.GetTiFlashStoreCount(tk.Session().GetStore())
	require.NoError(t, err)
	require.Equal(t, uint64(2), replicas)

	// Keep column replica as not available
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/ddl/MockCheckColumnarReplicaAvailability", `return(0)`))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/ddl/MockCheckColumnarReplicaAvailability"))
	}()

	defer func() {
		ddl.TestBarrierCheckColumnarReplicaAvailability = nil
	}()

	// Test: table is dropped while waiting replica (Should fail)
	ddl.TestBarrierCheckColumnarReplicaAvailability = make(chan int, 1)
	g := errgroup.Group{}
	g.Go(func() error {
		tk2.MustContainErrMsg(`create table t(
			v VECTOR(3),
			VECTOR INDEX ((VEC_L2_DISTANCE(v)))
		)`, `Table 'test.t' not found`)
		return nil
	})
	<-ddl.TestBarrierCheckColumnarReplicaAvailability
	tk.MustExec(`drop table t`)
	g.Wait()

	// Test: table is renamed while waiting replica (Should success)
	ddl.TestBarrierCheckColumnarReplicaAvailability = make(chan int, 1)
	g = errgroup.Group{}
	g.Go(func() error {
		tk2.MustExec(`create table t(
			v VECTOR(3),
			VECTOR INDEX ((VEC_L2_DISTANCE(v)))
		)`)
		return nil
	})
	<-ddl.TestBarrierCheckColumnarReplicaAvailability
	tk.MustExec(`alter table t rename to t2`)
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/ddl/MockCheckColumnarReplicaAvailability", `return(1)`))
	g.Wait()
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/ddl/MockCheckColumnarReplicaAvailability", `return(0)`))
	tk.MustExec(`drop table t2`)

	// Test: kill statement while waiting replica (Should fail)
	ddl.TestBarrierCheckColumnarReplicaAvailability = make(chan int, 1)
	g = errgroup.Group{}
	g.Go(func() error {
		tk2.MustContainErrMsg(`create table t(
			v VECTOR(3),
			VECTOR INDEX ((VEC_L2_DISTANCE(v)))
		)`, `DDL job is cancelled`)
		return nil
	})
	<-ddl.TestBarrierCheckColumnarReplicaAvailability
	tk2.Session().GetSessionVars().SQLKiller.SendKillSignal(sqlkiller.QueryInterrupted)
	g.Wait()
	tk.MustExec(`drop table t`)

	// Test: table replica is set back to 0 while waiting replica (Should fail)
	ddl.TestBarrierCheckColumnarReplicaAvailability = make(chan int, 1)
	g = errgroup.Group{}
	g.Go(func() error {
		tk3.MustContainErrMsg(`create table t(
			v VECTOR(3),
			VECTOR INDEX ((VEC_L2_DISTANCE(v)))
		)`, `Columnar replica is not configured for table 'test.t'`)
		return nil
	})
	<-ddl.TestBarrierCheckColumnarReplicaAvailability
	tk.MustExec(`alter table t set tiflash replica 0`)
	g.Wait()
	tk.MustExec(`drop table t`)
}
