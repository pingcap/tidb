// Copyright 2026 PingCAP, Inc.
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

	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/config/deploymode"
	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/store/mockstore"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/pingcap/tidb/pkg/util/sqlkiller"
	"github.com/stretchr/testify/require"
)

func TestFTSReplicaOnDemand(t *testing.T) {
	if !kerneltype.IsNextGen() {
		t.Skip("FTS requires Starter")
	}
	previous := deploymode.Get()
	require.NoError(t, deploymode.Set(deploymode.Starter))
	t.Cleanup(func() { require.NoError(t, deploymode.Set(previous)) })
	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/ddl/MockCheckColumnarIndexProcess", "return(1)")
	store, dom := testkit.CreateMockStoreAndDomain(t, mockstore.WithMockTiFlash(2))
	tiflash := infosync.NewMockTiFlash()
	infosync.SetMockTiFlash(tiflash)
	t.Cleanup(func() { tiflash.StatusServer.Close() })
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (id int, body text)")
	tk.MustContainErrMsg("alter table t add fulltext index ft (body)", "columnar replica must exist")
	tk.MustExec("alter table t add fulltext index ft (body) ADD_COLUMNAR_REPLICA_ON_DEMAND")
	tbl, err := dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t"))
	require.NoError(t, err)
	require.Equal(t, uint64(1), tbl.Meta().TiFlashReplica.Count)
	require.True(t, tbl.Meta().Indices[0].IsColumnarIndex())
	tk.MustExec("create table existing (id int, body text)")
	tk.MustExec("alter table existing set tiflash replica 2 location labels 'zone'")
	tk.MustExec("create fulltext index ft on existing (body) ADD_COLUMNAR_REPLICA_ON_DEMAND")
	tbl, err = dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("existing"))
	require.NoError(t, err)
	require.Equal(t, uint64(2), tbl.Meta().TiFlashReplica.Count)
	require.Equal(t, []string{"zone"}, tbl.Meta().TiFlashReplica.LocationLabels)
	restore := config.RestoreFunc()
	t.Cleanup(restore)
	config.UpdateGlobal(func(conf *config.Config) { conf.CSE.ColumnarStoreType = "columnar" })
	tk.MustExec("create table gated (body text)")
	tk.MustExec("set global tidb_columnar_storage_enabled = 'OFF'")
	tk.MustContainErrMsg("alter table gated add fulltext index ft(body) ADD_COLUMNAR_REPLICA_ON_DEMAND", "columnar")
	tbl, err = dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("gated"))
	require.NoError(t, err)
	require.Nil(t, tbl.Meta().TiFlashReplica)
	require.Empty(t, tbl.Meta().Indices)
	tk.MustExec("set global tidb_columnar_storage_enabled = 'ON'")
	tk.MustContainErrMsg("create index ordinary on t(id) ADD_COLUMNAR_REPLICA_ON_DEMAND", "only be used")
}

func TestFTSReplicaOnDemandWaitCancellation(t *testing.T) {
	if !kerneltype.IsNextGen() {
		t.Skip("FTS requires Starter")
	}
	previous := deploymode.Get()
	require.NoError(t, deploymode.Set(deploymode.Starter))
	t.Cleanup(func() { require.NoError(t, deploymode.Set(previous)) })
	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/ddl/MockCheckColumnarIndexProcess", "return(1)")
	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/ddl/MockCheckColumnarReplicaAvailability", "return(0)")
	store, dom := testkit.CreateMockStoreAndDomain(t, mockstore.WithMockTiFlash(1))
	tiflash := infosync.NewMockTiFlash()
	infosync.SetMockTiFlash(tiflash)
	t.Cleanup(func() { tiflash.StatusServer.Close() })
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t(body text)")
	waiting := make(chan struct{}, 1)
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/waitColumnarReplicaAvailable", func() {
		select {
		case waiting <- struct{}{}:
		default:
		}
	})
	done := make(chan error, 1)
	go func() {
		done <- tk.ExecToErr("alter table t add fulltext index ft(body) ADD_COLUMNAR_REPLICA_ON_DEMAND")
	}()
	select {
	case <-waiting:
	case err := <-done:
		t.Fatalf("did not wait: %v", err)
	case <-time.After(30 * time.Second):
		t.Fatal("wait did not start")
	}
	// Availability updates must be free to run after the index DDL has finished.
	tbl, err := dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t"))
	require.NoError(t, err)
	require.Equal(t, "public", tbl.Meta().Indices[0].State.String())
	tk.Session().GetSessionVars().SQLKiller.SendKillSignal(sqlkiller.QueryInterrupted)
	select {
	case err := <-done:
		require.Error(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("replica wait ignored cancellation")
	}
}
