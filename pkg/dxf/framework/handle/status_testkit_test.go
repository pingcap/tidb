// Copyright 2025 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package handle_test

import (
	"cmp"
	"context"
	"slices"
	"testing"
	"time"

	"github.com/ngaut/pools"
	"github.com/pingcap/tidb/pkg/dxf/framework/handle"
	"github.com/pingcap/tidb/pkg/dxf/framework/proto"
	"github.com/pingcap/tidb/pkg/dxf/framework/schstatus"
	"github.com/pingcap/tidb/pkg/dxf/framework/storage"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/util"
)

func TestGetScheduleStatus(t *testing.T) {
	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/util/cpu/mockNumCpu", "return(16)")
	store := testkit.CreateMockStore(t)
	gtk := testkit.NewTestKit(t, store)
	pool := pools.NewResourcePool(func() (pools.Resource, error) {
		return gtk.Session(), nil
	}, 1, 1, time.Second)
	defer pool.Close()
	storage.SetTaskManager(storage.NewTaskManager(pool))
	ctx := context.Background()
	status, err := handle.GetScheduleStatus(ctx)
	require.NoError(t, err)
	require.Equal(t, &schstatus.Status{
		Version:    schstatus.Version1,
		TaskQueue:  schstatus.TaskQueue{},
		TiDBWorker: schstatus.NodeGroup{CPUCount: 16, RequiredCount: 1, CurrentCount: 1, BusyNodes: []schstatus.Node{{ID: ":4000", IsOwner: true}}},
		TiKVWorker: schstatus.NodeGroup{RequiredCount: 1},
		Flags:      make(map[schstatus.Flag]schstatus.TTLFlag),
	}, status)
}

func TestNodeInfoAndBusyNodes(t *testing.T) {
	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/domain/MockDisableDistTask", "return(true)")
	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/util/cpu/mockNumCpu", "return(16)")
	store := testkit.CreateMockStore(t)
	gtk := testkit.NewTestKit(t, store)
	pool := pools.NewResourcePool(func() (pools.Resource, error) {
		return gtk.Session(), nil
	}, 1, 1, time.Second)
	defer pool.Close()
	ctx := util.WithInternalSourceType(context.Background(), "handle_test")
	taskMgr := storage.NewTaskManager(pool)
	nodeCount, cpuCount, err := handle.GetNodesInfo(ctx, taskMgr)
	require.NoError(t, err)
	require.Zero(t, nodeCount)
	require.Equal(t, 16, cpuCount)
	// add one node
	require.NoError(t, taskMgr.InitMeta(ctx, "node1", ""))
	nodeCount, cpuCount, err = handle.GetNodesInfo(ctx, taskMgr)
	require.NoError(t, err)
	require.Equal(t, 1, nodeCount)
	require.Equal(t, 16, cpuCount)

	// busy nodes include owner node
	busyNodes, err := handle.GetBusyNodes(ctx, taskMgr)
	require.NoError(t, err)
	require.EqualValues(t, []schstatus.Node{{ID: ":4000", IsOwner: true}}, busyNodes)
	// busy nodes include node with subtasks
	require.NoError(t, taskMgr.WithNewSession(func(se sessionctx.Context) error {
		_, err2 := sqlexec.ExecSQL(ctx, se.GetSQLExecutor(), `
			insert into mysql.tidb_background_subtask(`+storage.InsertSubtaskColumns+`) values`+
			`(%?, %?, %?, %?, %?, %?, %?, NULL, CURRENT_TIMESTAMP(), '{}', '{}')`,
			1, "1", ":4000", "{}", proto.SubtaskStatePending, 1, 1)
		require.NoError(t, err2)
		_, err2 = sqlexec.ExecSQL(ctx, se.GetSQLExecutor(), `
			insert into mysql.tidb_background_subtask(`+storage.InsertSubtaskColumns+`) values`+
			`(%?, %?, %?, %?, %?, %?, %?, NULL, CURRENT_TIMESTAMP(), '{}', '{}')`,
			1, "1", ":4001", "{}", proto.SubtaskStateRunning, 1, 1)
		require.NoError(t, err2)
		return nil
	}))
	busyNodes, err = handle.GetBusyNodes(ctx, taskMgr)
	require.NoError(t, err)
	slices.SortFunc(busyNodes, func(a, b schstatus.Node) int {
		return cmp.Compare(a.ID, b.ID)
	})
	require.EqualValues(t, []schstatus.Node{{ID: ":4000", IsOwner: true}, {ID: ":4001"}}, busyNodes)
}

func TestScheduleFlag(t *testing.T) {
	testkit.CreateMockStore(t)
	ctx := context.Background()
	taskMgr, err := storage.GetTaskManager()
	require.NoError(t, err)
	// not set
	flags, err := handle.GetScheduleFlags(ctx, taskMgr)
	require.NoError(t, err)
	require.Empty(t, flags)
	// enabled flag
	// in CI, the stored and unmarshalled time might have different time zone,
	// so unify to UTC.
	expireTime := time.Unix(time.Now().Add(time.Hour).Unix(), 0).In(time.UTC)
	flag := schstatus.TTLFlag{Enabled: true, TTLInfo: schstatus.TTLInfo{TTL: time.Hour, ExpireTime: expireTime}}
	require.NoError(t, handle.UpdatePauseScaleInFlag(ctx, &flag))
	flags, err = handle.GetScheduleFlags(ctx, taskMgr)
	require.NoError(t, err)
	require.Len(t, flags, 1)
	gotFlag := flags[schstatus.PauseScaleInFlag]
	gotFlag.ExpireTime = gotFlag.ExpireTime.In(time.UTC)
	require.EqualValues(t, flag, gotFlag)
	// expired flag
	flag.ExpireTime = time.Now().Add(-time.Hour)
	require.NoError(t, handle.UpdatePauseScaleInFlag(ctx, &flag))
	flags, err = handle.GetScheduleFlags(ctx, taskMgr)
	require.NoError(t, err)
	require.Empty(t, flags)
	// disabled flag
	flag = schstatus.TTLFlag{Enabled: false}
	require.NoError(t, handle.UpdatePauseScaleInFlag(ctx, &flag))
	flags, err = handle.GetScheduleFlags(ctx, taskMgr)
	require.NoError(t, err)
	require.Empty(t, flags)
}

func TestGetScheduleTuneFactors(t *testing.T) {
	store := testkit.CreateMockStore(t)
	ctx := context.Background()
	// factors not set
	factors, err := handle.GetScheduleTuneFactors(ctx, store.GetKeyspace())
	require.NoError(t, err)
	require.EqualValues(t, schstatus.GetDefaultTuneFactors(), factors)
	// non expired factors
	ctx2 := util.WithInternalSourceType(ctx, kv.InternalDistTask)
	// in CI, the stored and unmarshalled time might have different time zone,
	// so unify to UTC.
	expireTime := time.Unix(time.Now().Add(time.Hour).Unix(), 0).In(time.UTC)
	ttlFactors := schstatus.TTLTuneFactors{
		TTLInfo:     schstatus.TTLInfo{TTL: time.Hour, ExpireTime: expireTime},
		TuneFactors: schstatus.TuneFactors{AmplifyFactor: 1.5},
	}
	require.NoError(t, kv.RunInNewTxn(ctx2, store, true, func(ctx context.Context, txn kv.Transaction) error {
		m := meta.NewMutator(txn)
		return m.SetDXFScheduleTuneFactors(store.GetKeyspace(), &ttlFactors)
	}))
	factors, err = handle.GetScheduleTuneFactors(ctx, store.GetKeyspace())
	require.NoError(t, err)
	require.EqualValues(t, &ttlFactors.TuneFactors, factors)
	// expired factors
	ttlFactors.ExpireTime = time.Now().Add(-time.Hour)
	require.NoError(t, kv.RunInNewTxn(ctx2, store, true, func(ctx context.Context, txn kv.Transaction) error {
		m := meta.NewMutator(txn)
		return m.SetDXFScheduleTuneFactors(store.GetKeyspace(), &ttlFactors)
	}))
	factors, err = handle.GetScheduleTuneFactors(ctx, store.GetKeyspace())
	require.NoError(t, err)
	require.EqualValues(t, schstatus.GetDefaultTuneFactors(), factors)
}
