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

package storage_test

import (
	"testing"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/pingcap/tidb/pkg/disttask/framework/testutil"
	"github.com/stretchr/testify/require"
)

func TestDistFrameworkMeta(t *testing.T) {
	_, sm, ctx := testutil.InitTableTest(t)

	// when no node
	_, err := sm.GetCPUCountOfManagedNode(ctx)
	require.ErrorContains(t, err, "no managed nodes")
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/util/cpu/mockNumCpu", "return(0)"))
	require.NoError(t, sm.InitMeta(ctx, ":4000", "background"))
	_, err = sm.GetCPUCountOfManagedNode(ctx)
	require.ErrorContains(t, err, "no managed node have enough resource")

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/util/cpu/mockNumCpu", "return(100)"))
	require.NoError(t, sm.InitMeta(ctx, ":4000", "background"))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/util/cpu/mockNumCpu", "return(8)"))
	require.NoError(t, sm.InitMeta(ctx, ":4001", ""))
	require.NoError(t, sm.InitMeta(ctx, ":4002", "background"))
	nodes, err := sm.GetAllNodes(ctx)
	require.NoError(t, err)
	require.Equal(t, []proto.ManagedNode{
		{ID: ":4000", Role: "background", CPUCount: 100},
		{ID: ":4001", Role: "", CPUCount: 8},
		{ID: ":4002", Role: "background", CPUCount: 8},
	}, nodes)

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/util/cpu/mockNumCpu", "return(100)"))
	require.NoError(t, sm.InitMeta(ctx, ":4002", ""))
	require.NoError(t, sm.InitMeta(ctx, ":4003", "background"))

	nodes, err = sm.GetAllNodes(ctx)
	require.NoError(t, err)
	require.Equal(t, []proto.ManagedNode{
		{ID: ":4000", Role: "background", CPUCount: 100},
		{ID: ":4001", Role: "", CPUCount: 8},
		{ID: ":4002", Role: "", CPUCount: 100},
		{ID: ":4003", Role: "background", CPUCount: 100},
	}, nodes)
	cpuCount, err := sm.GetCPUCountOfManagedNode(ctx)
	require.NoError(t, err)
	require.Equal(t, 100, cpuCount)

	require.NoError(t, sm.InitMeta(ctx, ":4002", "background"))

	require.NoError(t, sm.MarkDeadNodes(ctx, []string{":4000"}))
	nodes, err = sm.GetManagedNodes(ctx)
	require.NoError(t, err)
	require.Equal(t, []proto.ManagedNode{
		{ID: ":4002", Role: "background", CPUCount: 100},
		{ID: ":4003", Role: "background", CPUCount: 100},
	}, nodes)

	require.NoError(t, sm.MarkDeadNodes(ctx, []string{":4003"}))
	nodes, err = sm.GetManagedNodes(ctx)
	require.NoError(t, err)
	require.Equal(t, []proto.ManagedNode{
		{ID: ":4002", Role: "background", CPUCount: 100},
	}, nodes)

	require.NoError(t, sm.MarkDeadNodes(ctx, []string{":4002"}))
	nodes, err = sm.GetManagedNodes(ctx)
	require.NoError(t, err)
	require.Equal(t, []proto.ManagedNode{}, nodes)
	_, err = sm.GetCPUCountOfManagedNode(ctx)
	require.Error(t, err)

	require.NoError(t, sm.RecoverMeta(ctx, ":4002", "background"))
	nodes, err = sm.GetManagedNodes(ctx)
	require.NoError(t, err)
	require.Equal(t, []proto.ManagedNode{
		{ID: ":4002", Role: "background", CPUCount: 100},
	}, nodes)
	// should not reset role
	require.NoError(t, sm.RecoverMeta(ctx, ":4002", ""))
	nodes, err = sm.GetManagedNodes(ctx)
	require.NoError(t, err)
	require.Equal(t, []proto.ManagedNode{
		{ID: ":4002", Role: "background", CPUCount: 100},
	}, nodes)
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/util/cpu/mockNumCpu"))
}

func TestAllNormalNodesForDistFramework(t *testing.T) {
	_, sm, ctx := testutil.InitTableTest(t)

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/util/cpu/mockNumCpu", "return(0)"))
	require.NoError(t, sm.InitMeta(ctx, ":4000", ""))
	_, err := sm.GetCPUCountOfManagedNode(ctx)
	require.ErrorContains(t, err, "no managed node have enough resource")

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/util/cpu/mockNumCpu", "return(100)"))
	require.NoError(t, sm.InitMeta(ctx, ":4000", ""))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/util/cpu/mockNumCpu", "return(8)"))
	require.NoError(t, sm.InitMeta(ctx, ":4001", ""))
	require.NoError(t, sm.InitMeta(ctx, ":4002", ""))
	nodes, err := sm.GetAllNodes(ctx)
	require.NoError(t, err)
	require.Equal(t, []proto.ManagedNode{
		{ID: ":4000", Role: "", CPUCount: 100},
		{ID: ":4001", Role: "", CPUCount: 8},
		{ID: ":4002", Role: "", CPUCount: 8},
	}, nodes)

	cpuCount, err := sm.GetCPUCountOfManagedNode(ctx)
	require.NoError(t, err)
	require.Equal(t, 100, cpuCount)

	require.NoError(t, sm.MarkDeadNodes(ctx, []string{":4000"}))
	nodes, err = sm.GetAllNodes(ctx)
	require.NoError(t, err)
	require.Equal(t, []proto.ManagedNode{
		{ID: ":4000", Role: "", CPUCount: 100, Dead: true},
		{ID: ":4001", Role: "", CPUCount: 8},
		{ID: ":4002", Role: "", CPUCount: 8},
	}, nodes)
	nodes, err = sm.GetManagedNodes(ctx)
	require.NoError(t, err)
	require.Equal(t, []proto.ManagedNode{
		{ID: ":4001", Role: "", CPUCount: 8},
		{ID: ":4002", Role: "", CPUCount: 8},
	}, nodes)

	require.NoError(t, sm.RecoverMeta(ctx, ":4000", "background"))
	nodes, err = sm.GetManagedNodes(ctx)
	require.NoError(t, err)
	require.Equal(t, []proto.ManagedNode{
		{ID: ":4000", Role: "", CPUCount: 8},
		{ID: ":4001", Role: "", CPUCount: 8},
		{ID: ":4002", Role: "", CPUCount: 8},
	}, nodes)

	require.NoError(t, sm.InitMeta(ctx, ":4000", "background"))
	nodes, err = sm.GetManagedNodes(ctx)
	require.NoError(t, err)
	require.Equal(t, []proto.ManagedNode{
		{ID: ":4000", Role: "background", CPUCount: 8},
	}, nodes)

	require.NoError(t, sm.MarkDeadNodes(ctx, []string{":4000"}))
	nodes, err = sm.GetAllNodes(ctx)
	require.NoError(t, err)
	require.Equal(t, []proto.ManagedNode{
		{ID: ":4000", Role: "background", CPUCount: 8, Dead: true},
		{ID: ":4001", Role: "", CPUCount: 8},
		{ID: ":4002", Role: "", CPUCount: 8},
	}, nodes)
	nodes, err = sm.GetManagedNodes(ctx)
	require.NoError(t, err)
	require.Equal(t, []proto.ManagedNode{}, nodes)
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/util/cpu/mockNumCpu"))
}
