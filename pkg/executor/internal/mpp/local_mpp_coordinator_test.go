// Copyright 2023 PingCAP, Inc.
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

package mpp

import (
	"testing"

	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tipb/go-tipb"
	"github.com/stretchr/testify/require"
)

func TestNeedReportExecutionSummary(t *testing.T) {
	tableScan := &plannercore.PhysicalTableScan{}
	limit := &plannercore.PhysicalLimit{}
	passSender := &plannercore.PhysicalExchangeSender{
		ExchangeType: tipb.ExchangeType_PassThrough,
	}
	passSender.SetID(10)
	tableReader := &plannercore.PhysicalTableReader{}
	tableReader.SetTablePlanForTest(passSender)
	limitTIDB := &plannercore.PhysicalLimit{}
	limitTIDB.SetChildren(tableReader)
	passSender.SetChildren(limit)
	limit.SetChildren(tableScan)

	require.True(t, needReportExecutionSummary(limitTIDB, 10, false))
	require.False(t, needReportExecutionSummary(limitTIDB, 11, false))

	projection := &plannercore.PhysicalProjection{}
	projection.SetChildren(tableReader)
	require.False(t, needReportExecutionSummary(projection, 10, false))

	join := &plannercore.PhysicalHashJoin{}
	tableScan2 := &plannercore.PhysicalTableScan{}
	tableScan2.SetID(20)
	tableReader2 := &plannercore.PhysicalTableReader{}
	tableReader2.SetTablePlanForTest(tableScan2)
	join.SetChildren(tableReader2, projection)
	limitTIDB2 := &plannercore.PhysicalLimit{}
	limitTIDB2.SetChildren(join)
	require.True(t, needReportExecutionSummary(limitTIDB2, 10, false))
}

func mockTaskZoneInfoHelper(isRoot bool, taskZone string, tidbZone string, storeZoneMpp map[string]string, exchangeZoneInfo map[string][]string) taskZoneInfoHelper {
	helper := taskZoneInfoHelper{
		tidbZone:           tidbZone,
		currentTaskZone:    taskZone,
		isRoot:             isRoot,
		allTiFlashZoneInfo: storeZoneMpp,
		exchangeZoneInfo:   exchangeZoneInfo,
	}
	return helper
}

func TestZoneHelperTryQuickFill(t *testing.T) {
	slots := 3
	allTiflashZoneInfo := make(map[string]string, slots)
	exchangeZoneInfo := make(map[string][]string, 2)
	helper := mockTaskZoneInfoHelper(false, "", "east", allTiflashZoneInfo, exchangeZoneInfo)
	exchangeSenderID := "ExchangeSender_1"
	sender := &tipb.Executor{
		ExecutorId: &exchangeSenderID,
		Tp:         tipb.ExecType_TypeExchangeSender,
		ExchangeSender: &tipb.ExchangeSender{
			UpstreamCteTaskMeta: nil,
		},
	}
	sameZoneFlags := make([]bool, 0, slots)
	quickFill := false
	// When task zone is empty, then the function returns true, and all sameZoneFlags are true
	quickFill, sameZoneFlags = helper.tryQuickFillWithUncertainZones(sender, slots, sameZoneFlags)
	require.True(t, quickFill)
	require.Equal(t, slots, len(sameZoneFlags))
	for i := 0; i < slots; i++ {
		require.True(t, sameZoneFlags[i])
	}

	// When task is root task, and executor is exchangeSender then the function compares tidbZone with currentTaskZone
	helper.isRoot = true
	helper.currentTaskZone = "west"
	slots = 1
	sameZoneFlags = make([]bool, 0, slots)
	quickFill, sameZoneFlags = helper.tryQuickFillWithUncertainZones(sender, slots, sameZoneFlags)
	require.True(t, quickFill)
	require.Equal(t, slots, len(sameZoneFlags))
	for i := 0; i < slots; i++ {
		require.False(t, sameZoneFlags[i])
	}

	helper.currentTaskZone = "east"
	sameZoneFlags = make([]bool, 0, slots)
	quickFill, sameZoneFlags = helper.tryQuickFillWithUncertainZones(sender, slots, sameZoneFlags)
	require.True(t, quickFill)
	require.Equal(t, slots, len(sameZoneFlags))
	for i := 0; i < slots; i++ {
		require.True(t, sameZoneFlags[i])
	}

	// When task is neither root exchange sender nor current task zone is empty, return false, and empty sameZoneFlags
	helper.isRoot = false
	helper.currentTaskZone = "west"
	slots = 3
	sameZoneFlags = make([]bool, 0, slots)
	quickFill, sameZoneFlags = helper.tryQuickFillWithUncertainZones(sender, slots, sameZoneFlags)
	require.False(t, quickFill)
	require.Equal(t, 0, len(sameZoneFlags))

	helper.isRoot = true
	helper.currentTaskZone = "west"
	slots = 3
	sameZoneFlags = make([]bool, 0, slots)
	exchangeReceiverID := "ExchangeReceiver_2"
	receiver := &tipb.Executor{
		ExecutorId: &exchangeReceiverID,
		Tp:         tipb.ExecType_TypeExchangeReceiver,
		ExchangeReceiver: &tipb.ExchangeReceiver{
			OriginalCtePrdocuerTaskMeta: nil,
		},
	}
	quickFill, sameZoneFlags = helper.tryQuickFillWithUncertainZones(receiver, slots, sameZoneFlags)
	require.False(t, quickFill)
	require.Equal(t, 0, len(sameZoneFlags))
}
