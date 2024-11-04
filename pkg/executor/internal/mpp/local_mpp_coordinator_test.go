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
