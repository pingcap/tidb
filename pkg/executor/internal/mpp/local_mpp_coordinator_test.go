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

	passSender.SetChildren(limit)
	limit.SetChildren(tableScan)
	require.True(t, needReportExecutionSummary(passSender))

	passSender.SetChildren(tableScan)
	require.False(t, needReportExecutionSummary(passSender))
}
