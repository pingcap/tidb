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

package kv

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMPPBuildTasksRequestToStringTiCIUnambiguous(t *testing.T) {
	ranges := []KeyRange{
		{StartKey: Key("a"), EndKey: Key("b")},
	}

	// Without explicit field prefixes, the following pair is ambiguous:
	//   executor_id="..._1", table_id=23
	//   executor_id="..._12", table_id=3
	reqA := &MPPBuildTasksRequest{
		TiCI:           true,
		TiCIIndexID:    100,
		TiCIExecutorID: "IndexRangeScan_1",
		TiCITableID:    23,
		TiCIKeyRanges:  ranges,
	}
	reqB := &MPPBuildTasksRequest{
		TiCI:           true,
		TiCIIndexID:    100,
		TiCIExecutorID: "IndexRangeScan_12",
		TiCITableID:    3,
		TiCIKeyRanges:  ranges,
	}

	require.NotEqual(t, reqA.ToString(), reqB.ToString())
}
