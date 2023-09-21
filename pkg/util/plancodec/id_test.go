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

package plancodec

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPlanIDChanged(t *testing.T) {
	// Attention: for compatibility, shouldn't modify the below test, you can only add test when add new plan ID.
	testCases := []struct {
		Value    int
		Expected int
	}{
		{typeSelID, 1},
		{typeSetID, 2},
		{typeProjID, 3},
		{typeAggID, 4},
		{typeStreamAggID, 5},
		{typeHashAggID, 6},
		{typeShowID, 7},
		{typeJoinID, 8},
		{typeUnionID, 9},
		{typeTableScanID, 10},
		{typeMemTableScanID, 11},
		{typeUnionScanID, 12},
		{typeIdxScanID, 13},
		{typeSortID, 14},
		{typeTopNID, 15},
		{typeLimitID, 16},
		{typeHashJoinID, 17},
		{typeMergeJoinID, 18},
		{typeIndexJoinID, 19},
		{typeIndexMergeJoinID, 20},
		{typeIndexHashJoinID, 21},
		{typeApplyID, 22},
		{typeMaxOneRowID, 23},
		{typeExistsID, 24},
		{typeDualID, 25},
		{typeLockID, 26},
		{typeInsertID, 27},
		{typeUpdateID, 28},
		{typeDeleteID, 29},
		{typeIndexLookUpID, 30},
		{typeTableReaderID, 31},
		{typeIndexReaderID, 32},
		{typeWindowID, 33},
		{typeTiKVSingleGatherID, 34},
		{typeIndexMergeID, 35},
		{typePointGet, 36},
		{typeShowDDLJobs, 37},
		{typeBatchPointGet, 38},
		{typeClusterMemTableReader, 39},
		{typeDataSourceID, 40},
		{typeLoadDataID, 41},
		{typeTableSampleID, 42},
		{typeTableFullScanID, 43},
		{typeTableRangeScanID, 44},
		{typeTableRowIDScanID, 45},
		{typeIndexFullScanID, 46},
		{typeIndexRangeScanID, 47},
		{typeExchangeReceiverID, 48},
		{typeExchangeSenderID, 49},
		{typeCTEID, 50},
		{typeCTEDefinitionID, 51},
		{typeCTETableID, 52},
		{typePartitionUnionID, 53},
		{typeShuffleID, 54},
		{typeShuffleReceiverID, 55},
		{typeImportIntoID, 59},
	}

	for _, testcase := range testCases {
		require.Equal(t, testcase.Expected, testcase.Value)
	}
}

func TestReverse(t *testing.T) {
	for i := 1; i <= 55; i++ {
		require.Equal(t, TypeStringToPhysicalID(PhysicalIDToTypeString(i)), i)
	}
}
