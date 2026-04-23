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

package plancodec_test

import (
	"testing"

	"github.com/pingcap/tidb/pkg/util/plancodec"
	"github.com/stretchr/testify/require"
)

func RunPlanIDChanged(t *testing.T) {
	// Attention: for compatibility, shouldn't modify the below test, you can only add test when add new plan ID.
	testCases := []struct {
		Value    int
		Expected int
	}{
		{plancodec.typeSelID, 1},
		{plancodec.typeSetID, 2},
		{plancodec.typeProjID, 3},
		{plancodec.typeAggID, 4},
		{plancodec.typeStreamAggID, 5},
		{plancodec.typeHashAggID, 6},
		{plancodec.typeShowID, 7},
		{plancodec.typeJoinID, 8},
		{plancodec.typeUnionID, 9},
		{plancodec.typeTableScanID, 10},
		{plancodec.typeMemTableScanID, 11},
		{plancodec.typeUnionScanID, 12},
		{plancodec.typeIdxScanID, 13},
		{plancodec.typeSortID, 14},
		{plancodec.typeTopNID, 15},
		{plancodec.typeLimitID, 16},
		{plancodec.typeHashJoinID, 17},
		{plancodec.typeMergeJoinID, 18},
		{plancodec.typeIndexJoinID, 19},
		{plancodec.typeIndexMergeJoinID, 20},
		{plancodec.typeIndexHashJoinID, 21},
		{plancodec.typeApplyID, 22},
		{plancodec.typeMaxOneRowID, 23},
		{plancodec.typeExistsID, 24},
		{plancodec.typeDualID, 25},
		{plancodec.typeLockID, 26},
		{plancodec.typeInsertID, 27},
		{plancodec.typeUpdateID, 28},
		{plancodec.typeDeleteID, 29},
		{plancodec.typeIndexLookUpID, 30},
		{plancodec.typeTableReaderID, 31},
		{plancodec.typeIndexReaderID, 32},
		{plancodec.typeWindowID, 33},
		{plancodec.typeTiKVSingleGatherID, 34},
		{plancodec.typeIndexMergeID, 35},
		{plancodec.typePointGet, 36},
		{plancodec.typeShowDDLJobs, 37},
		{plancodec.typeBatchPointGet, 38},
		{plancodec.typeClusterMemTableReader, 39},
		{plancodec.typeDataSourceID, 40},
		{plancodec.typeLoadDataID, 41},
		{plancodec.typeTableSampleID, 42},
		{plancodec.typeTableFullScanID, 43},
		{plancodec.typeTableRangeScanID, 44},
		{plancodec.typeTableRowIDScanID, 45},
		{plancodec.typeIndexFullScanID, 46},
		{plancodec.typeIndexRangeScanID, 47},
		{plancodec.typeExchangeReceiverID, 48},
		{plancodec.typeExchangeSenderID, 49},
		{plancodec.typeCTEID, 50},
		{plancodec.typeCTEDefinitionID, 51},
		{plancodec.typeCTETableID, 52},
		{plancodec.typePartitionUnionID, 53},
		{plancodec.typeShuffleID, 54},
		{plancodec.typeShuffleReceiverID, 55},
		{plancodec.typeImportIntoID, 59},
		{plancodec.typeLocalIndexLookUpID, 61},
	}

	for _, testcase := range testCases {
		require.Equal(t, testcase.Expected, testcase.Value)
	}
}

func RunReverse(t *testing.T) {
	for i := 1; i <= 61; i++ {
		require.Equal(t, plancodec.TypeStringToPhysicalID(plancodec.PhysicalIDToTypeString(i)), i)
	}
}
