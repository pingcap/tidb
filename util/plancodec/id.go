// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package plancodec

import "strconv"

const (
	// TypeSel is the type of Selection.
	TypeSel = "Selection"
	// TypeSet is the type of Set.
	TypeSet = "Set"
	// TypeProj is the type of Projection.
	TypeProj = "Projection"
	// TypeAgg is the type of Aggregation.
	TypeAgg = "Aggregation"
	// TypeStreamAgg is the type of StreamAgg.
	TypeStreamAgg = "StreamAgg"
	// TypeHashAgg is the type of HashAgg.
	TypeHashAgg = "HashAgg"
	// TypeShow is the type of show.
	TypeShow = "Show"
	// TypeJoin is the type of Join.
	TypeJoin = "Join"
	// TypeUnion is the type of Union.
	TypeUnion = "Union"
	// TypePartitionUnion is the type of PartitionUnion
	TypePartitionUnion = "PartitionUnion"
	// TypeTableScan is the type of TableScan.
	TypeTableScan = "TableScan"
	// TypeMemTableScan is the type of TableScan.
	TypeMemTableScan = "MemTableScan"
	// TypeUnionScan is the type of UnionScan.
	TypeUnionScan = "UnionScan"
	// TypeIdxScan is the type of IndexScan.
	TypeIdxScan = "IndexScan"
	// TypeSort is the type of Sort.
	TypeSort = "Sort"
	// TypeTopN is the type of TopN.
	TypeTopN = "TopN"
	// TypeLimit is the type of Limit.
	TypeLimit = "Limit"
	// TypeHashJoin is the type of hash join.
	TypeHashJoin = "HashJoin"
	// TypeBroadcastJoin is the type of broad cast join.
	TypeBroadcastJoin = "BroadcastJoin"
	// TypeExchangeSender is the type of mpp exchanger sender.
	TypeExchangeSender = "ExchangeSender"
	// TypeExchangeReceiver is the type of mpp exchanger receiver.
	TypeExchangeReceiver = "ExchangeReceiver"
	// TypeMergeJoin is the type of merge join.
	TypeMergeJoin = "MergeJoin"
	// TypeIndexJoin is the type of index look up join.
	TypeIndexJoin = "IndexJoin"
	// TypeIndexMergeJoin is the type of index look up merge join.
	TypeIndexMergeJoin = "IndexMergeJoin"
	// TypeIndexHashJoin is the type of index nested loop hash join.
	TypeIndexHashJoin = "IndexHashJoin"
	// TypeApply is the type of Apply.
	TypeApply = "Apply"
	// TypeMaxOneRow is the type of MaxOneRow.
	TypeMaxOneRow = "MaxOneRow"
	// TypeExists is the type of Exists.
	TypeExists = "Exists"
	// TypeDual is the type of TableDual.
	TypeDual = "TableDual"
	// TypeLock is the type of SelectLock.
	TypeLock = "SelectLock"
	// TypeInsert is the type of Insert
	TypeInsert = "Insert"
	// TypeUpdate is the type of Update.
	TypeUpdate = "Update"
	// TypeDelete is the type of Delete.
	TypeDelete = "Delete"
	// TypeIndexLookUp is the type of IndexLookUp.
	TypeIndexLookUp = "IndexLookUp"
	// TypeTableReader is the type of TableReader.
	TypeTableReader = "TableReader"
	// TypeIndexReader is the type of IndexReader.
	TypeIndexReader = "IndexReader"
	// TypeWindow is the type of Window.
	TypeWindow = "Window"
	// TypeShuffle is the type of Shuffle.
	TypeShuffle = "Shuffle"
	// TypeShuffleDataSourceStub is the type of Shuffle.
	TypeShuffleDataSourceStub = "ShuffleDataSourceStub"
	// TypeTiKVSingleGather is the type of TiKVSingleGather.
	TypeTiKVSingleGather = "TiKVSingleGather"
	// TypeIndexMerge is the type of IndexMergeReader
	TypeIndexMerge = "IndexMerge"
	// TypePointGet is the type of PointGetPlan.
	TypePointGet = "Point_Get"
	// TypeShowDDLJobs is the type of show ddl jobs.
	TypeShowDDLJobs = "ShowDDLJobs"
	// TypeBatchPointGet is the type of BatchPointGetPlan.
	TypeBatchPointGet = "Batch_Point_Get"
	// TypeClusterMemTableReader is the type of TableReader.
	TypeClusterMemTableReader = "ClusterMemTableReader"
	// TypeDataSource is the type of DataSource.
	TypeDataSource = "DataSource"
	// TypeLoadData is the type of LoadData.
	TypeLoadData = "LoadData"
)

// plan id.
// Attention: for compatibility of encode/decode plan, The plan id shouldn't be changed.
const (
	typeSelID                 int = 1
	typeSetID                 int = 2
	typeProjID                int = 3
	typeAggID                 int = 4
	typeStreamAggID           int = 5
	typeHashAggID             int = 6
	typeShowID                int = 7
	typeJoinID                int = 8
	typeUnionID               int = 9
	typeTableScanID           int = 10
	typeMemTableScanID        int = 11
	typeUnionScanID           int = 12
	typeIdxScanID             int = 13
	typeSortID                int = 14
	typeTopNID                int = 15
	typeLimitID               int = 16
	typeHashJoinID            int = 17
	typeMergeJoinID           int = 18
	typeIndexJoinID           int = 19
	typeIndexMergeJoinID      int = 20
	typeIndexHashJoinID       int = 21
	typeApplyID               int = 22
	typeMaxOneRowID           int = 23
	typeExistsID              int = 24
	typeDualID                int = 25
	typeLockID                int = 26
	typeInsertID              int = 27
	typeUpdateID              int = 28
	typeDeleteID              int = 29
	typeIndexLookUpID         int = 30
	typeTableReaderID         int = 31
	typeIndexReaderID         int = 32
	typeWindowID              int = 33
	typeTiKVSingleGatherID    int = 34
	typeIndexMergeID          int = 35
	typePointGet              int = 36
	typeShowDDLJobs           int = 37
	typeBatchPointGet         int = 38
	typeClusterMemTableReader int = 39
	typeDataSourceID          int = 40
	typeLoadDataID            int = 41
)

// TypeStringToPhysicalID converts the plan type string to plan id.
func TypeStringToPhysicalID(tp string) int {
	switch tp {
	case TypeSel:
		return typeSelID
	case TypeSet:
		return typeSetID
	case TypeProj:
		return typeProjID
	case TypeAgg:
		return typeAggID
	case TypeStreamAgg:
		return typeStreamAggID
	case TypeHashAgg:
		return typeHashAggID
	case TypeShow:
		return typeShowID
	case TypeJoin:
		return typeJoinID
	case TypeUnion:
		return typeUnionID
	case TypeTableScan:
		return typeTableScanID
	case TypeMemTableScan:
		return typeMemTableScanID
	case TypeUnionScan:
		return typeUnionScanID
	case TypeIdxScan:
		return typeIdxScanID
	case TypeSort:
		return typeSortID
	case TypeTopN:
		return typeTopNID
	case TypeLimit:
		return typeLimitID
	case TypeHashJoin:
		return typeHashJoinID
	case TypeMergeJoin:
		return typeMergeJoinID
	case TypeIndexJoin:
		return typeIndexJoinID
	case TypeIndexMergeJoin:
		return typeIndexMergeJoinID
	case TypeIndexHashJoin:
		return typeIndexHashJoinID
	case TypeApply:
		return typeApplyID
	case TypeMaxOneRow:
		return typeMaxOneRowID
	case TypeExists:
		return typeExistsID
	case TypeDual:
		return typeDualID
	case TypeLock:
		return typeLockID
	case TypeInsert:
		return typeInsertID
	case TypeUpdate:
		return typeUpdateID
	case TypeDelete:
		return typeDeleteID
	case TypeIndexLookUp:
		return typeIndexLookUpID
	case TypeTableReader:
		return typeTableReaderID
	case TypeIndexReader:
		return typeIndexReaderID
	case TypeWindow:
		return typeWindowID
	case TypeTiKVSingleGather:
		return typeTiKVSingleGatherID
	case TypeIndexMerge:
		return typeIndexMergeID
	case TypePointGet:
		return typePointGet
	case TypeShowDDLJobs:
		return typeShowDDLJobs
	case TypeBatchPointGet:
		return typeBatchPointGet
	case TypeClusterMemTableReader:
		return typeClusterMemTableReader
	case TypeDataSource:
		return typeDataSourceID
	case TypeLoadData:
		return typeLoadDataID
	}
	// Should never reach here.
	return 0
}

// PhysicalIDToTypeString converts the plan id to plan type string.
func PhysicalIDToTypeString(id int) string {
	switch id {
	case typeSelID:
		return TypeSel
	case typeSetID:
		return TypeSet
	case typeProjID:
		return TypeProj
	case typeAggID:
		return TypeAgg
	case typeStreamAggID:
		return TypeStreamAgg
	case typeHashAggID:
		return TypeHashAgg
	case typeShowID:
		return TypeShow
	case typeJoinID:
		return TypeJoin
	case typeUnionID:
		return TypeUnion
	case typeTableScanID:
		return TypeTableScan
	case typeMemTableScanID:
		return TypeMemTableScan
	case typeUnionScanID:
		return TypeUnionScan
	case typeIdxScanID:
		return TypeIdxScan
	case typeSortID:
		return TypeSort
	case typeTopNID:
		return TypeTopN
	case typeLimitID:
		return TypeLimit
	case typeHashJoinID:
		return TypeHashJoin
	case typeMergeJoinID:
		return TypeMergeJoin
	case typeIndexJoinID:
		return TypeIndexJoin
	case typeIndexMergeJoinID:
		return TypeIndexMergeJoin
	case typeIndexHashJoinID:
		return TypeIndexHashJoin
	case typeApplyID:
		return TypeApply
	case typeMaxOneRowID:
		return TypeMaxOneRow
	case typeExistsID:
		return TypeExists
	case typeDualID:
		return TypeDual
	case typeLockID:
		return TypeLock
	case typeInsertID:
		return TypeInsert
	case typeUpdateID:
		return TypeUpdate
	case typeDeleteID:
		return TypeDelete
	case typeIndexLookUpID:
		return TypeIndexLookUp
	case typeTableReaderID:
		return TypeTableReader
	case typeIndexReaderID:
		return TypeIndexReader
	case typeWindowID:
		return TypeWindow
	case typeTiKVSingleGatherID:
		return TypeTiKVSingleGather
	case typeIndexMergeID:
		return TypeIndexMerge
	case typePointGet:
		return TypePointGet
	case typeShowDDLJobs:
		return TypeShowDDLJobs
	case typeBatchPointGet:
		return TypeBatchPointGet
	case typeClusterMemTableReader:
		return TypeClusterMemTableReader
	case typeLoadDataID:
		return TypeLoadData
	}

	// Should never reach here.
	return "UnknownPlanID" + strconv.Itoa(id)
}
