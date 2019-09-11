package plan

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
	// TypeHashLeftJoin is the type of left hash join.
	TypeHashLeftJoin = "HashLeftJoin"
	// TypeHashRightJoin is the type of right hash join.
	TypeHashRightJoin = "HashRightJoin"
	// TypeMergeJoin is the type of merge join.
	TypeMergeJoin = "MergeJoin"
	// TypeIndexJoin is the type of index look up join.
	TypeIndexJoin = "IndexJoin"
	// TypeIndexMergeJoin is the type of index look up merge join.
	TypeIndexMergeJoin = "IndexMergeJoin"
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
	// TypeTableGather is the type of TableGather.
	TypeTableGather = "TableGather"
)

const (
	TypeSelID int = iota + 1
	TypeSetID
	TypeProjID
	TypeAggID
	TypeStreamAggID
	TypeHashAggID
	TypeShowID
	TypeJoinID
	TypeUnionID
	TypeTableScanID
	TypeMemTableScanID
	TypeUnionScanID
	TypeIdxScanID
	TypeSortID
	TypeTopNID
	TypeLimitID
	TypeHashLeftJoinID
	TypeHashRightJoinID
	TypeMergeJoinID
	TypeIndexJoinID
	TypeIndexMergeJoinID
	TypeApplyID
	TypeMaxOneRowID
	TypeExistsID
	TypeDualID
	TypeLockID
	TypeInsertID
	TypeUpdateID
	TypeDeleteID
	TypeIndexLookUpID
	TypeTableReaderID
	TypeIndexReaderID
	TypeWindowID
)

func TypeStringToPhysicalID(tp string) int {
	switch tp {
	case TypeSel:
		return TypeSelID
	case TypeSet:
		return TypeSetID
	case TypeProj:
		return TypeProjID
	case TypeAgg:
		return TypeAggID
	case TypeStreamAgg:
		return TypeStreamAggID
	case TypeHashAgg:
		return TypeHashAggID
	case TypeShow:
		return TypeShowID
	case TypeJoin:
		return TypeJoinID
	case TypeUnion:
		return TypeUnionID
	case TypeTableScan:
		return TypeTableScanID
	case TypeMemTableScan:
		return TypeMemTableScanID
	case TypeUnionScan:
		return TypeUnionScanID
	case TypeIdxScan:
		return TypeIdxScanID
	case TypeSort:
		return TypeSortID
	case TypeTopN:
		return TypeTopNID
	case TypeLimit:
		return TypeLimitID
	case TypeHashLeftJoin:
		return TypeHashLeftJoinID
	case TypeHashRightJoin:
		return TypeHashRightJoinID
	case TypeMergeJoin:
		return TypeMergeJoinID
	case TypeIndexJoin:
		return TypeIndexJoinID
	case TypeIndexMergeJoin:
		return TypeIndexMergeJoinID
	case TypeApply:
		return TypeApplyID
	case TypeMaxOneRow:
		return TypeMaxOneRowID
	case TypeExists:
		return TypeExistsID
	case TypeDual:
		return TypeDualID
	case TypeLock:
		return TypeLockID
	case TypeInsert:
		return TypeInsertID
	case TypeUpdate:
		return TypeUpdateID
	case TypeDelete:
		return TypeDeleteID
	case TypeIndexLookUp:
		return TypeIndexLookUpID
	case TypeTableReader:
		return TypeTableReaderID
	case TypeIndexReader:
		return TypeIndexReaderID
	case TypeWindow:
		return TypeWindowID
	}
	// Should never reach here.
	return 0
}

func PhysicalIDToTypeString(id int) string {
	switch id {
	case TypeSelID:
		return TypeSel
	case TypeSetID:
		return TypeSet
	case TypeProjID:
		return TypeProj
	case TypeAggID:
		return TypeAgg
	case TypeStreamAggID:
		return TypeStreamAgg
	case TypeHashAggID:
		return TypeHashAgg
	case TypeShowID:
		return TypeShow
	case TypeJoinID:
		return TypeJoin
	case TypeUnionID:
		return TypeUnion
	case TypeTableScanID:
		return TypeTableScan
	case TypeMemTableScanID:
		return TypeMemTableScan
	case TypeUnionScanID:
		return TypeUnionScan
	case TypeIdxScanID:
		return TypeIdxScan
	case TypeSortID:
		return TypeSort
	case TypeTopNID:
		return TypeTopN
	case TypeLimitID:
		return TypeLimit
	case TypeHashLeftJoinID:
		return TypeHashLeftJoin
	case TypeHashRightJoinID:
		return TypeHashRightJoin
	case TypeMergeJoinID:
		return TypeMergeJoin
	case TypeIndexJoinID:
		return TypeIndexJoin
	case TypeIndexMergeJoinID:
		return TypeIndexMergeJoin
	case TypeApplyID:
		return TypeApply
	case TypeMaxOneRowID:
		return TypeMaxOneRow
	case TypeExistsID:
		return TypeExists
	case TypeDualID:
		return TypeDual
	case TypeLockID:
		return TypeLock
	case TypeInsertID:
		return TypeInsert
	case TypeUpdateID:
		return TypeUpdate
	case TypeDeleteID:
		return TypeDelete
	case TypeIndexLookUpID:
		return TypeIndexLookUp
	case TypeTableReaderID:
		return TypeTableReader
	case TypeIndexReaderID:
		return TypeIndexReader
	case TypeWindowID:
		return TypeWindow
	}

	// Should never reach here.
	return "unknowPlanID" + strconv.Itoa(id)
}
