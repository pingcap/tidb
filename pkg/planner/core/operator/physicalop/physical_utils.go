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

package physicalop

import (
	"context"
	"strconv"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/access"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/rule"
	"github.com/pingcap/tidb/pkg/planner/util/partitionpruning"
	"github.com/pingcap/tidb/pkg/planner/util/utilfuncp"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/codec"
)

// ClonePhysicalPlan clones physical plans.
func ClonePhysicalPlan(sctx base.PlanContext, plans []base.PhysicalPlan) ([]base.PhysicalPlan, error) {
	cloned := make([]base.PhysicalPlan, 0, len(plans))
	for _, p := range plans {
		c, err := p.Clone(sctx)
		if err != nil {
			return nil, err
		}
		cloned = append(cloned, c)
	}
	return cloned, nil
}

// flattenPlanWithPreorderTraversal flattens a plan tree to a list with preorder traversal.
func flattenPlanWithPreorderTraversal(plan base.PhysicalPlan, plans []base.PhysicalPlan) []base.PhysicalPlan {
	plans = append(plans, plan)
	for _, child := range plan.Children() {
		plans = flattenPlanWithPreorderTraversal(child, plans)
	}
	return plans
}

// FlattenListPushDownPlan converts a plan tree to a list, whose head is the leaf node like table scan.
// It is used to flatten the plan tree that all parents have only one child.
func FlattenListPushDownPlan(p base.PhysicalPlan) []base.PhysicalPlan {
	plans := make([]base.PhysicalPlan, 0, 5)
	plans = flattenPlanWithPreorderTraversal(p, plans)
	for i := range len(plans) / 2 {
		j := len(plans) - i - 1
		plans[i], plans[j] = plans[j], plans[i]
	}
	return plans
}

// flattenPlanWithPostorderTraversal flattens a plan tree to a list with postorder traversal.
// The unNatureOrdersMaps the childPlanIndex => parentPlanIndex for those plans whose parent's index
// is not equal to the child's index + 1.
func flattenPlanWithPostorderTraversal(plan base.PhysicalPlan, plans []base.PhysicalPlan, unNatureOrdersMap map[int]int) ([]base.PhysicalPlan, map[int]int) {
	//nolint: prealloc
	var unNatureOrderChildren []int
	children := plan.Children()
	if len(children) > 1 && unNatureOrdersMap == nil {
		unNatureOrdersMap = make(map[int]int)
		unNatureOrderChildren = make([]int, 0, len(children))
	}
	for _, child := range children {
		plans, unNatureOrdersMap = flattenPlanWithPostorderTraversal(child, plans, unNatureOrdersMap)
		unNatureOrderChildren = append(unNatureOrderChildren, len(plans)-1)
	}
	plans = append(plans, plan)
	for _, childIndex := range unNatureOrderChildren {
		if parentIndex := len(plans) - 1; parentIndex != childIndex+1 {
			unNatureOrdersMap[childIndex] = parentIndex
		}
	}
	return plans, unNatureOrdersMap
}

// FlattenTreePushDownPlan converts a plan tree to a list, whose head is the leaf node like table scan.
// The returned list follows the order of depth-first search with post-order traversal.
// That is: left child first, then right child, then parent.
// For example, a DAG:
//
//	         A
//	        /
//	       B
//	     /   \
//	    C     D
//	   /     / \
//	  E     F   G
//	 /
//	H
//
// Its order should be: [H, E, C, F, G, D, B, A]
// This function also returns a map which records the childPlanIndex => parentPlanIndex if
// the child's index + 1 is not equal to the parent's index.
// This function should NOT be used by TiFlash
func FlattenTreePushDownPlan(p base.PhysicalPlan) ([]base.PhysicalPlan, map[int]int) {
	return flattenPlanWithPostorderTraversal(p, make([]base.PhysicalPlan, 0, 5), nil)
}

// GetTblStats returns the tbl-stats of this plan, which contains all columns before pruning.
func GetTblStats(copTaskPlan base.PhysicalPlan) *statistics.HistColl {
	switch x := copTaskPlan.(type) {
	case *PhysicalTableScan:
		return x.TblColHists
	case *PhysicalIndexScan:
		return x.TblColHists
	default:
		return GetTblStats(copTaskPlan.Children()[0])
	}
}

// GetDynamicAccessPartition get the dynamic access partition.
func GetDynamicAccessPartition(sctx base.PlanContext, tblInfo *model.TableInfo, physPlanPartInfo *PhysPlanPartInfo, asName string) (res *access.DynamicPartitionAccessObject) {
	pi := tblInfo.GetPartitionInfo()
	if pi == nil || !sctx.GetSessionVars().StmtCtx.UseDynamicPartitionPrune() {
		return nil
	}

	res = &access.DynamicPartitionAccessObject{}
	tblName := tblInfo.Name.O
	if len(asName) > 0 {
		tblName = asName
	}
	res.Table = tblName
	is := sctx.GetInfoSchema().(infoschema.InfoSchema)
	db, ok := infoschema.SchemaByTable(is, tblInfo)
	if ok {
		res.Database = db.Name.O
	}
	tmp, ok := is.TableByID(context.Background(), tblInfo.ID)
	if !ok {
		res.Err = "partition table not found:" + strconv.FormatInt(tblInfo.ID, 10)
		return res
	}
	tbl := tmp.(table.PartitionedTable)

	idxArr, err := partitionpruning.PartitionPruning(sctx, tbl, physPlanPartInfo.PruningConds, physPlanPartInfo.PartitionNames, physPlanPartInfo.Columns, physPlanPartInfo.ColumnNames)
	if err != nil {
		res.Err = "partition pruning error:" + err.Error()
		return res
	}

	if len(idxArr) == 1 && idxArr[0] == rule.FullRange {
		res.AllPartitions = true
		return res
	}

	for _, idx := range idxArr {
		res.Partitions = append(res.Partitions, pi.Definitions[idx].Name.O)
	}
	return res
}

// ResolveIndicesForVirtualColumn resolves dependent columns's indices for virtual columns.
func ResolveIndicesForVirtualColumn(result []*expression.Column, schema *expression.Schema) error {
	for _, col := range result {
		if col.VirtualExpr != nil {
			newExpr, _, err := col.VirtualExpr.ResolveIndices(schema, true)
			if err != nil {
				return err
			}
			col.VirtualExpr = newExpr
		}
	}
	return nil
}

// ClonePhysicalPlansForPlanCache clones physical plans for plan cache usage.
func ClonePhysicalPlansForPlanCache(newCtx base.PlanContext, plans []base.PhysicalPlan) ([]base.PhysicalPlan, bool) {
	clonedPlans := make([]base.PhysicalPlan, len(plans))
	for i, plan := range plans {
		cloned, ok := plan.CloneForPlanCache(newCtx)
		if !ok {
			return nil, false
		}
		clonedPlans[i] = cloned.(base.PhysicalPlan)
	}
	return clonedPlans, true
}

// EncodeUniqueIndexKey encodes a unique index key.
func EncodeUniqueIndexKey(ctx sessionctx.Context, tblInfo *model.TableInfo, idxInfo *model.IndexInfo, idxVals []types.Datum, tID int64) (_ []byte, err error) {
	encodedIdxVals, err := EncodeUniqueIndexValuesForKey(ctx, tblInfo, idxInfo, idxVals)
	if err != nil {
		return nil, err
	}
	return tablecodec.EncodeIndexSeekKey(tID, idxInfo.ID, encodedIdxVals), nil
}

// EncodeUniqueIndexValuesForKey encodes unique index values for a key.
func EncodeUniqueIndexValuesForKey(ctx sessionctx.Context, tblInfo *model.TableInfo, idxInfo *model.IndexInfo, idxVals []types.Datum) (_ []byte, err error) {
	sc := ctx.GetSessionVars().StmtCtx
	for i := range idxVals {
		colInfo := tblInfo.Columns[idxInfo.Columns[i].Offset]
		// table.CastValue will append 0x0 if the string value's length is smaller than the BINARY column's length.
		// So we don't use CastValue for string value for now.
		// TODO: The first if branch should have been removed, because the functionality of set the collation of the datum
		// have been moved to util/ranger (normal path) and getNameValuePairs/getPointGetValue (fast path). But this change
		// will be cherry-picked to a hotfix, so we choose to be a bit conservative and keep this for now.
		if colInfo.GetType() == mysql.TypeString || colInfo.GetType() == mysql.TypeVarString || colInfo.GetType() == mysql.TypeVarchar {
			var str string
			str, err = idxVals[i].ToString()
			idxVals[i].SetString(str, idxVals[i].Collation())
		} else if colInfo.GetType() == mysql.TypeEnum && (idxVals[i].Kind() == types.KindString || idxVals[i].Kind() == types.KindBytes || idxVals[i].Kind() == types.KindBinaryLiteral) {
			var str string
			var e types.Enum
			str, err = idxVals[i].ToString()
			if err != nil {
				return nil, kv.ErrNotExist
			}
			e, err = types.ParseEnumName(colInfo.FieldType.GetElems(), str, colInfo.FieldType.GetCollate())
			if err != nil {
				return nil, kv.ErrNotExist
			}
			idxVals[i].SetMysqlEnum(e, colInfo.FieldType.GetCollate())
		} else {
			// If a truncated error or an overflow error is thrown when converting the type of `idxVal[i]` to
			// the type of `colInfo`, the `idxVal` does not exist in the `idxInfo` for sure.
			idxVals[i], err = table.CastValue(ctx, idxVals[i], colInfo, true, false)
			if types.ErrOverflow.Equal(err) || types.ErrDataTooLong.Equal(err) ||
				types.ErrTruncated.Equal(err) || types.ErrTruncatedWrongVal.Equal(err) {
				return nil, kv.ErrNotExist
			}
		}
		if err != nil {
			return nil, err
		}
	}

	encodedIdxVals, err := codec.EncodeKey(sc.TimeZone(), nil, idxVals...)
	err = sc.HandleError(err)
	if err != nil {
		return nil, err
	}
	return encodedIdxVals, nil
}

// TODO: Since these functions are about plan cache cloning,
// we should move them in the future with all following functions:
// - cloneExpressionsForPlanCache
// - cloneExpression2DForPlanCache
// - cloneColumnsForPlanCache
// - ... (other related functions)
// to plan_clone_utils.go for better code organization.

// CloneConstant2DForPlanCache clones a 2D slice of *expression.Constant.
func CloneConstant2DForPlanCache(constants [][]*expression.Constant) [][]*expression.Constant {
	if constants == nil {
		return nil
	}
	cloned := make([][]*expression.Constant, 0, len(constants))
	for _, c := range constants {
		cloned = append(cloned, utilfuncp.CloneConstantsForPlanCache(c, nil))
	}
	return cloned
}

// ExpandVirtualColumn expands the virtual column's dependent columns to ts's schema and column.
func ExpandVirtualColumn(columns []*model.ColumnInfo, schema *expression.Schema,
	colsInfo []*model.ColumnInfo) []*model.ColumnInfo {
	copyColumn := make([]*model.ColumnInfo, 0, len(columns))
	copyColumn = append(copyColumn, columns...)

	oldNumColumns := len(schema.Columns)
	numExtraColumns := 0
	ordinaryColumnExists := false
	for i := oldNumColumns - 1; i >= 0; i-- {
		cid := schema.Columns[i].ID
		// Move extra columns to the end.
		// ExtraRowChecksumID is ignored here since it's treated as an ordinary column.
		// https://github.com/pingcap/tidb/blob/3c407312a986327bc4876920e70fdd6841b8365f/pkg/util/rowcodec/decoder.go#L206-L222
		if cid != model.ExtraHandleID && cid != model.ExtraPhysTblID {
			ordinaryColumnExists = true
			break
		}
		numExtraColumns++
	}
	if ordinaryColumnExists && numExtraColumns > 0 {
		extraColumns := make([]*expression.Column, numExtraColumns)
		copy(extraColumns, schema.Columns[oldNumColumns-numExtraColumns:])
		schema.Columns = schema.Columns[:oldNumColumns-numExtraColumns]

		extraColumnModels := make([]*model.ColumnInfo, numExtraColumns)
		copy(extraColumnModels, copyColumn[len(copyColumn)-numExtraColumns:])
		copyColumn = copyColumn[:len(copyColumn)-numExtraColumns]

		copyColumn = expandVirtualColumn(schema, copyColumn, colsInfo)
		schema.Columns = append(schema.Columns, extraColumns...)
		copyColumn = append(copyColumn, extraColumnModels...)
		return copyColumn
	}
	return expandVirtualColumn(schema, copyColumn, colsInfo)
}

// expandVirtualColumn expands the virtual column's dependent columns to ts's schema and column.
func expandVirtualColumn(schema *expression.Schema, copyColumn []*model.ColumnInfo, colsInfo []*model.ColumnInfo) []*model.ColumnInfo {
	schemaColumns := schema.Columns
	for _, col := range schemaColumns {
		if col.VirtualExpr == nil {
			continue
		}

		baseCols := expression.ExtractDependentColumns(col.VirtualExpr)
		for _, baseCol := range baseCols {
			if !schema.Contains(baseCol) {
				schema.Columns = append(schema.Columns, baseCol)
				copyColumn = append(copyColumn, model.FindColumnInfoByID(colsInfo, baseCol.ID)) // nozero
			}
		}
	}
	return copyColumn
}
