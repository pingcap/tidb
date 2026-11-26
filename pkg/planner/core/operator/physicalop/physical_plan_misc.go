// Copyright 2025 PingCAP, Inc.
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
	"fmt"
	"strings"
	"unsafe"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	plannerutil "github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/planner/util/utilfuncp"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/dbterror/plannererrors"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/size"
	"github.com/pingcap/tipb/go-tipb"
	"go.uber.org/zap"
)

// PushedDownLimit is the limit operator pushed down into PhysicalIndexLookUpReader.
type PushedDownLimit struct {
	Offset uint64
	Count  uint64
}

// Clone clones this pushed-down list.
func (p *PushedDownLimit) Clone() *PushedDownLimit {
	if p == nil {
		return nil
	}
	cloned := new(PushedDownLimit)
	*cloned = *p
	return cloned
}

const pushedDownLimitSize = size.SizeOfUint64 * 2

// MemoryUsage return the memory usage of PushedDownLimit
func (p *PushedDownLimit) MemoryUsage() (sum int64) {
	if p == nil {
		return
	}

	return pushedDownLimitSize
}

// SplitSelCondsWithVirtualColumn filter the select conditions which contain virtual column
func SplitSelCondsWithVirtualColumn(conds []expression.Expression) (
	withoutVirt []expression.Expression, withVirt []expression.Expression) {
	for i := range conds {
		if expression.ContainVirtualColumn(conds[i : i+1]) {
			withVirt = append(withVirt, conds[i])
		} else {
			withoutVirt = append(withoutVirt, conds[i])
		}
	}
	return withoutVirt, withVirt
}

// SubstituteVirtualColumnsForPushDown rewrites each condition that references a
// virtual generated column so that the column is replaced by its generation
// expression, e.g. `c_lower = 'x'` with `c_lower AS (lower(c)) VIRTUAL` becomes
// `lower(c) = 'x'`. Storage engines do not hold virtual columns, so a condition
// on one is otherwise always evaluated in TiDB above the scan, which also blocks
// pushing TopN/Limit below it. The rewrite is kept only when every virtual column
// in the condition evaluates to the same value as its expression (see
// VirtualExprPreservesValue) and the rewritten condition can be pushed to
// storeType; otherwise the original condition is returned unchanged.
//
// The scan still lists the virtual column in its schema, so ExpandVirtualColumn
// later adds the base columns the rewritten condition reads.
func SubstituteVirtualColumnsForPushDown(sctx base.PlanContext, conds []expression.Expression, storeType kv.StoreType) []expression.Expression {
	var out []expression.Expression
	pctx := plannerutil.GetPushDownCtx(sctx)
	for i := range conds {
		if !expression.ContainVirtualColumn(conds[i : i+1]) {
			continue
		}
		newCond, ok := substituteVirtualColumns(sctx, conds[i])
		if !ok || !expression.CanExprsPushDown(pctx, []expression.Expression{newCond}, storeType) {
			continue
		}
		if out == nil {
			out = make([]expression.Expression, len(conds))
			copy(out, conds)
		}
		out[i] = newCond
	}
	if out == nil {
		return conds
	}
	return out
}

// MaterializeVirtualColumnsInTiFlashScan lets a TiFlash table scan serve
// virtual generated columns. TiFlash does not store virtual columns, and TiDB
// only fills them after the data is returned, which is why a scan whose schema
// contains one cannot be part of an MPP plan. Every virtual column in the
// scan's schema that VirtualColumnComputableInTiFlash accepts is computed in
// TiFlash instead: the scan reads the base columns, and the returned
// expressions compute the virtual columns in a Projection placed above the scan
// (and its pushed-down Selection). The Projection's output keeps the original
// UniqueIDs, so parent operators resolve the columns unchanged. Other virtual
// columns stay in the scan's schema and are filled by TiDB as before.
// It returns ok=false and leaves ts untouched when no virtual column can be
// computed this way.
func MaterializeVirtualColumnsInTiFlashScan(ts *PhysicalTableScan) (exprs []expression.Expression, outSchema *expression.Schema, ok bool) {
	sctx := ts.SCtx()
	schema := ts.Schema()
	if len(ts.Columns) != len(schema.Columns) {
		return nil, nil, false
	}
	exprs = make([]expression.Expression, 0, len(schema.Columns))
	computed := make([]bool, len(schema.Columns))
	anyComputed := false
	for i, col := range schema.Columns {
		var expr expression.Expression = col
		if col.VirtualExpr != nil {
			if newExpr, ok := virtualColumnTiFlashExpr(sctx, col); ok {
				expr = newExpr
				computed[i] = true
				anyComputed = true
			}
		}
		exprs = append(exprs, expr)
	}
	if !anyComputed {
		return nil, nil, false
	}

	// The scan outputs the columns it already had, except the computed virtual
	// columns, plus any base column the generation expressions read.
	newCols := make([]*expression.Column, 0, len(schema.Columns))
	newInfos := make([]*model.ColumnInfo, 0, len(ts.Columns))
	for i, col := range schema.Columns {
		if !computed[i] {
			newCols = append(newCols, col)
			newInfos = append(newInfos, ts.Columns[i])
		}
	}
	scanSchema := expression.NewSchema(newCols...)
	for i, e := range exprs {
		if !computed[i] {
			continue
		}
		for _, base := range expression.ExtractColumns(e) {
			if scanSchema.Contains(base) {
				continue
			}
			info := model.FindColumnInfoByID(ts.Table.Columns, base.ID)
			if info == nil {
				return nil, nil, false
			}
			scanSchema.Append(base)
			newInfos = append(newInfos, info)
		}
	}

	// Computed columns are plain columns from here up: TiDB must not try to
	// fill them again.
	outCols := make([]*expression.Column, len(schema.Columns))
	for i, col := range schema.Columns {
		outCols[i] = col
		if computed[i] {
			c := col.Clone().(*expression.Column)
			c.VirtualExpr = nil
			outCols[i] = c
		}
	}
	outSchema = expression.NewSchema(outCols...)
	// TiDB still fills the remaining virtual columns, so the base columns they
	// depend on must reach it through the Projection.
	for i, col := range schema.Columns {
		if col.VirtualExpr == nil || computed[i] {
			continue
		}
		for _, base := range expression.ExtractDependentColumns(col.VirtualExpr) {
			if base.VirtualExpr != nil {
				continue
			}
			if !scanSchema.Contains(base) {
				info := model.FindColumnInfoByID(ts.Table.Columns, base.ID)
				if info == nil {
					return nil, nil, false
				}
				scanSchema.Append(base)
				newInfos = append(newInfos, info)
			}
			if !outSchema.Contains(base) {
				outSchema.Append(base)
				exprs = append(exprs, base)
			}
		}
	}
	ts.SetSchema(scanSchema)
	ts.Columns = newInfos
	return exprs, outSchema, true
}

// PruneUncomputedVirtualColumns drops, from the Projection built by
// MaterializeVirtualColumnsInTiFlashScan, the computed virtual columns that no
// operator needs: those not in parentUsed (the DataSource's output columns, see
// DataSource.ColsRequiringFullLen) and not referenced by rootConds. A virtual
// column kept only for a filter is not needed once the filter has been
// rewritten to the expression and pushed down. It returns ok=false when the
// Projection is no longer needed at all, i.e. it would only pass columns through.
// A nil parentUsed keeps every column.
func PruneUncomputedVirtualColumns(exprs []expression.Expression, outSchema *expression.Schema, childSchema *expression.Schema,
	parentUsed []*expression.Column, rootConds []expression.Expression) ([]expression.Expression, *expression.Schema, bool) {
	if parentUsed == nil {
		return exprs, outSchema, true
	}
	needed := expression.NewSchema(parentUsed...)
	for _, c := range expression.ExtractColumnsFromExpressions(rootConds, nil) {
		needed.Append(c)
	}
	newExprs := make([]expression.Expression, 0, len(exprs))
	newCols := make([]*expression.Column, 0, len(exprs))
	computes := false
	for i, e := range exprs {
		col := outSchema.Columns[i]
		if c, isCol := e.(*expression.Column); isCol && c.UniqueID == col.UniqueID {
			newExprs = append(newExprs, e)
			newCols = append(newCols, col)
			continue
		}
		if !needed.Contains(col) {
			continue
		}
		computes = true
		newExprs = append(newExprs, e)
		newCols = append(newCols, col)
	}
	if !computes {
		// Only pass-through columns remain; the child already outputs them.
		for _, c := range newCols {
			if !childSchema.Contains(c) {
				return newExprs, expression.NewSchema(newCols...), true
			}
		}
		return nil, nil, false
	}
	return newExprs, expression.NewSchema(newCols...), true
}

// virtualColumnTiFlashExpr returns the expression that computes the virtual
// column col from stored columns, if that expression equals the column's value
// and can be evaluated by TiFlash.
func virtualColumnTiFlashExpr(sctx base.PlanContext, col *expression.Column) (expression.Expression, bool) {
	newExpr, ok := substituteVirtualColumns(sctx, col)
	if !ok || !expression.CanExprsPushDown(plannerutil.GetPushDownCtx(sctx), []expression.Expression{newExpr}, kv.TiFlash) {
		return nil, false
	}
	return newExpr, true
}

// ContainVirtualColumnForStore reports whether exprs reference a virtual
// column that storeType cannot see. TiFlash table scans compute the virtual
// columns accepted by virtualColumnTiFlashExpr (MaterializeVirtualColumnsInTiFlashScan),
// so operators above them may use those columns; no other store sees any
// virtual column.
func ContainVirtualColumnForStore(sctx base.PlanContext, exprs []expression.Expression, storeType kv.StoreType) bool {
	if storeType != kv.TiFlash {
		return expression.ContainVirtualColumn(exprs)
	}
	for _, e := range exprs {
		for _, col := range expression.ExtractColumns(e) {
			if col.VirtualExpr == nil {
				continue
			}
			if _, ok := virtualColumnTiFlashExpr(sctx, col); !ok {
				return true
			}
		}
	}
	return false
}

// substituteVirtualColumns replaces every virtual column in expr with its
// generation expression, repeating for generated columns that are defined on
// other generated columns. It fails if any virtual column cannot be replaced.
func substituteVirtualColumns(sctx base.PlanContext, expr expression.Expression) (expression.Expression, bool) {
	evalCtx := sctx.GetExprCtx().GetEvalCtx()
	// A generation expression can only reference earlier columns, so the depth
	// is bounded by the number of generated columns; cap it defensively.
	for range 16 {
		var cols []*expression.Column
		var exprs []expression.Expression
		for _, col := range expression.ExtractColumns(expr) {
			if col.VirtualExpr == nil {
				continue
			}
			if !VirtualExprPreservesValue(evalCtx, col) {
				return nil, false
			}
			cols = append(cols, col)
			exprs = append(exprs, col.VirtualExpr.Clone())
		}
		if len(cols) == 0 {
			return expr, true
		}
		hasFail, newExpr := expression.ColumnSubstituteAll(sctx.GetExprCtx(), expr, expression.NewSchema(cols...), exprs)
		if hasFail {
			return nil, false
		}
		expr = newExpr
	}
	return nil, false
}

// VirtualExprPreservesValue reports whether a virtual column always equals its
// generation expression. The stored value is the expression cast to the
// column's type, so the two can differ when that cast changes the value
// (numeric precision, CHAR padding, truncation to a shorter length, a
// different charset/collation, or NOT NULL coercing a NULL). Accept identical
// types, and string expressions whose charset and collation match a
// VARCHAR/TEXT/BLOB column that is long enough for any value of the
// expression, compared in bytes for TEXT/BLOB and allowing for case mapping
// growing a value in bytes; in both cases a NOT NULL column needs a NOT NULL
// expression. A value can't be relied on to fail the write instead: ADD COLUMN
// doesn't check existing rows, and non-strict mode truncates.
func VirtualExprPreservesValue(ctx expression.EvalContext, col *expression.Column) bool {
	exprTp := col.VirtualExpr.GetType(ctx)
	colTp := col.RetType
	if exprTp.IsArray() || colTp.IsArray() {
		return false
	}
	// A NOT NULL column stores a non-NULL value (e.g. 0 under INSERT IGNORE)
	// where its expression is NULL, so the two are not interchangeable.
	// FieldType.Equal does not compare the flag (see issue #61290).
	if mysql.HasNotNullFlag(colTp.GetFlag()) && !mysql.HasNotNullFlag(exprTp.GetFlag()) {
		return false
	}
	if exprTp.Equal(colTp) {
		return true
	}
	if exprTp.EvalType() != types.ETString || colTp.EvalType() != types.ETString {
		return false
	}
	if exprTp.GetCharset() != colTp.GetCharset() || exprTp.GetCollate() != colTp.GetCollate() {
		return false
	}
	switch colTp.GetType() {
	case mysql.TypeVarchar, mysql.TypeVarString, mysql.TypeTinyBlob, mysql.TypeBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob:
	default:
		return false
	}
	switch exprTp.GetType() {
	case mysql.TypeVarchar, mysql.TypeVarString, mysql.TypeTinyBlob, mysql.TypeBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob:
	default:
		return false
	}
	exprLen, colLen := exprTp.GetFlen(), colTp.GetFlen()
	if exprLen == types.UnspecifiedLength || colLen == types.UnspecifiedLength {
		return false
	}
	if !types.IsTypeBlob(colTp.GetType()) {
		// A VARCHAR column's length is in characters, and case mapping maps one
		// character to one character.
		return exprLen <= colLen
	}
	// A TEXT/BLOB column's length is in bytes. A VARCHAR expression's length is
	// in characters, unless the expression only reads TEXT/BLOB columns, whose
	// byte lengths it inherits (lower(text) is var_string(65535)).
	exprBytes := exprLen
	if !types.IsTypeBlob(exprTp.GetType()) && !readsOnlyBlobStrings(col.VirtualExpr) {
		cs, err := charset.GetCharsetInfo(exprTp.GetCharset())
		if err != nil {
			return false
		}
		exprBytes *= cs.Maxlen
	}
	// Case mapping keeps the declared length but can grow a value in bytes: in
	// UTF-8 by up to half (lower(U+023A) and upper(U+023F) are 2 bytes to 3).
	// Unless the column has room for that, a value can be truncated where the
	// expression isn't - for rows that existed when the column was added (ADD
	// COLUMN doesn't check them) or that were written in non-strict mode.
	if containsCaseMapping(col.VirtualExpr) {
		switch exprTp.GetCharset() {
		case charset.CharsetUTF8, charset.CharsetUTF8MB4:
			exprBytes += (exprBytes + 1) / 2
		default:
			return false
		}
	}
	return exprBytes <= colLen
}

// containsCaseMapping reports whether expr calls LOWER or UPPER.
func containsCaseMapping(expr expression.Expression) bool {
	f, ok := expr.(*expression.ScalarFunction)
	if !ok {
		return false
	}
	switch f.FuncName.L {
	case ast.Lower, ast.Lcase, ast.Upper, ast.Ucase:
		return true
	}
	for _, arg := range f.GetArgs() {
		if containsCaseMapping(arg) {
			return true
		}
	}
	return false
}

// readsOnlyBlobStrings reports whether every string column expr reads is a
// TEXT/BLOB column.
func readsOnlyBlobStrings(expr expression.Expression) bool {
	for _, c := range expression.ExtractColumns(expr) {
		if c.RetType.EvalType() == types.ETString && !types.IsTypeBlob(c.RetType.GetType()) {
			return false
		}
	}
	return true
}

// PhysPlanPartInfo indicates partition helper info in physical plan.
type PhysPlanPartInfo struct {
	PruningConds   []expression.Expression
	PartitionNames []ast.CIStr
	Columns        []*expression.Column
	ColumnNames    types.NameSlice
}

// GetPruningConds returns the pruning conditions for partition pruning.
func (pi *PhysPlanPartInfo) GetPruningConds() []expression.Expression {
	return pi.PruningConds
}

// GetPartitionNames returns the partition names.
func (pi *PhysPlanPartInfo) GetPartitionNames() []ast.CIStr {
	return pi.PartitionNames
}

// GetColumns returns the columns used for partition pruning.
func (pi *PhysPlanPartInfo) GetColumns() []*expression.Column {
	return pi.Columns
}

// GetColumnNames returns the column names used for partition pruning.
func (pi *PhysPlanPartInfo) GetColumnNames() types.NameSlice {
	return pi.ColumnNames
}

const emptyPartitionInfoSize = int64(unsafe.Sizeof(PhysPlanPartInfo{}))

// CloneForPlanCache clones the PhysPlanPartInfo for plan cache.
func (pi *PhysPlanPartInfo) CloneForPlanCache() *PhysPlanPartInfo {
	if pi == nil {
		return nil
	}
	cloned := new(PhysPlanPartInfo)
	cloned.PruningConds = utilfuncp.CloneExpressionsForPlanCache(pi.PruningConds, nil)
	cloned.PartitionNames = pi.PartitionNames
	cloned.Columns = utilfuncp.CloneColumnsForPlanCache(pi.Columns, nil)
	cloned.ColumnNames = pi.ColumnNames
	return cloned
}

// Clone clones the PhysPlanPartInfo.
func (pi *PhysPlanPartInfo) Clone() *PhysPlanPartInfo {
	if pi == nil {
		return nil
	}
	cloned := new(PhysPlanPartInfo)
	cloned.PruningConds = plannerutil.CloneExprs(pi.PruningConds)
	cloned.PartitionNames = append([]ast.CIStr(nil), pi.PartitionNames...)
	cloned.Columns = plannerutil.CloneCols(pi.Columns)
	cloned.ColumnNames = types.NameSlice(plannerutil.CloneFieldNames(pi.ColumnNames))
	return cloned
}

// MemoryUsage return the memory usage of PhysPlanPartInfo
func (pi *PhysPlanPartInfo) MemoryUsage() (sum int64) {
	if pi == nil {
		return
	}

	sum = emptyPartitionInfoSize
	for _, cond := range pi.PruningConds {
		sum += cond.MemoryUsage()
	}
	for _, cis := range pi.PartitionNames {
		sum += cis.MemoryUsage()
	}
	for _, col := range pi.Columns {
		sum += col.MemoryUsage()
	}
	for _, colName := range pi.ColumnNames {
		sum += colName.MemoryUsage()
	}
	return
}

// TableScanAndPartitionInfo is to save PhysicalTableScan and PhysPlanPartInfo Info
type TableScanAndPartitionInfo struct {
	// TODO(hawkingrei): we can make it private after moving PhysicalTableReader into physicalop
	TableScan        *PhysicalTableScan
	PhysPlanPartInfo *PhysPlanPartInfo
}

// MemoryUsage return the memory usage of TableScanAndPartitionInfo
func (t *TableScanAndPartitionInfo) MemoryUsage() (sum int64) {
	if t == nil {
		return
	}

	sum += t.PhysPlanPartInfo.MemoryUsage()
	if t.TableScan != nil {
		sum += t.TableScan.MemoryUsage()
	}
	return
}

// RuntimeFilterType "IN"
type RuntimeFilterType = variable.RuntimeFilterType

// RuntimeFilterMode "OFF", "LOCAL"
type RuntimeFilterMode = variable.RuntimeFilterMode

// RuntimeFilter structure is generated by the Runtime Filter Generator.
// At present, it has a one-to-one correspondence with the equivalent expression in HashJoin.
// For example:
// Query: select * from t1, t2 where t1.k1=t2.k1
// PhysicalPlanTree:
//
//	HashJoin_2(t1.k2=t2.k1)
//	/           \
//
// TableScan_0(t1)     ExchangeNode_1
// RuntimeFilter struct:
//
//	id: 0
//	buildNode: HashJoin
//	srcExprList: [t2.k1]
//	targetExprList: [t1.k1]
//	rfType:  IN
//	rfMode: LOCAL
//	buildNodeID: 2
//	targetNodeID: 0
//
// Although srcExprList and targetExprList attributes are lists, in fact, there will only be one value in them at present.
// The reason why it is designed as a list is because:
// Reserve an interface for subsequent multiple join equivalent expressions corresponding to one rf.
// (Since IN supports multiple columns, this construction can be more efficient at the execution level)
type RuntimeFilter struct {
	// runtime filter id, unique in one query plan
	id             int
	BuildNode      *PhysicalHashJoin
	srcExprList    []*expression.Column
	targetExprList []*expression.Column
	rfType         RuntimeFilterType
	// The following properties need to be set after assigning a scan node to RF
	RfMode     RuntimeFilterMode
	targetNode *PhysicalTableScan
	// The plan id will be set when runtime filter clone()
	// It is only used for runtime filter pb
	buildNodeID  int
	targetNodeID int
}

// NewRuntimeFilter construct runtime filter by Join and equal predicate of Join
func NewRuntimeFilter(rfIDGenerator *util.IDGenerator, eqPredicate *expression.ScalarFunction, buildNode *PhysicalHashJoin) ([]*RuntimeFilter, int64) {
	rightSideIsBuild := buildNode.RightIsBuildSide()
	var srcExprList []*expression.Column
	var targetExprUniqueID int64
	l, r := expression.ExtractColumnsFromColOpCol(eqPredicate)
	if rightSideIsBuild {
		srcExprList = append(srcExprList, r)
		targetExprUniqueID = l.UniqueID
	} else {
		srcExprList = append(srcExprList, l)
		targetExprUniqueID = r.UniqueID
	}

	rfTypes := buildNode.SCtx().GetSessionVars().GetRuntimeFilterTypes()
	result := make([]*RuntimeFilter, 0, len(rfTypes))
	for _, rfType := range rfTypes {
		rf := &RuntimeFilter{
			id:          rfIDGenerator.GetNextID(),
			BuildNode:   buildNode,
			srcExprList: srcExprList,
			rfType:      rfType,
		}
		result = append(result, rf)
	}
	return result, targetExprUniqueID
}

// Assign is to assign PhysicalTableScan's info and exprssion into RuntimeFilter
func (rf *RuntimeFilter) Assign(targetNode *PhysicalTableScan,
	targetExpr *expression.Column) {
	rf.targetNode = targetNode
	if len(rf.targetNode.runtimeFilterList) == 0 {
		// todo use session variables instead
		rf.targetNode.maxWaitTimeMs = 10000
	}
	rf.targetExprList = append(rf.targetExprList, targetExpr)
	rf.BuildNode.runtimeFilterList = append(rf.BuildNode.runtimeFilterList, rf)
	rf.targetNode.runtimeFilterList = append(rf.targetNode.runtimeFilterList, rf)
	logutil.BgLogger().Debug("Assign RF to target node",
		zap.String("RuntimeFilter", rf.String()))
}

// ExplainInfo explain info of runtime filter
func (rf *RuntimeFilter) ExplainInfo(isBuildNode bool, ctx expression.EvalContext) string {
	var builder strings.Builder
	fmt.Fprintf(&builder, "%d[%s]", rf.id, rf.rfType)
	if isBuildNode {
		fmt.Fprintf(&builder, " <- ")
		for i, srcExpr := range rf.srcExprList {
			if i != 0 {
				fmt.Fprintf(&builder, ",")
			}
			fmt.Fprintf(&builder, "%s", srcExpr.ExplainInfo(ctx))
		}
	} else {
		fmt.Fprintf(&builder, " -> ")
		for i, targetExpr := range rf.targetExprList {
			if i != 0 {
				fmt.Fprintf(&builder, ",")
			}
			fmt.Fprintf(&builder, "%s", targetExpr.ExplainInfo(ctx))
		}
	}
	return builder.String()
}

func (rf *RuntimeFilter) String() string {
	var builder strings.Builder
	fmt.Fprintf(&builder, "id=%d", rf.id)
	builder.WriteString(", ")
	fmt.Fprintf(&builder, "buildNodeID=%d", rf.BuildNode.ID())
	builder.WriteString(", ")
	if rf.targetNode == nil {
		fmt.Fprintf(&builder, "targetNodeID=nil")
	} else {
		fmt.Fprintf(&builder, "targetNodeID=%d", rf.targetNode.ID())
	}
	builder.WriteString(", ")
	fmt.Fprintf(&builder, "srcColumn=")
	for _, srcExpr := range rf.srcExprList {
		fmt.Fprintf(&builder, "%s,", srcExpr.String())
	}
	builder.WriteString(", ")
	fmt.Fprintf(&builder, "targetColumn=")
	for _, targetExpr := range rf.targetExprList {
		fmt.Fprintf(&builder, "%s,", targetExpr.String())
	}
	builder.WriteString(", ")
	fmt.Fprintf(&builder, "rfType=%s", rf.rfType)
	builder.WriteString(", ")
	if rf.RfMode == 0 {
		fmt.Fprintf(&builder, "rfMode=nil")
	} else {
		fmt.Fprintf(&builder, "rfMode=%s", rf.RfMode)
	}
	builder.WriteString(".")
	return builder.String()
}

// Clone deep copy of runtime filter
func (rf *RuntimeFilter) Clone() *RuntimeFilter {
	cloned := new(RuntimeFilter)
	cloned.id = rf.id
	// Because build node only needs to get its executor id attribute when converting to pb format,
	// so we only copy explain id here
	if rf.BuildNode == nil {
		cloned.buildNodeID = rf.buildNodeID
	} else {
		cloned.buildNodeID = rf.BuildNode.ID()
	}
	if rf.targetNode == nil {
		cloned.targetNodeID = rf.targetNodeID
	} else {
		cloned.targetNodeID = rf.targetNode.ID()
	}

	for _, srcExpr := range rf.srcExprList {
		cloned.srcExprList = append(cloned.srcExprList, srcExpr.Clone().(*expression.Column))
	}
	for _, targetExpr := range rf.targetExprList {
		cloned.targetExprList = append(cloned.targetExprList, targetExpr.Clone().(*expression.Column))
	}
	cloned.rfType = rf.rfType
	cloned.RfMode = rf.RfMode
	return cloned
}

// RuntimeFilterListToPB convert runtime filter list to PB list
func RuntimeFilterListToPB(ctx *base.BuildPBContext, runtimeFilterList []*RuntimeFilter, client kv.Client) ([]*tipb.RuntimeFilter, error) {
	result := make([]*tipb.RuntimeFilter, 0, len(runtimeFilterList))
	for _, runtimeFilter := range runtimeFilterList {
		rfPB, err := runtimeFilter.ToPB(ctx, client)
		if err != nil {
			return nil, err
		}
		result = append(result, rfPB)
	}
	return result, nil
}

// ToPB convert runtime filter to PB
func (rf *RuntimeFilter) ToPB(ctx *base.BuildPBContext, client kv.Client) (*tipb.RuntimeFilter, error) {
	ectx := ctx.GetExprCtx().GetEvalCtx()
	pc := expression.NewPBConverter(client, ectx)
	srcExprListPB := make([]*tipb.Expr, 0, len(rf.srcExprList))
	for _, srcExpr := range rf.srcExprList {
		srcExprPB := pc.ExprToPB(srcExpr)
		if srcExprPB == nil {
			return nil, plannererrors.ErrInternal.GenWithStack("failed to transform src expr %s to pb in runtime filter", srcExpr.StringWithCtx(ectx, errors.RedactLogDisable))
		}
		srcExprListPB = append(srcExprListPB, srcExprPB)
	}
	targetExprListPB := make([]*tipb.Expr, 0, len(rf.targetExprList))
	for _, targetExpr := range rf.targetExprList {
		targetExprPB := pc.ExprToPB(targetExpr)
		if targetExprPB == nil {
			return nil, plannererrors.ErrInternal.GenWithStack("failed to transform target expr %s to pb in runtime filter", targetExpr.StringWithCtx(ectx, errors.RedactLogDisable))
		}
		targetExprListPB = append(targetExprListPB, targetExprPB)
	}
	rfTypePB := tipb.RuntimeFilterType_IN
	switch rf.rfType {
	case variable.In:
		rfTypePB = tipb.RuntimeFilterType_IN
	case variable.MinMax:
		rfTypePB = tipb.RuntimeFilterType_MIN_MAX
	}
	rfModePB := tipb.RuntimeFilterMode_LOCAL
	switch rf.RfMode {
	case variable.RFLocal:
		rfModePB = tipb.RuntimeFilterMode_LOCAL
	case variable.RFGlobal:
		rfModePB = tipb.RuntimeFilterMode_GLOBAL
	}
	result := &tipb.RuntimeFilter{
		Id:               int32(rf.id),
		SourceExprList:   srcExprListPB,
		TargetExprList:   targetExprListPB,
		SourceExecutorId: fmt.Sprint(rf.buildNodeID),
		TargetExecutorId: fmt.Sprint(rf.targetNodeID),
		RfType:           rfTypePB,
		RfMode:           rfModePB,
	}
	return result, nil
}
