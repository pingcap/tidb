// Copyright 2018 PingCAP, Inc.
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

package tables

import (
	"bytes"
	"context"
	stderr "errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/mock"
	"go.uber.org/zap"
)

// Both partition and partitionedTable implement the table.Table interface.
var _ table.Table = &partition{}
var _ table.Table = &partitionedTable{}

// partitionedTable implements the table.PartitionedTable interface.
var _ table.PartitionedTable = &partitionedTable{}

// partition is a feature from MySQL:
// See https://dev.mysql.com/doc/refman/8.0/en/partitioning.html
// A partition table may contain many partitions, each partition has a unique partition
// id. The underlying representation of a partition and a normal table (a table with no
// partitions) is basically the same.
// partition also implements the table.Table interface.
type partition struct {
	TableCommon
}

// GetPhysicalID implements table.Table GetPhysicalID interface.
func (p *partition) GetPhysicalID() int64 {
	return p.physicalTableID
}

// partitionedTable implements the table.PartitionedTable interface.
// partitionedTable is a table, it contains many Partitions.
type partitionedTable struct {
	TableCommon
	partitionExpr   *PartitionExpr
	partitions      map[int64]*partition
	evalBufferTypes []*types.FieldType
	evalBufferPool  sync.Pool
}

func newPartitionedTable(tbl *TableCommon, tblInfo *model.TableInfo) (table.Table, error) {
	ret := &partitionedTable{TableCommon: *tbl}
	partitionExpr, err := newPartitionExpr(tblInfo)
	if err != nil {
		return nil, errors.Trace(err)
	}
	ret.partitionExpr = partitionExpr
	initEvalBufferType(ret)
	ret.evalBufferPool = sync.Pool{
		New: func() interface{} {
			return initEvalBuffer(ret)
		},
	}
	if err := initTableIndices(&ret.TableCommon); err != nil {
		return nil, errors.Trace(err)
	}
	pi := tblInfo.GetPartitionInfo()
	partitions := make(map[int64]*partition, len(pi.Definitions))
	for _, p := range pi.Definitions {
		var t partition
		err := initTableCommonWithIndices(&t.TableCommon, tblInfo, p.ID, tbl.Columns, tbl.allocs)
		if err != nil {
			return nil, errors.Trace(err)
		}
		partitions[p.ID] = &t
	}
	ret.partitions = partitions
	return ret, nil
}

func newPartitionExpr(tblInfo *model.TableInfo) (*PartitionExpr, error) {
	ctx := mock.NewContext()
	dbName := model.NewCIStr(ctx.GetSessionVars().CurrentDB)
	columns, names, err := expression.ColumnInfos2ColumnsAndNames(ctx, dbName, tblInfo.Name, tblInfo.Cols(), tblInfo)
	if err != nil {
		return nil, err
	}
	pi := tblInfo.GetPartitionInfo()
	switch pi.Type {
	case model.PartitionTypeRange:
		return generateRangePartitionExpr(ctx, pi, columns, names)
	case model.PartitionTypeHash:
		return generateHashPartitionExpr(ctx, pi, columns, names)
	case model.PartitionTypeList:
		return generateListPartitionExpr(ctx, tblInfo, columns, names)
	}
	panic("cannot reach here")
}

// PartitionExpr is the partition definition expressions.
type PartitionExpr struct {
	// UpperBounds: (x < y1); (x < y2); (x < y3), used by locatePartition.
	UpperBounds []expression.Expression
	// OrigExpr is the partition expression ast used in point get.
	OrigExpr ast.ExprNode
	// Expr is the hash partition expression.
	Expr expression.Expression
	// Used in the range pruning process.
	*ForRangePruning
	// Used in the range column pruning process.
	*ForRangeColumnsPruning
	// ColOffset is the offsets of partition columns.
	ColumnOffset []int
	// InValues: x in (1,2); x in (3,4); x in (5,6), used for list partition.
	InValues []expression.Expression
	*ForListPruning
}

func initEvalBufferType(t *partitionedTable) {
	hasExtraHandle := false
	numCols := len(t.Cols())
	if !t.Meta().PKIsHandle {
		hasExtraHandle = true
		numCols++
	}
	t.evalBufferTypes = make([]*types.FieldType, numCols)
	for i, col := range t.Cols() {
		t.evalBufferTypes[i] = &col.FieldType
	}

	if hasExtraHandle {
		t.evalBufferTypes[len(t.evalBufferTypes)-1] = types.NewFieldType(mysql.TypeLonglong)
	}
}

func initEvalBuffer(t *partitionedTable) *chunk.MutRow {
	evalBuffer := chunk.MutRowFromTypes(t.evalBufferTypes)
	return &evalBuffer
}

// ForRangeColumnsPruning is used for range partition pruning.
type ForRangeColumnsPruning struct {
	LessThan []expression.Expression
	MaxValue bool
}

func dataForRangeColumnsPruning(ctx sessionctx.Context, pi *model.PartitionInfo, schema *expression.Schema, names []*types.FieldName, p *parser.Parser) (*ForRangeColumnsPruning, error) {
	var res ForRangeColumnsPruning
	res.LessThan = make([]expression.Expression, len(pi.Definitions))
	for i := 0; i < len(pi.Definitions); i++ {
		if strings.EqualFold(pi.Definitions[i].LessThan[0], "MAXVALUE") {
			// Use a bool flag instead of math.MaxInt64 to avoid the corner cases.
			res.MaxValue = true
		} else {
			tmp, err := parseSimpleExprWithNames(p, ctx, pi.Definitions[i].LessThan[0], schema, names)
			if err != nil {
				return nil, err
			}
			res.LessThan[i] = tmp
		}
	}
	return &res, nil
}

// parseSimpleExprWithNames parses simple expression string to Expression.
// The expression string must only reference the column in the given NameSlice.
func parseSimpleExprWithNames(p *parser.Parser, ctx sessionctx.Context, exprStr string, schema *expression.Schema, names types.NameSlice) (expression.Expression, error) {
	exprNode, err := parseExpr(p, exprStr)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return expression.RewriteSimpleExprWithNames(ctx, exprNode, schema, names)
}

// ForListPruning is used for list partition pruning.
type ForListPruning struct {
	// LocateExpr uses to locate partition by row.
	LocateExpr expression.Expression
	// PruneExpr uses to prune partition in partition pruner.
	PruneExpr expression.Expression
	ExprCols  []*expression.Column
	// valueMap is column value -> partition idx
	valueMap map[int64]int
	// nullPartitionIdx is the partition idx for null value.
	nullPartitionIdx int

	// For list columns partition
	ColPrunes []*ForListColumnPruning
}

// ForListColumnPruning is used for list columns partition pruning.
type ForListColumnPruning struct {
	ExprCol  *expression.Column
	valueTp  *types.FieldType
	valueMap map[string]ListPartitionLocation
}

type ListPartitionLocation []ListPartitionGroup

func (ps ListPartitionLocation) IsEmpty() bool {
	for _, pg := range ps {
		if len(pg.GroupIdxs) > 0 {
			return false
		}
	}
	return true
}

func (ps ListPartitionLocation) FindByPartitionIdx(partIdx int) int {
	for i, p := range ps {
		if p.PartIdx == partIdx {
			return i
		}
	}
	return -1
}

func (ps ListPartitionLocation) GetPartitionIdx() map[int]struct{} {
	pidxs := make(map[int]struct{}, len(ps))
	for _, p := range ps {
		pidxs[p.PartIdx] = struct{}{}
	}
	return pidxs
}

type listPartitionLocationMerger struct {
	location ListPartitionLocation
}

func NewListPartitionLocationMerger() *listPartitionLocationMerger {
	return &listPartitionLocationMerger{}
}

func (p *listPartitionLocationMerger) GetLocation() ListPartitionLocation {
	return p.location
}

func (p *listPartitionLocationMerger) MergePartitionGroup(pg ListPartitionGroup) {
	idx := p.location.FindByPartitionIdx(pg.PartIdx)
	if idx < 0 {
		p.location = append(p.location, pg.Copy())
		return
	}
	p.location[idx].GroupIdxs = append(p.location[idx].GroupIdxs, pg.GroupIdxs...)
}

func (p *listPartitionLocationMerger) Merge(location ListPartitionLocation) {
	for _, pg := range location {
		p.MergePartitionGroup(pg)
	}
}

type listPartitionLocationIntersect struct {
	initialized bool
	location    ListPartitionLocation
}

func NewListPartitionLocationIntersect() *listPartitionLocationIntersect {
	return &listPartitionLocationIntersect{}
}

func (p *listPartitionLocationIntersect) GetLocation() ListPartitionLocation {
	return p.location
}

func (p *listPartitionLocationIntersect) Intersect(inputPgs []ListPartitionGroup) bool {
	if !p.initialized {
		p.initialized = true
		p.location = make([]ListPartitionGroup, 0, len(inputPgs))
		p.location = append(p.location, inputPgs...)
		return true
	}
	currPgs := p.location
	var remainPgs []ListPartitionGroup
	for _, inputPg := range inputPgs {
		pgIdx := currPgs.FindByPartitionIdx(inputPg.PartIdx)
		if pgIdx < 0 {
			continue
		}
		pg := currPgs[pgIdx]
		if !pg.Intersect(inputPg) {
			continue
		}
		remainPgs = append(remainPgs, pg)
	}
	p.location = remainPgs
	return len(remainPgs) > 0
}

func (pg *ListPartitionGroup) Intersect(inputPg ListPartitionGroup) bool {
	if pg.PartIdx != inputPg.PartIdx {
		return false
	}
	groupIdxs := make([]int, 0, 2)
	for _, gidx := range inputPg.GroupIdxs {
		if pg.findGroupIdx(gidx) {
			groupIdxs = append(groupIdxs, gidx)
		}
	}
	pg.GroupIdxs = groupIdxs
	return len(groupIdxs) > 0
}

func (pg *ListPartitionGroup) findGroupIdx(inputGroupIdx int) bool {
	for _, gidx := range pg.GroupIdxs {
		if gidx == inputGroupIdx {
			return true
		}
	}
	return false
}

//type ColumnValueToPartition struct {
//	// Such as: list columns (a,b) (partition p0 values in ((5,50)), partition p1 values in ((6,60),(7,70)));
//	// The ValueMap of column a will be:    {5: 0, 6: 1, 7: 1}.
//	// The ValueMap of column b will be:    {50:0, 60:1, 70:1}.
//	// So, if a = 6, use the ValueMap to locate on partition p1.
//	//
//	// The GroupIdxMap of column a will be: {5: 0, 6: 0, 7: 1}
//	// The GroupIdxMap of column b will be: {50:0, 60:0, 70:1}
//	// For the row `(a,b) values (6,70)`, if only use the ValueMap will locate on partition p1.
//	// But combine with GroupIdxMap, the value-group-index for `a=6` is `0`, but the value-group-index for `b=70` is 1,
//	// the value-group-index are different, so there is no partition for the row `(a,b) values (6,70)`.
//
//}

type ListPartitionGroup struct {
	PartIdx   int
	GroupIdxs []int
}

func (p ListPartitionGroup) Copy() ListPartitionGroup {
	groupIdxs := make([]int, len(p.GroupIdxs))
	copy(groupIdxs, p.GroupIdxs)
	return ListPartitionGroup{
		PartIdx:   p.PartIdx,
		GroupIdxs: groupIdxs,
	}
}

// ForRangePruning is used for range partition pruning.
type ForRangePruning struct {
	LessThan []int64
	MaxValue bool
	Unsigned bool
}

// dataForRangePruning extracts the less than parts from 'partition p0 less than xx ... partitoin p1 less than ...'
func dataForRangePruning(sctx sessionctx.Context, pi *model.PartitionInfo) (*ForRangePruning, error) {
	var maxValue bool
	var unsigned bool
	lessThan := make([]int64, len(pi.Definitions))
	for i := 0; i < len(pi.Definitions); i++ {
		if strings.EqualFold(pi.Definitions[i].LessThan[0], "MAXVALUE") {
			// Use a bool flag instead of math.MaxInt64 to avoid the corner cases.
			maxValue = true
		} else {
			var err error
			lessThan[i], err = strconv.ParseInt(pi.Definitions[i].LessThan[0], 10, 64)
			var numErr *strconv.NumError
			if stderr.As(err, &numErr) && numErr.Err == strconv.ErrRange {
				var tmp uint64
				tmp, err = strconv.ParseUint(pi.Definitions[i].LessThan[0], 10, 64)
				lessThan[i] = int64(tmp)
				unsigned = true
			}
			if err != nil {
				val, ok := fixOldVersionPartitionInfo(sctx, pi.Definitions[i].LessThan[0])
				if !ok {
					logutil.BgLogger().Error("wrong partition definition", zap.String("less than", pi.Definitions[i].LessThan[0]))
					return nil, errors.WithStack(err)
				}
				lessThan[i] = val
			}
		}
	}
	return &ForRangePruning{
		LessThan: lessThan,
		MaxValue: maxValue,
		Unsigned: unsigned,
	}, nil
}

func fixOldVersionPartitionInfo(sctx sessionctx.Context, str string) (int64, bool) {
	// less than value should be calculate to integer before persistent.
	// Old version TiDB may not do it and store the raw expression.
	tmp, err := parseSimpleExprWithNames(parser.New(), sctx, str, nil, nil)
	if err != nil {
		return 0, false
	}
	ret, isNull, err := tmp.EvalInt(sctx, chunk.Row{})
	if err != nil || isNull {
		return 0, false
	}
	return ret, true
}

// rangePartitionString returns the partition string for a range typed partition.
func rangePartitionString(pi *model.PartitionInfo) string {
	// partition by range expr
	if len(pi.Columns) == 0 {
		return pi.Expr
	}

	// partition by range columns (c1)
	if len(pi.Columns) == 1 {
		return "`" + pi.Columns[0].L + "`"
	}

	// partition by range columns (c1, c2, ...)
	panic("create table assert len(columns) = 1")
}

func generateRangePartitionExpr(ctx sessionctx.Context, pi *model.PartitionInfo,
	columns []*expression.Column, names types.NameSlice) (*PartitionExpr, error) {
	// The caller should assure partition info is not nil.
	locateExprs := make([]expression.Expression, 0, len(pi.Definitions))
	var buf bytes.Buffer
	p := parser.New()
	schema := expression.NewSchema(columns...)
	partStr := rangePartitionString(pi)
	for i := 0; i < len(pi.Definitions); i++ {
		if strings.EqualFold(pi.Definitions[i].LessThan[0], "MAXVALUE") {
			// Expr less than maxvalue is always true.
			fmt.Fprintf(&buf, "true")
		} else {
			fmt.Fprintf(&buf, "((%s) < (%s))", partStr, pi.Definitions[i].LessThan[0])
		}

		expr, err := parseSimpleExprWithNames(p, ctx, buf.String(), schema, names)
		if err != nil {
			// If it got an error here, ddl may hang forever, so this error log is important.
			logutil.BgLogger().Error("wrong table partition expression", zap.String("expression", buf.String()), zap.Error(err))
			return nil, errors.Trace(err)
		}
		locateExprs = append(locateExprs, expr)
		buf.Reset()
	}
	ret := &PartitionExpr{
		UpperBounds: locateExprs,
	}

	// build column offset.
	partExp := pi.Expr
	if len(pi.Columns) == 1 {
		partExp = "`" + pi.Columns[0].L + "`"
	}
	exprs, err := parseSimpleExprWithNames(p, ctx, partExp, schema, names)
	if err != nil {
		return nil, err
	}
	partitionCols := expression.ExtractColumns(exprs)
	offset := make([]int, len(partitionCols))
	for i, col := range columns {
		for j, partitionCol := range partitionCols {
			if partitionCol.UniqueID == col.UniqueID {
				offset[j] = i
			}
		}
	}
	ret.ColumnOffset = offset

	switch len(pi.Columns) {
	case 0:
		tmp, err := dataForRangePruning(ctx, pi)
		if err != nil {
			return nil, errors.Trace(err)
		}
		ret.Expr = exprs
		ret.ForRangePruning = tmp
	case 1:
		tmp, err := dataForRangeColumnsPruning(ctx, pi, schema, names, p)
		if err != nil {
			return nil, errors.Trace(err)
		}
		ret.ForRangeColumnsPruning = tmp
	default:
		panic("range column partition currently support only one column")
	}
	return ret, nil
}

func getColumnsOffset(cols, columns []*expression.Column) []int {
	colsOffset := make([]int, len(cols))
	for i, col := range columns {
		if idx := findIdxByColUniqueID(cols, col); idx >= 0 {
			colsOffset[idx] = i
		}
	}
	return colsOffset
}

func findIdxByColUniqueID(cols []*expression.Column, col *expression.Column) int {
	for idx, c := range cols {
		if c.UniqueID == col.UniqueID {
			return idx
		}
	}
	return -1
}

func extractListPartitionExprColumns(ctx sessionctx.Context, pi *model.PartitionInfo, columns []*expression.Column, names types.NameSlice) ([]*expression.Column, []int, error) {
	var cols []*expression.Column
	if len(pi.Columns) == 0 {
		schema := expression.NewSchema(columns...)
		exprs, err := expression.ParseSimpleExprsWithNames(ctx, pi.Expr, schema, names)
		if err != nil {
			return nil, nil, err
		}
		cols = expression.ExtractColumns(exprs[0])
	} else {
		for _, col := range pi.Columns {
			idx := expression.FindFieldNameIdxByColName(names, col.L)
			if idx < 0 {
				panic("should never happen")
			}
			cols = append(cols, columns[idx])
		}
	}
	offset := getColumnsOffset(cols, columns)
	deDupCols := make([]*expression.Column, 0, len(cols))
	for _, col := range cols {
		if findIdxByColUniqueID(deDupCols, col) < 0 {
			c := col.Clone().(*expression.Column)
			//c.Index = len(deDupCols)
			deDupCols = append(deDupCols, c)
		}
	}
	return deDupCols, offset, nil
}

func generateListPartitionExpr(ctx sessionctx.Context, tblInfo *model.TableInfo,
	columns []*expression.Column, names types.NameSlice) (*PartitionExpr, error) {
	// The caller should assure partition info is not nil.
	pi := tblInfo.GetPartitionInfo()
	schema := expression.NewSchema(columns...)
	exprCols, offset, err := extractListPartitionExprColumns(ctx, pi, columns, names)
	if err != nil {
		return nil, err
	}
	p := parser.New()
	listPrune := &ForListPruning{}
	if len(pi.Columns) == 0 {
		err = listPrune.generateListPruneExpr(ctx, tblInfo, exprCols, schema, names, p)
	} else {
		// list columns pruner..
		err = listPrune.generateListColumnsPruneExpr(ctx, tblInfo, schema, columns, names, p)
	}
	ret := &PartitionExpr{
		ForListPruning: listPrune,
		ColumnOffset:   offset,
	}
	return ret, nil
}

func (lp *ForListPruning) generateListPruneExpr(ctx sessionctx.Context, tblInfo *model.TableInfo, exprCols []*expression.Column,
	schema *expression.Schema, names types.NameSlice, p *parser.Parser) error {
	pi := tblInfo.GetPartitionInfo()
	expr, err := parseSimpleExprWithNames(p, ctx, pi.Expr, schema, names)
	if err != nil {
		// If it got an error here, ddl may hang forever, so this error log is important.
		logutil.BgLogger().Error("wrong table partition expression", zap.String("expression", pi.Expr), zap.Error(err))
		return errors.Trace(err)
	}
	pruneExpr, _ := updateExprColIdxForPartitionPrune(expr, exprCols)
	lp.LocateExpr = expr.Clone()
	lp.PruneExpr = pruneExpr
	lp.ExprCols = exprCols
	err = lp.buildListPartitionValueMap(ctx, tblInfo, schema, names, p)
	if err != nil {
		return err
	}
	return nil
}

func (lp *ForListPruning) generateListColumnsPruneExpr(ctx sessionctx.Context, tblInfo *model.TableInfo,
	schema *expression.Schema, columns []*expression.Column, names types.NameSlice, p *parser.Parser) error {
	pi := tblInfo.GetPartitionInfo()
	if len(pi.Columns) == 0 {
		return nil
	}
	colPrunes := make([]*ForListColumnPruning, 0, len(pi.Columns))
	for colIdx := range pi.Columns {
		colInfo := model.FindColumnInfo(tblInfo.Columns, pi.Columns[colIdx].L)
		if colInfo == nil {
			panic("should never happen")
		}
		idx := expression.FindFieldNameIdxByColName(names, pi.Columns[colIdx].L)
		if idx < 0 {
			panic("should never happen")
		}
		colPrune := &ForListColumnPruning{
			ExprCol:  columns[idx],
			valueTp:  &colInfo.FieldType,
			valueMap: make(map[string]ListPartitionLocation),
		}
		err := colPrune.buildPartitionValueMap(ctx, tblInfo, colIdx, schema, names, p)
		if err != nil {
			return err
		}
		colPrunes = append(colPrunes, colPrune)
	}
	lp.ColPrunes = colPrunes
	return nil
}

func updateExprColIdxForPartitionPrune(expr expression.Expression, deDupCols []*expression.Column) (expression.Expression, []*expression.Column) {
	// Since need to change the column index of the expresion, clone the expression first.
	expr = expr.Clone()
	cols := expression.ExtractColumns(expr)
	for _, c := range cols {
		idx := findIdxByColUniqueID(deDupCols, c)
		if idx < 0 {
			panic("should never happen")
		}
		c.Index = idx
	}
	return expr, cols
}

// buildListColumnValueToPartitionMap builds list column value map.
// The map is column value -> partition index.
// colIdx is the column index in the list columns.
func (b *ForListPruning) buildListPartitionValueMap(ctx sessionctx.Context, tblInfo *model.TableInfo,
	schema *expression.Schema, names types.NameSlice, p *parser.Parser) error {
	pi := tblInfo.GetPartitionInfo()
	b.valueMap = map[int64]int{}
	b.nullPartitionIdx = -1
	for partitionIdx, def := range pi.Definitions {
		for _, vs := range def.InValues {
			expr, err := parseSimpleExprWithNames(p, ctx, vs[0], schema, names)
			if err != nil {
				return errors.Trace(err)
			}
			v, isNull, err := expr.EvalInt(ctx, chunk.Row{})
			if err != nil {
				return errors.Trace(err)
			}
			if isNull {
				b.nullPartitionIdx = partitionIdx
				continue
			}
			b.valueMap[v] = partitionIdx
		}
	}
	return nil
}

// buildListColumnValueToPartitionMap builds list column value map.
// The map is column value -> partition index.
// colIdx is the column index in the list columns.
func (b *ForListColumnPruning) buildPartitionValueMap(ctx sessionctx.Context, tblInfo *model.TableInfo, colIdx int,
	schema *expression.Schema, names types.NameSlice, p *parser.Parser) error {
	pi := tblInfo.GetPartitionInfo()
	sc := ctx.GetSessionVars().StmtCtx
	for partitionIdx, def := range pi.Definitions {
		for groupIdx, vs := range def.InValues {
			key, err := b.genConstExprKey(ctx, sc, vs[colIdx], schema, names, p)
			if err != nil {
				return errors.Trace(err)
			}
			location, ok := b.valueMap[key]
			if ok {
				idx := location.FindByPartitionIdx(partitionIdx)
				if idx != -1 {
					location[idx].GroupIdxs = append(location[idx].GroupIdxs, groupIdx)
					continue
				}
			}
			location = append(location, ListPartitionGroup{
				PartIdx:   partitionIdx,
				GroupIdxs: []int{groupIdx},
			})
			b.valueMap[key] = location
		}
	}
	return nil
}

func (b *ForListColumnPruning) genConstExprKey(ctx sessionctx.Context, sc *stmtctx.StatementContext, exprStr string,
	schema *expression.Schema, names types.NameSlice, p *parser.Parser) (string, error) {
	expr, err := parseSimpleExprWithNames(p, ctx, exprStr, schema, names)
	if err != nil {
		return "", errors.Trace(err)
	}
	v, err := expr.Eval(chunk.Row{})
	if err != nil {
		return "", errors.Trace(err)
	}
	key, err := b.genKey(sc, v)
	if err != nil {
		return "", errors.Trace(err)
	}
	return string(key), nil
}

func (b *ForListColumnPruning) genKey(sc *stmtctx.StatementContext, v types.Datum) ([]byte, error) {
	v, err := v.ConvertTo(sc, b.valueTp)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return codec.EncodeKey(sc, nil, v)
}

// LocatePartition locates partition by the column value
func (b *ForListPruning) LocatePartition(value int64, isNull bool) (int, error) {
	if isNull {
		return b.nullPartitionIdx, nil
	}
	partitionIdx, ok := b.valueMap[value]
	if !ok {
		return -1, nil
	}
	return partitionIdx, nil
}

// LocatePartition locates partition by the column value
func (b *ForListColumnPruning) LocatePartition(sc *stmtctx.StatementContext, v types.Datum) (ListPartitionLocation, error) {
	key, err := b.genKey(sc, v)
	if err != nil {
		return nil, errors.Trace(err)
	}
	location, ok := b.valueMap[string(key)]
	if !ok {
		return nil, nil
	}
	return location, nil
}

func generateListColumnsPartitionExprStr(ctx sessionctx.Context, pi *model.PartitionInfo,
	schema *expression.Schema, names types.NameSlice, def *model.PartitionDefinition, p *parser.Parser) (string, error) {
	var partStr string
	if len(pi.Columns) == 0 {
		partStr = pi.Expr
	} else if len(pi.Columns) == 1 {
		partStr = "`" + pi.Columns[0].L + "`"
	} else {
		return generateMultiListColumnsPartitionExprStr(ctx, pi, schema, names, def, p)
	}
	var buf, nullCondBuf bytes.Buffer
	fmt.Fprintf(&buf, "(%s in (", partStr)
	for i, vs := range def.InValues {
		if i > 0 {
			buf.WriteByte(',')
		}
		buf.WriteString(vs[0])
	}
	buf.WriteString("))")
	for _, vs := range def.InValues {
		nullCondBuf.Reset()
		hasNull := false
		for i, value := range vs {
			expr, err := parseSimpleExprWithNames(p, ctx, value, schema, names)
			if err != nil {
				return "", errors.Trace(err)
			}
			v, err := expr.Eval(chunk.Row{})
			if err != nil {
				return "", errors.Trace(err)
			}
			if i > 0 {
				nullCondBuf.WriteString(" and ")
			}
			if v.IsNull() {
				hasNull = true
				fmt.Fprintf(&nullCondBuf, "%s is null", partStr)
			} else {
				fmt.Fprintf(&nullCondBuf, "%s = %s", partStr, value)
			}
		}
		if hasNull {
			fmt.Fprintf(&buf, " or (%s) ", nullCondBuf.String())
		}
	}
	return buf.String(), nil
}

func generateMultiListColumnsPartitionExprStr(ctx sessionctx.Context, pi *model.PartitionInfo,
	schema *expression.Schema, names types.NameSlice, def *model.PartitionDefinition, p *parser.Parser) (string, error) {
	var buf, nullCondBuf bytes.Buffer
	var partStr string
	for i, col := range pi.Columns {
		if i > 0 {
			partStr += ","
		}
		partStr += "`" + col.L + "`"
	}
	fmt.Fprintf(&buf, "((%s) in (", partStr)
	for i, vs := range def.InValues {
		if i > 0 {
			buf.WriteByte(',')
		}
		fmt.Fprintf(&buf, "(%s)", strings.Join(vs, ","))
	}
	buf.WriteString("))")
	for _, vs := range def.InValues {
		nullCondBuf.Reset()
		hasNull := false
		for i, value := range vs {
			expr, err := parseSimpleExprWithNames(p, ctx, value, schema, names)
			if err != nil {
				return "", errors.Trace(err)
			}
			v, err := expr.Eval(chunk.Row{})
			if err != nil {
				return "", errors.Trace(err)
			}
			if i > 0 {
				nullCondBuf.WriteString(" and ")
			}
			if v.IsNull() {
				hasNull = true
				fmt.Fprintf(&nullCondBuf, "%s is null", pi.Columns[i])
			} else {
				fmt.Fprintf(&nullCondBuf, "%s = %s", pi.Columns[i], value)
			}
		}
		if hasNull {
			fmt.Fprintf(&buf, " or (%s) ", nullCondBuf.String())
		}
	}
	return buf.String(), nil
}

func generateHashPartitionExpr(ctx sessionctx.Context, pi *model.PartitionInfo,
	columns []*expression.Column, names types.NameSlice) (*PartitionExpr, error) {
	// The caller should assure partition info is not nil.
	schema := expression.NewSchema(columns...)
	origExpr, err := parseExpr(parser.New(), pi.Expr)
	if err != nil {
		return nil, err
	}
	exprs, err := rewritePartitionExpr(ctx, origExpr, schema, names)
	if err != nil {
		// If it got an error here, ddl may hang forever, so this error log is important.
		logutil.BgLogger().Error("wrong table partition expression", zap.String("expression", pi.Expr), zap.Error(err))
		return nil, errors.Trace(err)
	}
	// build column offset.
	partitionCols := expression.ExtractColumns(exprs)
	offset := make([]int, len(partitionCols))
	for i, col := range columns {
		for j, partitionCol := range partitionCols {
			if partitionCol.UniqueID == col.UniqueID {
				offset[j] = i
			}
		}
	}
	exprs.HashCode(ctx.GetSessionVars().StmtCtx)
	return &PartitionExpr{
		Expr:         exprs,
		OrigExpr:     origExpr,
		ColumnOffset: offset,
	}, nil
}

// PartitionExpr returns the partition expression.
func (t *partitionedTable) PartitionExpr() (*PartitionExpr, error) {
	return t.partitionExpr, nil
}

// PartitionRecordKey is exported for test.
func PartitionRecordKey(pid int64, handle int64) kv.Key {
	recordPrefix := tablecodec.GenTableRecordPrefix(pid)
	return tablecodec.EncodeRecordKey(recordPrefix, kv.IntHandle(handle))
}

// locatePartition returns the partition ID of the input record.
func (t *partitionedTable) locatePartition(ctx sessionctx.Context, pi *model.PartitionInfo, r []types.Datum) (int64, error) {
	var err error
	var idx int
	switch t.meta.Partition.Type {
	case model.PartitionTypeRange:
		if len(pi.Columns) == 0 {
			idx, err = t.locateRangePartition(ctx, pi, r)
		} else {
			idx, err = t.locateRangeColumnPartition(ctx, pi, r)
		}
	case model.PartitionTypeHash:
		idx, err = t.locateHashPartition(ctx, pi, r)
	case model.PartitionTypeList:
		idx, err = t.locateListPartition(ctx, pi, r)
	}
	if err != nil {
		return 0, errors.Trace(err)
	}
	return pi.Definitions[idx].ID, nil
}

func (t *partitionedTable) locateRangeColumnPartition(ctx sessionctx.Context, pi *model.PartitionInfo, r []types.Datum) (int, error) {
	var err error
	var isNull bool
	partitionExprs := t.partitionExpr.UpperBounds
	evalBuffer := t.evalBufferPool.Get().(*chunk.MutRow)
	defer t.evalBufferPool.Put(evalBuffer)
	idx := sort.Search(len(partitionExprs), func(i int) bool {
		evalBuffer.SetDatums(r...)
		ret, isNull, err := partitionExprs[i].EvalInt(ctx, evalBuffer.ToRow())
		if err != nil {
			return true // Break the search.
		}
		if isNull {
			// If the column value used to determine the partition is NULL, the row is inserted into the lowest partition.
			// See https://dev.mysql.com/doc/mysql-partitioning-excerpt/5.7/en/partitioning-handling-nulls.html
			return true // Break the search.
		}
		return ret > 0
	})
	if err != nil {
		return 0, errors.Trace(err)
	}
	if isNull {
		idx = 0
	}
	if idx < 0 || idx >= len(partitionExprs) {
		// The data does not belong to any of the partition returns `table has no partition for value %s`.
		var valueMsg string
		if pi.Expr != "" {
			e, err := expression.ParseSimpleExprWithTableInfo(ctx, pi.Expr, t.meta)
			if err == nil {
				val, _, err := e.EvalInt(ctx, chunk.MutRowFromDatums(r).ToRow())
				if err == nil {
					valueMsg = fmt.Sprintf("%d", val)
				}
			}
		} else {
			// When the table is partitioned by range columns.
			valueMsg = "from column_list"
		}
		return 0, table.ErrNoPartitionForGivenValue.GenWithStackByArgs(valueMsg)
	}
	return idx, nil
}

func (t *partitionedTable) locateListPartition(ctx sessionctx.Context, pi *model.PartitionInfo, r []types.Datum) (int, error) {
	listPrune := t.partitionExpr.ForListPruning
	sc := ctx.GetSessionVars().StmtCtx
	partitionIdx := -1
	if listPrune.ColPrunes != nil {
		intersect := NewListPartitionLocationIntersect()
		for _, colPrune := range listPrune.ColPrunes {
			location, err := colPrune.LocatePartition(sc, r[colPrune.ExprCol.Index])
			if err != nil {
				return 0, errors.Trace(err)
			}
			if !intersect.Intersect(location) {
				break
			}
		}
		location := intersect.GetLocation()
		if location.IsEmpty() {
			partitionIdx = -1
		} else {
			partitionIdx = location[0].PartIdx
		}
	} else {
		value, isNull, err := listPrune.LocateExpr.EvalInt(ctx, chunk.MutRowFromDatums(r).ToRow())
		if err != nil {
			return 0, errors.Trace(err)
		}
		partitionIdx, err = listPrune.LocatePartition(value, isNull)
		if err != nil {
			return 0, errors.Trace(err)
		}
	}
	if partitionIdx > -1 {
		return partitionIdx, nil
	}
	// The data does not belong to any of the partition returns `table has no partition for value %s`.
	var valueMsg string
	if pi.Expr != "" {
		e, err := expression.ParseSimpleExprWithTableInfo(ctx, pi.Expr, t.meta)
		if err == nil {
			val, isNull, err := e.EvalInt(ctx, chunk.MutRowFromDatums(r).ToRow())
			if err == nil {
				if isNull {
					valueMsg = fmt.Sprintf("NULL")
				} else {
					valueMsg = fmt.Sprintf("%d", val)
				}
			}
		}
	} else {
		// When the table is partitioned by list columns.
		valueMsg = "from column_list"
	}
	return 0, table.ErrNoPartitionForGivenValue.GenWithStackByArgs(valueMsg)
}

func (t *partitionedTable) locateRangePartition(ctx sessionctx.Context, pi *model.PartitionInfo, r []types.Datum) (int, error) {
	var (
		ret    int64
		val    int64
		isNull bool
		err    error
	)
	if col, ok := t.partitionExpr.Expr.(*expression.Column); ok {
		if r[col.Index].IsNull() {
			isNull = true
		}
		ret = r[col.Index].GetInt64()
	} else {
		evalBuffer := t.evalBufferPool.Get().(*chunk.MutRow)
		defer t.evalBufferPool.Put(evalBuffer)
		evalBuffer.SetDatums(r...)
		val, isNull, err = t.partitionExpr.Expr.EvalInt(ctx, evalBuffer.ToRow())
		if err != nil {
			return 0, err
		}
		ret = val
	}
	unsigned := mysql.HasUnsignedFlag(t.partitionExpr.Expr.GetType().Flag)
	ranges := t.partitionExpr.ForRangePruning
	length := len(ranges.LessThan)
	pos := sort.Search(length, func(i int) bool {
		if isNull {
			return true
		}
		return ranges.compare(i, ret, unsigned) > 0
	})
	if isNull {
		pos = 0
	}
	if pos < 0 || pos >= length {
		// The data does not belong to any of the partition returns `table has no partition for value %s`.
		var valueMsg string
		if pi.Expr != "" {
			e, err := expression.ParseSimpleExprWithTableInfo(ctx, pi.Expr, t.meta)
			if err == nil {
				val, _, err := e.EvalInt(ctx, chunk.MutRowFromDatums(r).ToRow())
				if err == nil {
					valueMsg = fmt.Sprintf("%d", val)
				}
			}
		} else {
			// When the table is partitioned by range columns.
			valueMsg = "from column_list"
		}
		return 0, table.ErrNoPartitionForGivenValue.GenWithStackByArgs(valueMsg)
	}
	return pos, nil
}

// TODO: supports linear hashing
func (t *partitionedTable) locateHashPartition(ctx sessionctx.Context, pi *model.PartitionInfo, r []types.Datum) (int, error) {
	if col, ok := t.partitionExpr.Expr.(*expression.Column); ok {
		var data types.Datum
		switch r[col.Index].Kind() {
		case types.KindInt64, types.KindUint64:
			data = r[col.Index]
		default:
			var err error
			data, err = r[col.Index].ConvertTo(ctx.GetSessionVars().StmtCtx, types.NewFieldType(mysql.TypeLong))
			if err != nil {
				return 0, err
			}
		}
		ret := data.GetInt64()
		ret = ret % int64(t.meta.Partition.Num)
		if ret < 0 {
			ret = -ret
		}
		return int(ret), nil
	}
	evalBuffer := t.evalBufferPool.Get().(*chunk.MutRow)
	defer t.evalBufferPool.Put(evalBuffer)
	evalBuffer.SetDatums(r...)
	ret, isNull, err := t.partitionExpr.Expr.EvalInt(ctx, evalBuffer.ToRow())
	if err != nil {
		return 0, err
	}
	if isNull {
		return 0, nil
	}
	ret = ret % int64(t.meta.Partition.Num)
	if ret < 0 {
		ret = -ret
	}
	return int(ret), nil
}

// GetPartition returns a Table, which is actually a partition.
func (t *partitionedTable) GetPartition(pid int64) table.PhysicalTable {
	// Attention, can't simply use `return t.partitions[pid]` here.
	// Because A nil of type *partition is a kind of `table.PhysicalTable`
	p, ok := t.partitions[pid]
	if !ok {
		return nil
	}
	return p
}

// GetPartitionByRow returns a Table, which is actually a Partition.
func (t *partitionedTable) GetPartitionByRow(ctx sessionctx.Context, r []types.Datum) (table.PhysicalTable, error) {
	pid, err := t.locatePartition(ctx, t.Meta().GetPartitionInfo(), r)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return t.partitions[pid], nil
}

// AddRecord implements the AddRecord method for the table.Table interface.
func (t *partitionedTable) AddRecord(ctx sessionctx.Context, r []types.Datum, opts ...table.AddRecordOption) (recordID kv.Handle, err error) {
	return partitionedTableAddRecord(ctx, t, r, nil, opts)
}

func partitionedTableAddRecord(ctx sessionctx.Context, t *partitionedTable, r []types.Datum, partitionSelection map[int64]struct{}, opts []table.AddRecordOption) (recordID kv.Handle, err error) {
	partitionInfo := t.meta.GetPartitionInfo()
	pid, err := t.locatePartition(ctx, partitionInfo, r)
	if err != nil {
		return nil, errors.Trace(err)
	}

	if partitionSelection != nil {
		if _, ok := partitionSelection[pid]; !ok {
			return nil, errors.WithStack(table.ErrRowDoesNotMatchGivenPartitionSet)
		}
	}
	tbl := t.GetPartition(pid)
	return tbl.AddRecord(ctx, r, opts...)
}

// partitionTableWithGivenSets is used for this kind of grammar: partition (p0,p1)
// Basically it is the same as partitionedTable except that partitionTableWithGivenSets
// checks the given partition set for AddRecord/UpdateRecord operations.
type partitionTableWithGivenSets struct {
	*partitionedTable
	partitions map[int64]struct{}
}

// NewPartitionTableithGivenSets creates a new partition table from a partition table.
func NewPartitionTableithGivenSets(tbl table.PartitionedTable, partitions map[int64]struct{}) table.PartitionedTable {
	if raw, ok := tbl.(*partitionedTable); ok {
		return &partitionTableWithGivenSets{
			partitionedTable: raw,
			partitions:       partitions,
		}
	}
	return tbl
}

// AddRecord implements the AddRecord method for the table.Table interface.
func (t *partitionTableWithGivenSets) AddRecord(ctx sessionctx.Context, r []types.Datum, opts ...table.AddRecordOption) (recordID kv.Handle, err error) {
	return partitionedTableAddRecord(ctx, t.partitionedTable, r, t.partitions, opts)
}

func (t *partitionTableWithGivenSets) GetAllPartitionIDs() []int64 {
	ptIDs := make([]int64, 0, len(t.partitions))
	for id := range t.partitions {
		ptIDs = append(ptIDs, id)
	}
	return ptIDs
}

// RemoveRecord implements table.Table RemoveRecord interface.
func (t *partitionedTable) RemoveRecord(ctx sessionctx.Context, h kv.Handle, r []types.Datum) error {
	partitionInfo := t.meta.GetPartitionInfo()
	pid, err := t.locatePartition(ctx, partitionInfo, r)
	if err != nil {
		return errors.Trace(err)
	}

	tbl := t.GetPartition(pid)
	return tbl.RemoveRecord(ctx, h, r)
}

func (t *partitionedTable) GetAllPartitionIDs() []int64 {
	ptIDs := make([]int64, 0, len(t.partitions))
	for id := range t.partitions {
		ptIDs = append(ptIDs, id)
	}
	return ptIDs
}

// UpdateRecord implements table.Table UpdateRecord interface.
// `touched` means which columns are really modified, used for secondary indices.
// Length of `oldData` and `newData` equals to length of `t.WritableCols()`.
func (t *partitionedTable) UpdateRecord(ctx context.Context, sctx sessionctx.Context, h kv.Handle, currData, newData []types.Datum, touched []bool) error {
	return partitionedTableUpdateRecord(ctx, sctx, t, h, currData, newData, touched, nil)
}

func (t *partitionTableWithGivenSets) UpdateRecord(ctx context.Context, sctx sessionctx.Context, h kv.Handle, currData, newData []types.Datum, touched []bool) error {
	return partitionedTableUpdateRecord(ctx, sctx, t.partitionedTable, h, currData, newData, touched, t.partitions)
}

func partitionedTableUpdateRecord(gctx context.Context, ctx sessionctx.Context, t *partitionedTable, h kv.Handle, currData, newData []types.Datum, touched []bool, partitionSelection map[int64]struct{}) error {
	partitionInfo := t.meta.GetPartitionInfo()
	from, err := t.locatePartition(ctx, partitionInfo, currData)
	if err != nil {
		return errors.Trace(err)
	}
	to, err := t.locatePartition(ctx, partitionInfo, newData)
	if err != nil {
		return errors.Trace(err)
	}
	if partitionSelection != nil {
		if _, ok := partitionSelection[to]; !ok {
			return errors.WithStack(table.ErrRowDoesNotMatchGivenPartitionSet)
		}
	}

	// The old and new data locate in different partitions.
	// Remove record from old partition and add record to new partition.
	if from != to {
		_, err = t.GetPartition(to).AddRecord(ctx, newData)
		if err != nil {
			return errors.Trace(err)
		}
		// UpdateRecord should be side effect free, but there're two steps here.
		// What would happen if step1 succeed but step2 meets error? It's hard
		// to rollback.
		// So this special order is chosen: add record first, errors such as
		// 'Key Already Exists' will generally happen during step1, errors are
		// unlikely to happen in step2.
		err = t.GetPartition(from).RemoveRecord(ctx, h, currData)
		if err != nil {
			logutil.BgLogger().Error("update partition record fails", zap.String("message", "new record inserted while old record is not removed"), zap.Error(err))
			return errors.Trace(err)
		}
		return nil
	}

	tbl := t.GetPartition(to)
	return tbl.UpdateRecord(gctx, ctx, h, currData, newData, touched)
}

// FindPartitionByName finds partition in table meta by name.
func FindPartitionByName(meta *model.TableInfo, parName string) (int64, error) {
	// Hash partition table use p0, p1, p2, p3 as partition names automatically.
	parName = strings.ToLower(parName)
	for _, def := range meta.Partition.Definitions {
		if strings.EqualFold(def.Name.L, parName) {
			return def.ID, nil
		}
	}
	return -1, errors.Trace(table.ErrUnknownPartition.GenWithStackByArgs(parName, meta.Name.O))
}

func parseExpr(p *parser.Parser, exprStr string) (ast.ExprNode, error) {
	exprStr = "select " + exprStr
	stmts, _, err := p.Parse(exprStr, "", "")
	if err != nil {
		return nil, util.SyntaxWarn(err)
	}
	fields := stmts[0].(*ast.SelectStmt).Fields.Fields
	return fields[0].Expr, nil
}

func rewritePartitionExpr(ctx sessionctx.Context, field ast.ExprNode, schema *expression.Schema, names types.NameSlice) (expression.Expression, error) {
	expr, err := expression.RewriteSimpleExprWithNames(ctx, field, schema, names)
	return expr, err
}

func compareUnsigned(v1, v2 int64) int {
	switch {
	case uint64(v1) > uint64(v2):
		return 1
	case uint64(v1) == uint64(v2):
		return 0
	}
	return -1
}

func (lt *ForRangePruning) compare(ith int, v int64, unsigned bool) int {
	if ith == len(lt.LessThan)-1 {
		if lt.MaxValue {
			return 1
		}
	}
	if unsigned {
		return compareUnsigned(lt.LessThan[ith], v)
	}
	switch {
	case lt.LessThan[ith] > v:
		return 1
	case lt.LessThan[ith] == v:
		return 0
	}
	return -1
}
