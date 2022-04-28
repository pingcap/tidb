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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
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

	"github.com/google/btree"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/hack"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/mock"
	"github.com/pingcap/tidb/util/ranger"
	"go.uber.org/zap"
)

const (
	btreeDegree = 32
)

// Both partition and partitionedTable implement the table.Table interface.
var _ table.PhysicalTable = &partition{}
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
	// LocateExpr uses to locate list partition by row.
	LocateExpr expression.Expression
	// PruneExpr uses to prune list partition in partition pruner.
	PruneExpr expression.Expression
	// PruneExprCols is the columns of PruneExpr, it has removed the duplicate columns.
	PruneExprCols []*expression.Column
	// valueMap is column value -> partition idx, uses to locate list partition.
	valueMap map[int64]int
	// nullPartitionIdx is the partition idx for null value.
	nullPartitionIdx int

	// For list columns partition pruning
	ColPrunes []*ForListColumnPruning
}

// btreeListColumnItem is BTree's Item that uses string to compare.
type btreeListColumnItem struct {
	key      string
	location ListPartitionLocation
}

func newBtreeListColumnItem(key string, location ListPartitionLocation) *btreeListColumnItem {
	return &btreeListColumnItem{
		key:      key,
		location: location,
	}
}

func newBtreeListColumnSearchItem(key string) *btreeListColumnItem {
	return &btreeListColumnItem{
		key: key,
	}
}

func (item *btreeListColumnItem) Less(other btree.Item) bool {
	return item.key < other.(*btreeListColumnItem).key
}

// ForListColumnPruning is used for list columns partition pruning.
type ForListColumnPruning struct {
	ExprCol  *expression.Column
	valueTp  *types.FieldType
	valueMap map[string]ListPartitionLocation
	sorted   *btree.BTree

	// To deal with the location partition failure caused by inconsistent NewCollationEnabled values(see issue #32416).
	// The following fields are used to delay building valueMap.
	ctx     sessionctx.Context
	tblInfo *model.TableInfo
	schema  *expression.Schema
	names   types.NameSlice
	colIdx  int
}

// ListPartitionGroup indicate the group index of the column value in a partition.
type ListPartitionGroup struct {
	// Such as: list columns (a,b) (partition p0 values in ((1,5),(1,6)));
	// For the column a which value is 1, the ListPartitionGroup is:
	// ListPartitionGroup {
	//     PartIdx: 0,            // 0 is the partition p0 index in all partitions.
	//     GroupIdxs: []int{0,1}, // p0 has 2 value group: (1,5) and (1,6), and they both contain the column a where value is 1;
	// }                          // the value of GroupIdxs `0,1` is the index of the value group that contain the column a which value is 1.
	PartIdx   int
	GroupIdxs []int
}

// ListPartitionLocation indicate the partition location for the column value in list columns partition.
// Here is an example:
// Suppose the list columns partition is: list columns (a,b) (partition p0 values in ((1,5),(1,6)), partition p1 values in ((1,7),(9,9)));
// How to express the location of the column a which value is 1?
// For the column a which value is 1, both partition p0 and p1 contain the column a which value is 1.
// In partition p0, both value group0 (1,5) and group1 (1,6) are contain the column a which value is 1.
// In partition p1, value group0 (1,7) contains the column a which value is 1.
// So, the ListPartitionLocation of column a which value is 1 is:
// []ListPartitionGroup{
// 	{
// 		PartIdx: 0,               // `0` is the partition p0 index in all partitions.
// 		GroupIdxs: []int{0, 1}    // `0,1` is the index of the value group0, group1.
// 	},
// 	{
// 		PartIdx: 1,               // `1` is the partition p1 index in all partitions.
// 		GroupIdxs: []int{0}       // `0` is the index of the value group0.
// 	},
// }
type ListPartitionLocation []ListPartitionGroup

// IsEmpty returns true if the ListPartitionLocation is empty.
func (ps ListPartitionLocation) IsEmpty() bool {
	for _, pg := range ps {
		if len(pg.GroupIdxs) > 0 {
			return false
		}
	}
	return true
}

func (ps ListPartitionLocation) findByPartitionIdx(partIdx int) int {
	for i, p := range ps {
		if p.PartIdx == partIdx {
			return i
		}
	}
	return -1
}

type listPartitionLocationHelper struct {
	initialized bool
	location    ListPartitionLocation
}

// NewListPartitionLocationHelper returns a new listPartitionLocationHelper.
func NewListPartitionLocationHelper() *listPartitionLocationHelper {
	return &listPartitionLocationHelper{}
}

// GetLocation gets the list partition location.
func (p *listPartitionLocationHelper) GetLocation() ListPartitionLocation {
	return p.location
}

// UnionPartitionGroup unions with the list-partition-value-group.
func (p *listPartitionLocationHelper) UnionPartitionGroup(pg ListPartitionGroup) {
	idx := p.location.findByPartitionIdx(pg.PartIdx)
	if idx < 0 {
		// copy the group idx.
		groupIdxs := make([]int, len(pg.GroupIdxs))
		copy(groupIdxs, pg.GroupIdxs)
		p.location = append(p.location, ListPartitionGroup{
			PartIdx:   pg.PartIdx,
			GroupIdxs: groupIdxs,
		})
		return
	}
	p.location[idx].union(pg)
}

// Union unions with the other location.
func (p *listPartitionLocationHelper) Union(location ListPartitionLocation) {
	for _, pg := range location {
		p.UnionPartitionGroup(pg)
	}
}

// Intersect intersect with other location.
func (p *listPartitionLocationHelper) Intersect(location ListPartitionLocation) bool {
	if !p.initialized {
		p.initialized = true
		p.location = make([]ListPartitionGroup, 0, len(location))
		p.location = append(p.location, location...)
		return true
	}
	currPgs := p.location
	remainPgs := make([]ListPartitionGroup, 0, len(location))
	for _, pg := range location {
		idx := currPgs.findByPartitionIdx(pg.PartIdx)
		if idx < 0 {
			continue
		}
		if !currPgs[idx].intersect(pg) {
			continue
		}
		remainPgs = append(remainPgs, currPgs[idx])
	}
	p.location = remainPgs
	return len(remainPgs) > 0
}

func (pg *ListPartitionGroup) intersect(otherPg ListPartitionGroup) bool {
	if pg.PartIdx != otherPg.PartIdx {
		return false
	}
	var groupIdxs []int
	for _, gidx := range otherPg.GroupIdxs {
		if pg.findGroupIdx(gidx) {
			groupIdxs = append(groupIdxs, gidx)
		}
	}
	pg.GroupIdxs = groupIdxs
	return len(groupIdxs) > 0
}

func (pg *ListPartitionGroup) union(otherPg ListPartitionGroup) {
	if pg.PartIdx != otherPg.PartIdx {
		return
	}
	pg.GroupIdxs = append(pg.GroupIdxs, otherPg.GroupIdxs...)
}

func (pg *ListPartitionGroup) findGroupIdx(groupIdx int) bool {
	for _, gidx := range pg.GroupIdxs {
		if gidx == groupIdx {
			return true
		}
	}
	return false
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

func extractListPartitionExprColumns(ctx sessionctx.Context, pi *model.PartitionInfo, columns []*expression.Column, names types.NameSlice) (expression.Expression, []*expression.Column, []int, error) {
	var cols []*expression.Column
	var partExpr expression.Expression
	if len(pi.Columns) == 0 {
		schema := expression.NewSchema(columns...)
		exprs, err := expression.ParseSimpleExprsWithNames(ctx, pi.Expr, schema, names)
		if err != nil {
			return nil, nil, nil, err
		}
		cols = expression.ExtractColumns(exprs[0])
		partExpr = exprs[0]
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
			deDupCols = append(deDupCols, c)
		}
	}
	return partExpr, deDupCols, offset, nil
}

func generateListPartitionExpr(ctx sessionctx.Context, tblInfo *model.TableInfo,
	columns []*expression.Column, names types.NameSlice) (*PartitionExpr, error) {
	// The caller should assure partition info is not nil.
	pi := tblInfo.GetPartitionInfo()
	partExpr, exprCols, offset, err := extractListPartitionExprColumns(ctx, pi, columns, names)
	if err != nil {
		return nil, err
	}
	listPrune := &ForListPruning{}
	if len(pi.Columns) == 0 {
		err = listPrune.buildListPruner(ctx, tblInfo, exprCols, columns, names)
	} else {
		err = listPrune.buildListColumnsPruner(ctx, tblInfo, columns, names)
	}
	if err != nil {
		return nil, err
	}
	ret := &PartitionExpr{
		ForListPruning: listPrune,
		ColumnOffset:   offset,
		Expr:           partExpr,
	}
	return ret, nil
}

func (lp *ForListPruning) buildListPruner(ctx sessionctx.Context, tblInfo *model.TableInfo, exprCols []*expression.Column,
	columns []*expression.Column, names types.NameSlice) error {
	pi := tblInfo.GetPartitionInfo()
	schema := expression.NewSchema(columns...)
	p := parser.New()
	expr, err := parseSimpleExprWithNames(p, ctx, pi.Expr, schema, names)
	if err != nil {
		// If it got an error here, ddl may hang forever, so this error log is important.
		logutil.BgLogger().Error("wrong table partition expression", zap.String("expression", pi.Expr), zap.Error(err))
		return errors.Trace(err)
	}
	// Since need to change the column index of the expresion, clone the expression first.
	lp.LocateExpr = expr.Clone()
	lp.PruneExprCols = exprCols
	lp.PruneExpr = expr.Clone()
	cols := expression.ExtractColumns(lp.PruneExpr)
	for _, c := range cols {
		idx := findIdxByColUniqueID(exprCols, c)
		if idx < 0 {
			return table.ErrUnknownColumn.GenWithStackByArgs(c.OrigName)
		}
		c.Index = idx
	}
	err = lp.buildListPartitionValueMap(ctx, tblInfo, schema, names, p)
	if err != nil {
		return err
	}
	return nil
}

func (lp *ForListPruning) buildListColumnsPruner(ctx sessionctx.Context, tblInfo *model.TableInfo,
	columns []*expression.Column, names types.NameSlice) error {
	pi := tblInfo.GetPartitionInfo()
	schema := expression.NewSchema(columns...)
	p := parser.New()
	colPrunes := make([]*ForListColumnPruning, 0, len(pi.Columns))
	for colIdx := range pi.Columns {
		colInfo := model.FindColumnInfo(tblInfo.Columns, pi.Columns[colIdx].L)
		if colInfo == nil {
			return table.ErrUnknownColumn.GenWithStackByArgs(pi.Columns[colIdx].L)
		}
		idx := expression.FindFieldNameIdxByColName(names, pi.Columns[colIdx].L)
		if idx < 0 {
			return table.ErrUnknownColumn.GenWithStackByArgs(pi.Columns[colIdx].L)
		}
		colPrune := &ForListColumnPruning{
			ctx:      ctx,
			tblInfo:  tblInfo,
			schema:   schema,
			names:    names,
			colIdx:   colIdx,
			ExprCol:  columns[idx],
			valueTp:  &colInfo.FieldType,
			valueMap: make(map[string]ListPartitionLocation),
			sorted:   btree.New(btreeDegree),
		}
		err := colPrune.buildPartitionValueMapAndSorted(p)
		if err != nil {
			return err
		}
		colPrunes = append(colPrunes, colPrune)
	}
	lp.ColPrunes = colPrunes
	return nil
}

// buildListPartitionValueMap builds list partition value map.
// The map is column value -> partition index.
// colIdx is the column index in the list columns.
func (lp *ForListPruning) buildListPartitionValueMap(ctx sessionctx.Context, tblInfo *model.TableInfo,
	schema *expression.Schema, names types.NameSlice, p *parser.Parser) error {
	pi := tblInfo.GetPartitionInfo()
	lp.valueMap = map[int64]int{}
	lp.nullPartitionIdx = -1
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
				lp.nullPartitionIdx = partitionIdx
				continue
			}
			lp.valueMap[v] = partitionIdx
		}
	}
	return nil
}

// LocatePartition locates partition by the column value
func (lp *ForListPruning) LocatePartition(value int64, isNull bool) int {
	if isNull {
		return lp.nullPartitionIdx
	}
	partitionIdx, ok := lp.valueMap[value]
	if !ok {
		return -1
	}
	return partitionIdx
}

func (lp *ForListPruning) locateListPartitionByRow(ctx sessionctx.Context, r []types.Datum) (int, error) {
	value, isNull, err := lp.LocateExpr.EvalInt(ctx, chunk.MutRowFromDatums(r).ToRow())
	if err != nil {
		return -1, errors.Trace(err)
	}
	idx := lp.LocatePartition(value, isNull)
	if idx >= 0 {
		return idx, nil
	}
	if isNull {
		return -1, table.ErrNoPartitionForGivenValue.GenWithStackByArgs("NULL")
	}
	return -1, table.ErrNoPartitionForGivenValue.GenWithStackByArgs(strconv.FormatInt(value, 10))
}

func (lp *ForListPruning) locateListColumnsPartitionByRow(ctx sessionctx.Context, r []types.Datum) (int, error) {
	helper := NewListPartitionLocationHelper()
	sc := ctx.GetSessionVars().StmtCtx
	for _, colPrune := range lp.ColPrunes {
		location, err := colPrune.LocatePartition(sc, r[colPrune.ExprCol.Index])
		if err != nil {
			return -1, errors.Trace(err)
		}
		if !helper.Intersect(location) {
			break
		}
	}
	location := helper.GetLocation()
	if location.IsEmpty() {
		return -1, table.ErrNoPartitionForGivenValue.GenWithStackByArgs("from column_list")
	}
	return location[0].PartIdx, nil
}

// buildPartitionValueMapAndSorted builds list columns partition value map for the specified column.
// It also builds list columns partition value btree for the specified column.
// colIdx is the specified column index in the list columns.
func (lp *ForListColumnPruning) buildPartitionValueMapAndSorted(p *parser.Parser) error {
	l := len(lp.valueMap)
	if l != 0 {
		return nil
	}

	return lp.buildListPartitionValueMapAndSorted(p)
}

// RebuildPartitionValueMapAndSorted rebuilds list columns partition value map for the specified column.
func (lp *ForListColumnPruning) RebuildPartitionValueMapAndSorted(p *parser.Parser) error {
	lp.valueMap = make(map[string]ListPartitionLocation, len(lp.valueMap))
	lp.sorted.Clear(false)
	return lp.buildListPartitionValueMapAndSorted(p)
}

func (lp *ForListColumnPruning) buildListPartitionValueMapAndSorted(p *parser.Parser) error {
	pi := lp.tblInfo.GetPartitionInfo()
	sc := lp.ctx.GetSessionVars().StmtCtx
	for partitionIdx, def := range pi.Definitions {
		for groupIdx, vs := range def.InValues {
			keyBytes, err := lp.genConstExprKey(lp.ctx, sc, vs[lp.colIdx], lp.schema, lp.names, p)
			if err != nil {
				return errors.Trace(err)
			}
			key := string(keyBytes)
			location, ok := lp.valueMap[key]
			if ok {
				idx := location.findByPartitionIdx(partitionIdx)
				if idx != -1 {
					location[idx].GroupIdxs = append(location[idx].GroupIdxs, groupIdx)
					continue
				}
			}
			location = append(location, ListPartitionGroup{
				PartIdx:   partitionIdx,
				GroupIdxs: []int{groupIdx},
			})
			lp.valueMap[key] = location
			lp.sorted.ReplaceOrInsert(newBtreeListColumnItem(key, location))
		}
	}
	return nil
}

func (lp *ForListColumnPruning) genConstExprKey(ctx sessionctx.Context, sc *stmtctx.StatementContext, exprStr string,
	schema *expression.Schema, names types.NameSlice, p *parser.Parser) ([]byte, error) {
	expr, err := parseSimpleExprWithNames(p, ctx, exprStr, schema, names)
	if err != nil {
		return nil, errors.Trace(err)
	}
	v, err := expr.Eval(chunk.Row{})
	if err != nil {
		return nil, errors.Trace(err)
	}
	key, err := lp.genKey(sc, v)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return key, nil
}

func (lp *ForListColumnPruning) genKey(sc *stmtctx.StatementContext, v types.Datum) ([]byte, error) {
	v, err := v.ConvertTo(sc, lp.valueTp)
	if err != nil {
		return nil, errors.Trace(err)
	}
	valByte, err := codec.EncodeKey(sc, nil, v)
	return valByte, err
}

// LocatePartition locates partition by the column value
func (lp *ForListColumnPruning) LocatePartition(sc *stmtctx.StatementContext, v types.Datum) (ListPartitionLocation, error) {
	key, err := lp.genKey(sc, v)
	if err != nil {
		return nil, errors.Trace(err)
	}
	location, ok := lp.valueMap[string(key)]
	if !ok {
		return nil, nil
	}
	return location, nil
}

// LocateRanges locates partition ranges by the column range
func (lp *ForListColumnPruning) LocateRanges(sc *stmtctx.StatementContext, r *ranger.Range) ([]ListPartitionLocation, error) {
	var lowKey, highKey []byte
	var err error
	lowVal := r.LowVal[0]
	if r.LowVal[0].Kind() == types.KindMinNotNull {
		lowVal = types.GetMinValue(lp.ExprCol.GetType())
	}
	highVal := r.HighVal[0]
	if r.HighVal[0].Kind() == types.KindMaxValue {
		highVal = types.GetMaxValue(lp.ExprCol.GetType())
	}

	// For string type, values returned by GetMinValue and GetMaxValue are already encoded,
	// so it's unnecessary to invoke genKey to encode them.
	if lp.ExprCol.GetType().EvalType() == types.ETString && r.LowVal[0].Kind() == types.KindMinNotNull {
		lowKey = (&lowVal).GetBytes()
	} else {
		lowKey, err = lp.genKey(sc, lowVal)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}

	if lp.ExprCol.GetType().EvalType() == types.ETString && r.HighVal[0].Kind() == types.KindMaxValue {
		highKey = (&highVal).GetBytes()
	} else {
		highKey, err = lp.genKey(sc, highVal)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}

	if r.LowExclude {
		lowKey = kv.Key(lowKey).PrefixNext()
	}
	if !r.HighExclude {
		highKey = kv.Key(highKey).PrefixNext()
	}

	locations := make([]ListPartitionLocation, 0, lp.sorted.Len())
	lp.sorted.AscendRange(newBtreeListColumnSearchItem(string(hack.String(lowKey))), newBtreeListColumnSearchItem(string(hack.String(highKey))), func(item btree.Item) bool {
		locations = append(locations, item.(*btreeListColumnItem).location)
		return true
	})
	return locations, nil
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

func (t *partitionedTable) GetPartitionColumnNames() []model.CIStr {
	// PARTITION BY {LIST|RANGE} COLUMNS uses columns directly without expressions
	pi := t.Meta().Partition
	if len(pi.Columns) > 0 {
		return pi.Columns
	}

	partitionCols := expression.ExtractColumns(t.partitionExpr.Expr)
	colIDs := make([]int64, 0, len(partitionCols))
	for _, col := range partitionCols {
		colIDs = append(colIDs, col.ID)
	}
	colNames := make([]model.CIStr, 0, len(partitionCols))
	for _, colID := range colIDs {
		for _, col := range t.Cols() {
			if col.ID == colID {
				colNames = append(colNames, col.Name)
			}
		}
	}
	return colNames
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
					valueMsg = strconv.FormatInt(val, 10)
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
	lp := t.partitionExpr.ForListPruning
	if len(lp.ColPrunes) == 0 {
		return lp.locateListPartitionByRow(ctx, r)
	}
	return lp.locateListColumnsPartitionByRow(ctx, r)
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
	unsigned := mysql.HasUnsignedFlag(t.partitionExpr.Expr.GetType().GetFlag())
	ranges := t.partitionExpr.ForRangePruning
	length := len(ranges.LessThan)
	pos := sort.Search(length, func(i int) bool {
		if isNull {
			return true
		}
		return ranges.Compare(i, ret, unsigned) > 0
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

// GetPartitionByRow returns a Table, which is actually a Partition.
func (t *partitionTableWithGivenSets) GetPartitionByRow(ctx sessionctx.Context, r []types.Datum) (table.PhysicalTable, error) {
	pid, err := t.locatePartition(ctx, t.Meta().GetPartitionInfo(), r)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if _, ok := t.givenSetPartitions[pid]; !ok {
		return nil, errors.WithStack(table.ErrRowDoesNotMatchGivenPartitionSet)
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
	givenSetPartitions map[int64]struct{}
}

// NewPartitionTableWithGivenSets creates a new partition table from a partition table.
func NewPartitionTableWithGivenSets(tbl table.PartitionedTable, partitions map[int64]struct{}) table.PartitionedTable {
	if raw, ok := tbl.(*partitionedTable); ok {
		return &partitionTableWithGivenSets{
			partitionedTable:   raw,
			givenSetPartitions: partitions,
		}
	}
	return tbl
}

// AddRecord implements the AddRecord method for the table.Table interface.
func (t *partitionTableWithGivenSets) AddRecord(ctx sessionctx.Context, r []types.Datum, opts ...table.AddRecordOption) (recordID kv.Handle, err error) {
	return partitionedTableAddRecord(ctx, t.partitionedTable, r, t.givenSetPartitions, opts)
}

func (t *partitionTableWithGivenSets) GetAllPartitionIDs() []int64 {
	ptIDs := make([]int64, 0, len(t.partitions))
	for id := range t.givenSetPartitions {
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
	return partitionedTableUpdateRecord(ctx, sctx, t.partitionedTable, h, currData, newData, touched, t.givenSetPartitions)
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
		// Should not have been read from this partition! Checked already in GetPartitionByRow()
		if _, ok := partitionSelection[from]; !ok {
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
	stmts, _, err := p.ParseSQL(exprStr)
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

// Compare is to be used in the binary search to locate partition
func (lt *ForRangePruning) Compare(ith int, v int64, unsigned bool) int {
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
