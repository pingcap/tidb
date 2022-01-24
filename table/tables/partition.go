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
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
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
	tableCommon
}

// GetPhysicalID implements table.Table GetPhysicalID interface.
func (p *partition) GetPhysicalID() int64 {
	return p.physicalTableID
}

// partitionedTable implements the table.PartitionedTable interface.
// partitionedTable is a table, it contains many Partitions.
type partitionedTable struct {
	Table
	partitionExpr   *PartitionExpr
	partitions      map[int64]*partition
	evalBufferTypes []*types.FieldType
	evalBufferPool  sync.Pool
}

func newPartitionedTable(tbl *Table, tblInfo *model.TableInfo) (table.Table, error) {
	ret := &partitionedTable{Table: *tbl}
	partitionExpr, err := newPartitionExpr(tblInfo)
	if err != nil {
		return nil, errors.Trace(err)
	}
	ret.partitionExpr = partitionExpr

	if err := initTableIndices(&ret.tableCommon); err != nil {
		return nil, errors.Trace(err)
	}
	partitions := make(map[int64]*partition)
	pi := tblInfo.GetPartitionInfo()
	for _, p := range pi.Definitions {
		var t partition
		err := initTableCommonWithIndices(&t.tableCommon, tblInfo, p.ID, tbl.Columns, tbl.alloc)
		if err != nil {
			return nil, errors.Trace(err)
		}
		partitions[p.ID] = &t
	}
	ret.partitions = partitions
	initEvalBufferType(ret)
	ret.evalBufferPool = sync.Pool{
		New: func() interface{} {
			return initEvalBuffer(ret)
		},
	}
	return ret, nil
}

func newPartitionExpr(tblInfo *model.TableInfo) (*PartitionExpr, error) {
	ctx := mock.NewContext()
	dbName := model.NewCIStr(ctx.GetSessionVars().CurrentDB)
	columns := expression.ColumnInfos2ColumnsWithDBName(ctx, dbName, tblInfo.Name, tblInfo.Columns)
	return newPartitionExprBySchema(ctx, tblInfo, columns)
}

func newPartitionExprBySchema(ctx sessionctx.Context, tblInfo *model.TableInfo, columns []*expression.Column) (*PartitionExpr, error) {
	pi := tblInfo.GetPartitionInfo()
	switch pi.Type {
	case model.PartitionTypeRange:
		return generatePartitionExpr(ctx, pi, columns)
	case model.PartitionTypeHash:
		return generateHashPartitionExpr(ctx, pi, columns)
	}
	panic("cannot reach here")
}

// PartitionExpr is the partition definition expressions.
// There are two expressions exist, because Locate use binary search, which requires:
// Given a compare function, for any partition range i, if cmp[i] > 0, then cmp[i+1] > 0.
// While partition prune must use the accurate range to do prunning.
// partition by range (x)
//   (partition
//      p1 values less than (y1)
//      p2 values less than (y2)
//      p3 values less than (y3))
// Ranges: (x < y1 or x is null); (y1 <= x < y2); (y2 <= x < y3)
// UpperBounds: (x < y1); (x < y2); (x < y3)
type PartitionExpr struct {
	// Column is the column appeared in the by range expression, partition pruning need this to work.
	Column      *expression.Column
	Ranges      []expression.Expression
	UpperBounds []expression.Expression
	// Expr is the hash partition expression.
	Expr expression.Expression
	// The new range partition pruning
	*ForRangePruning
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

// ForRangePruning is used for range partition pruning.
type ForRangePruning struct {
	LessThan []int64
	MaxValue bool
	Unsigned bool
}

// makeLessThanData extracts the less than parts from 'partition p0 less than xx ... partitoin p1 less than ...'
func makeLessThanData(pi *model.PartitionInfo, res *ForRangePruning) error {
	lessThan := make([]int64, len(pi.Definitions))
	for i := 0; i < len(pi.Definitions); i++ {
		if strings.EqualFold(pi.Definitions[i].LessThan[0], "MAXVALUE") {
			// Use a bool flag instead of math.MaxInt64 to avoid the corner cases.
			res.MaxValue = true
		} else {
			var err error
			lessThan[i], err = strconv.ParseInt(pi.Definitions[i].LessThan[0], 10, 64)
			var numErr *strconv.NumError
			if stderr.As(err, &numErr) && numErr.Err == strconv.ErrRange {
				var tmp uint64
				tmp, err = strconv.ParseUint(pi.Definitions[i].LessThan[0], 10, 64)
				lessThan[i] = int64(tmp)
				res.Unsigned = true
			}
			if err != nil {
				return errors.WithStack(err)
			}
		}
	}
	res.LessThan = lessThan
	return nil
}

// rangePartitionString returns the partition string for a range typed partition.
func rangePartitionString(pi *model.PartitionInfo) string {
	// partition by range expr
	if len(pi.Columns) == 0 {
		return pi.Expr
	}

	// partition by range columns (c1)
	if len(pi.Columns) == 1 {
		return pi.Columns[0].L
	}

	// partition by range columns (c1, c2, ...)
	panic("create table assert len(columns) = 1")
}

func generatePartitionExpr(ctx sessionctx.Context, pi *model.PartitionInfo,
	columns []*expression.Column) (*PartitionExpr, error) {
	var column *expression.Column
	// The caller should assure partition info is not nil.
	partitionPruneExprs := make([]expression.Expression, 0, len(pi.Definitions))
	locateExprs := make([]expression.Expression, 0, len(pi.Definitions))
	var buf bytes.Buffer
	schema := expression.NewSchema(columns...)
	partStr := rangePartitionString(pi)
	for i := 0; i < len(pi.Definitions); i++ {

		if strings.EqualFold(pi.Definitions[i].LessThan[0], "MAXVALUE") {
			// Expr less than maxvalue is always true.
			fmt.Fprintf(&buf, "true")
		} else {
			fmt.Fprintf(&buf, "((%s) < (%s))", partStr, pi.Definitions[i].LessThan[0])
		}

		exprs, err := expression.ParseSimpleExprsWithSchema(ctx, buf.String(), schema)
		if err != nil {
			// If it got an error here, ddl may hang forever, so this error log is important.
			logutil.Logger(context.Background()).Error("wrong table partition expression", zap.String("expression", buf.String()), zap.Error(err))
			return nil, errors.Trace(err)
		}
		locateExprs = append(locateExprs, exprs[0])
		if i > 0 {
			fmt.Fprintf(&buf, " and ((%s) >= (%s))", partStr, pi.Definitions[i-1].LessThan[0])
		} else {
			// NULL will locate in the first partition, so its expression is (expr < value or expr is null).
			fmt.Fprintf(&buf, " or ((%s) is null)", partStr)

			// Extracts the column of the partition expression, it will be used by partition prunning.
			if tmps, err1 := expression.ParseSimpleExprsWithSchema(ctx, partStr, schema); err1 == nil {
				if col, ok := tmps[0].(*expression.Column); ok {
					column = col
				}
			}
			if column == nil {
				logutil.Logger(context.Background()).Warn("partition pruning not applicable", zap.String("expression", partStr))
			}
		}

		exprs, err = expression.ParseSimpleExprsWithSchema(ctx, buf.String(), schema)
		if err != nil {
			// If it got an error here, ddl may hang forever, so this error log is important.
			logutil.Logger(context.Background()).Error("wrong table partition expression", zap.String("expression", buf.String()), zap.Error(err))
			return nil, errors.Trace(err)
		}
		// Get a hash code in advance to prevent data race afterwards.
		exprs[0].HashCode(ctx.GetSessionVars().StmtCtx)
		partitionPruneExprs = append(partitionPruneExprs, exprs[0])
		buf.Reset()
	}

	var rangePruning *ForRangePruning
	var partitionExpr expression.Expression
	if len(pi.Columns) == 0 {
		exprs, err := expression.ParseSimpleExprsWithSchema(ctx, pi.Expr, schema)
		if err != nil {
			return nil, err
		}
		rangePruning = &ForRangePruning{}
		err = makeLessThanData(pi, rangePruning)
		if err != nil {
			return nil, errors.Trace(err)
		}
		partitionExpr = exprs[0]
	}

	return &PartitionExpr{
		Column:          column,
		Ranges:          partitionPruneExprs,
		UpperBounds:     locateExprs,
		ForRangePruning: rangePruning,
		Expr:            partitionExpr,
	}, nil
}

func generateHashPartitionExpr(ctx sessionctx.Context, pi *model.PartitionInfo,
	columns []*expression.Column) (*PartitionExpr, error) {
	var column *expression.Column
	// The caller should assure partition info is not nil.
	partitionPruneExprs := make([]expression.Expression, 0, len(pi.Definitions))
	var buf bytes.Buffer
	schema := expression.NewSchema(columns...)
	for i := 0; i < int(pi.Num); i++ {
		fmt.Fprintf(&buf, "MOD(ABS(%s),(%d))=%d", pi.Expr, pi.Num, i)
		exprs, err := expression.ParseSimpleExprsWithSchema(ctx, buf.String(), schema)
		if err != nil {
			// If it got an error here, ddl may hang forever, so this error log is important.
			logutil.Logger(context.Background()).Error("wrong table partition expression", zap.String("expression", buf.String()), zap.Error(err))
			return nil, errors.Trace(err)
		}
		// Get a hash code in advance to prevent data race afterwards.
		exprs[0].HashCode(ctx.GetSessionVars().StmtCtx)
		partitionPruneExprs = append(partitionPruneExprs, exprs[0])
		buf.Reset()
	}
	exprs, err := expression.ParseSimpleExprsWithSchema(ctx, pi.Expr, schema)
	if err != nil {
		// If it got an error here, ddl may hang forever, so this error log is important.
		logutil.Logger(context.Background()).Error("wrong table partition expression", zap.String("expression", pi.Expr), zap.Error(err))
		return nil, errors.Trace(err)
	}
	exprs[0].HashCode(ctx.GetSessionVars().StmtCtx)
	if col, ok := exprs[0].(*expression.Column); ok {
		column = col
	}
	return &PartitionExpr{
		Column: column,
		Expr:   exprs[0],
		Ranges: partitionPruneExprs,
	}, nil
}

// PartitionExpr returns the partition expression.
func (t *partitionedTable) PartitionExpr(ctx sessionctx.Context, columns []*expression.Column) (*PartitionExpr, error) {
	// A simple trick to get the ForRangePruning
	if columns == nil {
		return t.partitionExpr, nil
	}
	// TODO: a better performance implementation:
	// traverse the Expression, find all columns and rewrite them.
	return newPartitionExprBySchema(ctx, t.meta, columns)
}

// PartitionRecordKey returns the partition recordKey.
func PartitionRecordKey(pid int64, handle int64) kv.Key {
	recordPrefix := tablecodec.GenTableRecordPrefix(pid)
	return tablecodec.EncodeRecordKey(recordPrefix, handle)
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
		ret := r[col.Index].GetInt64()
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
	if ret < 0 {
		ret = 0 - ret
	}
	return int(ret % int64(t.meta.Partition.Num)), nil
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
func (t *partitionedTable) AddRecord(ctx sessionctx.Context, r []types.Datum, opts ...*table.AddRecordOpt) (recordID int64, err error) {
	partitionInfo := t.meta.GetPartitionInfo()
	pid, err := t.locatePartition(ctx, partitionInfo, r)
	if err != nil {
		return 0, errors.Trace(err)
	}

	tbl := t.GetPartition(pid)
	return tbl.AddRecord(ctx, r, opts...)
}

// RemoveRecord implements table.Table RemoveRecord interface.
func (t *partitionedTable) RemoveRecord(ctx sessionctx.Context, h int64, r []types.Datum) error {
	partitionInfo := t.meta.GetPartitionInfo()
	pid, err := t.locatePartition(ctx, partitionInfo, r)
	if err != nil {
		return errors.Trace(err)
	}

	tbl := t.GetPartition(pid)
	return tbl.RemoveRecord(ctx, h, r)
}

// UpdateRecord implements table.Table UpdateRecord interface.
// `touched` means which columns are really modified, used for secondary indices.
// Length of `oldData` and `newData` equals to length of `t.WritableCols()`.
func (t *partitionedTable) UpdateRecord(ctx sessionctx.Context, h int64, currData, newData []types.Datum, touched []bool) error {
	partitionInfo := t.meta.GetPartitionInfo()
	from, err := t.locatePartition(ctx, partitionInfo, currData)
	if err != nil {
		return errors.Trace(err)
	}
	to, err := t.locatePartition(ctx, partitionInfo, newData)
	if err != nil {
		return errors.Trace(err)
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
			logutil.Logger(context.Background()).Error("update partition record fails", zap.String("message", "new record inserted while old record is not removed"), zap.Error(err))
			return errors.Trace(err)
		}
		return nil
	}

	tbl := t.GetPartition(to)
	return tbl.UpdateRecord(ctx, h, currData, newData, touched)
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
