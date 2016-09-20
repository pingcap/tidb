// Copyright 2016 PingCAP, Inc.
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

package executor

import (
	"math"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/distsql"
	"github.com/pingcap/tidb/evaluator"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/parser/opcode"
	"github.com/pingcap/tidb/plan"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/util/arena"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/types"
	"github.com/pingcap/tipb/go-tipb"
)

// pool size: 128 256 512 1K 2K 4K 8K 16K
var allocPools [8]sync.Pool

func init() {
	for i := 0; i < 8; i++ {
		allocPools[i].New = func() interface{} {
			return arena.NewAllocator(128 << uint(i))
		}
	}
}

// getAllocPool returns a suitable alloc pool using the estimated size. it's thread-safe
// because allocPools is read-only and the returned sync.Pool is thread-safe.
func getAllocPool(estimateSize int) *sync.Pool {
	idx := 0
	// is there any O(1) algorithm?
	for poolSize := 128; idx < 8; poolSize = poolSize << 1 {
		if poolSize >= estimateSize {
			break
		}
		idx++
	}
	return &allocPools[idx]
}

const defaultConcurrency int = 10

func resultRowToRow(t table.Table, h int64, data []types.Datum, tableAsName *model.CIStr) *Row {
	entry := &RowKeyEntry{
		Handle:      h,
		Tbl:         t,
		TableAsName: tableAsName,
	}
	return &Row{Data: data, RowKeys: []*RowKeyEntry{entry}}
}

// BaseLookupTableTaskSize represents base number of handles for a lookupTableTask.
var BaseLookupTableTaskSize = 1024

// MaxLookupTableTaskSize represents max number of handles for a lookupTableTask.
var MaxLookupTableTaskSize = 20480

type lookupTableTask struct {
	handles    []int64
	rows       []*Row
	cursor     int
	done       bool
	doneCh     chan error
	indexOrder map[int64]int
}

func (task *lookupTableTask) getRow() (*Row, error) {
	if !task.done {
		err := <-task.doneCh
		if err != nil {
			return nil, errors.Trace(err)
		}
		task.done = true
	}

	if task.cursor < len(task.rows) {
		row := task.rows[task.cursor]
		task.cursor++
		return row, nil
	}

	return nil, nil
}

type rowsSorter struct {
	order map[int64]int
	rows  []*Row
}

func (s *rowsSorter) Less(i, j int) bool {
	x := s.order[s.rows[i].RowKeys[0].Handle]
	y := s.order[s.rows[j].RowKeys[0].Handle]
	return x < y
}

func (s *rowsSorter) Len() int {
	return len(s.rows)
}

func (s *rowsSorter) Swap(i, j int) {
	s.rows[i], s.rows[j] = s.rows[j], s.rows[i]
}

func tableRangesToKVRanges(tid int64, tableRanges []plan.TableRange, alloc arena.Allocator) []kv.KeyRange {
	krs := make([]kv.KeyRange, 0, len(tableRanges))
	for _, tableRange := range tableRanges {
		startKey := encodeRowKeyWithHandle(tid, tableRange.LowVal, alloc)
		hi := tableRange.HighVal
		if hi != math.MaxInt64 {
			hi++
		}
		endKey := encodeRowKeyWithHandle(tid, hi, alloc)
		krs = append(krs, kv.KeyRange{StartKey: startKey, EndKey: endKey})
	}
	return krs
}

/*
 * Convert sorted handle to kv ranges.
 * For continuous handles, we should merge them to a single key range.
 */
func tableHandlesToKVRanges(tid int64, handles []int64, alloc arena.Allocator) []kv.KeyRange {
	krs := make([]kv.KeyRange, 0, len(handles))
	i := 0
	for i < len(handles) {
		h := handles[i]
		if h == math.MaxInt64 {
			// We can't convert MaxInt64 into an left closed, right open range.
			i++
			continue
		}
		j := i + 1
		endHandle := h + 1
		for ; j < len(handles); j++ {
			if handles[j] == endHandle {
				endHandle = handles[j] + 1
				continue
			}
			break
		}
		startKey := encodeRowKeyWithHandle(tid, h, alloc)
		endKey := encodeRowKeyWithHandle(tid, endHandle, alloc)
		krs = append(krs, kv.KeyRange{StartKey: startKey, EndKey: endKey})
		i = j
	}
	return krs
}

func encodeRowKeyWithHandle(tableID int64, handle int64, alloc arena.Allocator) kv.Key {
	buf := alloc.AllocWithLen(0, tablecodec.RowKeyWithHandleLen)
	return tablecodec.EncodeRowKeyWithHandle(tableID, handle, buf)
}

func indexRangesToKVRanges(tid, idxID int64, ranges []*plan.IndexRange, fieldTypes []*types.FieldType) ([]kv.KeyRange, error) {
	krs := make([]kv.KeyRange, 0, len(ranges))
	for _, ran := range ranges {
		err := convertIndexRangeTypes(ran, fieldTypes)
		if err != nil {
			return nil, errors.Trace(err)
		}

		low, err := codec.EncodeKey(nil, ran.LowVal...)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if ran.LowExclude {
			low = []byte(kv.Key(low).PrefixNext())
		}
		high, err := codec.EncodeKey(nil, ran.HighVal...)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if !ran.HighExclude {
			high = []byte(kv.Key(high).PrefixNext())
		}
		startKey := tablecodec.EncodeIndexSeekKey(tid, idxID, low)
		endKey := tablecodec.EncodeIndexSeekKey(tid, idxID, high)
		krs = append(krs, kv.KeyRange{StartKey: startKey, EndKey: endKey})
	}
	return krs, nil
}

func convertIndexRangeTypes(ran *plan.IndexRange, fieldTypes []*types.FieldType) error {
	for i := range ran.LowVal {
		if ran.LowVal[i].Kind() == types.KindMinNotNull {
			ran.LowVal[i].SetBytes([]byte{})
			continue
		}
		converted, err := ran.LowVal[i].ConvertTo(fieldTypes[i])
		if err != nil {
			return errors.Trace(err)
		}
		cmp, err := converted.CompareDatum(ran.LowVal[i])
		if err != nil {
			return errors.Trace(err)
		}
		ran.LowVal[i] = converted
		if cmp == 0 {
			continue
		}
		if cmp < 0 && !ran.LowExclude {
			// For int column a, a >= 1.1 is converted to a > 1.
			ran.LowExclude = true
		} else if cmp > 0 && ran.LowExclude {
			// For int column a, a > 1.9 is converted to a >= 2.
			ran.LowExclude = false
		}
		// The converted value has changed, the other column values doesn't matter.
		// For equal condition, converted value changed means there will be no match.
		// For non equal condition, this column would be the last one to build the range.
		// Break here to prevent the rest columns modify LowExclude again.
		break
	}
	for i := range ran.HighVal {
		if ran.HighVal[i].Kind() == types.KindMaxValue {
			continue
		}
		converted, err := ran.HighVal[i].ConvertTo(fieldTypes[i])
		if err != nil {
			return errors.Trace(err)
		}
		cmp, err := converted.CompareDatum(ran.HighVal[i])
		if err != nil {
			return errors.Trace(err)
		}
		ran.HighVal[i] = converted
		if cmp == 0 {
			continue
		}
		// For int column a, a < 1.1 is converted to a <= 1.
		if cmp < 0 && ran.HighExclude {
			ran.HighExclude = false
		}
		// For int column a, a <= 1.9 is converted to a < 2.
		if cmp > 0 && !ran.HighExclude {
			ran.HighExclude = true
		}
		break
	}
	return nil
}

// extractHandlesFromIndexResult gets some handles from SelectResult.
// It should be called in a loop until finished or error happened.
func extractHandlesFromIndexResult(idxResult distsql.SelectResult) (handles []int64, finish bool, err error) {
	subResult, e0 := idxResult.Next()
	if e0 != nil {
		err = errors.Trace(e0)
		return
	}
	if subResult == nil {
		finish = true
		return
	}
	handles, err = extractHandlesFromIndexSubResult(subResult)
	if err != nil {
		err = errors.Trace(err)
	}
	return
}

func extractHandlesFromIndexSubResult(subResult distsql.PartialResult) ([]int64, error) {
	var handles []int64
	for {
		h, data, err := subResult.Next()
		if err != nil {
			return nil, errors.Trace(err)
		}
		if data == nil {
			break
		}
		handles = append(handles, h)
	}
	return handles, nil
}

type int64Slice []int64

func (p int64Slice) Len() int           { return len(p) }
func (p int64Slice) Less(i, j int) bool { return p[i] < p[j] }
func (p int64Slice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

// exprToPBExpr converts an ast.ExprNode to a tipb.Expr, if not supported, nil will be returned.
func (b *executorBuilder) exprToPBExpr(client kv.Client, expr ast.ExprNode, tn *ast.TableName) *tipb.Expr {
	switch x := expr.(type) {
	case *ast.ValueExpr, *ast.ParamMarkerExpr:
		return b.datumToPBExpr(client, *expr.GetDatum())
	case *ast.ColumnNameExpr:
		return b.columnNameToPBExpr(client, x, tn)
	case *ast.BinaryOperationExpr:
		return b.binopToPBExpr(client, x, tn)
	case *ast.ParenthesesExpr:
		return b.exprToPBExpr(client, x.Expr, tn)
	case *ast.PatternLikeExpr:
		return b.likeToPBExpr(client, x, tn)
	case *ast.UnaryOperationExpr:
		return b.unaryToPBExpr(client, x, tn)
	case *ast.PatternInExpr:
		return b.patternInToPBExpr(client, x, tn)
	case *ast.SubqueryExpr:
		return b.subqueryToPBExpr(client, x)
	case *ast.AggregateFuncExpr:
		return b.aggFuncToPBExpr(client, x, tn)
	default:
		return nil
	}
}

func (b *executorBuilder) groupByItemToPB(client kv.Client, item *ast.ByItem, tn *ast.TableName) *tipb.ByItem {
	expr := b.exprToPBExpr(client, item.Expr, tn)
	if expr == nil {
		return nil
	}
	return &tipb.ByItem{Expr: expr}
}

func (b *executorBuilder) aggFuncToPBExpr(client kv.Client, af *ast.AggregateFuncExpr, tn *ast.TableName) *tipb.Expr {
	name := strings.ToLower(af.F)
	var tp tipb.ExprType
	switch name {
	case ast.AggFuncCount:
		tp = tipb.ExprType_Count
	case ast.AggFuncFirstRow:
		tp = tipb.ExprType_First
	case ast.AggFuncGroupConcat:
		tp = tipb.ExprType_GroupConcat
	case ast.AggFuncMax:
		tp = tipb.ExprType_Max
	case ast.AggFuncMin:
		tp = tipb.ExprType_Min
	case ast.AggFuncSum:
		tp = tipb.ExprType_Sum
	case ast.AggFuncAvg:
		tp = tipb.ExprType_Avg
	}
	if !client.SupportRequestType(kv.ReqTypeSelect, int64(tp)) {
		return nil
	}
	// convert it to pb
	children := make([]*tipb.Expr, 0, len(af.Args))
	for _, arg := range af.Args {
		pbArg := b.exprToPBExpr(client, arg, tn)
		if pbArg == nil {
			return nil
		}
		children = append(children, pbArg)
	}
	return &tipb.Expr{Tp: tp, Children: children}
}

func (b *executorBuilder) columnNameToPBExpr(client kv.Client, column *ast.ColumnNameExpr, tn *ast.TableName) *tipb.Expr {
	if !client.SupportRequestType(kv.ReqTypeSelect, int64(tipb.ExprType_ColumnRef)) {
		return nil
	}
	// Zero Column ID is not a column from table, can not support for now.
	if column.Refer.Column.ID == 0 {
		return nil
	}
	switch column.Refer.Expr.GetType().Tp {
	case mysql.TypeBit, mysql.TypeSet, mysql.TypeEnum, mysql.TypeGeometry:
		return nil
	}
	matched := false
	for _, f := range tn.GetResultFields() {
		if f.TableName == column.Refer.TableName && f.Column.ID == column.Refer.Column.ID {
			matched = true
			break
		}
	}
	if matched {
		pbExpr := new(tipb.Expr)
		pbExpr.Tp = tipb.ExprType_ColumnRef
		pbExpr.Val = codec.EncodeInt(nil, column.Refer.Column.ID)
		return pbExpr
	}
	// If the column ID isn't in fields, it means the column is from an outer table,
	// its value is available to use.
	return b.datumToPBExpr(client, *column.Refer.Expr.GetDatum())
}

func (b *executorBuilder) datumToPBExpr(client kv.Client, d types.Datum) *tipb.Expr {
	var tp tipb.ExprType
	var val []byte
	switch d.Kind() {
	case types.KindNull:
		tp = tipb.ExprType_Null
	case types.KindInt64:
		tp = tipb.ExprType_Int64
		val = codec.EncodeInt(nil, d.GetInt64())
	case types.KindUint64:
		tp = tipb.ExprType_Uint64
		val = codec.EncodeUint(nil, d.GetUint64())
	case types.KindString:
		tp = tipb.ExprType_String
		val = d.GetBytes()
	case types.KindBytes:
		tp = tipb.ExprType_Bytes
		val = d.GetBytes()
	case types.KindFloat32:
		tp = tipb.ExprType_Float32
		val = codec.EncodeFloat(nil, d.GetFloat64())
	case types.KindFloat64:
		tp = tipb.ExprType_Float64
		val = codec.EncodeFloat(nil, d.GetFloat64())
	case types.KindMysqlDuration:
		tp = tipb.ExprType_MysqlDuration
		val = codec.EncodeInt(nil, int64(d.GetMysqlDuration().Duration))
	case types.KindMysqlDecimal:
		tp = tipb.ExprType_MysqlDecimal
		val = codec.EncodeDecimal(nil, d)
	default:
		return nil
	}
	if !client.SupportRequestType(kv.ReqTypeSelect, int64(tp)) {
		return nil
	}
	return &tipb.Expr{Tp: tp, Val: val}
}

func (b *executorBuilder) binopToPBExpr(client kv.Client, expr *ast.BinaryOperationExpr, tn *ast.TableName) *tipb.Expr {
	var tp tipb.ExprType
	switch expr.Op {
	case opcode.LT:
		tp = tipb.ExprType_LT
	case opcode.LE:
		tp = tipb.ExprType_LE
	case opcode.EQ:
		tp = tipb.ExprType_EQ
	case opcode.NE:
		tp = tipb.ExprType_NE
	case opcode.GE:
		tp = tipb.ExprType_GE
	case opcode.GT:
		tp = tipb.ExprType_GT
	case opcode.NullEQ:
		tp = tipb.ExprType_NullEQ
	case opcode.AndAnd:
		tp = tipb.ExprType_And
	case opcode.OrOr:
		tp = tipb.ExprType_Or
	case opcode.Plus:
		tp = tipb.ExprType_Plus
	case opcode.Div:
		tp = tipb.ExprType_Div
	default:
		return nil
	}
	if !client.SupportRequestType(kv.ReqTypeSelect, int64(tp)) {
		return nil
	}
	leftExpr := b.exprToPBExpr(client, expr.L, tn)
	if leftExpr == nil {
		return nil
	}
	rightExpr := b.exprToPBExpr(client, expr.R, tn)
	if rightExpr == nil {
		return nil
	}
	return &tipb.Expr{Tp: tp, Children: []*tipb.Expr{leftExpr, rightExpr}}
}

// Only patterns like 'abc', '%abc', 'abc%', '%abc%' can be converted to *tipb.Expr for now.
func (b *executorBuilder) likeToPBExpr(client kv.Client, expr *ast.PatternLikeExpr, tn *ast.TableName) *tipb.Expr {
	if expr.Escape != '\\' {
		return nil
	}
	patternDatum := expr.Pattern.GetDatum()
	if patternDatum.Kind() != types.KindString {
		return nil
	}
	patternStr := patternDatum.GetString()
	for i, r := range patternStr {
		switch r {
		case '\\', '_':
			return nil
		case '%':
			if i != 0 && i != len(patternStr)-1 {
				return nil
			}
		}
	}
	patternExpr := b.exprToPBExpr(client, expr.Pattern, tn)
	if patternExpr == nil {
		return nil
	}
	targetExpr := b.exprToPBExpr(client, expr.Expr, tn)
	if targetExpr == nil {
		return nil
	}
	likeExpr := &tipb.Expr{Tp: tipb.ExprType_Like, Children: []*tipb.Expr{targetExpr, patternExpr}}
	if !expr.Not {
		return likeExpr
	}
	return &tipb.Expr{Tp: tipb.ExprType_Not, Children: []*tipb.Expr{likeExpr}}
}

func (b *executorBuilder) unaryToPBExpr(client kv.Client, expr *ast.UnaryOperationExpr, tn *ast.TableName) *tipb.Expr {
	switch expr.Op {
	case opcode.Not:
		if !client.SupportRequestType(kv.ReqTypeSelect, int64(tipb.ExprType_Not)) {
			return nil
		}
	default:
		return nil
	}
	child := b.exprToPBExpr(client, expr.V, tn)
	if child == nil {
		return nil
	}
	return &tipb.Expr{Tp: tipb.ExprType_Not, Children: []*tipb.Expr{child}}
}

func (b *executorBuilder) subqueryToPBExpr(client kv.Client, expr *ast.SubqueryExpr) *tipb.Expr {
	if !client.SupportRequestType(kv.ReqTypeSelect, int64(tipb.ExprType_ValueList)) {
		return nil
	}
	if expr.Correlated || len(expr.Query.GetResultFields()) != 1 {
		// We only push down evaluated non-correlated subquery which has only one field.
		return nil
	}
	err := evaluator.EvalSubquery(b.ctx, expr)
	if err != nil {
		b.err = errors.Trace(err)
		return nil
	}
	if expr.Datum.Kind() != types.KindRow {
		// Do not push down datum kind is not row.
		return nil
	}
	return b.datumsToValueList(expr.Datum.GetRow())
}

func (b *executorBuilder) patternInToPBExpr(client kv.Client, expr *ast.PatternInExpr, tn *ast.TableName) *tipb.Expr {
	if !client.SupportRequestType(kv.ReqTypeSelect, int64(tipb.ExprType_In)) {
		return nil
	}
	pbExpr := b.exprToPBExpr(client, expr.Expr, tn)
	if pbExpr == nil {
		return nil
	}
	var listExpr *tipb.Expr
	if expr.Sel != nil {
		listExpr = b.exprToPBExpr(client, expr.Sel, tn)
	} else {
		listExpr = b.exprListToPBExpr(client, expr.List, tn)
	}
	if listExpr == nil {
		return nil
	}
	inExpr := &tipb.Expr{Tp: tipb.ExprType_In, Children: []*tipb.Expr{pbExpr, listExpr}}
	if !expr.Not {
		return inExpr
	}
	return &tipb.Expr{Tp: tipb.ExprType_Not, Children: []*tipb.Expr{inExpr}}
}

func (b *executorBuilder) exprListToPBExpr(client kv.Client, list []ast.ExprNode, tn *ast.TableName) *tipb.Expr {
	if !client.SupportRequestType(kv.ReqTypeSelect, int64(tipb.ExprType_ValueList)) {
		return nil
	}
	// Only list of *ast.ValueExpr can be push down.
	datums := make([]types.Datum, 0, len(list))
	for _, v := range list {
		x, ok := v.(*ast.ValueExpr)
		if !ok {
			return nil
		}
		if b.datumToPBExpr(client, x.Datum) == nil {
			return nil
		}
		datums = append(datums, x.Datum)
	}
	return b.datumsToValueList(datums)
}

func (b *executorBuilder) datumsToValueList(datums []types.Datum) *tipb.Expr {
	// Don't push value list that has different datum kind.
	prevKind := types.KindNull
	for _, d := range datums {
		if prevKind == types.KindNull {
			prevKind = d.Kind()
		}
		if !d.IsNull() && d.Kind() != prevKind {
			return nil
		}
	}
	err := types.SortDatums(datums)
	if err != nil {
		b.err = errors.Trace(err)
		return nil
	}
	val, err := codec.EncodeValue(nil, datums...)
	if err != nil {
		b.err = errors.Trace(err)
		return nil
	}
	return &tipb.Expr{Tp: tipb.ExprType_ValueList, Val: val}
}

// XExecutor defines some interfaces used by dist-sql.
type XExecutor interface {
	// AddAggregate adds aggregate info into an executor.
	AddAggregate(funcs []*tipb.Expr, byItems []*tipb.ByItem, fields []*types.FieldType)
	// GetTable gets the TableInfo of this XExecutor.
	GetTable() *model.TableInfo
	// AddLimit try to add limit to XExecutor. If success, return true.
	AddLimit(l *plan.Limit) bool
}

// Closeable is a interface for closeable structures.
type Closeable interface {
	// Close closes the object.
	Close() error
}

func closeAll(objs ...Closeable) error {
	for _, obj := range objs {
		if obj != nil {
			err := obj.Close()
			if err != nil {
				return errors.Trace(err)
			}
		}
	}
	return nil
}

// XSelectIndexExec represents XAPI select index executor without result fields.
type XSelectIndexExec struct {
	tableInfo     *model.TableInfo
	table         table.Table
	asName        *model.CIStr
	ctx           context.Context
	supportDesc   bool
	isMemDB       bool
	result        distsql.SelectResult
	partialResult distsql.PartialResult
	where         *tipb.Expr
	startTS       uint64

	tasks    chan *lookupTableTask
	tasksErr error // not nil if tasks closed due to error.
	taskCurr *lookupTableTask

	indexPlan *plan.PhysicalIndexScan

	returnedRows uint64 // returned row count

	mu sync.Mutex

	/*
	   The following attributes are used for aggregation push down.
	   aggFuncs is the aggregation functions in protobuf format. They will be added to distsql request msg.
	   byItem is the groupby items in protobuf format. They will be added to distsql request msg.
	   aggFields is used to decode returned rows from distsql.
	   aggregate indicates of the executor is handling aggregate result.
	   It is more convenient to use a single varible than use a long condition.
	*/
	aggFuncs  []*tipb.Expr
	byItems   []*tipb.ByItem
	aggFields []*types.FieldType
	aggregate bool
}

// AddAggregate implements XExecutor interface.
func (e *XSelectIndexExec) AddAggregate(funcs []*tipb.Expr, byItems []*tipb.ByItem, fields []*types.FieldType) {
	e.aggFuncs = funcs
	e.byItems = byItems
	e.aggFields = fields
	e.aggregate = true
	client := e.ctx.GetClient()
	if !client.SupportRequestType(kv.ReqTypeIndex, kv.ReqSubTypeGroupBy) {
		e.indexPlan.DoubleRead = true
	}
}

// AddLimit implements XExecutor interface.
func (e *XSelectIndexExec) AddLimit(limit *plan.Limit) bool {
	cnt := int64(limit.Offset + limit.Count)
	if e.indexPlan.LimitCount == nil {
		e.indexPlan.LimitCount = &cnt
		return true
	}
	return false
}

// GetTable implements XExecutor interface.
func (e *XSelectIndexExec) GetTable() *model.TableInfo {
	return e.tableInfo
}

// Fields implements Exec Fields interface.
func (e *XSelectIndexExec) Fields() []*ast.ResultField {
	return nil
}

// Schema implements Exec Schema interface.
func (e *XSelectIndexExec) Schema() expression.Schema {
	return e.indexPlan.GetSchema()
}

// Close implements Exec Close interface.
func (e *XSelectIndexExec) Close() error {
	err := closeAll(e.result, e.partialResult)
	if err != nil {
		return errors.Trace(err)
	}
	e.result = nil
	e.partialResult = nil
	e.taskCurr = nil
	e.tasks = nil
	e.returnedRows = 0
	return nil
}

// Next implements Executor Next interface.
func (e *XSelectIndexExec) Next() (*Row, error) {
	if e.indexPlan.LimitCount != nil && e.returnedRows >= uint64(*e.indexPlan.LimitCount) {
		return nil, nil
	}
	e.returnedRows++
	if e.indexPlan.DoubleRead {
		return e.nextForDoubleRead()
	}
	return e.nextForSingleRead()
}

func (e *XSelectIndexExec) nextForSingleRead() (*Row, error) {
	if e.result == nil {
		var err error
		e.result, err = e.doIndexRequest()
		if err != nil {
			return nil, errors.Trace(err)
		}
		if e.aggregate {
			// The returned rows should be aggregate partial result.
			e.result.SetFields(e.aggFields)
		}
		e.result.Fetch()
	}
	for {
		if e.partialResult == nil {
			var err error
			e.partialResult, err = e.result.Next()
			if err != nil {
				return nil, errors.Trace(err)
			}
			if e.partialResult == nil {
				return nil, nil
			}
		}
		h, rowData, err := e.partialResult.Next()
		if err != nil {
			return nil, errors.Trace(err)
		}
		if rowData == nil {
			e.partialResult = nil
			continue
		}
		if e.aggregate {
			return &Row{Data: rowData}, nil
		}
		rowData = e.indexRowToTableRow(h, rowData)
		return resultRowToRow(e.table, h, rowData, e.asName), nil
	}
}

func (e *XSelectIndexExec) indexRowToTableRow(handle int64, indexRow []types.Datum) []types.Datum {
	tableRow := make([]types.Datum, len(e.indexPlan.Columns))
	for i, tblCol := range e.indexPlan.Columns {
		if mysql.HasPriKeyFlag(tblCol.Flag) && e.indexPlan.Table.PKIsHandle {
			tableRow[i] = types.NewIntDatum(handle)
			continue
		}
		for j, idxCol := range e.indexPlan.Index.Columns {
			if tblCol.Name.L == idxCol.Name.L {
				tableRow[i] = indexRow[j]
				break
			}
		}
	}
	return tableRow
}

func (e *XSelectIndexExec) nextForDoubleRead() (*Row, error) {
	var startTs time.Time
	if e.tasks == nil {
		startTs = time.Now()
		idxResult, err := e.doIndexRequest()
		if err != nil {
			return nil, errors.Trace(err)
		}
		idxResult.IgnoreData()
		idxResult.Fetch()

		// Use a background goroutine to fetch index, put the result in e.tasks.
		// e.tasks serves as a pipeline, so fetch index and get table data would
		// run concurrency.
		e.tasks = make(chan *lookupTableTask, 50)
		go e.fetchHandles(idxResult, e.tasks)
	}

	for {
		if e.taskCurr == nil {
			taskCurr, ok := <-e.tasks
			if !ok {
				log.Debugf("[TIME_INDEX_TABLE_SCAN] time: %v", time.Since(startTs))
				return nil, e.tasksErr
			}
			e.taskCurr = taskCurr
		}

		row, err := e.taskCurr.getRow()
		if err != nil || row != nil {
			return row, errors.Trace(err)
		}
		e.taskCurr = nil
	}
}

const concurrencyLimit int = 30

// addWorker add a worker for lookupTableTask.
// It's not thread-safe and should be called in fetchHandles goroutine only.
func addWorker(e *XSelectIndexExec, ch chan *lookupTableTask, concurrency *int) {
	if *concurrency <= concurrencyLimit {
		go e.pickAndExecTask(ch)
		*concurrency = *concurrency + 1
	}
}

func (e *XSelectIndexExec) fetchHandles(idxResult distsql.SelectResult, ch chan<- *lookupTableTask) {
	defer close(ch)

	workCh := make(chan *lookupTableTask, 1)
	defer close(workCh)

	var concurrency int
	addWorker(e, workCh, &concurrency)

	totalHandles := 0
	startTs := time.Now()
	for {
		handles, finish, err := extractHandlesFromIndexResult(idxResult)
		if err != nil || finish {
			e.tasksErr = errors.Trace(err)
			log.Debugf("[TIME_INDEX_SCAN] time: %v handles: %d concurrency: %d",
				time.Since(startTs),
				totalHandles,
				concurrency)
			return
		}

		totalHandles += len(handles)
		tasks := e.buildTableTasks(handles)
		for _, task := range tasks {
			if concurrency < len(tasks) {
				addWorker(e, workCh, &concurrency)
			}

			select {
			case workCh <- task:
			default:
				addWorker(e, workCh, &concurrency)
				workCh <- task
			}
			ch <- task
		}
	}
}

func (e *XSelectIndexExec) doIndexRequest() (distsql.SelectResult, error) {
	selIdxReq := new(tipb.SelectRequest)
	selIdxReq.StartTs = e.startTS
	selIdxReq.IndexInfo = distsql.IndexToProto(e.table.Meta(), e.indexPlan.Index)
	if e.indexPlan.Desc {
		selIdxReq.OrderBy = append(selIdxReq.OrderBy, &tipb.ByItem{Desc: e.indexPlan.Desc})
	}
	fieldTypes := make([]*types.FieldType, len(e.indexPlan.Index.Columns))
	for i, v := range e.indexPlan.Index.Columns {
		fieldTypes[i] = &(e.table.Cols()[v.Offset].FieldType)
	}
	if !e.indexPlan.DoubleRead || e.where == nil {
		// TODO: when where condition is all index columns limit can be pushed too.
		selIdxReq.Limit = e.indexPlan.LimitCount
	}
	concurrency := 1
	if !e.indexPlan.DoubleRead {
		concurrency = defaultConcurrency
		selIdxReq.Aggregates = e.aggFuncs
		selIdxReq.GroupBy = e.byItems
		selIdxReq.Where = e.where
	} else if e.indexPlan.OutOfOrder {
		concurrency = defaultConcurrency
	}

	keyRanges, err := indexRangesToKVRanges(e.table.Meta().ID, e.indexPlan.Index.ID, e.indexPlan.Ranges, fieldTypes)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return distsql.Select(e.ctx.GetClient(), selIdxReq, keyRanges, concurrency, !e.indexPlan.OutOfOrder)
}

func (e *XSelectIndexExec) buildTableTasks(handles []int64) []*lookupTableTask {
	// Build tasks with increasing batch size.
	var taskSizes []int
	total := len(handles)
	batchSize := BaseLookupTableTaskSize
	for total > 0 {
		if batchSize > total {
			batchSize = total
		}
		taskSizes = append(taskSizes, batchSize)
		total -= batchSize
		if batchSize < MaxLookupTableTaskSize {
			batchSize *= 2
		}
	}

	var indexOrder map[int64]int
	if !e.indexPlan.OutOfOrder {
		// Save the index order.
		indexOrder = make(map[int64]int, len(handles))
		for i, h := range handles {
			indexOrder[h] = i
		}
	}

	tasks := make([]*lookupTableTask, len(taskSizes))
	for i, size := range taskSizes {
		task := &lookupTableTask{
			handles:    handles[:size],
			indexOrder: indexOrder,
		}
		task.doneCh = make(chan error, 1)
		handles = handles[size:]

		tasks[i] = task
	}

	return tasks
}

// pickAndExecTask is a worker function, the common usage is
// go e.pickAndExecTask(ch)
func (e *XSelectIndexExec) pickAndExecTask(ch <-chan *lookupTableTask) {
	for task := range ch {
		err := e.executeTask(task)
		task.doneCh <- err
	}
}

func (e *XSelectIndexExec) executeTask(task *lookupTableTask) error {
	sort.Sort(int64Slice(task.handles))
	tblResult, err := e.doTableRequest(task.handles)
	if err != nil {
		return errors.Trace(err)
	}
	task.rows, err = e.extractRowsFromTableResult(e.table, tblResult)
	if err != nil {
		return errors.Trace(err)
	}
	if !e.indexPlan.OutOfOrder {
		// Restore the index order.
		sorter := &rowsSorter{order: task.indexOrder, rows: task.rows}
		if e.indexPlan.Desc && !e.supportDesc {
			sort.Sort(sort.Reverse(sorter))
		} else {
			sort.Sort(sorter)
		}
	}
	return nil
}

func (e *XSelectIndexExec) extractRowsFromTableResult(t table.Table, tblResult distsql.SelectResult) ([]*Row, error) {
	var rows []*Row
	for {
		partialResult, err := tblResult.Next()
		if err != nil {
			return nil, errors.Trace(err)
		}
		if partialResult == nil {
			break
		}
		subRows, err := e.extractRowsFromPartialResult(t, partialResult)
		if err != nil {
			return nil, errors.Trace(err)
		}
		rows = append(rows, subRows...)
	}
	return rows, nil
}

func (e *XSelectIndexExec) extractRowsFromPartialResult(t table.Table, partialResult distsql.PartialResult) ([]*Row, error) {
	var rows []*Row
	for {
		h, rowData, err := partialResult.Next()
		if err != nil {
			return nil, errors.Trace(err)
		}
		if rowData == nil {
			break
		}
		row := resultRowToRow(t, h, rowData, e.indexPlan.TableAsName)
		rows = append(rows, row)
	}
	return rows, nil
}

func (e *XSelectIndexExec) doTableRequest(handles []int64) (distsql.SelectResult, error) {
	// The handles are not in original index order, so we can't push limit here.
	selTableReq := new(tipb.SelectRequest)
	if e.indexPlan.OutOfOrder {
		selTableReq.Limit = e.indexPlan.LimitCount
	}
	selTableReq.StartTs = e.startTS
	selTableReq.TableInfo = &tipb.TableInfo{
		TableId: e.table.Meta().ID,
	}
	selTableReq.TableInfo.Columns = distsql.ColumnsToProto(e.indexPlan.Columns, e.table.Meta().PKIsHandle)
	selTableReq.Where = e.where
	// Aggregate Info
	selTableReq.Aggregates = e.aggFuncs
	selTableReq.GroupBy = e.byItems
	keyRanges := tableHandlesToKVRanges(e.table.Meta().ID, handles, arena.StdAllocator)
	resp, err := distsql.Select(e.ctx.GetClient(), selTableReq, keyRanges, defaultConcurrency, false)
	if err != nil {
		return nil, errors.Trace(err)
	}

	if e.aggregate {
		// The returned rows should be aggregate partial result.
		resp.SetFields(e.aggFields)
	}
	resp.Fetch()
	return resp, nil
}

// keyRanger can provide []kv.KeyRange and estimate the size of
// memory needed to encode []kv.KeyRange
type keyRanger interface {
	estimateSize() int
	keyRanges(arena.Allocator) []kv.KeyRange
}

type tableHandleRanger struct {
	tid     int64
	handles []int64
}

func (tr *tableHandleRanger) estimateSize() int {
	return 2*len(tr.handles)*tablecodec.RowKeyWithHandleLen + 20
}

func (tr *tableHandleRanger) keyRanges(allocator arena.Allocator) []kv.KeyRange {
	return tableHandlesToKVRanges(tr.tid, tr.handles, allocator)
}

type tableRangeRanger struct {
	tid         int64
	tableRanges []plan.TableRange
}

func (tr *tableRangeRanger) estimateSize() int {
	return 2*len(tr.tableRanges)*tablecodec.RowKeyWithHandleLen + 20
}

func (tr *tableRangeRanger) keyRanges(allocator arena.Allocator) []kv.KeyRange {
	return tableRangesToKVRanges(tr.tid, tr.tableRanges, allocator)
}

// distsqlSelect is a wrapper of distsql.Select, it provides a allocator for generating []kv.KeyRange.
// prepare []kv.KeyRange and call distsql.Select is conceptually equivalent to provide a keyRanger
// and call distsqlSelect.
func distsqlSelect(client kv.Client, req *tipb.SelectRequest, kr keyRanger, concurrency int, keepOrder bool) (distsql.SelectResult, error) {
	allocPool := getAllocPool(kr.estimateSize())
	allocator := allocPool.Get().(arena.Allocator)

	keyRanges := kr.keyRanges(allocator)
	resp, err := distsql.Select(client, req, keyRanges, concurrency, keepOrder)

	return selectResultFinalize{
		resp, allocator, allocPool,
	}, err
}

type selectResultFinalize struct {
	distsql.SelectResult
	allocator arena.Allocator
	allocPool *sync.Pool
}

func (s selectResultFinalize) Close() error {
	err := s.SelectResult.Close()
	s.allocator.Reset()
	s.allocPool.Put(s.allocator)
	return err
}

// XSelectTableExec represents XAPI select executor without result fields.
type XSelectTableExec struct {
	tableInfo     *model.TableInfo
	table         table.Table
	asName        *model.CIStr
	ctx           context.Context
	supportDesc   bool
	isMemDB       bool
	result        distsql.SelectResult
	partialResult distsql.PartialResult
	where         *tipb.Expr
	Columns       []*model.ColumnInfo
	schema        expression.Schema
	ranges        []plan.TableRange
	desc          bool
	limitCount    *int64
	returnedRows  uint64 // returned rowCount
	keepOrder     bool
	startTS       uint64

	/*
	   The following attributes are used for aggregation push down.
	   aggFuncs is the aggregation functions in protobuf format. They will be added to distsql request msg.
	   byItem is the groupby items in protobuf format. They will be added to distsql request msg.
	   aggFields is used to decode returned rows from distsql.
	   aggregate indicates of the executor is handling aggregate result.
	   It is more convenient to use a single varible than use a long condition.
	*/
	aggFuncs  []*tipb.Expr
	byItems   []*tipb.ByItem
	aggFields []*types.FieldType
	aggregate bool
}

// AddLimit implements XExecutor interface.
func (e *XSelectTableExec) AddLimit(limit *plan.Limit) bool {
	cnt := int64(limit.Offset + limit.Count)
	if e.limitCount == nil {
		e.limitCount = &cnt
		return true
	}
	return false
}

// Schema implements Executor Schema interface.
func (e *XSelectTableExec) Schema() expression.Schema {
	return e.schema
}

func (e *XSelectTableExec) doRequest() error {
	var err error
	selReq := new(tipb.SelectRequest)
	selReq.StartTs = e.startTS
	selReq.Where = e.where
	columns := e.Columns
	selReq.TableInfo = &tipb.TableInfo{
		TableId: e.tableInfo.ID,
	}
	if e.supportDesc && e.desc {
		selReq.OrderBy = append(selReq.OrderBy, &tipb.ByItem{Desc: e.desc})
	}
	selReq.Limit = e.limitCount
	selReq.TableInfo.Columns = distsql.ColumnsToProto(columns, e.tableInfo.PKIsHandle)
	// Aggregate Info
	selReq.Aggregates = e.aggFuncs
	selReq.GroupBy = e.byItems

	kvRanges := tableRangesToKVRanges(e.table.Meta().ID, e.ranges, arena.StdAllocator)
	e.result, err = distsql.Select(e.ctx.GetClient(), selReq, kvRanges, defaultConcurrency, e.keepOrder)
	if err != nil {
		return errors.Trace(err)
	}
	//if len(selReq.Aggregates) > 0 || len(selReq.GroupBy) > 0 {
	if e.aggregate {
		// The returned rows should be aggregate partial result.
		e.result.SetFields(e.aggFields)
	}
	e.result.Fetch()
	return nil
}

// Close implements Executor Close interface.
func (e *XSelectTableExec) Close() error {
	err := closeAll(e.result, e.partialResult)
	if err != nil {
		return errors.Trace(err)
	}
	e.result = nil
	e.partialResult = nil
	e.returnedRows = 0
	return nil
}

// Next implements Executor interface.
func (e *XSelectTableExec) Next() (*Row, error) {
	if e.limitCount != nil && e.returnedRows >= uint64(*e.limitCount) {
		return nil, nil
	}
	if e.result == nil {
		err := e.doRequest()
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	for {
		if e.partialResult == nil {
			var err error
			startTs := time.Now()
			e.partialResult, err = e.result.Next()
			if err != nil {
				return nil, errors.Trace(err)
			}
			if e.partialResult == nil {
				return nil, nil
			}
			duration := time.Since(startTs)
			if duration > 30*time.Millisecond {
				log.Infof("[TIME_TABLE_SCAN] %v", duration)
			} else {
				log.Debugf("[TIME_TABLE_SCAN] %v", duration)
			}
		}
		h, rowData, err := e.partialResult.Next()
		if err != nil {
			return nil, errors.Trace(err)
		}
		if rowData == nil {
			e.partialResult = nil
			continue
		}
		e.returnedRows++
		if e.aggregate {
			// compose aggreagte row
			return &Row{Data: rowData}, nil
		}
		return resultRowToRow(e.table, h, rowData, e.asName), nil
	}
}

// AddAggregate implements XExecutor interface.
func (e *XSelectTableExec) AddAggregate(funcs []*tipb.Expr, byItems []*tipb.ByItem, fields []*types.FieldType) {
	e.aggFuncs = funcs
	e.byItems = byItems
	e.aggFields = fields
	e.aggregate = true
}

// GetTable implements XExecutor interface.
func (e *XSelectTableExec) GetTable() *model.TableInfo {
	return e.tableInfo
}

// Fields implements Executor interface.
func (e *XSelectTableExec) Fields() []*ast.ResultField {
	return nil
}
