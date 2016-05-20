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
	"sync"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/evaluator"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/optimizer/plan"
	"github.com/pingcap/tidb/parser/opcode"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/types"
	"github.com/pingcap/tidb/xapi"
	"github.com/pingcap/tidb/xapi/tablecodec"
	"github.com/pingcap/tipb/go-tipb"
)

// XSelectTableExec represents XAPI select executor.
type XSelectTableExec struct {
	table            table.Table
	tablePlan        *plan.TableScan
	where            *tipb.Expr
	ctx              context.Context
	result           *xapi.SelectResult
	subResult        *xapi.SubResult
	supportDesc      bool
	allFiltersPushed bool
}

// Next implements Executor Next interface.
func (e *XSelectTableExec) Next() (*Row, error) {
	if e.result == nil {
		err := e.doRequest()
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	for {
		if e.subResult == nil {
			var err error
			startTs := time.Now()
			e.subResult, err = e.result.Next()
			if err != nil {
				return nil, errors.Trace(err)
			}
			if e.subResult == nil {
				return nil, nil
			}
			log.Debugf("[TIME_TABLE_SCAN] %v", time.Now().Sub(startTs))
		}
		h, rowData, err := e.subResult.Next()
		if err != nil {
			return nil, errors.Trace(err)
		}
		if rowData == nil {
			e.subResult = nil
			continue
		}
		fullRowData := make([]types.Datum, len(e.tablePlan.Fields()))
		var j int
		for i, field := range e.tablePlan.Fields() {
			if field.Referenced {
				fullRowData[i] = rowData[j]
				field.Expr.SetDatum(rowData[j])
				j++
			}
		}
		return resultRowToRow(e.table, h, fullRowData, e.tablePlan.TableAsName), nil
	}
}

// Fields implements Executor Fields interface.
func (e *XSelectTableExec) Fields() []*ast.ResultField {
	return e.tablePlan.Fields()
}

// Close implements Executor Close interface.
func (e *XSelectTableExec) Close() error {
	if e.result != nil {
		e.result.Close()
	}
	return nil
}

func resultRowToRow(t table.Table, h int64, data []types.Datum, tableAsName *model.CIStr) *Row {
	entry := &RowKeyEntry{
		Handle:      h,
		Tbl:         t,
		TableAsName: tableAsName,
	}
	return &Row{Data: data, RowKeys: []*RowKeyEntry{entry}}
}

func (e *XSelectTableExec) doRequest() error {
	txn, err := e.ctx.GetTxn(false)
	if err != nil {
		return errors.Trace(err)
	}
	selReq := new(tipb.SelectRequest)
	startTs := txn.StartTS()
	selReq.StartTs = &startTs
	selReq.Fields = resultFieldsToPBExpression(e.tablePlan.Fields())
	selReq.Where = e.where
	selReq.Ranges = tableRangesToPBRanges(e.tablePlan.Ranges)
	if e.supportDesc {
		if e.tablePlan.Desc {
			selReq.OrderBy = append(selReq.OrderBy, &tipb.ByItem{Desc: &e.tablePlan.Desc})
		}
		// Limit can be pushed only if desc is supported.
		if e.allFiltersPushed {
			selReq.Limit = e.tablePlan.LimitCount
		}
	}
	columns := make([]*model.ColumnInfo, 0, len(e.tablePlan.Fields()))
	for _, v := range e.tablePlan.Fields() {
		if v.Referenced {
			columns = append(columns, v.Column)
		}
	}
	selReq.TableInfo = &tipb.TableInfo{
		TableId: proto.Int64(e.table.Meta().ID),
	}
	selReq.TableInfo.Columns = tablecodec.ColumnsToProto(columns, e.table.Meta().PKIsHandle)
	e.result, err = xapi.Select(txn.GetClient(), selReq, 1)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

// XSelectIndexExec represents XAPI select index executor.
type XSelectIndexExec struct {
	table       table.Table
	indexPlan   *plan.IndexScan
	ctx         context.Context
	where       *tipb.Expr
	supportDesc bool

	tasks      []*lookupTableTask
	taskCursor int
	indexOrder map[int64]int

	mu sync.Mutex
}

// BaseLookupTableTaskSize represents base number of handles for a lookupTableTask.
var BaseLookupTableTaskSize = 1024

// MaxLookupTableTaskSize represents max number of handles for a lookupTableTask.
var MaxLookupTableTaskSize = 1024

const (
	taskNew int = iota
	taskRunning
	taskDone
)

type lookupTableTask struct {
	handles []int64
	rows    []*Row
	cursor  int
	status  int
	done    bool
	doneCh  chan error
}

// Fields implements Executor Fields interface.
func (e *XSelectIndexExec) Fields() []*ast.ResultField {
	return e.indexPlan.Fields()
}

// Next implements Executor Next interface.
func (e *XSelectIndexExec) Next() (*Row, error) {
	if e.tasks == nil {
		startTs := time.Now()
		handles, err := e.fetchHandles()
		if err != nil {
			return nil, errors.Trace(err)
		}
		log.Debugf("[TIME_INDEX_SCAN] time: %v handles: %d", time.Now().Sub(startTs), len(handles))
		e.buildTableTasks(handles)
		limitCount := int64(-1)
		if e.indexPlan.LimitCount != nil {
			limitCount = *e.indexPlan.LimitCount
		}
		concurrency := 2
		if e.indexPlan.NoLimit {
			concurrency = len(e.tasks)
		} else if limitCount > int64(BaseLookupTableTaskSize) {
			concurrency = int(limitCount/int64(BaseLookupTableTaskSize) + 1)
		}
		log.Debugf("[TIME_INDEX_TABLE_CONCURRENT_SCAN] start %d workers", concurrency)
		e.runTableTasks(concurrency)
	}
	for {
		if e.taskCursor >= len(e.tasks) {
			return nil, nil
		}
		task := e.tasks[e.taskCursor]
		if !task.done {
			startTs := time.Now()
			err := <-task.doneCh
			if err != nil {
				return nil, errors.Trace(err)
			}
			task.done = true
			log.Debugf("[TIME_INDEX_TABLE_SCAN] time: %v handles: %d", time.Now().Sub(startTs), len(task.handles))
		}
		if task.cursor < len(task.rows) {
			row := task.rows[task.cursor]
			for i, field := range e.indexPlan.Fields() {
				field.Expr.SetDatum(row.Data[i])
			}
			task.cursor++
			return row, nil
		}
		e.taskCursor++
	}
}

func (e *XSelectIndexExec) pickAndExecTask() {
	for {
		// Pick a new task.
		e.mu.Lock()
		var task *lookupTableTask
		for _, t := range e.tasks {
			if t.status == taskNew {
				task = t
				task.status = taskRunning
				break
			}
		}
		e.mu.Unlock()
		if task == nil {
			// No more task to run.
			break
		}
		// Execute the picked task.
		err := e.executeTask(task)
		e.mu.Lock()
		task.status = taskDone
		e.mu.Unlock()
		task.doneCh <- err
	}
}

func (e *XSelectIndexExec) runTableTasks(n int) {
	for i := 0; i < n && i < len(e.tasks); i++ {
		go e.pickAndExecTask()
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
		sorter := &rowsSorter{order: e.indexOrder, rows: task.rows}
		if e.indexPlan.Desc && !e.supportDesc {
			sort.Sort(sort.Reverse(sorter))
		} else {
			sort.Sort(sorter)
		}
	}
	return nil
}

// Close implements Executor Close interface.
func (e *XSelectIndexExec) Close() error {
	return nil
}

// fetchHandle fetches all handles from the index.
// This should be optimized to fetch handles in batch.
func (e *XSelectIndexExec) fetchHandles() ([]int64, error) {
	idxResult, err := e.doIndexRequest()
	if err != nil {
		return nil, errors.Trace(err)
	}
	handles, err := extractHandlesFromIndexResult(idxResult)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if !e.indexPlan.OutOfOrder {
		// Save the index order.
		e.indexOrder = make(map[int64]int)
		for i, h := range handles {
			e.indexOrder[h] = i
		}
	}
	return handles, nil
}

func (e *XSelectIndexExec) buildTableTasks(handles []int64) {
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
	if e.indexPlan.Desc && !e.supportDesc {
		// Reverse tasks sizes.
		for i := 0; i < len(taskSizes)/2; i++ {
			j := len(taskSizes) - i - 1
			taskSizes[i], taskSizes[j] = taskSizes[j], taskSizes[i]
		}
	}
	e.tasks = make([]*lookupTableTask, len(taskSizes))
	for i, size := range taskSizes {
		task := &lookupTableTask{
			handles: handles[:size],
		}
		task.doneCh = make(chan error, 1)
		handles = handles[size:]
		e.tasks[i] = task
	}
	if e.indexPlan.Desc && !e.supportDesc {
		// Reverse tasks order.
		for i := 0; i < len(e.tasks)/2; i++ {
			j := len(e.tasks) - i - 1
			e.tasks[i], e.tasks[j] = e.tasks[j], e.tasks[i]
		}
	}
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

func (e *XSelectIndexExec) doIndexRequest() (*xapi.SelectResult, error) {
	txn, err := e.ctx.GetTxn(false)
	if err != nil {
		return nil, errors.Trace(err)
	}
	selIdxReq := new(tipb.SelectRequest)
	startTs := txn.StartTS()
	selIdxReq.StartTs = &startTs
	selIdxReq.IndexInfo = tablecodec.IndexToProto(e.table.Meta(), e.indexPlan.Index)
	if len(e.indexPlan.FilterConditions) == 0 {
		// Push limit to index request only if there is not filter conditions.
		selIdxReq.Limit = e.indexPlan.LimitCount
	}
	if e.indexPlan.Desc {
		selIdxReq.OrderBy = append(selIdxReq.OrderBy, &tipb.ByItem{Desc: &e.indexPlan.Desc})
	}
	fieldTypes := make([]*types.FieldType, len(e.indexPlan.Index.Columns))
	for i, v := range e.indexPlan.Index.Columns {
		fieldTypes[i] = &(e.table.Cols()[v.Offset].FieldType)
	}
	selIdxReq.Ranges, err = indexRangesToPBRanges(e.indexPlan.Ranges, fieldTypes)
	if err != nil {
		return nil, errors.Trace(err)
	}
	concurrency := 1
	if e.indexPlan.OutOfOrder {
		concurrency = 10
	}
	return xapi.Select(txn.GetClient(), selIdxReq, concurrency)
}

func (e *XSelectIndexExec) doTableRequest(handles []int64) (*xapi.SelectResult, error) {
	txn, err := e.ctx.GetTxn(false)
	if err != nil {
		return nil, errors.Trace(err)
	}
	// The handles are not in original index order, so we can't push limit here.
	selTableReq := new(tipb.SelectRequest)
	startTs := txn.StartTS()
	selTableReq.StartTs = &startTs
	columns := make([]*model.ColumnInfo, 0, len(e.indexPlan.Fields()))
	for _, v := range e.indexPlan.Fields() {
		if v.Referenced {
			columns = append(columns, v.Column)
		}
	}
	selTableReq.TableInfo = &tipb.TableInfo{
		TableId: proto.Int64(e.table.Meta().ID),
	}
	selTableReq.TableInfo.Columns = tablecodec.ColumnsToProto(columns, e.table.Meta().PKIsHandle)
	selTableReq.Fields = resultFieldsToPBExpression(e.indexPlan.Fields())
	for _, h := range handles {
		if h == math.MaxInt64 {
			// We can't convert MaxInt64 into an left closed, right open range.
			continue
		}
		pbRange := new(tipb.KeyRange)
		pbRange.Low = codec.EncodeInt(nil, h)
		pbRange.High = kv.Key(pbRange.Low).PrefixNext()
		selTableReq.Ranges = append(selTableReq.Ranges, pbRange)
	}
	selTableReq.Where = e.where
	return xapi.Select(txn.GetClient(), selTableReq, 10)
}

// conditionsToPBExpr tries to convert filter conditions to a tipb.Expr.
// not supported conditions will be returned in remained.
func (b *executorBuilder) conditionsToPBExpr(client kv.Client, exprs []ast.ExprNode, tn *ast.TableName) (pbexpr *tipb.Expr,
	remained []ast.ExprNode) {
	for _, expr := range exprs {
		v := b.exprToPBExpr(client, expr, tn)
		if v == nil {
			remained = append(remained, expr)
		} else {
			if pbexpr == nil {
				pbexpr = v
			} else {
				// merge multiple converted pb expression into an AND expression.
				pbexpr = &tipb.Expr{Tp: tipb.ExprType_And.Enum(), Children: []*tipb.Expr{pbexpr, v}}
			}
		}
	}
	return
}

func resultFieldsToPBExpression(fields []*ast.ResultField) []*tipb.Expr {
	return nil
}

func tableRangesToPBRanges(tableRanges []plan.TableRange) []*tipb.KeyRange {
	hrs := make([]*tipb.KeyRange, 0, len(tableRanges))
	for _, tableRange := range tableRanges {
		pbRange := new(tipb.KeyRange)
		pbRange.Low = codec.EncodeInt(nil, tableRange.LowVal)
		hi := tableRange.HighVal
		if hi != math.MaxInt64 {
			hi++
		}
		pbRange.High = codec.EncodeInt(nil, hi)
		hrs = append(hrs, pbRange)
	}
	return hrs
}

func indexRangesToPBRanges(ranges []*plan.IndexRange, fieldTypes []*types.FieldType) ([]*tipb.KeyRange, error) {
	keyRanges := make([]*tipb.KeyRange, 0, len(ranges))
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
		keyRanges = append(keyRanges, &tipb.KeyRange{Low: low, High: high})
	}
	return keyRanges, nil
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

func extractHandlesFromIndexResult(idxResult *xapi.SelectResult) ([]int64, error) {
	var handles []int64
	for {
		subResult, err := idxResult.Next()
		if err != nil {
			return nil, errors.Trace(err)
		}
		if subResult == nil {
			break
		}
		subHandles, err := extractHandlesFromIndexSubResult(subResult)
		if err != nil {
			return nil, errors.Trace(err)
		}
		handles = append(handles, subHandles...)
	}
	return handles, nil
}

func extractHandlesFromIndexSubResult(subResult *xapi.SubResult) ([]int64, error) {
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

func (e *XSelectIndexExec) extractRowsFromTableResult(t table.Table, tblResult *xapi.SelectResult) ([]*Row, error) {
	var rows []*Row
	for {
		subResult, err := tblResult.Next()
		if err != nil {
			return nil, errors.Trace(err)
		}
		if subResult == nil {
			break
		}
		subRows, err := e.extractRowsFromSubResult(t, subResult)
		if err != nil {
			return nil, errors.Trace(err)
		}
		rows = append(rows, subRows...)
	}
	return rows, nil
}

func (e *XSelectIndexExec) extractRowsFromSubResult(t table.Table, subResult *xapi.SubResult) ([]*Row, error) {
	var rows []*Row
	for {
		h, rowData, err := subResult.Next()
		if err != nil {
			return nil, errors.Trace(err)
		}
		if rowData == nil {
			break
		}
		fullRowData := make([]types.Datum, len(e.indexPlan.Fields()))
		var j int
		for i, field := range e.indexPlan.Fields() {
			if field.Referenced {
				fullRowData[i] = rowData[j]
				j++
			}
		}
		row := resultRowToRow(t, h, fullRowData, e.indexPlan.TableAsName)
		rows = append(rows, row)
	}
	return rows, nil
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
	default:
		return nil
	}
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
	case mysql.TypeBit, mysql.TypeSet, mysql.TypeEnum, mysql.TypeDecimal, mysql.TypeGeometry,
		mysql.TypeDate, mysql.TypeNewDate, mysql.TypeDatetime, mysql.TypeTimestamp, mysql.TypeYear:
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
		pbExpr.Tp = tipb.ExprType_ColumnRef.Enum()
		pbExpr.Val = codec.EncodeInt(nil, column.Refer.Column.ID)
		return pbExpr
	}
	// If the column ID is in fields, it means the column is from an outer table,
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
		val = codec.EncodeDecimal(nil, d.GetMysqlDecimal())
	default:
		return nil
	}
	if !client.SupportRequestType(kv.ReqTypeSelect, int64(tp)) {
		return nil
	}
	return &tipb.Expr{Tp: tp.Enum(), Val: val}
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
	return &tipb.Expr{Tp: tp.Enum(), Children: []*tipb.Expr{leftExpr, rightExpr}}
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
	likeExpr := &tipb.Expr{Tp: tipb.ExprType_Like.Enum(), Children: []*tipb.Expr{targetExpr, patternExpr}}
	if !expr.Not {
		return likeExpr
	}
	return &tipb.Expr{Tp: tipb.ExprType_Not.Enum(), Children: []*tipb.Expr{likeExpr}}
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
	return &tipb.Expr{Tp: tipb.ExprType_Not.Enum(), Children: []*tipb.Expr{child}}
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
	inExpr := &tipb.Expr{Tp: tipb.ExprType_In.Enum(), Children: []*tipb.Expr{pbExpr, listExpr}}
	if !expr.Not {
		return inExpr
	}
	return &tipb.Expr{Tp: tipb.ExprType_Not.Enum(), Children: []*tipb.Expr{inExpr}}
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
		if d.Kind() != types.KindNull && d.Kind() != prevKind {
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
	return &tipb.Expr{Tp: tipb.ExprType_ValueList.Enum(), Val: val}
}
