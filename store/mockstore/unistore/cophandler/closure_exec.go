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

package cophandler

import (
	"bytes"
	"fmt"
	"math"
	"sort"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/expression/aggregation"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/store/mockstore/unistore/tikv/dbreader"
	"github.com/pingcap/tidb/store/mockstore/unistore/tikv/mvcc"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/codec"
	mockpkg "github.com/pingcap/tidb/util/mock"
	"github.com/pingcap/tidb/util/rowcodec"
	"github.com/pingcap/tidb/util/timeutil"
	"github.com/pingcap/tipb/go-tipb"
)

const chunkMaxRows = 1024

const (
	pkColNotExists = iota
	pkColIsSigned
	pkColIsUnsigned
	pkColIsCommon
)

func mapPkStatusToHandleStatus(pkStatus int) tablecodec.HandleStatus {
	switch pkStatus {
	case pkColNotExists:
		return tablecodec.HandleNotNeeded
	case pkColIsCommon | pkColIsSigned:
		return tablecodec.HandleDefault
	case pkColIsUnsigned:
		return tablecodec.HandleIsUnsigned
	}
	return tablecodec.HandleDefault
}

func getExecutorListFromRootExec(rootExec *tipb.Executor) ([]*tipb.Executor, error) {
	executors := make([]*tipb.Executor, 0, 3)
	currentExec := rootExec
	for !isScanNode(currentExec) {
		executors = append(executors, currentExec)
		switch currentExec.Tp {
		case tipb.ExecType_TypeTopN:
			currentExec = currentExec.TopN.Child
		case tipb.ExecType_TypeStreamAgg, tipb.ExecType_TypeAggregation:
			currentExec = currentExec.Aggregation.Child
		case tipb.ExecType_TypeLimit:
			currentExec = currentExec.Limit.Child
		case tipb.ExecType_TypeExchangeSender:
			currentExec = currentExec.ExchangeSender.Child
		case tipb.ExecType_TypeSelection:
			currentExec = currentExec.Selection.Child
		default:
			return nil, errors.New("unsupported executor type " + currentExec.Tp.String())
		}
	}
	executors = append(executors, currentExec)
	for i, j := 0, len(executors)-1; i < j; i, j = i+1, j-1 {
		executors[i], executors[j] = executors[j], executors[i]
	}
	return executors, nil
}

func getExecutorList(dagReq *tipb.DAGRequest) ([]*tipb.Executor, error) {
	if len(dagReq.Executors) > 0 {
		return dagReq.Executors, nil
	}
	// convert TiFlash executors tree to executor list
	return getExecutorListFromRootExec(dagReq.RootExecutor)
}

func buildClosureExecutorFromExecutorList(dagCtx *dagContext, executors []*tipb.Executor, ce *closureExecutor) error {
	scanExec := executors[0]
	if scanExec.Tp == tipb.ExecType_TypeTableScan {
		ce.processor = &tableScanProcessor{closureExecutor: ce}
	} else if scanExec.Tp == tipb.ExecType_TypeIndexScan {
		ce.processor = &indexScanProcessor{closureExecutor: ce}
	}
	outputFieldTypes := make([]*types.FieldType, 0, 1)
	lastExecutor := executors[len(executors)-1]
	originalOutputFieldTypes := dagCtx.fieldTps
	if lastExecutor.Tp == tipb.ExecType_TypeAggregation || lastExecutor.Tp == tipb.ExecType_TypeStreamAgg {
		originalOutputFieldTypes = nil
		for _, agg := range lastExecutor.Aggregation.AggFunc {
			originalOutputFieldTypes = append(originalOutputFieldTypes, expression.PbTypeToFieldType(agg.FieldType))
		}
		for _, gby := range lastExecutor.Aggregation.GroupBy {
			originalOutputFieldTypes = append(originalOutputFieldTypes, expression.PbTypeToFieldType(gby.FieldType))
		}
	}
	if ce.outputOff != nil {
		for _, idx := range ce.outputOff {
			outputFieldTypes = append(outputFieldTypes, originalOutputFieldTypes[idx])
		}
	} else {
		outputFieldTypes = append(outputFieldTypes, originalOutputFieldTypes...)
	}
	if len(executors) == 1 {
		ce.resultFieldType = outputFieldTypes
		return nil
	}
	var err error
	if secondExec := executors[1]; secondExec.Tp == tipb.ExecType_TypeSelection {
		ce.selectionCtx.conditions, err = convertToExprs(ce.sc, ce.fieldTps, secondExec.Selection.Conditions)
		if err != nil {
			return errors.Trace(err)
		}
		ce.selectionCtx.execDetail = new(execDetail)
		ce.processor = &selectionProcessor{closureExecutor: ce}
	}
	switch lastExecutor.Tp {
	case tipb.ExecType_TypeLimit:
		ce.limit = int(lastExecutor.Limit.Limit)
	case tipb.ExecType_TypeTopN:
		err = buildTopNProcessor(ce, lastExecutor.TopN)
	case tipb.ExecType_TypeAggregation:
		err = buildHashAggProcessor(ce, dagCtx, lastExecutor.Aggregation)
	case tipb.ExecType_TypeStreamAgg:
		err = buildStreamAggProcessor(ce, dagCtx, executors)
	case tipb.ExecType_TypeSelection:
		ce.processor = &selectionProcessor{closureExecutor: ce}
	default:
		panic("unsupported executor type " + lastExecutor.Tp.String())
	}
	if err != nil {
		return err
	}
	ce.resultFieldType = outputFieldTypes
	return nil
}

// buildClosureExecutor build a closureExecutor for the DAGRequest.
// Currently the composition of executors are:
// 	tableScan|indexScan [selection] [topN | limit | agg]
func buildClosureExecutor(dagCtx *dagContext, dagReq *tipb.DAGRequest) (*closureExecutor, error) {
	scanExec, err := getScanExec(dagReq)
	if err != nil {
		return nil, err
	}
	ce, err := newClosureExecutor(dagCtx, dagReq.OutputOffsets, scanExec, dagReq.GetCollectRangeCounts())
	if err != nil {
		return nil, errors.Trace(err)
	}
	executors, err1 := getExecutorList(dagReq)
	if err1 != nil {
		return nil, err1
	}

	err = buildClosureExecutorFromExecutorList(dagCtx, executors, ce)
	if err != nil {
		return nil, err
	}
	return ce, nil
}

func convertToExprs(sc *stmtctx.StatementContext, fieldTps []*types.FieldType, pbExprs []*tipb.Expr) ([]expression.Expression, error) {
	exprs := make([]expression.Expression, 0, len(pbExprs))
	for _, expr := range pbExprs {
		e, err := expression.PBToExpr(expr, fieldTps, sc)
		if err != nil {
			return nil, errors.Trace(err)
		}
		exprs = append(exprs, e)
	}
	return exprs, nil
}

func isScanNode(executor *tipb.Executor) bool {
	switch executor.Tp {
	case tipb.ExecType_TypeTableScan,
		tipb.ExecType_TypeIndexScan:
		return true
	default:
		return false
	}
}

func getScanExecFromRootExec(rootExec *tipb.Executor) (*tipb.Executor, error) {
	currentExec := rootExec
	for !isScanNode(currentExec) {
		switch currentExec.Tp {
		case tipb.ExecType_TypeAggregation, tipb.ExecType_TypeStreamAgg:
			currentExec = currentExec.Aggregation.Child
		case tipb.ExecType_TypeLimit:
			currentExec = currentExec.Limit.Child
		case tipb.ExecType_TypeSelection:
			currentExec = currentExec.Selection.Child
		case tipb.ExecType_TypeTopN:
			currentExec = currentExec.TopN.Child
		case tipb.ExecType_TypeExchangeSender:
			currentExec = currentExec.ExchangeSender.Child
		default:
			return nil, errors.New("Unsupported DAG request")
		}
	}
	return currentExec, nil
}

func getScanExec(dagReq *tipb.DAGRequest) (*tipb.Executor, error) {
	if len(dagReq.Executors) > 0 {
		return dagReq.Executors[0], nil
	}
	return getScanExecFromRootExec(dagReq.RootExecutor)
}

func newClosureExecutor(dagCtx *dagContext, outputOffsets []uint32, scanExec *tipb.Executor, collectRangeCounts bool) (*closureExecutor, error) {
	e := &closureExecutor{
		dagContext: dagCtx,
		outputOff:  outputOffsets,
		startTS:    dagCtx.startTS,
		limit:      math.MaxInt64,
	}
	seCtx := mockpkg.NewContext()
	seCtx.GetSessionVars().StmtCtx = e.sc
	e.seCtx = seCtx
	switch scanExec.Tp {
	case tipb.ExecType_TypeTableScan:
		dagCtx.setColumnInfo(scanExec.TblScan.Columns)
		dagCtx.primaryCols = scanExec.TblScan.PrimaryColumnIds
		tblScan := scanExec.TblScan
		e.unique = true
		e.scanCtx.desc = tblScan.Desc
		e.scanType = TableScan
	case tipb.ExecType_TypeIndexScan:
		dagCtx.setColumnInfo(scanExec.IdxScan.Columns)
		idxScan := scanExec.IdxScan
		e.unique = idxScan.GetUnique()
		e.scanCtx.desc = idxScan.Desc
		e.initIdxScanCtx(idxScan)
		if collectRangeCounts {
			e.idxScanCtx.collectNDV = true
			e.idxScanCtx.prevVals = make([][]byte, e.idxScanCtx.columnLen)
		}
		e.scanType = IndexScan
	default:
		panic(fmt.Sprintf("unknown first executor type %s", scanExec.Tp))
	}
	ranges, err := extractKVRanges(dagCtx.dbReader.StartKey, dagCtx.dbReader.EndKey, dagCtx.keyRanges, e.scanCtx.desc)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if collectRangeCounts {
		e.counts = make([]int64, len(ranges))
		e.ndvs = make([]int64, len(ranges))
	}
	e.kvRanges = ranges
	e.scanCtx.chk = chunk.NewChunkWithCapacity(e.fieldTps, 32)
	if e.scanType == TableScan {
		e.scanCtx.decoder, err = newRowDecoder(e.evalContext.columnInfos, e.evalContext.fieldTps, e.evalContext.primaryCols, e.evalContext.sc.TimeZone)
		if err != nil {
			return nil, errors.Trace(err)
		}
		e.scanCtx.execDetail = new(execDetail)
	}
	return e, nil
}

func (e *closureExecutor) initIdxScanCtx(idxScan *tipb.IndexScan) {
	e.idxScanCtx = new(idxScanCtx)
	e.idxScanCtx.columnLen = len(e.columnInfos)
	e.idxScanCtx.pkStatus = pkColNotExists
	e.idxScanCtx.execDetail = new(execDetail)

	e.idxScanCtx.primaryColumnIds = idxScan.PrimaryColumnIds
	lastColumn := e.columnInfos[len(e.columnInfos)-1]
	if lastColumn.GetColumnId() == model.ExtraPidColID {
		lastColumn = e.columnInfos[len(e.columnInfos)-2]
		e.idxScanCtx.columnLen--
	}

	if len(e.idxScanCtx.primaryColumnIds) == 0 {
		if lastColumn.GetPkHandle() {
			if mysql.HasUnsignedFlag(uint(lastColumn.GetFlag())) {
				e.idxScanCtx.pkStatus = pkColIsUnsigned
			} else {
				e.idxScanCtx.pkStatus = pkColIsSigned
			}
			e.idxScanCtx.columnLen--
		} else if lastColumn.ColumnId == model.ExtraHandleID {
			e.idxScanCtx.pkStatus = pkColIsSigned
			e.idxScanCtx.columnLen--
		}
	} else {
		e.idxScanCtx.pkStatus = pkColIsCommon
		e.idxScanCtx.columnLen -= len(e.idxScanCtx.primaryColumnIds)
	}

	colInfos := make([]rowcodec.ColInfo, len(e.columnInfos))
	for i := range colInfos {
		col := e.columnInfos[i]
		colInfos[i] = rowcodec.ColInfo{
			ID:         col.ColumnId,
			Ft:         e.fieldTps[i],
			IsPKHandle: col.GetPkHandle(),
		}
	}
	e.idxScanCtx.colInfos = colInfos

	colIDs := make(map[int64]int, len(colInfos))
	for i, col := range colInfos[:e.idxScanCtx.columnLen] {
		colIDs[col.ID] = i
	}
}

func isCountAgg(pbAgg *tipb.Aggregation) bool {
	if len(pbAgg.AggFunc) == 1 && len(pbAgg.GroupBy) == 0 {
		aggFunc := pbAgg.AggFunc[0]
		if aggFunc.Tp == tipb.ExprType_Count && len(aggFunc.Children) == 1 {
			return true
		}
	}
	return false
}

func tryBuildCountProcessor(e *closureExecutor, executors []*tipb.Executor) (bool, error) {
	if len(executors) > 2 {
		return false, nil
	}
	agg := executors[1].Aggregation
	if !isCountAgg(agg) {
		return false, nil
	}
	child := agg.AggFunc[0].Children[0]
	switch child.Tp {
	case tipb.ExprType_ColumnRef:
		_, idx, err := codec.DecodeInt(child.Val)
		if err != nil {
			return false, errors.Trace(err)
		}
		e.aggCtx.col = e.columnInfos[idx]
		if e.aggCtx.col.PkHandle {
			e.processor = &countStarProcessor{skipVal: skipVal(true), closureExecutor: e}
		} else {
			e.processor = &countColumnProcessor{closureExecutor: e}
		}
	case tipb.ExprType_Null, tipb.ExprType_ScalarFunc:
		return false, nil
	default:
		e.processor = &countStarProcessor{skipVal: skipVal(true), closureExecutor: e}
	}
	e.aggCtx.execDetail = new(execDetail)
	return true, nil
}

func buildTopNProcessor(e *closureExecutor, topN *tipb.TopN) error {
	heap, conds, err := getTopNInfo(e.evalContext, topN)
	if err != nil {
		return errors.Trace(err)
	}

	ctx := &topNCtx{
		heap:         heap,
		orderByExprs: conds,
		sortRow:      newTopNSortRow(len(conds)),
		execDetail:   new(execDetail),
	}

	e.topNCtx = ctx
	e.processor = &topNProcessor{closureExecutor: e}
	return nil
}

func buildHashAggProcessor(e *closureExecutor, ctx *dagContext, agg *tipb.Aggregation) error {
	aggs, groupBys, err := getAggInfo(ctx, agg)
	if err != nil {
		return err
	}
	e.processor = &hashAggProcessor{
		closureExecutor: e,
		aggExprs:        aggs,
		groupByExprs:    groupBys,
		groups:          map[string]struct{}{},
		groupKeys:       nil,
		aggCtxsMap:      map[string][]*aggregation.AggEvaluateContext{},
	}
	e.aggCtx.execDetail = new(execDetail)
	return nil
}

func buildStreamAggProcessor(e *closureExecutor, ctx *dagContext, executors []*tipb.Executor) error {
	ok, err := tryBuildCountProcessor(e, executors)
	if err != nil || ok {
		return err
	}
	return buildHashAggProcessor(e, ctx, executors[len(executors)-1].Aggregation)
}

type execDetail struct {
	timeProcessed   time.Duration
	numProducedRows int
	numIterations   int
}

func (e *execDetail) update(begin time.Time, gotRow bool) {
	e.timeProcessed += time.Since(begin)
	e.numIterations++
	if gotRow {
		e.numProducedRows++
	}
}

func (e *execDetail) updateOnlyRows(gotRow int) {
	e.numProducedRows += gotRow
}

func (e *execDetail) buildSummary() *tipb.ExecutorExecutionSummary {
	costNs := uint64(e.timeProcessed / time.Nanosecond)
	rows := uint64(e.numProducedRows)
	numIter := uint64(e.numIterations)
	return &tipb.ExecutorExecutionSummary{
		TimeProcessedNs: &costNs,
		NumProducedRows: &rows,
		NumIterations:   &numIter,
	}
}

type scanType uint8

const (
	// TableScan means reading from a table by table scan
	TableScan scanType = iota
	// IndexScan means reading from a table by index scan
	IndexScan
)

// closureExecutor is an execution engine that flatten the DAGRequest.Executors to a single closure `processor` that
// process key/value pairs. We can define many closures for different kinds of requests, try to use the specially
// optimized one for some frequently used query.
type closureExecutor struct {
	*dagContext
	outputOff       []uint32
	resultFieldType []*types.FieldType
	seCtx           sessionctx.Context
	kvRanges        []kv.KeyRange
	startTS         uint64
	ignoreLock      bool
	lockChecked     bool
	scanType        scanType
	scanCtx         scanCtx
	idxScanCtx      *idxScanCtx
	selectionCtx    selectionCtx
	aggCtx          aggCtx
	topNCtx         *topNCtx
	mockReader      *mockReader

	rowCount int
	unique   bool
	limit    int

	oldChunks []tipb.Chunk
	oldRowBuf []byte
	processor closureProcessor

	counts []int64
	ndvs   []int64
	curNdv int64
}

func pbChunkToChunk(pbChk tipb.Chunk, chk *chunk.Chunk, fieldTypes []*types.FieldType) error {
	rowsData := pbChk.RowsData
	var err error
	decoder := codec.NewDecoder(chk, timeutil.SystemLocation())
	for len(rowsData) > 0 {
		for i := 0; i < len(fieldTypes); i++ {
			rowsData, err = decoder.DecodeOne(rowsData, i, fieldTypes[i])
			if err != nil {
				return err
			}
		}
	}
	return nil
}

type closureProcessor interface {
	dbreader.ScanProcessor
	Finish() error
}

type scanCtx struct {
	chk     *chunk.Chunk
	desc    bool
	decoder *rowcodec.ChunkDecoder

	execDetail *execDetail
}

type idxScanCtx struct {
	pkStatus         int
	columnLen        int
	colInfos         []rowcodec.ColInfo
	primaryColumnIds []int64
	execDetail       *execDetail
	collectNDV       bool
	prevVals         [][]byte
}

type aggCtx struct {
	col        *tipb.ColumnInfo
	execDetail *execDetail
}

type selectionCtx struct {
	conditions []expression.Expression
	execDetail *execDetail
}

type topNCtx struct {
	heap         *topNHeap
	orderByExprs []expression.Expression
	sortRow      *sortRow
	execDetail   *execDetail
}

type mockReader struct {
	chk          *chunk.Chunk
	currentIndex int
}

func (e *closureExecutor) execute() ([]tipb.Chunk, error) {
	err := e.checkRangeLock()
	if err != nil {
		return nil, errors.Trace(err)
	}
	dbReader := e.dbReader
	for i, ran := range e.kvRanges {
		e.curNdv = 0
		if e.isPointGetRange(ran) {
			val, err := dbReader.Get(ran.StartKey, e.startTS)
			if err != nil {
				return nil, errors.Trace(err)
			}
			if len(val) == 0 {
				continue
			}
			if e.counts != nil {
				e.counts[i]++
				e.ndvs[i] = 1
			}
			err = e.processor.Process(ran.StartKey, val)
			if err != nil {
				return nil, errors.Trace(err)
			}
		} else {
			oldCnt := e.rowCount
			if e.scanCtx.desc {
				err = dbReader.ReverseScan(ran.StartKey, ran.EndKey, math.MaxInt64, e.startTS, e.processor)
			} else {
				err = dbReader.Scan(ran.StartKey, ran.EndKey, math.MaxInt64, e.startTS, e.processor)
			}
			delta := int64(e.rowCount - oldCnt)
			if e.counts != nil {
				e.counts[i] += delta
				e.ndvs[i] = e.curNdv
			}
			if err != nil {
				return nil, errors.Trace(err)
			}
		}
		if e.rowCount == e.limit {
			break
		}
	}
	err = e.processor.Finish()
	return e.oldChunks, err
}

func (e *closureExecutor) isPointGetRange(ran kv.KeyRange) bool {
	if len(e.primaryCols) > 0 {
		return false
	}
	return e.unique && ran.IsPoint()
}

func (e *closureExecutor) checkRangeLock() error {
	if !e.ignoreLock && !e.lockChecked {
		for _, ran := range e.kvRanges {
			err := e.checkRangeLockForRange(ran)
			if err != nil {
				return err
			}
		}
		e.lockChecked = true
	}
	return nil
}

func (e *closureExecutor) checkRangeLockForRange(ran kv.KeyRange) error {
	it := e.lockStore.NewIterator()
	for it.Seek(ran.StartKey); it.Valid(); it.Next() {
		if exceedEndKey(it.Key(), ran.EndKey) {
			break
		}
		lock := mvcc.DecodeLock(it.Value())
		err := checkLock(lock, it.Key(), e.startTS, e.resolvedLocks)
		if err != nil {
			return err
		}
	}
	return nil
}

type countStarProcessor struct {
	skipVal
	*closureExecutor
}

// countStarProcess is used for `count(*)`.
func (e *countStarProcessor) Process(key, value []byte) error {
	defer func(begin time.Time) {
		if e.idxScanCtx != nil {
			e.idxScanCtx.execDetail.update(begin, true)
		} else {
			e.scanCtx.execDetail.update(begin, true)
		}
		e.aggCtx.execDetail.update(begin, false)
	}(time.Now())
	e.rowCount++
	return nil
}

func (e *countStarProcessor) Finish() error {
	e.aggCtx.execDetail.updateOnlyRows(1)
	return e.countFinish()
}

// countFinish is used for `count(*)`.
func (e *closureExecutor) countFinish() error {
	d := types.NewIntDatum(int64(e.rowCount))
	rowData, err := codec.EncodeValue(e.sc, nil, d)
	if err != nil {
		return errors.Trace(err)
	}
	e.oldChunks = appendRow(e.oldChunks, rowData, 0)
	return nil
}

type countColumnProcessor struct {
	skipVal
	*closureExecutor
}

func (e *countColumnProcessor) Process(key, value []byte) error {
	gotRow := false
	defer func(begin time.Time) {
		if e.idxScanCtx != nil {
			e.idxScanCtx.execDetail.update(begin, gotRow)
		} else {
			e.scanCtx.execDetail.update(begin, gotRow)
		}
		e.aggCtx.execDetail.update(begin, false)
	}(time.Now())
	if e.mockReader != nil {
		row := e.mockReader.chk.GetRow(e.mockReader.currentIndex)
		isNull := false
		if e.aggCtx.col.ColumnId < int64(e.mockReader.chk.NumCols()) {
			isNull = row.IsNull(int(e.aggCtx.col.ColumnId))
		} else {
			isNull = e.aggCtx.col.DefaultVal == nil
		}
		if !isNull {
			e.rowCount++
			gotRow = true
		}
	} else if e.idxScanCtx != nil {
		values, _, err := tablecodec.CutIndexKeyNew(key, e.idxScanCtx.columnLen)
		if err != nil {
			return errors.Trace(err)
		}
		if values[0][0] != codec.NilFlag {
			e.rowCount++
			gotRow = true
		}
	} else {
		// Since the handle value doesn't affect the count result, we don't need to decode the handle.
		isNull, err := e.scanCtx.decoder.ColumnIsNull(value, e.aggCtx.col.ColumnId, e.aggCtx.col.DefaultVal)
		if err != nil {
			return errors.Trace(err)
		}
		if !isNull {
			e.rowCount++
			gotRow = true
		}
	}
	return nil
}

func (e *countColumnProcessor) Finish() error {
	e.aggCtx.execDetail.updateOnlyRows(1)
	return e.countFinish()
}

type skipVal bool

func (s skipVal) SkipValue() bool {
	return bool(s)
}

type tableScanProcessor struct {
	skipVal
	*closureExecutor
}

func (e *tableScanProcessor) Process(key, value []byte) error {
	if e.rowCount == e.limit {
		return dbreader.ErrScanBreak
	}
	e.rowCount++
	e.curNdv++
	err := e.tableScanProcessCore(key, value)
	if e.scanCtx.chk.NumRows() == chunkMaxRows {
		err = e.chunkToOldChunk(e.scanCtx.chk)
	}
	return err
}

func (e *tableScanProcessor) Finish() error {
	return e.scanFinish()
}

func (e *closureExecutor) processCore(key, value []byte) error {
	if e.mockReader != nil {
		return e.mockReadScanProcessCore(key, value)
	}
	if e.idxScanCtx != nil {
		return e.indexScanProcessCore(key, value)
	}
	return e.tableScanProcessCore(key, value)
}

func (e *closureExecutor) hasSelection() bool {
	return len(e.selectionCtx.conditions) > 0
}

func (e *closureExecutor) processSelection(needCollectDetail bool) (gotRow bool, err error) {
	if needCollectDetail {
		defer func(begin time.Time) {
			e.selectionCtx.execDetail.update(begin, gotRow)
		}(time.Now())
	}
	chk := e.scanCtx.chk
	row := chk.GetRow(chk.NumRows() - 1)
	gotRow = true
	for _, expr := range e.selectionCtx.conditions {
		wc := e.sc.WarningCount()
		d, err := expr.Eval(row)
		if err != nil {
			return false, errors.Trace(err)
		}

		if d.IsNull() {
			gotRow = false
		} else {
			isTrue, err := d.ToBool(e.sc)
			isTrue, err = expression.HandleOverflowOnSelection(e.sc, isTrue, err)
			if err != nil {
				return false, errors.Trace(err)
			}
			gotRow = isTrue != 0
		}
		if !gotRow {
			if e.sc.WarningCount() > wc {
				// Deep-copy error object here, because the data it referenced is going to be truncated.
				warns := e.sc.TruncateWarnings(int(wc))
				for i, warn := range warns {
					warns[i].Err = e.copyError(warn.Err)
				}
				e.sc.AppendWarnings(warns)
			}
			chk.TruncateTo(chk.NumRows() - 1)
			break
		}
	}
	return
}

func (e *closureExecutor) copyError(err error) error {
	if err == nil {
		return nil
	}
	var ret error
	x := errors.Cause(err)
	switch y := x.(type) {
	case *terror.Error:
		ret = terror.ToSQLError(y)
	default:
		ret = errors.New(err.Error())
	}
	return ret
}

func (e *closureExecutor) mockReadScanProcessCore(key, value []byte) error {
	e.scanCtx.chk.AppendRow(e.mockReader.chk.GetRow(e.mockReader.currentIndex))
	e.mockReader.currentIndex++
	return nil
}

func (e *closureExecutor) tableScanProcessCore(key, value []byte) error {
	incRow := false
	defer func(begin time.Time) {
		e.scanCtx.execDetail.update(begin, incRow)
	}(time.Now())
	handle, err := tablecodec.DecodeRowKey(key)
	if err != nil {
		return errors.Trace(err)
	}
	err = e.scanCtx.decoder.DecodeToChunk(value, handle, e.scanCtx.chk)
	if err != nil {
		return errors.Trace(err)
	}
	incRow = true
	return nil
}

func (e *closureExecutor) scanFinish() error {
	return e.chunkToOldChunk(e.scanCtx.chk)
}

type indexScanProcessor struct {
	skipVal
	*closureExecutor
}

func (e *indexScanProcessor) Process(key, value []byte) error {
	if e.rowCount == e.limit {
		return dbreader.ErrScanBreak
	}
	e.rowCount++
	err := e.indexScanProcessCore(key, value)
	if e.scanCtx.chk.NumRows() == chunkMaxRows {
		err = e.chunkToOldChunk(e.scanCtx.chk)
	}
	return err
}

func (e *indexScanProcessor) Finish() error {
	return e.scanFinish()
}

func (isc *idxScanCtx) checkVal(curVals [][]byte) bool {
	for i := 0; i < isc.columnLen; i++ {
		if !bytes.Equal(isc.prevVals[i], curVals[i]) {
			return false
		}
	}
	return true
}

func (e *closureExecutor) indexScanProcessCore(key, value []byte) error {
	gotRow := false
	defer func(begin time.Time) {
		e.idxScanCtx.execDetail.update(begin, gotRow)
	}(time.Now())
	handleStatus := mapPkStatusToHandleStatus(e.idxScanCtx.pkStatus)
	restoredCols := make([]rowcodec.ColInfo, 0, len(e.idxScanCtx.colInfos))
	for _, c := range e.idxScanCtx.colInfos {
		if c.ID != -1 {
			restoredCols = append(restoredCols, c)
		}
	}
	values, err := tablecodec.DecodeIndexKV(key, value, e.idxScanCtx.columnLen, handleStatus, restoredCols)
	if err != nil {
		return err
	}
	if e.idxScanCtx.collectNDV {
		if len(e.idxScanCtx.prevVals[0]) == 0 || !e.idxScanCtx.checkVal(values) {
			e.curNdv++
			for i := 0; i < e.idxScanCtx.columnLen; i++ {
				e.idxScanCtx.prevVals[i] = append(e.idxScanCtx.prevVals[i][:0], values[i]...)
			}
		}
	}
	chk := e.scanCtx.chk
	decoder := codec.NewDecoder(chk, e.sc.TimeZone)
	for i, colVal := range values {
		if i < len(e.fieldTps) {
			_, err = decoder.DecodeOne(colVal, i, e.fieldTps[i])
			if err != nil {
				return errors.Trace(err)
			}
		}
	}
	gotRow = true
	return nil
}

func (e *closureExecutor) chunkToOldChunk(chk *chunk.Chunk) error {
	var oldRow []types.Datum
	for i := 0; i < chk.NumRows(); i++ {
		oldRow = oldRow[:0]
		if e.outputOff != nil {
			for _, outputOff := range e.outputOff {
				d := chk.GetRow(i).GetDatum(int(outputOff), e.fieldTps[outputOff])
				oldRow = append(oldRow, d)
			}
		} else {
			for colIdx := 0; colIdx < chk.NumCols(); colIdx++ {
				d := chk.GetRow(i).GetDatum(colIdx, e.fieldTps[colIdx])
				oldRow = append(oldRow, d)
			}
		}
		var err error
		e.oldRowBuf, err = codec.EncodeValue(e.sc, e.oldRowBuf[:0], oldRow...)
		if err != nil {
			return errors.Trace(err)
		}
		e.oldChunks = appendRow(e.oldChunks, e.oldRowBuf, i)
	}
	chk.Reset()
	return nil
}

type selectionProcessor struct {
	skipVal
	*closureExecutor
}

func (e *selectionProcessor) Process(key, value []byte) error {
	var gotRow bool
	defer func(begin time.Time) {
		e.selectionCtx.execDetail.update(begin, gotRow)
	}(time.Now())
	if e.rowCount == e.limit {
		return dbreader.ErrScanBreak
	}
	err := e.processCore(key, value)
	if err != nil {
		return errors.Trace(err)
	}
	gotRow, err = e.processSelection(false)
	if err != nil {
		return err
	}
	if gotRow {
		e.rowCount++
		if e.scanCtx.chk.NumRows() == chunkMaxRows {
			err = e.chunkToOldChunk(e.scanCtx.chk)
		}
	}
	return err
}

func (e *selectionProcessor) Finish() error {
	return e.scanFinish()
}

type topNProcessor struct {
	skipVal
	*closureExecutor
}

func (e *topNProcessor) Process(key, value []byte) (err error) {
	gotRow := false
	defer func(begin time.Time) {
		e.topNCtx.execDetail.update(begin, gotRow)
	}(time.Now())
	if err = e.processCore(key, value); err != nil {
		return err
	}
	if e.hasSelection() {
		gotRow, err1 := e.processSelection(true)
		if err1 != nil || !gotRow {
			return err1
		}
	}

	ctx := e.topNCtx
	row := e.scanCtx.chk.GetRow(0)
	for i, expr := range ctx.orderByExprs {
		d, err := expr.Eval(row)
		if err != nil {
			return errors.Trace(err)
		}
		d.Copy(&ctx.sortRow.key[i])
	}
	e.scanCtx.chk.Reset()

	if ctx.heap.tryToAddRow(ctx.sortRow) {
		ctx.sortRow.data[0] = safeCopy(key)
		ctx.sortRow.data[1] = safeCopy(value)
		ctx.sortRow = newTopNSortRow(len(ctx.orderByExprs))
	}
	if ctx.heap.err == nil {
		gotRow = true
	}
	return errors.Trace(ctx.heap.err)
}

func newTopNSortRow(numOrderByExprs int) *sortRow {
	return &sortRow{
		key:  make([]types.Datum, numOrderByExprs),
		data: make([][]byte, 2),
	}
}

func (e *topNProcessor) Finish() error {
	ctx := e.topNCtx
	sort.Sort(&ctx.heap.topNSorter)
	chk := e.scanCtx.chk
	for _, row := range ctx.heap.rows {
		err := e.processCore(row.data[0], row.data[1])
		if err != nil {
			return err
		}
		if chk.NumRows() == chunkMaxRows {
			if err = e.chunkToOldChunk(chk); err != nil {
				return errors.Trace(err)
			}
		}
	}
	return e.chunkToOldChunk(chk)
}

type hashAggProcessor struct {
	skipVal
	*closureExecutor

	aggExprs     []aggregation.Aggregation
	groupByExprs []expression.Expression
	groups       map[string]struct{}
	groupKeys    [][]byte
	aggCtxsMap   map[string][]*aggregation.AggEvaluateContext
}

func (e *hashAggProcessor) Process(key, value []byte) (err error) {
	incRow := false
	defer func(begin time.Time) {
		e.aggCtx.execDetail.update(begin, incRow)
	}(time.Now())
	err = e.processCore(key, value)
	if err != nil {
		return err
	}
	if e.hasSelection() {
		gotRow, err1 := e.processSelection(true)
		if err1 != nil || !gotRow {
			return err1
		}
	}
	row := e.scanCtx.chk.GetRow(e.scanCtx.chk.NumRows() - 1)
	gk, err := e.getGroupKey(row)
	if _, ok := e.groups[string(gk)]; !ok {
		e.groups[string(gk)] = struct{}{}
		e.groupKeys = append(e.groupKeys, gk)
		incRow = true
	}
	// Update aggregate expressions.
	aggCtxs := e.getContexts(gk)
	for i, agg := range e.aggExprs {
		err = agg.Update(aggCtxs[i], e.sc, row)
		if err != nil {
			return errors.Trace(err)
		}
	}
	e.scanCtx.chk.Reset()
	return nil
}

func (e *hashAggProcessor) getGroupKey(row chunk.Row) ([]byte, error) {
	length := len(e.groupByExprs)
	if length == 0 {
		return nil, nil
	}
	key := make([]byte, 0, 32)
	for _, item := range e.groupByExprs {
		v, err := item.Eval(row)
		if err != nil {
			return nil, errors.Trace(err)
		}
		b, err := codec.EncodeValue(e.sc, nil, v)
		if err != nil {
			return nil, errors.Trace(err)
		}
		key = append(key, b...)
	}
	return key, nil
}

func (e *hashAggProcessor) getContexts(groupKey []byte) []*aggregation.AggEvaluateContext {
	aggCtxs, ok := e.aggCtxsMap[string(groupKey)]
	if !ok {
		aggCtxs = make([]*aggregation.AggEvaluateContext, 0, len(e.aggExprs))
		for _, agg := range e.aggExprs {
			aggCtxs = append(aggCtxs, agg.CreateContext(e.sc))
		}
		e.aggCtxsMap[string(groupKey)] = aggCtxs
	}
	return aggCtxs
}

func (e *hashAggProcessor) Finish() error {
	for i, gk := range e.groupKeys {
		aggCtxs := e.getContexts(gk)
		e.oldRowBuf = e.oldRowBuf[:0]
		for i, agg := range e.aggExprs {
			partialResults := agg.GetPartialResult(aggCtxs[i])
			var err error
			e.oldRowBuf, err = codec.EncodeValue(e.sc, e.oldRowBuf, partialResults...)
			if err != nil {
				return err
			}
		}
		e.oldRowBuf = append(e.oldRowBuf, gk...)
		e.oldChunks = appendRow(e.oldChunks, e.oldRowBuf, i)
	}
	if e.aggCtx.execDetail.numIterations == 0 && e.aggCtx.execDetail.numProducedRows == 0 &&
		len(e.aggCtxsMap) == 0 && len(e.outputOff) == 1 {
		for _, exec := range e.dagReq.GetExecutors() {
			if exec.Tp == tipb.ExecType_TypeStreamAgg {
				e.aggCtx.execDetail.updateOnlyRows(1)
				e.oldChunks = appendRow(e.oldChunks, make([]byte, 1), 0)
			}
		}
	}
	return nil
}

func safeCopy(b []byte) []byte {
	return append([]byte{}, b...)
}

func checkLock(lock mvcc.Lock, key []byte, startTS uint64, resolved []uint64) error {
	if isResolved(startTS, resolved) {
		return nil
	}
	lockVisible := lock.StartTS < startTS
	isWriteLock := lock.Op == uint8(kvrpcpb.Op_Put) || lock.Op == uint8(kvrpcpb.Op_Del)
	isPrimaryGet := startTS == math.MaxUint64 && bytes.Equal(lock.Primary, key)
	if lockVisible && isWriteLock && !isPrimaryGet {
		return BuildLockErr(key, lock.Primary, lock.StartTS, uint64(lock.TTL), lock.Op)
	}
	return nil
}

func isResolved(startTS uint64, resolved []uint64) bool {
	for _, v := range resolved {
		if startTS == v {
			return true
		}
	}
	return false
}

func exceedEndKey(current, endKey []byte) bool {
	if len(endKey) == 0 {
		return false
	}
	return bytes.Compare(current, endKey) >= 0
}
