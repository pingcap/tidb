// Copyright 2026 PingCAP, Inc.
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

package executor

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/errctx"
	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/terror"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessiontxn"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/table/tables"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/codec"
	contextutil "github.com/pingcap/tidb/pkg/util/context"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"github.com/pingcap/tidb/pkg/util/timeutil"
	"github.com/pingcap/tipb/go-tipb"
)

// SPFreshVectorSearchExec is a root executor for PhysicalSPFreshVectorScan.
type SPFreshVectorSearchExec struct {
	exec.BaseExecutor

	plan *plannercore.PhysicalSPFreshVectorScan

	requiredColumns []*tipb.ColumnInfo

	colPlans []spfreshOutputColPlan

	// needIntHandle is true for every non-common-handle table so each returned
	// row handle is validated even when no output column consumes it.
	needIntHandle bool

	// commonHandleBuf is reused per row to validate and decode common handles
	// without per-row allocations.
	commonHandleBuf [][]byte

	// virtual generated columns
	virtualRetTypes  []*types.FieldType
	virtualColumnIdx []int

	decodedRows *chunk.List
	rowIter     chunk.Iterator
}

// Open implements the Executor Open interface.
func (e *SPFreshVectorSearchExec) Open(ctx context.Context) error {
	if err := e.BaseExecutor.Open(ctx); err != nil {
		return err
	}

	if e.plan == nil || e.plan.Table == nil || e.plan.Index == nil {
		return errors.New("invalid SPFresh vector search plan")
	}
	stmtCtx := e.Ctx().GetSessionVars().StmtCtx
	stmtCtx.VectorSearchIsANNQuery = true
	stmtCtx.VectorSearchTopK = e.plan.TopK
	if _, err := spfreshRouter.configuredEndpointStates(); err != nil {
		return errors.Trace(err)
	}
	if !e.plan.Index.IsSPFreshIndex() {
		return errors.New("spfresh vector search requires a SPFresh vector index")
	}

	if err := e.initOutputColumnPlans(); err != nil {
		return err
	}

	e.decodedRows = nil
	e.rowIter = nil
	return nil
}

// Close implements the Executor Close interface.
func (e *SPFreshVectorSearchExec) Close() error {
	if e.decodedRows != nil {
		e.decodedRows.Clear()
		e.decodedRows.GetMemTracker().Detach()
		e.decodedRows = nil
	}
	e.rowIter = nil
	return e.BaseExecutor.Close()
}

func (e *SPFreshVectorSearchExec) executeVectorSearch(ctx context.Context) error {
	clusterID, err := getSPFreshClusterID(ctx, e.Ctx())
	if err != nil {
		return err
	}
	keyspaceID := uint32(e.Ctx().GetStore().GetCodec().GetKeyspaceID())
	version, err := sessiontxn.SPFreshReadVersion(ctx, e.Ctx())
	if err != nil {
		return errors.Trace(err)
	}
	if version.Ver == 0 {
		return errors.New("failed to get current version for SPFresh vector search")
	}

	queryVecBytes, err := queryVectorBytes(e.plan.QueryVec)
	if err != nil {
		return err
	}

	needDistance := false
	for _, p := range e.colPlans {
		if p.kind == spfreshFillDistance {
			needDistance = true
			break
		}
	}
	reqPB := &tipb.SPFreshSearchRequest{
		KeyspaceId: keyspaceID,
		TableId:    e.plan.Table.ID,
		IndexId:    e.plan.Index.ID,
		// SPFresh bypasses stale SnapshotTS semantics and uses this execution's fresh TSO.
		StartTs:          version.Ver,
		TopK:             e.plan.TopK,
		QueryVectorF32Le: queryVecBytes,
		RequiredColumns:  e.requiredColumns,
		NeedDistance:     needDistance,
		MaxResponseBytes: maxVectorSearchResponseBytes,
	}
	if filter := e.plan.Filter; filter != nil {
		reqPB.FilterExpr = filter.Expr
		reqPB.FilterExprColumns = filter.ExprColumns
		reqPB.EvalContext = e.spfreshEvalContext()
	}
	if cfg := e.plan.Index.VectorInfo.SearchConfig; cfg != nil {
		reqPB.SearchBeamSize = cfg.SearchBeamSize
		reqPB.OversampleFactor = cfg.OversampleFactor
		if cfg.ReadOnly != nil {
			reqPB.ReadOnly = *cfg.ReadOnly
		}
	}

	result, err := e.vectorSearch(ctx, clusterID, reqPB)
	if err != nil {
		return err
	}
	warnings, omittedWarningCount, err := e.spfreshFilterWarnings(result)
	if err != nil {
		return err
	}

	// /spfresh/search should return at most top_k rows (after worker-side rerank).
	if reqPB.TopK > 0 && uint64(len(result.Rows)) > reqPB.TopK {
		return errors.Errorf("spfresh /spfresh/search returns too many rows: %d > %d", len(result.Rows), reqPB.TopK)
	}
	stmtCtx := e.Ctx().GetSessionVars().StmtCtx
	warningStart := int(stmtCtx.WarningTotal())
	decodedRows, err := e.decodeSPFreshSearchRows(result.Rows)
	if err != nil {
		stmtCtx.TruncateWarnings(warningStart)
		return err
	}
	virtualColumnWarningCount := int(stmtCtx.WarningTotal()) - warningStart
	virtualColumnWarnings := stmtCtx.TruncateWarnings(warningStart)
	virtualColumnOmittedWarnings := max(0, virtualColumnWarningCount-len(virtualColumnWarnings))

	if coll := stmtCtx.RuntimeStatsColl; coll != nil {
		coll.RegisterStats(e.ID(), newVectorSearchRuntimeStats(result.Stats))
	}
	stmtCtx.AppendWarnings(warnings)
	stmtCtx.AddOmittedWarnings(omittedWarningCount)
	stmtCtx.AppendWarnings(virtualColumnWarnings)
	stmtCtx.AddOmittedWarnings(uint64(virtualColumnOmittedWarnings))
	e.decodedRows = decodedRows
	e.rowIter = chunk.NewIterator4List(decodedRows)
	e.rowIter.Begin()
	return nil
}

// isStructuredSPFreshSearchError reports whether err was produced from an SPFresh
// error envelope by `spfreshSearchError`, as opposed to a transport, status or
// decode failure.
func isStructuredSPFreshSearchError(err error) bool {
	_, ok := errors.Cause(err).(*terror.Error)
	return ok
}

func spfreshSearchError(err *tipb.Error) error {
	switch err.GetCode() {
	case int32(tipb.SPFreshErrorCode_SPFreshIndexCorruption):
		return ErrSPFreshIndexCorruption.FastGenByArgs(err.Msg)
	case int32(tipb.SPFreshErrorCode_SPFreshResponseTooLarge):
		return ErrSPFreshResponseTooLarge.FastGenByArgs(err.Msg)
	}
	return dbterror.ClassExpression.Synthesize(terror.ErrCode(err.Code), err.Msg)
}

const (
	maxSPFreshFilterWarningDetails = 64
)

// ErrSPFreshIndexCorruption reports deterministic corruption in a required
// SPFresh vector-index sidecar.
var ErrSPFreshIndexCorruption = dbterror.ClassTiKV.NewStdErr(
	terror.ErrCode(tipb.SPFreshErrorCode_SPFreshIndexCorruption),
	mysql.Message("SPFresh vector index corruption: %s", nil),
)

// ErrSPFreshResponseTooLarge reports that the worker could not encode the
// search result within the response budget supplied by TiDB.
var ErrSPFreshResponseTooLarge = dbterror.ClassTiKV.NewStdErr(
	terror.ErrCode(tipb.SPFreshErrorCode_SPFreshResponseTooLarge),
	mysql.Message("SPFresh vector search response exceeds limit: %s", nil),
)

func (e *SPFreshVectorSearchExec) spfreshEvalContext() *tipb.SPFreshEvalContext {
	vars := e.Ctx().GetSessionVars()
	tzName, tzOffset := timeutil.Zone(vars.Location())
	stmtCtx := vars.StmtCtx
	typeFlags := stmtCtx.TypeFlags()
	errCtx := stmtCtx.ErrCtx()
	errLevels := errCtx.LevelMap()
	truncateMode := tipb.SPFreshTruncateMode_TruncateError
	if typeFlags.IgnoreTruncateErr() {
		truncateMode = tipb.SPFreshTruncateMode_TruncateIgnore
	} else if typeFlags.TruncateAsWarning() {
		truncateMode = tipb.SPFreshTruncateMode_TruncateWarning
	}
	return &tipb.SPFreshEvalContext{
		TimeZoneOffset:         tzOffset,
		MaxWarningCount:        spfreshRemainingWarningDetailCapacity(stmtCtx.WarningCount()),
		TimeZoneName:           tzName,
		DivPrecisionIncrement:  uint32(vars.GetDivPrecisionIncrement()),
		TruncateMode:           truncateMode,
		OverflowAsWarning:      typeFlags.TruncateAsWarning(),
		IgnoreZeroInDate:       typeFlags.IgnoreZeroInDate(),
		DividedByZeroAsWarning: errLevels[errctx.ErrGroupDividedByZero] != errctx.LevelError,
		MaxAllowedPacket:       vars.MaxAllowedPacket,
		SqlMode:                spfreshSQLMode(vars.SQLMode),
	}
}

func spfreshSQLMode(mode mysql.SQLMode) uint64 {
	// SPFresh sends TiDB parser SQL modes over the TiKV evaluator ABI. Keep only
	// evaluator-recognized bits and map TiDB's ALLOW_INVALID_DATES to TiKV's
	// historical INVALID_DATES bit.
	const tikvSQLModeMask = mysql.ModeNoUnsignedSubtraction |
		mysql.ModeStrictTransTables |
		mysql.ModeStrictAllTables |
		mysql.ModeNoZeroInDate |
		mysql.ModeNoZeroDate |
		mysql.ModeInvalidDates |
		mysql.ModeErrorForDivisionByZero

	wireMode := mode & tikvSQLModeMask
	if mode.HasAllowInvalidDatesMode() {
		wireMode |= mysql.ModeInvalidDates
	}
	return uint64(wireMode)
}

func spfreshRemainingWarningDetailCapacity(current uint16) uint64 {
	remaining := int(^uint16(0) - current)
	if remaining > maxSPFreshFilterWarningDetails {
		remaining = maxSPFreshFilterWarningDetails
	}
	return uint64(remaining)
}

func (e *SPFreshVectorSearchExec) spfreshFilterWarnings(result *tipb.SPFreshSearchResult) ([]contextutil.SQLWarn, uint64, error) {
	if result == nil || result.GetWarningCount() == 0 && len(result.GetWarnings()) == 0 {
		return nil, 0, nil
	}
	warningCount := result.GetWarningCount()
	if warningCount < uint64(len(result.GetWarnings())) {
		return nil, 0, errors.Errorf("spfresh /spfresh/search returned warning_count %d smaller than warning details %d",
			warningCount, len(result.GetWarnings()))
	}
	for _, warning := range result.GetWarnings() {
		if warning == nil {
			return nil, 0, errors.New("spfresh /spfresh/search returned nil warning detail")
		}
	}
	stmtCtx := e.Ctx().GetSessionVars().StmtCtx
	remaining := int(^uint16(0) - stmtCtx.WarningCount())
	if remaining <= 0 {
		return nil, 0, nil
	}
	detailCount := len(result.GetWarnings())
	if detailCount > maxSPFreshFilterWarningDetails {
		detailCount = maxSPFreshFilterWarningDetails
	}
	if detailCount > remaining {
		detailCount = remaining
	}
	omittedCount := min(warningCount-uint64(detailCount), uint64(remaining-detailCount))
	warnings := make([]contextutil.SQLWarn, 0, detailCount)
	for _, warning := range result.GetWarnings()[:detailCount] {
		warnings = append(warnings, contextutil.SQLWarn{
			Level: contextutil.WarnLevelWarning,
			Err:   dbterror.ClassExpression.Synthesize(terror.ErrCode(warning.Code), warning.Msg),
		})
	}
	return warnings, omittedCount, nil
}

// Next implements the Executor Next interface.
func (e *SPFreshVectorSearchExec) Next(ctx context.Context, req *chunk.Chunk) error {
	requiredRows := req.RequiredRows()
	req.GrowAndReset(e.MaxChunkSize())
	req.SetRequiredRows(requiredRows, e.MaxChunkSize())
	if e.decodedRows == nil {
		if err := e.executeVectorSearch(ctx); err != nil {
			return err
		}
	}
	for row := e.rowIter.Current(); req.NumRows() < req.RequiredRows() && row != e.rowIter.End(); row = e.rowIter.Next() {
		req.AppendRow(row)
	}
	return nil
}

func (e *SPFreshVectorSearchExec) decodeSPFreshSearchRows(rows []*tipb.SPFreshSearchRow) (*chunk.List, error) {
	decodedRows := chunk.NewList(e.RetFieldTypes(), e.InitCap(), e.MaxChunkSize())
	memTracker := decodedRows.GetMemTracker()
	memTracker.AttachTo(e.Ctx().GetSessionVars().StmtCtx.MemTracker)
	succeeded := false
	defer func() {
		if !succeeded {
			decodedRows.Clear()
			memTracker.Detach()
		}
	}()
	batchSize := min(len(rows), e.InitCap())
	if batchSize == 0 {
		succeeded = true
		return decodedRows, nil
	}
	for start := 0; start < len(rows); start += batchSize {
		end := min(start+batchSize, len(rows))
		rowBuf := decodedRows.AllocChunk()
		if err := e.decodeSPFreshRows(rows[start:end], rowBuf); err != nil {
			return nil, err
		}
		if err := e.fillSPFreshVirtualColumns(rowBuf); err != nil {
			return nil, err
		}
		decodedRows.Add(rowBuf)
		clear(rows[start:end])
	}
	succeeded = true
	return decodedRows, nil
}

func (e *SPFreshVectorSearchExec) decodeSPFreshRows(rows []*tipb.SPFreshSearchRow, decodedRows *chunk.Chunk) error {
	outCols := e.Schema().Columns
	decoder := codec.NewDecoder(decodedRows, e.Ctx().GetSessionVars().Location())
	needCommonHandle := len(e.commonHandleBuf) > 0
	requiredColumnCount := len(e.requiredColumns)

	for _, row := range rows {
		if row == nil {
			return errors.New("spfresh /spfresh/search returned nil row")
		}
		if len(row.ColumnValues) != requiredColumnCount {
			return errors.Errorf("spfresh /spfresh/search returns mismatched column values: %d != %d",
				len(row.ColumnValues), requiredColumnCount)
		}
		var (
			intHandle      int64
			intHandleOK    bool
			commonHandleOK bool
		)
		if e.needIntHandle {
			var remain []byte
			var err error
			remain, intHandle, err = codec.DecodeInt(row.HandleBytes)
			if err != nil {
				return errors.Trace(err)
			}
			if len(remain) != 0 {
				return errors.New("invalid spfresh handle_bytes for int handle")
			}
			intHandleOK = true
		}
		if needCommonHandle {
			if err := decodeCommonHandle(e.commonHandleBuf, row.HandleBytes); err != nil {
				return err
			}
			commonHandleOK = true
		}

		for colIdx, cp := range e.colPlans {
			switch cp.kind {
			case spfreshFillFromWorker:
				val := row.ColumnValues[cp.requiredIdx]
				if len(val) == 0 {
					return errors.New("spfresh /spfresh/search returned empty column value")
				}
				remain, err := decoder.DecodeOne(val, colIdx, outCols[colIdx].RetType)
				if err != nil {
					return errors.Trace(err)
				}
				if len(remain) != 0 {
					return errors.New("invalid spfresh column value: trailing bytes")
				}
			case spfreshFillVirtualGenerated:
				decodedRows.AppendNull(colIdx)
			case spfreshFillDistance:
				decodedRows.AppendFloat64(colIdx, float64(row.Distance))
			case spfreshFillPhysTblID:
				decodedRows.AppendInt64(colIdx, e.plan.Table.ID)
			case spfreshFillIntHandle:
				if !intHandleOK {
					return errors.New("spfresh: missing int handle for handle-derived column")
				}
				decodedRows.AppendInt64(colIdx, intHandle)
			case spfreshFillCommonHandle:
				if !commonHandleOK {
					return errors.New("spfresh: missing common handle for handle-derived column")
				}
				val := e.commonHandleBuf[cp.commonHandlePos]
				remain, err := decoder.DecodeOne(val, colIdx, outCols[colIdx].RetType)
				if err != nil {
					return errors.Trace(err)
				}
				if len(remain) != 0 {
					return errors.New("invalid spfresh handle column value: trailing bytes")
				}
			default:
				return errors.New("unknown spfresh output column plan")
			}
		}
	}

	return nil
}

func (e *SPFreshVectorSearchExec) fillSPFreshVirtualColumns(rows *chunk.Chunk) error {
	return table.FillVirtualColumnValue(
		e.virtualRetTypes,
		e.virtualColumnIdx,
		e.Schema().Columns,
		e.plan.Columns,
		e.Ctx().GetExprCtx(),
		rows,
	)
}

type spfreshFillKind uint8

const (
	spfreshFillFromWorker spfreshFillKind = iota
	spfreshFillVirtualGenerated
	spfreshFillDistance
	spfreshFillPhysTblID
	spfreshFillIntHandle
	spfreshFillCommonHandle
)

type spfreshOutputColPlan struct {
	kind spfreshFillKind

	// requiredIdx is the index in request.required_columns/response.column_values when kind==spfreshFillFromWorker.
	requiredIdx int

	// commonHandlePos is the PK position in handle_bytes when kind==spfreshFillCommonHandle.
	commonHandlePos int
}

func (e *SPFreshVectorSearchExec) initOutputColumnPlans() error {
	e.requiredColumns = nil
	e.colPlans = nil
	e.needIntHandle = false
	e.commonHandleBuf = nil
	e.virtualRetTypes = nil
	e.virtualColumnIdx = nil

	schemaCols := e.Schema().Columns
	if len(schemaCols) != len(e.plan.Columns) {
		return errors.New("spfresh plan schema and columns length mismatch")
	}

	tblInfo := e.plan.Table
	isCommonHandle := tblInfo.IsCommonHandle
	pkIsHandle := tblInfo.PKIsHandle
	e.needIntHandle = !isCommonHandle

	var pkColIDs []int64
	var prefixColIDs []int64
	pkPosByColID := make(map[int64]int)
	if isCommonHandle {
		pkColIDs = tables.TryGetCommonPkColumnIds(tblInfo)
		prefixColIDs = tables.PrimaryPrefixColumnIDs(tblInfo)
		if len(pkColIDs) == 0 {
			return errors.New("spfresh common-handle table has no primary key columns")
		}
		e.commonHandleBuf = make([][]byte, len(pkColIDs))
		for i, id := range pkColIDs {
			pkPosByColID[id] = i
		}
	}

	colPlans := make([]spfreshOutputColPlan, len(schemaCols))
	requiredColumns := make([]*tipb.ColumnInfo, 0, len(schemaCols))
	requiredColumnInfos := make([]*model.ColumnInfo, 0, len(schemaCols))

	for colIdx, colInfo := range e.plan.Columns {
		if colInfo == nil {
			return errors.New("nil column info")
		}
		schemaCol := schemaCols[colIdx]
		switch {
		case schemaCol.VirtualExpr != nil || colInfo.IsVirtualGenerated():
			colPlans[colIdx] = spfreshOutputColPlan{kind: spfreshFillVirtualGenerated}
			e.virtualRetTypes = append(e.virtualRetTypes, schemaCol.RetType)
			e.virtualColumnIdx = append(e.virtualColumnIdx, colIdx)
			continue
		case colInfo.ID == model.VirtualColVecSearchDistanceID:
			colPlans[colIdx] = spfreshOutputColPlan{kind: spfreshFillDistance}
			continue
		case colInfo.ID == model.ExtraPhysTblID:
			colPlans[colIdx] = spfreshOutputColPlan{kind: spfreshFillPhysTblID}
			continue
		case colInfo.ID == model.ExtraHandleID:
			if isCommonHandle {
				return errors.New("spfresh does not support ExtraHandleID on common-handle tables")
			}
			colPlans[colIdx] = spfreshOutputColPlan{kind: spfreshFillIntHandle}
			e.needIntHandle = true
			continue
		case pkIsHandle && mysql.HasPriKeyFlag(colInfo.GetFlag()):
			colPlans[colIdx] = spfreshOutputColPlan{kind: spfreshFillIntHandle}
			e.needIntHandle = true
			continue
		case isCommonHandle && mysql.HasPriKeyFlag(colInfo.GetFlag()) && !types.NeedRestoredData(schemaCol.RetType):
			pkPos, ok := pkPosByColID[colInfo.ID]
			if ok && notPKPrefixCol(colInfo.ID, prefixColIDs) {
				colPlans[colIdx] = spfreshOutputColPlan{kind: spfreshFillCommonHandle, commonHandlePos: pkPos}
				continue
			}
		}

		requiredIdx := len(requiredColumns)
		requiredColumns = append(requiredColumns, util.ColumnToProto(colInfo, false, false))
		requiredColumnInfos = append(requiredColumnInfos, colInfo)
		colPlans[colIdx] = spfreshOutputColPlan{kind: spfreshFillFromWorker, requiredIdx: requiredIdx}
	}
	if err := tables.SetPBColumnsDefaultValue(e.Ctx().GetExprCtx(), requiredColumns, requiredColumnInfos); err != nil {
		return err
	}

	e.requiredColumns = requiredColumns
	e.colPlans = colPlans

	return nil
}

func decodeCommonHandle(dst [][]byte, handleBytes []byte) error {
	remain := handleBytes
	for i := range dst {
		if len(remain) == 0 {
			return errors.New("invalid spfresh handle_bytes for common handle: insufficient bytes")
		}
		if remain[0] == 0 {
			return errors.New("invalid spfresh handle_bytes for common handle: padded data before finishing pk")
		}
		var err error
		dst[i], remain, err = codec.CutOne(remain)
		if err != nil {
			return errors.Trace(err)
		}
	}
	for _, b := range remain {
		if b != 0 {
			return errors.New("invalid spfresh handle_bytes for common handle: trailing bytes")
		}
	}
	return nil
}

func queryVectorBytes(v types.VectorFloat32) ([]byte, error) {
	serialized := v.ZeroCopySerialize()
	if len(serialized) < 4 {
		return nil, errors.New("invalid query vector bytes")
	}
	return serialized[4:], nil
}

const (
	spfreshVectorSearchPath      = "/spfresh/search"
	maxVectorSearchRequestBytes  = 1 << 20  // 1MiB
	maxVectorSearchResponseBytes = 64 << 20 // 64MiB
	maxSPFreshErrorBodyBytes     = 4 << 10  // 4KiB
	maxSPFreshSearchTimeout      = 60 * time.Second
)

var (
	defaultSPFreshSearchTimeout = 20 * time.Second
	// Allow the worker to encode and deliver its timeout response without
	// falling back to the shared five-minute HTTP client timeout.
	spfreshSearchResponseGrace = time.Second
)

var spfreshClusterIDCache atomic.Uint64

func getSPFreshClusterID(ctx context.Context, sctx sessionctx.Context) (uint64, error) {
	if id := spfreshClusterIDCache.Load(); id != 0 {
		return id, nil
	}
	dom := domain.GetDomain(sctx)
	if dom == nil || dom.GetPDClient() == nil {
		return 0, errors.New("failed to get PD client for cluster_id")
	}
	id := dom.GetPDClient().GetClusterID(ctx)
	// Cluster ID should be stable; cache a non-zero value.
	if id != 0 {
		spfreshClusterIDCache.CompareAndSwap(0, id)
	}
	return id, nil
}

func (e *SPFreshVectorSearchExec) vectorSearch(ctx context.Context, clusterID uint64, reqPB *tipb.SPFreshSearchRequest) (*tipb.SPFreshSearchResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, errors.Trace(err)
	}
	timeoutMS := spfreshSearchTimeoutMS(ctx)
	if timeoutMS == 0 {
		return nil, context.DeadlineExceeded
	}
	workerDeadline := time.Now().Add(time.Duration(timeoutMS) * time.Millisecond)
	operationCtx, cancel := context.WithTimeout(ctx,
		time.Duration(timeoutMS)*time.Millisecond+spfreshSearchResponseGrace)
	defer cancel()
	reqPB.MaxResponseBytes = maxVectorSearchResponseBytes
	if !model.SPFreshTopKValid(reqPB.GetTopK()) {
		return nil, errors.Errorf("spfresh /spfresh/search top_k must be in [1, %d]: %d", model.SPFreshMaxTopK, reqPB.GetTopK())
	}
	if !model.SPFreshRerankCandidateBudgetValid(reqPB.GetTopK(), &model.SPFreshVectorSearchConfig{
		OversampleFactor: reqPB.GetOversampleFactor(),
	}) {
		return nil, errors.Errorf("spfresh /spfresh/search rough candidate target exceeds %d", model.SPFreshMaxRerankCandidates)
	}
	size := reqPB.Size()
	if size > maxVectorSearchRequestBytes {
		return nil, errors.Errorf("spfresh /spfresh/search request exceeds %d bytes", maxVectorSearchRequestBytes)
	}
	body := make([]byte, size)
	n, err := reqPB.MarshalTo(body)
	if err != nil {
		return nil, err
	}
	body = body[:n]

	endpoints, err := spfreshRouter.searchEndpointOrder()
	if err != nil {
		return nil, errors.Trace(err)
	}

	client := spfreshSearchClient()

	var lastErr error
	for _, endpoint := range endpoints {
		resp, retry, err := e.vectorSearchEndpoint(operationCtx, workerDeadline, client, endpoint, clusterID, body)
		if err == nil {
			return resp, nil
		}
		lastErr = err
		if !retry {
			return nil, err
		}
	}
	return nil, lastErr
}

func (e *SPFreshVectorSearchExec) vectorSearchEndpoint(
	ctx context.Context,
	workerDeadline time.Time,
	client *http.Client,
	endpoint *spfreshEndpointState,
	clusterID uint64,
	body []byte,
) (*tipb.SPFreshSearchResult, bool, error) {
	now := time.Now()
	timeoutMS := spfreshSearchTimeoutMSAt(ctx, now)
	if deadline, ok := ctx.Deadline(); !ok || workerDeadline.Before(deadline) {
		timeoutMS = spfreshSearchTimeoutMSAtDeadline(workerDeadline, now)
	}
	if timeoutMS == 0 {
		if err := ctx.Err(); err != nil {
			return nil, false, errors.Trace(err)
		}
		return nil, false, context.DeadlineExceeded
	}
	url := fmt.Sprintf("%s?cluster_id=%d&timeout_ms=%d",
		util.ComposeURL(endpoint.endpoint, spfreshVectorSearchPath), clusterID, timeoutMS)
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return nil, false, errors.Trace(err)
	}
	httpReq.Header.Set("Content-Type", "application/x-protobuf")

	endpoint.searchInflight.Add(1)
	defer endpoint.searchInflight.Add(-1)

	resp, err := client.Do(httpReq)
	if err != nil {
		if ctx.Err() != nil {
			return nil, false, errors.Trace(err)
		}
		endpoint.markFailure(time.Now())
		return nil, true, errors.Trace(err)
	}
	defer resp.Body.Close()

	maxResponseBytes := int64(maxVectorSearchResponseBytes)
	if resp.StatusCode != http.StatusOK {
		maxResponseBytes = maxSPFreshErrorBodyBytes
	}
	respBytes, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes+1))
	if err != nil {
		if ctx.Err() != nil {
			return nil, false, errors.Trace(err)
		}
		retry := resp.StatusCode == http.StatusOK || isRetryableSPFreshWorkerStatus(resp.StatusCode)
		if retry || resp.StatusCode >= http.StatusInternalServerError {
			endpoint.markFailure(time.Now())
		}
		return nil, retry, errors.Trace(err)
	}
	if int64(len(respBytes)) > maxResponseBytes {
		if resp.StatusCode == http.StatusOK {
			endpoint.markFailure(time.Now())
			return nil, false, errors.Errorf("spfresh /spfresh/search response exceeds %d bytes", maxVectorSearchResponseBytes)
		}
		retry := isRetryableSPFreshWorkerStatus(resp.StatusCode)
		if retry || resp.StatusCode >= http.StatusInternalServerError {
			endpoint.markFailure(time.Now())
		}
		return nil, retry,
			errors.Errorf("spfresh /spfresh/search error response exceeds %d bytes", maxResponseBytes)
	}
	if resp.StatusCode != http.StatusOK {
		retry := isRetryableSPFreshWorkerStatus(resp.StatusCode)
		if retry || resp.StatusCode >= http.StatusInternalServerError {
			endpoint.markFailure(time.Now())
		}
		return nil, retry, spfreshVectorSearchHTTPError(resp.StatusCode, respBytes)
	}
	result, err := decodeSPFreshSearchResponse(respBytes)
	if err != nil {
		// A structured SPFresh error means the worker answered the request and
		// reported a deterministic index or query failure; only transport, status
		// and malformed-response failures make the endpoint unhealthy.
		if !isStructuredSPFreshSearchError(err) {
			endpoint.markFailure(time.Now())
		}
		return nil, false, err
	}
	if result != nil {
		endpoint.markSuccess()
		return result, false, nil
	}
	endpoint.markFailure(time.Now())
	return nil, false, errors.New("spfresh /spfresh/search returned unset result")
}

func decodeSPFreshSearchResponse(data []byte) (*tipb.SPFreshSearchResult, error) {
	envelope := &tipb.SPFreshSearchResponse{}
	if err := proto.Unmarshal(data, envelope); err != nil {
		return nil, errors.Trace(err)
	}
	if result := envelope.GetSuccess(); result != nil {
		// Every current worker success, including an empty result, sets stats.
		if result.Stats == nil {
			return nil, errors.New("spfresh /spfresh/search returned success without stats")
		}
		return result, nil
	}
	if result := envelope.GetError(); result != nil {
		if result.GetCode() == 0 {
			return nil, errors.New("spfresh /spfresh/search returned invalid error result")
		}
		return nil, spfreshSearchError(result)
	}
	return nil, errors.New("spfresh /spfresh/search returned unset result")
}

func spfreshSearchTimeoutMS(ctx context.Context) uint64 {
	return spfreshSearchTimeoutMSAt(ctx, time.Now())
}

func spfreshSearchTimeoutMSAt(ctx context.Context, now time.Time) uint64 {
	deadline, ok := ctx.Deadline()
	if !ok {
		return uint64(defaultSPFreshSearchTimeout / time.Millisecond)
	}
	return spfreshSearchTimeoutMSAtDeadline(deadline, now)
}

func spfreshSearchTimeoutMSAtDeadline(deadline, now time.Time) uint64 {
	remaining := deadline.Sub(now)
	if remaining <= 0 {
		return 0
	}
	if remaining >= maxSPFreshSearchTimeout {
		return uint64(maxSPFreshSearchTimeout / time.Millisecond)
	}
	milliseconds := remaining / time.Millisecond
	if remaining%time.Millisecond != 0 {
		milliseconds++
	}
	return uint64(max(milliseconds, 1))
}

func spfreshVectorSearchHTTPError(statusCode int, msg []byte) error {
	msgStr := strings.TrimSpace(string(msg))
	switch statusCode {
	case http.StatusBadRequest:
		return errors.Errorf("spfresh /spfresh/search bad request: %s", msgStr)
	case http.StatusNotFound:
		return errors.Errorf("spfresh /spfresh/search index not found: %s", msgStr)
	case http.StatusConflict:
		return errors.Errorf("spfresh /spfresh/search index not ready or mismatch: %s", msgStr)
	case http.StatusServiceUnavailable:
		return errors.Errorf("spfresh /spfresh/search unavailable: %s", msgStr)
	default:
		return errors.Errorf("spfresh /spfresh/search failed: http %d: %s", statusCode, msgStr)
	}
}
