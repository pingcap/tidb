// Copyright 2019 PingCAP, Inc.
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
	"slices"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/coprocessor"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/store/mockstore/unistore/tikv/dbreader"
	"github.com/pingcap/tidb/pkg/store/mockstore/unistore/tikv/kverrors"
	"github.com/pingcap/tidb/pkg/types"
	contextutil "github.com/pingcap/tidb/pkg/util/context"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tidb/pkg/util/collate"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/pingcap/tidb/pkg/util/rowcodec"
	"github.com/pingcap/tipb/go-tipb"
)

func getTopNInfo(ctx *evalContext, topN *tipb.TopN) (heap *topNHeap, conds []expression.Expression, err error) {
	pbConds := make([]*tipb.Expr, len(topN.OrderBy))
	for i, item := range topN.OrderBy {
		pbConds[i] = item.Expr
	}
	heap = &topNHeap{
		totalCount: int(topN.Limit),
		topNSorter: topNSorter{
			orderByItems: topN.OrderBy,
			sc:           ctx.sctx.GetSessionVars().StmtCtx,
		},
	}
	if conds, err = convertToExprs(ctx.sctx, ctx.fieldTps, pbConds); err != nil {
		return nil, nil, errors.Trace(err)
	}

	return heap, conds, nil
}

type evalContext struct {
	columnInfos []*tipb.ColumnInfo
	fieldTps    []*types.FieldType
	primaryCols []int64
	sctx        sessionctx.Context
}

func (e *evalContext) setColumnInfo(cols []*tipb.ColumnInfo) {
	e.columnInfos = slices.Clone(cols)

	e.fieldTps = make([]*types.FieldType, 0, len(e.columnInfos))
	for _, col := range e.columnInfos {
		ft := fieldTypeFromPBColumn(col)
		e.fieldTps = append(e.fieldTps, ft)
	}
}

func newRowDecoder(columnInfos []*tipb.ColumnInfo, fieldTps []*types.FieldType, primaryCols []int64, timeZone *time.Location) (*rowcodec.ChunkDecoder, error) {
	var (
		pkCols []int64
		cols   = make([]rowcodec.ColInfo, 0, len(columnInfos))
	)
	for i := range columnInfos {
		info := columnInfos[i]
		if info.ColumnId == model.ExtraPhysTblID {
			// Skip since it needs to be filled in from the key
			continue
		}
		ft := fieldTps[i]
		col := rowcodec.ColInfo{
			ID:         info.ColumnId,
			Ft:         ft,
			IsPKHandle: info.PkHandle,
		}
		cols = append(cols, col)
		if info.PkHandle {
			pkCols = append(pkCols, info.ColumnId)
		}
	}
	if len(pkCols) == 0 {
		if primaryCols != nil {
			pkCols = primaryCols
		} else {
			pkCols = []int64{-1}
		}
	}
	def := func(i int, chk *chunk.Chunk) error {
		info := columnInfos[i]
		if info.PkHandle || len(info.DefaultVal) == 0 {
			chk.AppendNull(i)
			return nil
		}
		decoder := codec.NewDecoder(chk, timeZone)
		_, err := decoder.DecodeOne(info.DefaultVal, i, fieldTps[i])
		if err != nil {
			return err
		}
		return nil
	}
	return rowcodec.NewChunkDecoder(cols, pkCols, def, timeZone), nil
}

// flagsAndTzToSessionContext creates a sessionctx.Context from a `tipb.SelectRequest.Flags`.
func flagsAndTzToSessionContext(flags uint64, tz *time.Location) sessionctx.Context {
	sc := stmtctx.NewStmtCtx()
	sc.InitFromPBFlagAndTz(flags, tz)
	sctx := mock.NewContextDeprecated()
	sctx.GetSessionVars().StmtCtx = sc
	sctx.GetSessionVars().TimeZone = tz
	return sctx
}

// ErrLocked is returned when trying to Read/Write on a locked key. Client should
// backoff or cleanup the lock then retry.
type ErrLocked struct {
	Key      []byte
	Primary  []byte
	StartTS  uint64
	TTL      uint64
	LockType uint8
}

// BuildLockErr generates ErrKeyLocked objects
func BuildLockErr(key []byte, primaryKey []byte, startTS uint64, TTL uint64, lockType uint8) *ErrLocked {
	errLocked := &ErrLocked{
		Key:      key,
		Primary:  primaryKey,
		StartTS:  startTS,
		TTL:      TTL,
		LockType: lockType,
	}
	return errLocked
}

// Error formats the lock to a string.
func (e *ErrLocked) Error() string {
	return fmt.Sprintf("key is locked, key: %q, Type: %v, primary: %q, startTS: %v", e.Key, e.LockType, e.Primary, e.StartTS)
}

func flattenMppExec(exec mppExec, execs []mppExec) []mppExec {
	if execs == nil {
		execs = make([]mppExec, 0, 3)
	}
	for _, child := range exec.getChildren() {
		execs = flattenMppExec(child, execs)
	}
	execs = append(execs, exec)
	return execs
}

func genRespWithMPPExec(chunks []tipb.Chunk, intermediateOutput []*tipb.IntermediateOutput, lastRange *coprocessor.KeyRange, counts, ndvs []int64, exec mppExec, dagReq *tipb.DAGRequest, err error, warnings []contextutil.SQLWarn, dur time.Duration) *coprocessor.Response {
	resp := &coprocessor.Response{
		Range: lastRange,
	}
	selResp := &tipb.SelectResponse{
		Error:               toPBError(err),
		Chunks:              chunks,
		OutputCounts:        counts,
		IntermediateOutputs: intermediateOutput,
		Ndvs:                ndvs,
		EncodeType:          dagReq.EncodeType,
	}
	executors := dagReq.Executors
	mppExecs := flattenMppExec(exec, make([]mppExec, 0, len(executors)))
	if dagReq.GetCollectExecutionSummaries() {
		// for simplicity, we assume all executors to be spending the same amount of time as the request
		timeProcessed := uint64(dur / time.Nanosecond)
		execSummary := make([]*tipb.ExecutorExecutionSummary, len(executors))
		for i := len(executors) - 1; 0 <= i; i-- {
			e := mppExecs[i]
			execSummary[i] = e.buildSummary()
			execSummary[i].TimeProcessedNs = &timeProcessed
		}
		selResp.ExecutionSummaries = execSummary
	}
	if len(warnings) > 0 {
		selResp.Warnings = make([]*tipb.Error, 0, len(warnings))
		for i := range warnings {
			selResp.Warnings = append(selResp.Warnings, toPBError(warnings[i].Err))
		}
	}
	if locked, ok := errors.Cause(err).(*ErrLocked); ok {
		resp.Locked = &kvrpcpb.LockInfo{
			Key:         locked.Key,
			PrimaryLock: locked.Primary,
			LockVersion: locked.StartTS,
			LockTtl:     locked.TTL,
		}
	}
	resp.ExecDetails = &kvrpcpb.ExecDetails{
		TimeDetail: &kvrpcpb.TimeDetail{ProcessWallTimeMs: uint64(dur / time.Millisecond)},
	}
	resp.ExecDetailsV2 = &kvrpcpb.ExecDetailsV2{
		TimeDetail: resp.ExecDetails.TimeDetail,
	}
	data, mErr := proto.Marshal(selResp)
	if mErr != nil {
		resp.OtherError = mErr.Error()
		return resp
	}
	resp.Data = data
	if err != nil {
		if conflictErr, ok := errors.Cause(err).(*kverrors.ErrConflict); ok {
			resp.OtherError = conflictErr.Error()
		}
	}
	return resp
}

func toPBError(err error) *tipb.Error {
	if err == nil {
		return nil
	}
	perr := new(tipb.Error)
	e := errors.Cause(err)
	switch y := e.(type) {
	case *terror.Error:
		tmp := terror.ToSQLError(y)
		perr.Code = int32(tmp.Code)
		perr.Msg = tmp.Message
	case *mysql.SQLError:
		perr.Code = int32(y.Code)
		perr.Msg = y.Message
	default:
		perr.Code = int32(1)
		perr.Msg = err.Error()
	}
	return perr
}

// extractKVRanges extracts kv.KeyRanges slice from a SelectRequest.
func extractKVRanges(startKey, endKey []byte, keyRanges []*coprocessor.KeyRange, descScan bool) (kvRanges []kv.KeyRange, err error) {
	kvRanges = make([]kv.KeyRange, 0, len(keyRanges))
	for _, kran := range keyRanges {
		if bytes.Compare(kran.GetStart(), kran.GetEnd()) >= 0 {
			err = errors.Errorf("invalid range, start should be smaller than end: %v %v", kran.GetStart(), kran.GetEnd())
			return
		}

		upperKey := kran.GetEnd()
		if bytes.Compare(upperKey, startKey) <= 0 {
			continue
		}
		lowerKey := kran.GetStart()
		if len(endKey) != 0 && bytes.Compare(lowerKey, endKey) >= 0 {
			break
		}
		r := kv.KeyRange{
			StartKey: kv.Key(maxStartKey(lowerKey, startKey)),
			EndKey:   kv.Key(minEndKey(upperKey, endKey)),
		}
		kvRanges = append(kvRanges, r)
	}
	if descScan {
		reverseKVRanges(kvRanges)
	}
	return
}

func reverseKVRanges(kvRanges []kv.KeyRange) {
	for i := range len(kvRanges) / 2 {
		j := len(kvRanges) - i - 1
		kvRanges[i], kvRanges[j] = kvRanges[j], kvRanges[i]
	}
}

func maxStartKey(rangeStartKey kv.Key, regionStartKey []byte) []byte {
	if bytes.Compare([]byte(rangeStartKey), regionStartKey) > 0 {
		return []byte(rangeStartKey)
	}
	return regionStartKey
}

func minEndKey(rangeEndKey kv.Key, regionEndKey []byte) []byte {
	if len(regionEndKey) == 0 || bytes.Compare([]byte(rangeEndKey), regionEndKey) < 0 {
		return []byte(rangeEndKey)
	}
	return regionEndKey
}

const rowsPerChunk = 64

func appendRow(chunks []tipb.Chunk, data []byte, rowCnt int) []tipb.Chunk {
	if rowCnt%rowsPerChunk == 0 {
		chunks = append(chunks, tipb.Chunk{})
	}
	cur := &chunks[len(chunks)-1]
	cur.RowsData = append(cur.RowsData, data...)
	return chunks
}

// fieldTypeFromPBColumn creates a types.FieldType from tipb.ColumnInfo.
func fieldTypeFromPBColumn(col *tipb.ColumnInfo) *types.FieldType {
	charsetStr, collationStr, _ := charset.GetCharsetInfoByID(int(collate.RestoreCollationIDIfNeeded(col.GetCollation())))
	ft := &types.FieldType{}
	ft.SetType(byte(col.GetTp()))
	ft.SetFlag(uint(col.GetFlag()))
	ft.SetFlen(int(col.GetColumnLen()))
	ft.SetDecimal(int(col.GetDecimal()))
	ft.SetElems(col.Elems)
	ft.SetCharset(charsetStr)
	ft.SetCollate(collationStr)
	return ft
}

// handleCopChecksumRequest handles coprocessor check sum request.
func handleCopChecksumRequest(dbReader *dbreader.DBReader, req *coprocessor.Request) *coprocessor.Response {
	resp := &tipb.ChecksumResponse{
		Checksum:   1,
		TotalKvs:   1,
		TotalBytes: 1,
	}
	data, err := resp.Marshal()
	if err != nil {
		return &coprocessor.Response{OtherError: fmt.Sprintf("marshal checksum response error: %v", err)}
	}
	return &coprocessor.Response{Data: data}
}
