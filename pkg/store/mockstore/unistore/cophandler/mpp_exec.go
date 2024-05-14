// Copyright 2021 PingCAP, Inc.
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
	"encoding/binary"
	"hash/fnv"
	"io"
	"math"
	"sort"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/coprocessor"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/mpp"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/expression/aggregation"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/store/mockstore/unistore/lockstore"
	"github.com/pingcap/tidb/pkg/store/mockstore/unistore/tikv/dbreader"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tidb/pkg/util/rowcodec"
	"github.com/pingcap/tipb/go-tipb"
	"github.com/tikv/client-go/v2/tikvrpc"
)

var (
	// DefaultBatchSize is the default batch size for newly allocated chunk during execution.
	DefaultBatchSize = 32
)

// mpp executor that only servers for mpp execution
type mppExec interface {
	open() error
	next() (*chunk.Chunk, error)
	stop() error
	child() mppExec
	getFieldTypes() []*types.FieldType
	buildSummary() *tipb.ExecutorExecutionSummary
}

type baseMPPExec struct {
	sctx sessionctx.Context

	mppCtx *MPPCtx

	children []mppExec

	fieldTypes  []*types.FieldType
	execSummary execDetail
}

func (b *baseMPPExec) child() mppExec {
	return b.children[0]
}

func (b *baseMPPExec) getFieldTypes() []*types.FieldType {
	return b.fieldTypes
}

func (b *baseMPPExec) buildSummary() *tipb.ExecutorExecutionSummary {
	return b.execSummary.buildSummary()
}

func (b *baseMPPExec) open() error {
	panic("not implemented")
}

func (b *baseMPPExec) next() (*chunk.Chunk, error) {
	panic("not implemented")
}

func (b *baseMPPExec) stop() error {
	for _, child := range b.children {
		err := child.stop()
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

type scanResult struct {
	chk              *chunk.Chunk
	lastProcessedKey kv.Key
	err              error
}

type tableScanExec struct {
	baseMPPExec

	kvRanges      []kv.KeyRange
	startTS       uint64
	dbReader      *dbreader.DBReader
	lockStore     *lockstore.MemStore
	resolvedLocks []uint64
	counts        []int64
	ndvs          []int64
	rowCnt        int64

	chk    *chunk.Chunk
	result chan scanResult
	done   chan struct{}
	wg     util.WaitGroupWrapper

	decoder *rowcodec.ChunkDecoder
	desc    bool

	// if ExtraPhysTblIDCol is requested, fill in the physical table id in this column position
	physTblIDColIdx *int
	// This is used to update the paging range result, updated in next().
	paging *coprocessor.KeyRange
}

func (e *tableScanExec) SkipValue() bool { return false }

func (e *tableScanExec) Process(key, value []byte) error {
	handle, err := tablecodec.DecodeRowKey(key)
	if err != nil {
		return errors.Trace(err)
	}

	err = e.decoder.DecodeToChunk(value, handle, e.chk)
	if err != nil {
		return errors.Trace(err)
	}
	if e.physTblIDColIdx != nil {
		tblID := tablecodec.DecodeTableID(key)
		e.chk.AppendInt64(*e.physTblIDColIdx, tblID)
	}
	e.rowCnt++

	if e.chk.IsFull() {
		lastProcessed := kv.Key(append([]byte{}, key...)) // make a copy to avoid data race
		select {
		case e.result <- scanResult{chk: e.chk, lastProcessedKey: lastProcessed, err: nil}:
			e.chk = chunk.NewChunkWithCapacity(e.fieldTypes, DefaultBatchSize)
		case <-e.done:
			return dbreader.ErrScanBreak
		}
	}
	select {
	case <-e.done:
		return dbreader.ErrScanBreak
	default:
	}
	return nil
}

func (e *tableScanExec) open() error {
	var err error
	if e.lockStore != nil {
		for _, ran := range e.kvRanges {
			err = checkRangeLockForRange(e.lockStore, e.startTS, e.resolvedLocks, ran)
			if err != nil {
				return err
			}
		}
	}
	e.chk = chunk.NewChunkWithCapacity(e.fieldTypes, DefaultBatchSize)
	e.result = make(chan scanResult, 1)
	e.done = make(chan struct{})
	e.wg.Run(func() {
		// close the channel when done scanning, so that next() will got nil chunk
		defer close(e.result)
		var i int
		var ran kv.KeyRange
		for i, ran = range e.kvRanges {
			oldCnt := e.rowCnt
			if e.desc {
				err = e.dbReader.ReverseScan(ran.StartKey, ran.EndKey, math.MaxInt64, e.startTS, e)
			} else {
				err = e.dbReader.Scan(ran.StartKey, ran.EndKey, math.MaxInt64, e.startTS, e)
			}
			if len(e.counts) != 0 {
				e.counts[i] += e.rowCnt - oldCnt
				e.ndvs[i] += e.rowCnt - oldCnt
			}
			if err != nil {
				e.result <- scanResult{err: err}
				return
			}
		}

		// handle the last chunk
		if e.chk != nil && e.chk.NumRows() > 0 {
			select {
			case e.result <- scanResult{chk: e.chk, err: nil}:
				return
			case <-e.done:
			}
			return
		}
	})

	return nil
}

func (e *tableScanExec) next() (*chunk.Chunk, error) {
	result := <-e.result
	// Update the range for coprocessor paging protocol.
	if e.paging != nil && result.err == nil {
		if e.desc {
			if result.lastProcessedKey != nil {
				*e.paging = coprocessor.KeyRange{Start: result.lastProcessedKey}
			} else {
				*e.paging = coprocessor.KeyRange{Start: e.kvRanges[len(e.kvRanges)-1].StartKey}
			}
		} else {
			if result.lastProcessedKey != nil {
				*e.paging = coprocessor.KeyRange{End: result.lastProcessedKey.Next()}
			} else {
				*e.paging = coprocessor.KeyRange{End: e.kvRanges[len(e.kvRanges)-1].EndKey}
			}
		}
	}

	if result.chk == nil || result.err != nil {
		return nil, result.err
	}
	e.execSummary.updateOnlyRows(result.chk.NumRows())
	return result.chk, nil
}

func (e *tableScanExec) stop() error {
	// just in case the channel is not initialized
	if e.done != nil {
		close(e.done)
	}
	e.wg.Wait()
	return nil
}

type indexScanExec struct {
	baseMPPExec

	startTS       uint64
	kvRanges      []kv.KeyRange
	desc          bool
	dbReader      *dbreader.DBReader
	lockStore     *lockstore.MemStore
	resolvedLocks []uint64
	counts        []int64
	ndvs          []int64
	prevVals      [][]byte
	rowCnt        int64
	ndvCnt        int64
	chk           *chunk.Chunk
	chkIdx        int
	chunks        []*chunk.Chunk

	colInfos   []rowcodec.ColInfo
	numIdxCols int
	hdlStatus  tablecodec.HandleStatus

	// if ExtraPhysTblIDCol is requested, fill in the physical table id in this column position
	physTblIDColIdx *int
	// This is used to update the paging range result, updated in next().
	paging                 *coprocessor.KeyRange
	chunkLastProcessedKeys []kv.Key
}

func (e *indexScanExec) SkipValue() bool { return false }

func (e *indexScanExec) isNewVals(values [][]byte) bool {
	for i := 0; i < e.numIdxCols; i++ {
		if !bytes.Equal(e.prevVals[i], values[i]) {
			return true
		}
	}
	return false
}

func (e *indexScanExec) Process(key, value []byte) error {
	values, err := tablecodec.DecodeIndexKV(key, value, e.numIdxCols, e.hdlStatus, e.colInfos)
	if err != nil {
		return err
	}
	e.rowCnt++
	if len(e.counts) > 0 && (len(e.prevVals[0]) == 0 || e.isNewVals(values)) {
		e.ndvCnt++
		for i := 0; i < e.numIdxCols; i++ {
			e.prevVals[i] = append(e.prevVals[i][:0], values[i]...)
		}
	}
	decoder := codec.NewDecoder(e.chk, e.sctx.GetSessionVars().StmtCtx.TimeZone())
	for i, value := range values {
		if i < len(e.fieldTypes) {
			_, err = decoder.DecodeOne(value, i, e.fieldTypes[i])
			if err != nil {
				return errors.Trace(err)
			}
		}
	}
	if e.physTblIDColIdx != nil {
		tblID := tablecodec.DecodeTableID(key)
		e.chk.AppendInt64(*e.physTblIDColIdx, tblID)
	}
	if e.chk.IsFull() {
		e.chunks = append(e.chunks, e.chk)
		if e.paging != nil {
			lastProcessed := kv.Key(append([]byte{}, key...)) // need a deep copy to store the key
			e.chunkLastProcessedKeys = append(e.chunkLastProcessedKeys, lastProcessed)
		}
		e.chk = chunk.NewChunkWithCapacity(e.fieldTypes, DefaultBatchSize)
	}
	return nil
}

func (e *indexScanExec) open() error {
	var err error
	for _, ran := range e.kvRanges {
		err = checkRangeLockForRange(e.lockStore, e.startTS, e.resolvedLocks, ran)
		if err != nil {
			return err
		}
	}
	e.chk = chunk.NewChunkWithCapacity(e.fieldTypes, DefaultBatchSize)
	for i, rg := range e.kvRanges {
		oldCnt := e.rowCnt
		e.ndvCnt = 0
		if e.desc {
			err = e.dbReader.ReverseScan(rg.StartKey, rg.EndKey, math.MaxInt64, e.startTS, e)
		} else {
			err = e.dbReader.Scan(rg.StartKey, rg.EndKey, math.MaxInt64, e.startTS, e)
		}
		if len(e.counts) != 0 {
			e.counts[i] += e.rowCnt - oldCnt
			e.ndvs[i] += e.ndvCnt
		}
		if err != nil {
			return errors.Trace(err)
		}
	}
	if e.chk.NumRows() != 0 {
		e.chunks = append(e.chunks, e.chk)
	}
	return nil
}

func (e *indexScanExec) next() (*chunk.Chunk, error) {
	if e.chkIdx < len(e.chunks) {
		e.chkIdx++
		e.execSummary.updateOnlyRows(e.chunks[e.chkIdx-1].NumRows())
		if e.paging != nil {
			if e.desc {
				if e.chkIdx == len(e.chunks) {
					*e.paging = coprocessor.KeyRange{Start: e.kvRanges[len(e.kvRanges)-1].StartKey}
				} else {
					*e.paging = coprocessor.KeyRange{Start: e.chunkLastProcessedKeys[e.chkIdx-1]}
				}
			} else {
				if e.chkIdx == len(e.chunks) {
					*e.paging = coprocessor.KeyRange{End: e.kvRanges[len(e.kvRanges)-1].EndKey}
				} else {
					*e.paging = coprocessor.KeyRange{End: e.chunkLastProcessedKeys[e.chkIdx-1].Next()}
				}
			}
		}
		return e.chunks[e.chkIdx-1], nil
	}

	if e.paging != nil {
		if e.desc {
			*e.paging = coprocessor.KeyRange{Start: e.kvRanges[len(e.kvRanges)-1].StartKey}
		} else {
			*e.paging = coprocessor.KeyRange{End: e.kvRanges[len(e.kvRanges)-1].EndKey}
		}
	}
	return nil, nil
}

type limitExec struct {
	baseMPPExec

	limit uint64
}

func (e *limitExec) open() error {
	return e.children[0].open()
}

func (e *limitExec) next() (*chunk.Chunk, error) {
	chk, err := e.children[0].next()
	if err != nil || chk == nil || chk.NumRows() == 0 {
		return chk, err
	}
	if uint64(chk.NumRows()) <= e.limit {
		e.limit -= uint64(chk.NumRows())
	} else {
		chk.TruncateTo(int(e.limit))
		e.limit = 0
	}
	e.execSummary.updateOnlyRows(chk.NumRows())
	return chk, nil
}

// expandExec is the basic mock logic for expand executor in uniStore,
// with which we can validate the mpp plan correctness from explain result and result returned.
type expandExec struct {
	baseMPPExec

	lastNum              int
	lastChunk            *chunk.Chunk
	groupingSets         expression.GroupingSets
	groupingSetOffsetMap []map[int]struct{}
	groupingSetScope     map[int]struct{}
}

func (e *expandExec) open() error {
	if err := e.children[0].open(); err != nil {
		return err
	}
	// building the quick finding map
	e.groupingSetOffsetMap = make([]map[int]struct{}, 0, len(e.groupingSets))
	e.groupingSetScope = make(map[int]struct{}, len(e.groupingSets))
	for _, gs := range e.groupingSets {
		tmp := make(map[int]struct{}, len(gs))
		// for every grouping set, collect column offsets under this grouping set.
		for _, groupingExprs := range gs {
			for _, groupingExpr := range groupingExprs {
				col, ok := groupingExpr.(*expression.Column)
				if !ok {
					return errors.New("grouping set expr is not column ref")
				}
				tmp[col.Index] = struct{}{}
				e.groupingSetScope[col.Index] = struct{}{}
			}
		}
		e.groupingSetOffsetMap = append(e.groupingSetOffsetMap, tmp)
	}
	return nil
}

func (e *expandExec) isGroupingCol(index int) bool {
	if _, ok := e.groupingSetScope[index]; ok {
		return true
	}
	return false
}

func (e *expandExec) next() (*chunk.Chunk, error) {
	var (
		err error
	)
	if e.groupingSets.IsEmpty() {
		return e.children[0].next()
	}
	resChk := chunk.NewChunkWithCapacity(e.getFieldTypes(), DefaultBatchSize)
	for {
		if e.lastChunk == nil || e.lastChunk.NumRows() == e.lastNum {
			// fetch one chunk from children.
			e.lastChunk, err = e.children[0].next()
			if err != nil {
				return nil, err
			}
			e.lastNum = 0
			if e.lastChunk == nil || e.lastChunk.NumRows() == 0 {
				break
			}
			e.execSummary.updateOnlyRows(e.lastChunk.NumRows())
		}
		numRows := e.lastChunk.NumRows()
		numGroupingOffset := len(e.groupingSets)

		for i := e.lastNum; i < numRows; i++ {
			row := e.lastChunk.GetRow(i)
			e.lastNum++
			// for every grouping set, expand the base row N times.
			for g := 0; g < numGroupingOffset; g++ {
				repeatRow := chunk.MutRowFromTypes(e.fieldTypes)
				// for every targeted grouping set:
				// 1: for every column in this grouping set, setting them as it was.
				// 2: for every column in other target grouping set, setting them as null.
				// 3: for every column not in any grouping set, setting them as it was.
				//      * normal agg only aimed at one replica of them with groupingID = 1
				// 		* so we don't need to change non-related column to be nullable.
				//		* so we don't need to mutate the column to be null when groupingID > 1
				for datumOffset, datumType := range e.fieldTypes[:len(e.fieldTypes)-1] {
					if _, ok := e.groupingSetOffsetMap[g][datumOffset]; ok {
						repeatRow.SetDatum(datumOffset, row.GetDatum(datumOffset, datumType))
					} else if !e.isGroupingCol(datumOffset) {
						repeatRow.SetDatum(datumOffset, row.GetDatum(datumOffset, datumType))
					} else {
						repeatRow.SetDatum(datumOffset, types.NewDatum(nil))
					}
				}
				// the last one column should be groupingID col.
				groupingID := g + 1
				repeatRow.SetDatum(len(e.fieldTypes)-1, types.NewDatum(groupingID))
				resChk.AppendRow(repeatRow.ToRow())
			}
			if DefaultBatchSize-resChk.NumRows() < numGroupingOffset {
				// no enough room for another repeated N rows, return this chunk immediately.
				return resChk, nil
			}
		}
	}
	return resChk, nil
}

type topNExec struct {
	baseMPPExec

	topn  uint64
	idx   uint64
	heap  *topNHeap
	conds []expression.Expression
	row   *sortRow
	recv  []*chunk.Chunk

	// When dummy is true, topNExec just copy what it read from children to its parent.
	dummy bool
}

func (e *topNExec) open() error {
	var chk *chunk.Chunk
	var err error
	err = e.children[0].open()
	if err != nil {
		return err
	}

	if e.dummy {
		return nil
	}

	for {
		chk, err = e.children[0].next()
		if err != nil {
			return err
		}
		if chk == nil || chk.NumRows() == 0 {
			break
		}
		e.execSummary.updateOnlyRows(chk.NumRows())
		numRows := chk.NumRows()
		for i := 0; i < numRows; i++ {
			row := chk.GetRow(i)
			for j, cond := range e.conds {
				d, err := cond.Eval(e.sctx.GetExprCtx().GetEvalCtx(), row)
				if err != nil {
					return err
				}
				d.Copy(&e.row.key[j])
			}
			if e.heap.tryToAddRow(e.row) {
				e.row.data[0] = make([]byte, 4)
				binary.LittleEndian.PutUint32(e.row.data[0], uint32(len(e.recv)))
				e.row.data[1] = make([]byte, 4)
				binary.LittleEndian.PutUint32(e.row.data[1], uint32(i))
				e.row = newTopNSortRow(len(e.conds))
			}
		}
		e.recv = append(e.recv, chk)
	}
	sort.Sort(&e.heap.topNSorter)
	return nil
}

func (e *topNExec) next() (*chunk.Chunk, error) {
	if e.dummy {
		return e.children[0].next()
	}

	chk := chunk.NewChunkWithCapacity(e.getFieldTypes(), DefaultBatchSize)
	for ; !chk.IsFull() && e.idx < e.topn && e.idx < uint64(e.heap.heapSize); e.idx++ {
		row := e.heap.rows[e.idx]
		chkID := binary.LittleEndian.Uint32(row.data[0])
		rowID := binary.LittleEndian.Uint32(row.data[1])
		chk.AppendRow(e.recv[chkID].GetRow(int(rowID)))
	}
	return chk, nil
}

type exchSenderExec struct {
	baseMPPExec

	tunnels        []*ExchangerTunnel
	outputOffsets  []uint32
	exchangeTp     tipb.ExchangeType
	hashKeyOffsets []int
	hashKeyTypes   []*types.FieldType
}

func (e *exchSenderExec) open() error {
	return e.children[0].open()
}

func (e *exchSenderExec) toTiPBChunk(chk *chunk.Chunk) ([]tipb.Chunk, error) {
	var oldRow []types.Datum
	oldChunks := make([]tipb.Chunk, 0)
	sc := e.sctx.GetSessionVars().StmtCtx
	for i := 0; i < chk.NumRows(); i++ {
		oldRow = oldRow[:0]
		for _, outputOff := range e.outputOffsets {
			d := chk.GetRow(i).GetDatum(int(outputOff), e.fieldTypes[outputOff])
			oldRow = append(oldRow, d)
		}
		var err error
		var oldRowBuf []byte
		oldRowBuf, err = codec.EncodeValue(sc.TimeZone(), oldRowBuf[:0], oldRow...)
		err = sc.HandleError(err)
		if err != nil {
			return nil, errors.Trace(err)
		}
		oldChunks = appendRow(oldChunks, oldRowBuf, i)
	}
	return oldChunks, nil
}

func (e *exchSenderExec) next() (*chunk.Chunk, error) {
	defer func() {
		for _, tunnel := range e.tunnels {
			<-tunnel.connectedCh
			close(tunnel.ErrCh)
			close(tunnel.DataCh)
		}
		err := e.stop()
		if err != nil {
			panic(err)
		}
	}()

	sc := e.sctx.GetSessionVars().StmtCtx
	for {
		chk, err := e.children[0].next()
		if err != nil {
			for _, tunnel := range e.tunnels {
				tunnel.ErrCh <- err
			}
			return nil, nil
		} else if !(chk != nil && chk.NumRows() != 0) {
			return nil, nil
		}
		if e.exchangeTp == tipb.ExchangeType_Hash {
			rows := chk.NumRows()
			targetChunks := make([]*chunk.Chunk, 0, len(e.tunnels))
			for i := 0; i < len(e.tunnels); i++ {
				targetChunks = append(targetChunks, chunk.NewChunkWithCapacity(e.fieldTypes, rows))
			}
			hashVals := fnv.New64()
			payload := make([]byte, 1)
			for i := 0; i < rows; i++ {
				row := chk.GetRow(i)
				hashVals.Reset()
				// use hash values to get unique uint64 to mod.
				// collect all the hash key datum.
				err := codec.HashChunkRow(sc.TypeCtx(), hashVals, row, e.hashKeyTypes, e.hashKeyOffsets, payload)
				if err != nil {
					for _, tunnel := range e.tunnels {
						tunnel.ErrCh <- err
					}
					return nil, nil
				}
				hashKey := hashVals.Sum64() % uint64(len(e.tunnels))
				targetChunks[hashKey].AppendRow(row)
			}
			for i, tunnel := range e.tunnels {
				if targetChunks[i].NumRows() > 0 {
					tipbChunks, err := e.toTiPBChunk(targetChunks[i])
					if err != nil {
						for _, tunnel := range e.tunnels {
							tunnel.ErrCh <- err
						}
						return nil, nil
					}
					for j := range tipbChunks {
						tunnel.DataCh <- &tipbChunks[j]
					}
				}
			}
		} else {
			for _, tunnel := range e.tunnels {
				tipbChunks, err := e.toTiPBChunk(chk)
				if err != nil {
					for _, tunnel := range e.tunnels {
						tunnel.ErrCh <- err
					}
					return nil, nil
				}
				for i := range tipbChunks {
					tunnel.DataCh <- &tipbChunks[i]
				}
			}
		}
	}
}

type exchRecvExec struct {
	baseMPPExec

	exchangeReceiver *tipb.ExchangeReceiver
	chk              *chunk.Chunk
	lock             sync.Mutex
	wg               sync.WaitGroup
	err              error
	inited           bool
}

func (e *exchRecvExec) open() error {
	return nil
}

func (e *exchRecvExec) init() error {
	e.chk = chunk.NewChunkWithCapacity(e.fieldTypes, 0)
	serverMetas := make([]*mpp.TaskMeta, 0, len(e.exchangeReceiver.EncodedTaskMeta))
	for _, encodedMeta := range e.exchangeReceiver.EncodedTaskMeta {
		meta := new(mpp.TaskMeta)
		err := meta.Unmarshal(encodedMeta)
		if err != nil {
			return errors.Trace(err)
		}
		serverMetas = append(serverMetas, meta)
	}
	// for receiver: open conn worker for every receive meta.
	for _, meta := range serverMetas {
		e.wg.Add(1)
		go e.runTunnelWorker(e.mppCtx.TaskHandler, meta)
	}
	e.wg.Wait()
	return e.err
}

func (e *exchRecvExec) next() (*chunk.Chunk, error) {
	if !e.inited {
		e.inited = true
		if err := e.init(); err != nil {
			return nil, err
		}
	}
	if e.chk != nil {
		defer func() {
			e.chk = nil
		}()
	}
	return e.chk, nil
}

func (e *exchRecvExec) EstablishConnAndReceiveData(h *MPPTaskHandler, meta *mpp.TaskMeta) ([]*mpp.MPPDataPacket, error) {
	req := &mpp.EstablishMPPConnectionRequest{ReceiverMeta: h.Meta, SenderMeta: meta}
	rpcReq := tikvrpc.NewRequest(tikvrpc.CmdMPPConn, req, kvrpcpb.Context{})
	rpcResp, err := h.RPCClient.SendRequest(e.mppCtx.Ctx, meta.Address, rpcReq, 3600*time.Second)
	if err != nil {
		return nil, errors.Trace(err)
	}

	resp := rpcResp.Resp.(*tikvrpc.MPPStreamResponse)

	mppResponse := resp.MPPDataPacket
	ret := make([]*mpp.MPPDataPacket, 0, 3)
	for {
		if mppResponse == nil {
			return ret, nil
		}
		if mppResponse.Error != nil {
			return nil, errors.New(mppResponse.Error.Msg)
		}
		ret = append(ret, mppResponse)
		mppResponse, err = resp.Recv()
		if err != nil {
			if errors.Cause(err) == io.EOF {
				return ret, nil
			}
			return nil, errors.Trace(err)
		}
		// stream resp
		if mppResponse == nil {
			return ret, nil
		}
	}
}

func (e *exchRecvExec) runTunnelWorker(h *MPPTaskHandler, meta *mpp.TaskMeta) {
	defer func() {
		e.wg.Done()
	}()

	var (
		err  error
		resp []*mpp.MPPDataPacket
	)

	resp, err = e.EstablishConnAndReceiveData(h, meta)
	if err != nil {
		e.err = err
		return
	}
	for _, mppData := range resp {
		var selectResp tipb.SelectResponse
		err = selectResp.Unmarshal(mppData.Data)
		if err != nil {
			e.err = err
			return
		}
		for _, tipbChunk := range selectResp.Chunks {
			chk := chunk.NewChunkWithCapacity(e.fieldTypes, 0)
			err = pbChunkToChunk(tipbChunk, chk, e.fieldTypes)
			if err != nil {
				e.err = err
				return
			}
			e.lock.Lock()
			e.chk.Append(chk, 0, chk.NumRows())
			e.lock.Unlock()
		}
	}
}

// TODO: Let the join support conditions / multiple keys
type joinExec struct {
	baseMPPExec

	*tipb.Join

	hashMap map[string][]chunk.Row

	buildKey *expression.Column
	probeKey *expression.Column

	buildSideIdx int64

	buildChild mppExec
	probeChild mppExec

	idx          int
	reservedRows []chunk.Row

	defaultInner chunk.Row
	inited       bool
	// align the types of join keys and build keys
	comKeyTp *types.FieldType
}

func (e *joinExec) getHashKey(keyCol types.Datum) (str string, err error) {
	keyCol, err = keyCol.ConvertTo(e.sctx.GetSessionVars().StmtCtx.TypeCtx(), e.comKeyTp)
	if err != nil {
		return str, errors.Trace(err)
	}
	str, err = keyCol.ToString()
	if err != nil {
		return "", errors.Trace(err)
	}
	return str, nil
}

func (e *joinExec) buildHashTable() error {
	for {
		chk, err := e.buildChild.next()
		if err != nil {
			return errors.Trace(err)
		}
		if chk == nil || chk.NumRows() == 0 {
			return nil
		}
		rows := chk.NumRows()
		for i := 0; i < rows; i++ {
			row := chk.GetRow(i)
			keyCol := row.GetDatum(e.buildKey.Index, e.buildChild.getFieldTypes()[e.buildKey.Index])
			key, err := e.getHashKey(keyCol)
			if err != nil {
				return errors.Trace(err)
			}
			if rowSet, ok := e.hashMap[key]; ok {
				rowSet = append(rowSet, row)
				e.hashMap[key] = rowSet
			} else {
				e.hashMap[key] = []chunk.Row{row}
			}
		}
	}
}

func (e *joinExec) fetchRows() (bool, error) {
	chk, err := e.probeChild.next()
	if err != nil {
		return false, errors.Trace(err)
	}
	if chk == nil || chk.NumRows() == 0 {
		return true, nil
	}
	e.idx = 0
	e.reservedRows = make([]chunk.Row, 0)
	chkSize := chk.NumRows()
	for i := 0; i < chkSize; i++ {
		row := chk.GetRow(i)
		keyCol := row.GetDatum(e.probeKey.Index, e.probeChild.getFieldTypes()[e.probeKey.Index])
		key, err := e.getHashKey(keyCol)
		if err != nil {
			return false, errors.Trace(err)
		}
		if rowSet, ok := e.hashMap[key]; ok {
			for _, matched := range rowSet {
				newRow := chunk.MutRowFromTypes(e.fieldTypes)
				if e.buildSideIdx == 0 {
					newRow.ShallowCopyPartialRow(0, matched)
					newRow.ShallowCopyPartialRow(matched.Len(), row)
				} else {
					newRow.ShallowCopyPartialRow(0, row)
					newRow.ShallowCopyPartialRow(row.Len(), matched)
				}
				e.reservedRows = append(e.reservedRows, newRow.ToRow())
			}
		} else if e.Join.JoinType == tipb.JoinType_TypeLeftOuterJoin {
			newRow := chunk.MutRowFromTypes(e.fieldTypes)
			newRow.ShallowCopyPartialRow(0, row)
			newRow.ShallowCopyPartialRow(row.Len(), e.defaultInner)
			e.reservedRows = append(e.reservedRows, newRow.ToRow())
		} else if e.Join.JoinType == tipb.JoinType_TypeRightOuterJoin {
			newRow := chunk.MutRowFromTypes(e.fieldTypes)
			newRow.ShallowCopyPartialRow(0, e.defaultInner)
			newRow.ShallowCopyPartialRow(e.defaultInner.Len(), row)
			e.reservedRows = append(e.reservedRows, newRow.ToRow())
		}
	}
	return false, nil
}

func (e *joinExec) open() error {
	err := e.buildChild.open()
	if err != nil {
		return errors.Trace(err)
	}
	err = e.probeChild.open()
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (e *joinExec) stop() error {
	err := e.buildChild.stop()
	if err != nil {
		return errors.Trace(err)
	}
	err = e.probeChild.stop()
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (e *joinExec) next() (*chunk.Chunk, error) {
	if !e.inited {
		e.inited = true
		if err := e.buildHashTable(); err != nil {
			return nil, err
		}
	}
	for {
		if e.idx < len(e.reservedRows) {
			idx := e.idx
			e.idx++
			return e.reservedRows[idx].Chunk(), nil
		}
		eof, err := e.fetchRows()
		if err != nil {
			return nil, errors.Trace(err)
		}
		if eof {
			return nil, nil
		}
	}
}

type aggExec struct {
	baseMPPExec

	aggExprs     []aggregation.Aggregation
	groupByExprs []expression.Expression
	groups       map[string]struct{}
	groupKeys    [][]byte
	aggCtxsMap   map[string][]*aggregation.AggEvaluateContext

	groupByRows  []chunk.Row
	groupByTypes []*types.FieldType

	processed bool
}

func (e *aggExec) open() error {
	return e.children[0].open()
}

func (e *aggExec) getGroupKey(row chunk.Row) (*chunk.MutRow, []byte, error) {
	length := len(e.groupByExprs)
	if length == 0 {
		return nil, nil, nil
	}
	key := make([]byte, 0, DefaultBatchSize)
	gbyRow := chunk.MutRowFromTypes(e.groupByTypes)
	sc := e.sctx.GetSessionVars().StmtCtx
	for i, item := range e.groupByExprs {
		v, err := item.Eval(e.sctx.GetExprCtx().GetEvalCtx(), row)
		if err != nil {
			return nil, nil, errors.Trace(err)
		}
		gbyRow.SetDatum(i, v)
		b, err := codec.EncodeValue(sc.TimeZone(), nil, v)
		err = sc.HandleError(err)
		if err != nil {
			return nil, nil, errors.Trace(err)
		}
		key = append(key, b...)
	}
	return &gbyRow, key, nil
}

func (e *aggExec) getContexts(groupKey []byte) []*aggregation.AggEvaluateContext {
	aggCtxs, ok := e.aggCtxsMap[string(groupKey)]
	if !ok {
		aggCtxs = make([]*aggregation.AggEvaluateContext, 0, len(e.aggExprs))
		for _, agg := range e.aggExprs {
			aggCtxs = append(aggCtxs, agg.CreateContext(e.sctx.GetExprCtx().GetEvalCtx()))
		}
		e.aggCtxsMap[string(groupKey)] = aggCtxs
	}
	return aggCtxs
}

func (e *aggExec) processAllRows() (*chunk.Chunk, error) {
	for {
		chk, err := e.children[0].next()
		if err != nil {
			return nil, errors.Trace(err)
		}
		if chk == nil || chk.NumRows() == 0 {
			break
		}
		rows := chk.NumRows()
		for i := 0; i < rows; i++ {
			row := chk.GetRow(i)
			gbyRow, gk, err := e.getGroupKey(row)
			if err != nil {
				return nil, errors.Trace(err)
			}
			if _, ok := e.groups[string(gk)]; !ok {
				e.groups[string(gk)] = struct{}{}
				e.groupKeys = append(e.groupKeys, gk)
				if gbyRow != nil {
					e.groupByRows = append(e.groupByRows, gbyRow.ToRow())
				}
			}

			aggCtxs := e.getContexts(gk)
			for i, agg := range e.aggExprs {
				err = agg.Update(aggCtxs[i], e.sctx.GetSessionVars().StmtCtx, row)
				if err != nil {
					return nil, errors.Trace(err)
				}
			}
		}
	}

	chk := chunk.NewChunkWithCapacity(e.fieldTypes, 0)

	for i, gk := range e.groupKeys {
		newRow := chunk.MutRowFromTypes(e.fieldTypes)
		aggCtxs := e.getContexts(gk)
		for i, agg := range e.aggExprs {
			result := agg.GetResult(aggCtxs[i])
			if e.fieldTypes[i].GetType() == mysql.TypeLonglong && result.Kind() == types.KindMysqlDecimal {
				var err error
				result, err = result.ConvertTo(e.sctx.GetSessionVars().StmtCtx.TypeCtx(), e.fieldTypes[i])
				if err != nil {
					return nil, errors.Trace(err)
				}
			}
			newRow.SetDatum(i, result)
		}
		if len(e.groupByRows) > 0 {
			newRow.ShallowCopyPartialRow(len(e.aggExprs), e.groupByRows[i])
		}
		chk.AppendRow(newRow.ToRow())
	}
	e.execSummary.updateOnlyRows(chk.NumRows())
	return chk, nil
}

func (e *aggExec) next() (*chunk.Chunk, error) {
	if !e.processed {
		e.processed = true
		return e.processAllRows()
	}
	return nil, nil
}

type selExec struct {
	baseMPPExec

	conditions []expression.Expression
}

func (e *selExec) open() error {
	return e.children[0].open()
}

func (e *selExec) next() (*chunk.Chunk, error) {
	ret := chunk.NewChunkWithCapacity(e.getFieldTypes(), DefaultBatchSize)
	for !ret.IsFull() {
		chk, err := e.children[0].next()
		if err != nil {
			return nil, errors.Trace(err)
		}
		if chk == nil || chk.NumRows() == 0 {
			break
		}
		numRows := chk.NumRows()
		for rows := 0; rows < numRows; rows++ {
			row := chk.GetRow(rows)
			passCheck := true
			for _, cond := range e.conditions {
				d, err := cond.Eval(e.sctx.GetExprCtx().GetEvalCtx(), row)
				if err != nil {
					return nil, errors.Trace(err)
				}

				if d.IsNull() {
					passCheck = false
				} else {
					isBool, err := d.ToBool(e.sctx.GetSessionVars().StmtCtx.TypeCtx())
					if err != nil {
						return nil, errors.Trace(err)
					}
					passCheck = isBool != 0
				}
				if !passCheck {
					break
				}
			}
			if passCheck {
				ret.AppendRow(row)
				e.execSummary.updateOnlyRows(1)
			}
		}
	}

	return ret, nil
}

type projExec struct {
	baseMPPExec
	exprs []expression.Expression
}

func (e *projExec) open() error {
	return e.children[0].open()
}

func (e *projExec) next() (*chunk.Chunk, error) {
	chk, err := e.children[0].next()
	if err != nil {
		return nil, errors.Trace(err)
	}
	if chk == nil || chk.NumRows() == 0 {
		return nil, nil
	}
	e.baseMPPExec.execSummary.updateOnlyRows(chk.NumRows())
	newChunk := chunk.NewChunkWithCapacity(e.fieldTypes, 10)
	for i := 0; i < chk.NumRows(); i++ {
		row := chk.GetRow(i)
		newRow := chunk.MutRowFromTypes(e.fieldTypes)
		for i, expr := range e.exprs {
			d, err := expr.Eval(e.sctx.GetExprCtx().GetEvalCtx(), row)
			if err != nil {
				return nil, errors.Trace(err)
			}
			newRow.SetDatum(i, d)
		}
		newChunk.AppendRow(newRow.ToRow())
	}
	return newChunk, nil
}
