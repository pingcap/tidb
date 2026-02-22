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
	"encoding/binary"
	"hash/fnv"
	"io"
	"sort"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/mpp"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/codec"
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
	getChildren() []mppExec
	getIntermediateFieldTypes() []*types.FieldType
	takeIntermediateResults() []*chunk.Chunk
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

func (b *baseMPPExec) getChildren() []mppExec {
	return b.children
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

func (b *baseMPPExec) takeIntermediateResults() []*chunk.Chunk {
	panic("not implemented")
}

func (b *baseMPPExec) getIntermediateFieldTypes() []*types.FieldType {
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
			for g := range numGroupingOffset {
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
	evaluatorSuite := expression.NewEvaluatorSuite(e.conds, true)
	fieldTypes := make([]*types.FieldType, 0, len(e.conds))
	for i := range e.conds {
		fieldTypes = append(fieldTypes, e.conds[i].GetType(e.sctx.GetExprCtx().GetEvalCtx()))
	}
	evalChk := chunk.NewEmptyChunk(fieldTypes)
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

		err := evaluatorSuite.Run(e.sctx.GetExprCtx().GetEvalCtx(), true, chk, evalChk)
		if err != nil {
			return err
		}

		for i := range numRows {
			row := evalChk.GetRow(i)
			tmpDatums := row.GetDatumRow(fieldTypes)
			for j := range tmpDatums {
				tmpDatums[j].Copy(&e.row.key[j])
			}
			if e.heap.tryToAddRow(e.row) {
				e.row.data[0] = make([]byte, 4)
				binary.LittleEndian.PutUint32(e.row.data[0], uint32(len(e.recv)))
				e.row.data[1] = make([]byte, 4)
				binary.LittleEndian.PutUint32(e.row.data[1], uint32(i))
				e.row = newTopNSortRow(len(e.conds))
			}
		}
		evalChk.Reset()
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
	for i := range chk.NumRows() {
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
		e.execSummary.updateOnlyRows(chk.NumRows())
		if e.exchangeTp == tipb.ExchangeType_Hash {
			rows := chk.NumRows()
			targetChunks := make([]*chunk.Chunk, 0, len(e.tunnels))
			for range e.tunnels {
				targetChunks = append(targetChunks, chunk.NewChunkWithCapacity(e.fieldTypes, rows))
			}
			hashVals := fnv.New64()
			payload := make([]byte, 1)
			for i := range rows {
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
	if e.chk != nil {
		e.execSummary.updateOnlyRows(e.chk.NumRows())
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

