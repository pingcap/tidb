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
	"github.com/pingcap/tidb/pkg/expression/aggregation"
	"github.com/pingcap/tidb/pkg/parser/mysql"
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
		for i := range rows {
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
	for i := range chkSize {
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
			e.execSummary.updateOnlyRows(e.reservedRows[idx].Chunk().NumRows())
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
		for i := range rows {
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
	var selected []bool
	for !ret.IsFull() {
		chk, err := e.children[0].next()
		if err != nil {
			return nil, errors.Trace(err)
		}
		if chk == nil || chk.NumRows() == 0 {
			break
		}
		selected, err := expression.VectorizedFilter(e.sctx.GetExprCtx().GetEvalCtx(), true, e.conditions, chunk.NewIterator4Chunk(chk), selected)
		if err != nil {
			return nil, errors.Trace(err)
		}
		for i := range selected {
			if selected[i] {
				ret.AppendRow(chk.GetRow(i))
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
	for i := range chk.NumRows() {
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
