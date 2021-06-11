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
// See the License for the specific language governing permissions and
// limitations under the License.

package cophandler

import (
	"io"
	"math"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/mpp"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/expression/aggregation"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/store/mockstore/unistore/tikv/dbreader"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/rowcodec"
	"github.com/pingcap/tipb/go-tipb"
	"github.com/tikv/client-go/v2/tikvrpc"
)

// mpp executor that only servers for mpp execution
type mppExec interface {
	open() error
	next() (*chunk.Chunk, error)
	getFieldTypes() []*types.FieldType
}

type baseMPPExec struct {
	sc *stmtctx.StatementContext

	mppCtx *MPPCtx

	children []mppExec

	fieldTypes []*types.FieldType
}

func (b *baseMPPExec) getFieldTypes() []*types.FieldType {
	return b.fieldTypes
}

type tableScanExec struct {
	baseMPPExec

	kvRanges []kv.KeyRange
	startTS  uint64
	dbReader *dbreader.DBReader

	chunks []*chunk.Chunk
	chkIdx int

	decoder *rowcodec.ChunkDecoder
}

func (e *tableScanExec) SkipValue() bool { return false }

func (e *tableScanExec) Process(key, value []byte) error {
	handle, err := tablecodec.DecodeRowKey(key)
	if err != nil {
		return errors.Trace(err)
	}
	chk := chunk.NewChunkWithCapacity(e.fieldTypes, 0)
	err = e.decoder.DecodeToChunk(value, handle, chk)
	e.chunks = append(e.chunks, chk)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (e *tableScanExec) open() error {
	for _, ran := range e.kvRanges {
		err := e.dbReader.Scan(ran.StartKey, ran.EndKey, math.MaxInt64, e.startTS, e)
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func (e *tableScanExec) next() (*chunk.Chunk, error) {
	if e.chkIdx < len(e.chunks) {
		e.chkIdx++
		return e.chunks[e.chkIdx-1], nil
	}
	return nil, nil
}

type exchSenderExec struct {
	baseMPPExec

	tunnels       []*ExchangerTunnel
	outputOffsets []uint32
	exchangeTp    tipb.ExchangeType
	hashKeyOffset int
}

func (e *exchSenderExec) open() error {
	return e.children[0].open()
}

func (e *exchSenderExec) toTiPBChunk(chk *chunk.Chunk) ([]tipb.Chunk, error) {
	var oldRow []types.Datum
	oldChunks := make([]tipb.Chunk, 0)
	for i := 0; i < chk.NumRows(); i++ {
		oldRow = oldRow[:0]
		for _, outputOff := range e.outputOffsets {
			d := chk.GetRow(i).GetDatum(int(outputOff), e.fieldTypes[outputOff])
			oldRow = append(oldRow, d)
		}
		var err error
		var oldRowBuf []byte
		oldRowBuf, err = codec.EncodeValue(e.sc, oldRowBuf[:0], oldRow...)
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
	}()
	for {
		chk, err := e.children[0].next()
		if err != nil {
			for _, tunnel := range e.tunnels {
				tunnel.ErrCh <- err
			}
			return nil, nil
		} else if chk != nil {
			if e.exchangeTp == tipb.ExchangeType_Hash {
				rows := chk.NumRows()
				targetChunks := make([]*chunk.Chunk, 0, len(e.tunnels))
				for i := 0; i < len(e.tunnels); i++ {
					targetChunks = append(targetChunks, chunk.NewChunkWithCapacity(e.fieldTypes, rows))
				}
				for i := 0; i < rows; i++ {
					row := chk.GetRow(i)
					d := row.GetDatum(e.hashKeyOffset, e.fieldTypes[e.hashKeyOffset])
					if d.IsNull() {
						targetChunks[0].AppendRow(row)
					} else {
						hashKey := int(d.GetInt64() % int64(len(e.tunnels)))
						targetChunks[hashKey].AppendRow(row)
					}
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
						for _, tipbChunk := range tipbChunks {
							tunnel.DataCh <- &tipbChunk
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
					for _, tipbChunk := range tipbChunks {
						tunnel.DataCh <- &tipbChunk
					}
				}
			}
		} else {
			return nil, nil
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
	keyCol, err = keyCol.ConvertTo(e.sc, e.comKeyTp)
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
		if chk == nil {
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
	if chk == nil {
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
	key := make([]byte, 0, 32)
	gbyRow := chunk.MutRowFromTypes(e.groupByTypes)
	for i, item := range e.groupByExprs {
		v, err := item.Eval(row)
		if err != nil {
			return nil, nil, errors.Trace(err)
		}
		gbyRow.SetDatum(i, v)
		b, err := codec.EncodeValue(e.sc, nil, v)
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
			aggCtxs = append(aggCtxs, agg.CreateContext(e.sc))
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
		if chk == nil {
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
				err = agg.Update(aggCtxs[i], e.sc, row)
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
			if e.fieldTypes[i].Tp == mysql.TypeLonglong && result.Kind() == types.KindMysqlDecimal {
				var err error
				result, err = result.ConvertTo(e.sc, e.fieldTypes[i])
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
	chk, err := e.children[0].next()
	if err != nil {
		return nil, errors.Trace(err)
	}
	if chk == nil {
		return nil, nil
	}
	for rows := chk.NumRows() - 1; rows >= 0; rows-- {
		row := chk.GetRow(rows)
		for _, cond := range e.conditions {
			d, err := cond.Eval(row)
			if err != nil {
				return nil, errors.Trace(err)
			}

			var passCheck bool
			if d.IsNull() {
				passCheck = false
			} else {
				isBool, err := d.ToBool(e.sc)
				if err != nil {
					return nil, errors.Trace(err)
				}
				isBool, err = expression.HandleOverflowOnSelection(e.sc, isBool, err)
				if err != nil {
					return nil, errors.Trace(err)
				}
				passCheck = isBool != 0
			}
			if !passCheck {
				chk.TruncateTo(rows)
				break
			}
		}
	}
	return chk, nil
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
	if chk == nil {
		return nil, nil
	}
	newChunk := chunk.NewChunkWithCapacity(e.fieldTypes, 10)
	for i := 0; i < chk.NumRows(); i++ {
		row := chk.GetRow(i)
		newRow := chunk.MutRowFromTypes(e.fieldTypes)
		for i, expr := range e.exprs {
			d, err := expr.Eval(row)
			if err != nil {
				return nil, errors.Trace(err)
			}
			newRow.SetDatum(i, d)
		}
		newChunk.AppendRow(newRow.ToRow())
	}
	return newChunk, nil
}
