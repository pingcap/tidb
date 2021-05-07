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
// See the License for the specific language governing permissions and
// limitations under the License.

package cophandler

import (
	"context"
	"sync"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/coprocessor"
	"github.com/pingcap/kvproto/pkg/mpp"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/expression/aggregation"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/store/mockstore/unistore/client"
	"github.com/pingcap/tidb/store/mockstore/unistore/tikv/dbreader"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tipb/go-tipb"
	"github.com/uber-go/atomic"
)

const (
	// MPPErrTunnelNotFound means you can't find an expected tunnel.
	MPPErrTunnelNotFound = iota
	// MPPErrEstablishConnMultiTimes means we receive the Establish requests at least twice.
	MPPErrEstablishConnMultiTimes
)

type mppExecBuilder struct {
	sc       *stmtctx.StatementContext
	dbReader *dbreader.DBReader
	req      *coprocessor.Request
	mppCtx   *MPPCtx
	dagReq   *tipb.DAGRequest
}

func (b *mppExecBuilder) buildMPPTableScan(pb *tipb.TableScan) (*tableScanExec, error) {
	ranges, err := extractKVRanges(b.dbReader.StartKey, b.dbReader.EndKey, b.req.Ranges, false)
	if err != nil {
		return nil, errors.Trace(err)
	}
	ts := &tableScanExec{
		baseMPPExec: baseMPPExec{sc: b.sc, mppCtx: b.mppCtx},
		startTS:     b.req.StartTs,
		kvRanges:    ranges,
		dbReader:    b.dbReader,
	}
	for _, col := range pb.Columns {
		ft := fieldTypeFromPBColumn(col)
		ts.fieldTypes = append(ts.fieldTypes, ft)
	}
	ts.decoder, err = newRowDecoder(pb.Columns, ts.fieldTypes, pb.PrimaryColumnIds, b.sc.TimeZone)
	return ts, err
}

func (b *mppExecBuilder) buildMPPExchangeSender(pb *tipb.ExchangeSender) (*exchSenderExec, error) {
	child, err := b.buildMPPExecutor(pb.Child)
	if err != nil {
		return nil, err
	}

	e := &exchSenderExec{
		baseMPPExec: baseMPPExec{
			sc:         b.sc,
			mppCtx:     b.mppCtx,
			children:   []mppExec{child},
			fieldTypes: child.getFieldTypes(),
		},
		exchangeTp: pb.Tp,
	}
	if pb.Tp == tipb.ExchangeType_Hash {
		if len(pb.PartitionKeys) != 1 {
			return nil, errors.New("The number of hash key must be 1")
		}
		expr, err := expression.PBToExpr(pb.PartitionKeys[0], child.getFieldTypes(), b.sc)
		if err != nil {
			return nil, errors.Trace(err)
		}
		col, ok := expr.(*expression.Column)
		if !ok {
			return nil, errors.New("Hash key must be column type")
		}
		e.hashKeyOffset = col.Index
	}

	for _, taskMeta := range pb.EncodedTaskMeta {
		targetTask := new(mpp.TaskMeta)
		err := targetTask.Unmarshal(taskMeta)
		if err != nil {
			return nil, err
		}
		tunnel := &ExchangerTunnel{
			DataCh:      make(chan *tipb.Chunk, 10),
			sourceTask:  b.mppCtx.TaskHandler.Meta,
			targetTask:  targetTask,
			connectedCh: make(chan struct{}),
			ErrCh:       make(chan error, 1),
		}
		e.tunnels = append(e.tunnels, tunnel)
		err = b.mppCtx.TaskHandler.registerTunnel(tunnel)
		if err != nil {
			return nil, err
		}
	}
	e.outputOffsets = b.dagReq.OutputOffsets
	return e, nil
}

func (b *mppExecBuilder) buildMPPExchangeReceiver(pb *tipb.ExchangeReceiver) (*exchRecvExec, error) {
	e := &exchRecvExec{
		baseMPPExec: baseMPPExec{
			sc:     b.sc,
			mppCtx: b.mppCtx,
		},
		exchangeReceiver: pb,
	}

	for _, pbType := range pb.FieldTypes {
		tp := expression.FieldTypeFromPB(pbType)
		if tp.Tp == mysql.TypeEnum {
			tp.Elems = append(tp.Elems, pbType.Elems...)
		}
		e.fieldTypes = append(e.fieldTypes, tp)
	}
	return e, nil
}

func (b *mppExecBuilder) buildMPPJoin(pb *tipb.Join, children []*tipb.Executor) (*joinExec, error) {
	e := &joinExec{
		baseMPPExec: baseMPPExec{
			sc:     b.sc,
			mppCtx: b.mppCtx,
		},
		Join:         pb,
		hashMap:      make(map[string][]chunk.Row),
		buildSideIdx: pb.InnerIdx,
	}
	leftCh, err := b.buildMPPExecutor(children[0])
	if err != nil {
		return nil, errors.Trace(err)
	}
	rightCh, err := b.buildMPPExecutor(children[1])
	if err != nil {
		return nil, errors.Trace(err)
	}
	if pb.JoinType == tipb.JoinType_TypeLeftOuterJoin {
		for _, tp := range rightCh.getFieldTypes() {
			tp.Flag &= ^mysql.NotNullFlag
		}
		defaultInner := chunk.MutRowFromTypes(rightCh.getFieldTypes())
		for i := range rightCh.getFieldTypes() {
			defaultInner.SetDatum(i, types.NewDatum(nil))
		}
		e.defaultInner = defaultInner.ToRow()
	} else if pb.JoinType == tipb.JoinType_TypeRightOuterJoin {
		for _, tp := range leftCh.getFieldTypes() {
			tp.Flag &= ^mysql.NotNullFlag
		}
		defaultInner := chunk.MutRowFromTypes(leftCh.getFieldTypes())
		for i := range leftCh.getFieldTypes() {
			defaultInner.SetDatum(i, types.NewDatum(nil))
		}
		e.defaultInner = defaultInner.ToRow()
	}
	// because the field type is immutable, so this kind of appending is safe.
	e.fieldTypes = append(leftCh.getFieldTypes(), rightCh.getFieldTypes()...)
	if pb.InnerIdx == 1 {
		e.probeChild = leftCh
		e.buildChild = rightCh
		probeExpr, err := expression.PBToExpr(pb.LeftJoinKeys[0], leftCh.getFieldTypes(), b.sc)
		if err != nil {
			return nil, errors.Trace(err)
		}
		e.probeKey = probeExpr.(*expression.Column)
		buildExpr, err := expression.PBToExpr(pb.RightJoinKeys[0], rightCh.getFieldTypes(), b.sc)
		if err != nil {
			return nil, errors.Trace(err)
		}
		e.buildKey = buildExpr.(*expression.Column)
	} else {
		e.probeChild = rightCh
		e.buildChild = leftCh
		buildExpr, err := expression.PBToExpr(pb.LeftJoinKeys[0], leftCh.getFieldTypes(), b.sc)
		if err != nil {
			return nil, errors.Trace(err)
		}
		e.buildKey = buildExpr.(*expression.Column)
		probeExpr, err := expression.PBToExpr(pb.RightJoinKeys[0], rightCh.getFieldTypes(), b.sc)
		if err != nil {
			return nil, errors.Trace(err)
		}
		e.probeKey = probeExpr.(*expression.Column)
	}
	return e, nil
}

func (b *mppExecBuilder) buildMPPProj(proj *tipb.Projection) (*projExec, error) {
	e := &projExec{}

	chExec, err := b.buildMPPExecutor(proj.Child)
	if err != nil {
		return nil, errors.Trace(err)
	}
	e.children = []mppExec{chExec}

	for _, pbExpr := range proj.Exprs {
		expr, err := expression.PBToExpr(pbExpr, chExec.getFieldTypes(), b.sc)
		if err != nil {
			return nil, errors.Trace(err)
		}
		e.exprs = append(e.exprs, expr)
		e.fieldTypes = append(e.fieldTypes, expr.GetType())
	}
	return e, nil
}

func (b *mppExecBuilder) buildMPPSel(sel *tipb.Selection) (*selExec, error) {
	chExec, err := b.buildMPPExecutor(sel.Child)
	if err != nil {
		return nil, errors.Trace(err)
	}
	e := &selExec{
		baseMPPExec: baseMPPExec{
			fieldTypes: chExec.getFieldTypes(),
			sc:         b.sc,
			mppCtx:     b.mppCtx,
			children:   []mppExec{chExec},
		},
	}

	for _, pbExpr := range sel.Conditions {
		expr, err := expression.PBToExpr(pbExpr, chExec.getFieldTypes(), b.sc)
		if err != nil {
			return nil, errors.Trace(err)
		}
		e.conditions = append(e.conditions, expr)
	}
	return e, nil
}

func (b *mppExecBuilder) buildMPPAgg(agg *tipb.Aggregation) (*aggExec, error) {
	e := &aggExec{
		groups:     make(map[string]struct{}),
		aggCtxsMap: make(map[string][]*aggregation.AggEvaluateContext),
		processed:  false,
	}

	chExec, err := b.buildMPPExecutor(agg.Child)
	if err != nil {
		return nil, errors.Trace(err)
	}
	e.children = []mppExec{chExec}
	for _, aggFunc := range agg.AggFunc {
		ft := expression.PbTypeToFieldType(aggFunc.FieldType)
		e.fieldTypes = append(e.fieldTypes, ft)
		aggExpr, err := aggregation.NewDistAggFunc(aggFunc, chExec.getFieldTypes(), b.sc)
		if err != nil {
			return nil, errors.Trace(err)
		}
		e.aggExprs = append(e.aggExprs, aggExpr)
	}
	e.sc = b.sc

	for _, gby := range agg.GroupBy {
		ft := expression.PbTypeToFieldType(gby.FieldType)
		e.fieldTypes = append(e.fieldTypes, ft)
		e.groupByTypes = append(e.groupByTypes, ft)
		gbyExpr, err := expression.PBToExpr(gby, chExec.getFieldTypes(), b.sc)
		if err != nil {
			return nil, errors.Trace(err)
		}
		e.groupByExprs = append(e.groupByExprs, gbyExpr)
	}
	return e, nil
}

func (b *mppExecBuilder) buildMPPExecutor(exec *tipb.Executor) (mppExec, error) {
	switch exec.Tp {
	case tipb.ExecType_TypeTableScan:
		ts := exec.TblScan
		return b.buildMPPTableScan(ts)
	case tipb.ExecType_TypeExchangeReceiver:
		rec := exec.ExchangeReceiver
		return b.buildMPPExchangeReceiver(rec)
	case tipb.ExecType_TypeExchangeSender:
		send := exec.ExchangeSender
		return b.buildMPPExchangeSender(send)
	case tipb.ExecType_TypeJoin:
		join := exec.Join
		return b.buildMPPJoin(join, join.Children)
	case tipb.ExecType_TypeAggregation:
		agg := exec.Aggregation
		return b.buildMPPAgg(agg)
	case tipb.ExecType_TypeProjection:
		return b.buildMPPProj(exec.Projection)
	case tipb.ExecType_TypeSelection:
		return b.buildMPPSel(exec.Selection)
	default:
		return nil, errors.Errorf("Do not support executor %s", exec.Tp.String())
	}
}

// HandleMPPDAGReq handles a cop request that is converted from mpp request.
// It returns nothing. Real data will return by stream rpc.
func HandleMPPDAGReq(dbReader *dbreader.DBReader, req *coprocessor.Request, mppCtx *MPPCtx) *coprocessor.Response {
	dagReq := new(tipb.DAGRequest)
	err := proto.Unmarshal(req.Data, dagReq)
	if err != nil {
		return &coprocessor.Response{OtherError: err.Error()}
	}
	builder := mppExecBuilder{
		dbReader: dbReader,
		req:      req,
		mppCtx:   mppCtx,
		sc:       flagsToStatementContext(dagReq.Flags),
		dagReq:   dagReq,
	}
	mppExec, err := builder.buildMPPExecutor(dagReq.RootExecutor)
	if err != nil {
		panic("build error: " + err.Error())
	}
	err = mppExec.open()
	if err != nil {
		panic("open phase find error: " + err.Error())
	}
	_, err = mppExec.next()
	if err != nil {
		panic("running phase find error: " + err.Error())
	}
	return &coprocessor.Response{}
}

// MPPTaskHandler exists in a single store.
type MPPTaskHandler struct {
	// When a connect request comes, it contains server task (source) and client task (target), Exchanger dataCh set will find dataCh by client task.
	tunnelSetLock sync.Mutex
	TunnelSet     map[int64]*ExchangerTunnel

	Meta      *mpp.TaskMeta
	RPCClient client.Client

	Status atomic.Int32
	Err    error
}

// HandleEstablishConn handles EstablishMPPConnectionRequest
func (h *MPPTaskHandler) HandleEstablishConn(_ context.Context, req *mpp.EstablishMPPConnectionRequest) (*ExchangerTunnel, error) {
	meta := req.ReceiverMeta
	for i := 0; i < 10; i++ {
		tunnel, err := h.getAndActiveTunnel(req)
		if err == nil {
			return tunnel, nil
		}
		time.Sleep(time.Second)
	}
	return nil, errors.Errorf("cannot find client task %d registered in server task %d", meta.TaskId, req.SenderMeta.TaskId)
}

func (h *MPPTaskHandler) registerTunnel(tunnel *ExchangerTunnel) error {
	taskID := tunnel.targetTask.TaskId
	h.tunnelSetLock.Lock()
	defer h.tunnelSetLock.Unlock()
	_, ok := h.TunnelSet[taskID]
	if ok {
		return errors.Errorf("task id %d has been registered", taskID)
	}
	h.TunnelSet[taskID] = tunnel
	return nil
}

func (h *MPPTaskHandler) getAndActiveTunnel(req *mpp.EstablishMPPConnectionRequest) (*ExchangerTunnel, *mpp.Error) {
	targetID := req.ReceiverMeta.TaskId
	h.tunnelSetLock.Lock()
	defer h.tunnelSetLock.Unlock()
	if tunnel, ok := h.TunnelSet[targetID]; ok {
		close(tunnel.connectedCh)
		return tunnel, nil
	}
	// We dont find this dataCh, may be task not ready or have been deleted.
	return nil, &mpp.Error{Code: MPPErrTunnelNotFound, Msg: "task not found, please wait for a while"}
}

// ExchangerTunnel contains a channel that can transfer data.
// Only One Sender and Receiver use this channel, so it's safe to close it by sender.
type ExchangerTunnel struct {
	DataCh chan *tipb.Chunk

	sourceTask *mpp.TaskMeta // source task is nearer to the data source
	targetTask *mpp.TaskMeta // target task is nearer to the client end , as tidb.

	connectedCh chan struct{}
	ErrCh       chan error
}

// RecvChunk recive tipb chunk
func (tunnel *ExchangerTunnel) RecvChunk() (tipbChunk *tipb.Chunk, err error) {
	tipbChunk = <-tunnel.DataCh
	select {
	case err = <-tunnel.ErrCh:
	default:
	}
	return tipbChunk, err
}
