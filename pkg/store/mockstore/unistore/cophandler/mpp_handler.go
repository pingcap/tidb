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
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/coprocessor"
	"github.com/pingcap/kvproto/pkg/mpp"
	"github.com/pingcap/tidb/pkg/store/mockstore/unistore/client"
	"github.com/pingcap/tidb/pkg/store/mockstore/unistore/tikv/dbreader"
	"github.com/pingcap/tidb/pkg/util/timeutil"
	"github.com/pingcap/tipb/go-tipb"
)

func HandleMPPDAGReq(dbReader *dbreader.DBReader, req *coprocessor.Request, mppCtx *MPPCtx) *coprocessor.Response {
	dagReq := new(tipb.DAGRequest)
	err := proto.Unmarshal(req.Data, dagReq)
	if err != nil {
		return &coprocessor.Response{OtherError: err.Error()}
	}
	dagCtx := &dagContext{
		dbReader:  dbReader,
		startTS:   req.StartTs,
		keyRanges: req.Ranges,
	}
	if reqCtx := req.Context; reqCtx != nil {
		dagCtx.keyspaceID = reqCtx.KeyspaceId
	}
	tz, err := timeutil.ConstructTimeZone(dagReq.TimeZoneName, int(dagReq.TimeZoneOffset))
	builder := mppExecBuilder{
		dbReader: dbReader,
		mppCtx:   mppCtx,
		sctx:     flagsAndTzToSessionContext(dagReq.Flags, tz),
		dagReq:   dagReq,
		dagCtx:   dagCtx,
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
	for range 10 {
		tunnel, err := h.getAndActiveTunnel(req)
		if err == nil {
			return tunnel, nil
		}
		if err.Code == MPPErrMPPGatherIDMismatch {
			return nil, errors.New(err.Msg)
		}
		time.Sleep(time.Second)
	}
	return nil, errors.Errorf("cannot find client task %d registered in server task %d", meta.TaskId, req.SenderMeta.TaskId)
}

func (h *MPPTaskHandler) registerTunnel(tunnel *ExchangerTunnel) error {
	if h.Meta.GatherId != tunnel.sourceTask.GatherId {
		return errors.Errorf("mpp gather id mismatch, maybe a bug in MPP coordinator")
	}
	if h.Meta.GatherId != tunnel.targetTask.GatherId {
		return errors.Errorf("mpp gather id mismatch, maybe a bug in MPP coordinator")
	}
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
	if h.Meta.GatherId != req.ReceiverMeta.GatherId {
		return nil, &mpp.Error{Code: MPPErrMPPGatherIDMismatch, Msg: "mpp gather id mismatch, maybe a bug in MPP coordinator"}
	}
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

// RecvChunk receive tipb chunk
func (tunnel *ExchangerTunnel) RecvChunk() (tipbChunk *tipb.Chunk, err error) {
	tipbChunk = <-tunnel.DataCh
	select {
	case err = <-tunnel.ErrCh:
	default:
	}
	return tipbChunk, err
}
