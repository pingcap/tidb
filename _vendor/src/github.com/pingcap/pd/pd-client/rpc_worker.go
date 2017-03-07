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

package pd

import (
	"bufio"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/kvproto/pkg/msgpb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/kvproto/pkg/util"
	"github.com/pingcap/pd/pkg/metricutil"
	"github.com/twinj/uuid"
)

const (
	maxPipelineRequest    = 10000
	maxInitClusterRetries = 300
)

// errInvalidResponse represents response message is invalid.
var errInvalidResponse = errors.New("invalid response")

type tsoRequest struct {
	done     chan error
	physical int64
	logical  int64
}

type storeRequest struct {
	pbReq  *pdpb.GetStoreRequest
	done   chan error
	pbResp *pdpb.GetStoreResponse
}

type regionRequest struct {
	pbReq  *pdpb.GetRegionRequest
	done   chan error
	pbResp *pdpb.GetRegionResponse
}

type regionByIDRequest struct {
	pbReq  *pdpb.GetRegionByIDRequest
	done   chan error
	pbResp *pdpb.GetRegionResponse
}

type clusterConfigRequest struct {
	pbReq  *pdpb.GetClusterConfigRequest
	done   chan error
	pbResp *pdpb.GetClusterConfigResponse
}

type rpcWorker struct {
	urls      []string
	clusterID uint64
	requests  chan interface{}
	wg        sync.WaitGroup
	quit      chan struct{}
}

func newRPCWorker(addrs []string) (*rpcWorker, error) {
	w := &rpcWorker{
		urls:     addrsToUrls(addrs),
		requests: make(chan interface{}, maxPipelineRequest),
		quit:     make(chan struct{}),
	}

	if err := w.initClusterID(); err != nil {
		return nil, errors.Trace(err)
	}
	log.Infof("[pd] init cluster id %v", w.clusterID)

	w.wg.Add(1)
	go w.work()
	return w, nil
}

func (w *rpcWorker) stop(err error) {
	close(w.quit)
	w.wg.Wait()

	n := len(w.requests)
	for i := 0; i < n; i++ {
		req := <-w.requests
		switch r := req.(type) {
		case *tsoRequest:
			r.done <- err
		case *clusterConfigRequest:
			r.done <- err
		case *storeRequest:
			r.done <- err
		case *regionRequest:
			r.done <- err
		}
	}
}

func (w *rpcWorker) work() {
	defer w.wg.Done()

RECONNECT:
	log.Infof("[pd] connect to pd server %v", w.urls)
	conn := mustNewConn(w.urls, w.quit)
	if conn == nil {
		return // Closed.
	}
	log.Infof("[pd] connected to %v", conn.RemoteAddr())

	for {
		var pending []interface{}
		select {
		case req := <-w.requests:
			pending = append(pending, req)
		POP_ALL:
			for {
				select {
				case req := <-w.requests:
					pending = append(pending, req)
				default:
					break POP_ALL
				}
			}
			if ok := w.handleRequests(pending, conn.ReadWriter); !ok {
				conn.Close()
				goto RECONNECT
			}
		case <-w.quit:
			conn.Close()
			return
		case leaderConn := <-conn.ConnChan:
			conn.Close()
			conn = leaderConn
			log.Infof("[pd] reconnected to leader %v", conn.RemoteAddr())
		}
	}
}

func (w *rpcWorker) handleRequests(requests []interface{}, conn *bufio.ReadWriter) bool {
	var tsoRequests []*tsoRequest
	ok := true
	for _, req := range requests {
		switch r := req.(type) {
		case *tsoRequest:
			tsoRequests = append(tsoRequests, r)
		case *storeRequest:
			storeResp, err := w.getStoreFromRemote(conn, r.pbReq)
			if err != nil {
				ok = false
				log.Error(err)
				r.done <- err
			} else {
				r.pbResp = storeResp
				r.done <- nil
			}
		case *regionRequest:
			regionResp, err := w.getRegionFromRemote(conn, r.pbReq)
			if err != nil {
				ok = false
				log.Error(err)
				r.done <- err
			} else {
				r.pbResp = regionResp
				r.done <- nil
			}
		case *regionByIDRequest:
			regionResp, err := w.getRegionByIDFromRemote(conn, r.pbReq)
			if err != nil {
				ok = false
				log.Error(err)
				r.done <- err
			} else {
				r.pbResp = regionResp
				r.done <- nil
			}

		case *clusterConfigRequest:
			clusterConfigResp, err := w.getClusterConfigFromRemote(conn, r.pbReq)
			if err != nil {
				ok = false
				log.Error(err)
				r.done <- err
			} else {
				r.pbResp = clusterConfigResp
				r.done <- nil
			}
		default:
			log.Errorf("[pd] invalid request %v", r)
		}
	}

	// Check whether we have tso requests to process.
	if len(tsoRequests) > 0 {
		ts, err := w.getTSFromRemote(conn, len(tsoRequests))
		if err != nil {
			ok = false
			log.Error(err)
		}
		logicalHigh := ts.GetLogical()
		physical := ts.GetPhysical()
		for _, req := range tsoRequests {
			if err != nil {
				req.done <- err
			} else {
				req.physical = physical
				req.logical = logicalHigh
				req.done <- nil
				logicalHigh--
			}
		}
	}

	return ok
}

var msgID uint64

func newMsgID() uint64 {
	return atomic.AddUint64(&msgID, 1)
}

func (w *rpcWorker) initClusterID() error {
	for i := 0; i < maxInitClusterRetries; i++ {
		conn := mustNewConn(w.urls, w.quit)
		if conn == nil {
			return errors.New("client closed")
		}

		clusterID, err := w.getClusterID(conn.ReadWriter)
		// We need to close this connection no matter success or not.
		conn.Close()

		if err == nil {
			w.clusterID = clusterID
			return nil
		}

		log.Errorf("[pd] failed to get cluster id: %v", err)
		time.Sleep(time.Second)
	}

	return errors.New("failed to get cluster id")
}

func (w *rpcWorker) getClusterID(conn *bufio.ReadWriter) (uint64, error) {
	// PD will not check the cluster ID in the GetPDMembersRequest, so we
	// can send this request with any cluster ID, then PD will return its
	// cluster ID in the response header.
	req := &pdpb.Request{
		Header: &pdpb.RequestHeader{
			Uuid:      uuid.NewV4().Bytes(),
			ClusterId: w.clusterID,
		},
		CmdType:      pdpb.CommandType_GetPDMembers,
		GetPdMembers: &pdpb.GetPDMembersRequest{},
	}

	resp, err := w.callRPC(conn, req)
	if err != nil {
		return 0, errors.Trace(err)
	}

	return resp.GetHeader().GetClusterId(), nil
}

func (w *rpcWorker) getTSFromRemote(conn *bufio.ReadWriter, n int) (pdpb.Timestamp, error) {
	var timestampHigh = pdpb.Timestamp{}
	req := &pdpb.Request{
		Header: &pdpb.RequestHeader{
			Uuid:      uuid.NewV4().Bytes(),
			ClusterId: w.clusterID,
		},
		CmdType: pdpb.CommandType_Tso,
		Tso: &pdpb.TsoRequest{
			Count: uint32(n),
		},
	}
	resp, err := w.callRPC(conn, req)
	if err != nil {
		return timestampHigh, errors.Trace(err)
	}
	if resp.GetTso() == nil {
		return timestampHigh, errors.New("[pd] tso filed in rpc response not set")
	}
	timestampHigh = resp.GetTso().GetTimestamp()
	if resp.GetTso().GetCount() != uint32(n) {
		return timestampHigh, errors.New("[pd] tso length in rpc response is incorrect")
	}
	return timestampHigh, nil
}

func (w *rpcWorker) getStoreFromRemote(conn *bufio.ReadWriter, storeReq *pdpb.GetStoreRequest) (*pdpb.GetStoreResponse, error) {
	req := &pdpb.Request{
		Header: &pdpb.RequestHeader{
			Uuid:      uuid.NewV4().Bytes(),
			ClusterId: w.clusterID,
		},
		CmdType:  pdpb.CommandType_GetStore,
		GetStore: storeReq,
	}
	resp, err := w.callRPC(conn, req)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if resp.GetGetStore() == nil {
		return nil, errors.New("[pd] GetStore filed in rpc response not set")
	}
	return resp.GetGetStore(), nil
}

func (w *rpcWorker) getRegionFromRemote(conn *bufio.ReadWriter, regionReq *pdpb.GetRegionRequest) (*pdpb.GetRegionResponse, error) {
	req := &pdpb.Request{
		Header: &pdpb.RequestHeader{
			Uuid:      uuid.NewV4().Bytes(),
			ClusterId: w.clusterID,
		},
		CmdType:   pdpb.CommandType_GetRegion,
		GetRegion: regionReq,
	}
	rsp, err := w.callRPC(conn, req)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if rsp.GetGetRegion() == nil {
		return nil, errors.New("[pd] GetRegion field in rpc response not set")
	}
	return rsp.GetGetRegion(), nil
}

func (w *rpcWorker) getRegionByIDFromRemote(conn *bufio.ReadWriter, regionReq *pdpb.GetRegionByIDRequest) (*pdpb.GetRegionResponse, error) {
	req := &pdpb.Request{
		Header: &pdpb.RequestHeader{
			Uuid:      uuid.NewV4().Bytes(),
			ClusterId: w.clusterID,
		},
		CmdType:       pdpb.CommandType_GetRegionByID,
		GetRegionById: regionReq,
	}
	rsp, err := w.callRPC(conn, req)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if rsp.GetGetRegionById() == nil {
		return nil, errors.New("[pd] GetRegion field in rpc response not set")
	}
	return rsp.GetGetRegionById(), nil
}

func (w *rpcWorker) getClusterConfigFromRemote(conn *bufio.ReadWriter, clusterConfigReq *pdpb.GetClusterConfigRequest) (*pdpb.GetClusterConfigResponse, error) {
	req := &pdpb.Request{
		Header: &pdpb.RequestHeader{
			Uuid:      uuid.NewV4().Bytes(),
			ClusterId: w.clusterID,
		},
		CmdType:          pdpb.CommandType_GetClusterConfig,
		GetClusterConfig: clusterConfigReq,
	}
	rsp, err := w.callRPC(conn, req)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if rsp.GetGetClusterConfig() == nil {
		return nil, errors.New("[pd] GetClusterConfig field in rpc response not set")
	}
	return rsp.GetGetClusterConfig(), nil
}

func (w *rpcWorker) callRPC(conn *bufio.ReadWriter, req *pdpb.Request) (resp *pdpb.Response, err error) {
	// Record some metrics.
	start := time.Now()
	label := metricutil.GetCmdLabel(req)
	defer func() {
		if err == nil {
			cmdCounter.WithLabelValues(label).Inc()
			cmdDuration.WithLabelValues(label).Observe(time.Since(start).Seconds())
		} else {
			cmdFailedCounter.WithLabelValues(label).Inc()
			cmdFailedDuration.WithLabelValues(label).Observe(time.Since(start).Seconds())
		}
	}()

	msg := &msgpb.Message{
		MsgType: msgpb.MessageType_PdReq,
		PdReq:   req,
	}
	if err = util.WriteMessage(conn, newMsgID(), msg); err != nil {
		return nil, errors.Errorf("[pd] rpc failed: %v", err)
	}
	conn.Flush()
	if _, err = util.ReadMessage(conn, msg); err != nil {
		return nil, errors.Errorf("[pd] rpc failed: %v", err)
	}
	if msg.GetMsgType() != msgpb.MessageType_PdResp {
		return nil, errors.Trace(errInvalidResponse)
	}
	resp = msg.GetPdResp()
	if err = w.checkResponse(resp); err != nil {
		return nil, errors.Trace(err)
	}

	return resp, nil
}

func (w *rpcWorker) checkResponse(resp *pdpb.Response) error {
	header := resp.GetHeader()
	if header == nil {
		return errors.New("[pd] rpc response header not set")
	}
	if err := header.GetError(); err != nil {
		return errors.Errorf("[pd] rpc response with error: %v", err)
	}
	return nil
}

func addrsToUrls(addrs []string) []string {
	// Add default schema "http://" to addrs.
	urls := make([]string, 0, len(addrs))
	for _, addr := range addrs {
		if strings.Contains(addr, "://") {
			urls = append(urls, addr)
		} else {
			urls = append(urls, "http://"+addr)
		}
	}
	return urls
}
