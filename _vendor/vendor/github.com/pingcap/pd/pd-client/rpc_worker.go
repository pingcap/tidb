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
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/juju/errors"
	"github.com/ngaut/deadline"
	"github.com/ngaut/log"
	"github.com/pingcap/kvproto/pkg/msgpb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/kvproto/pkg/util"
	"github.com/twinj/uuid"
)

const (
	pdRPCPrefix      = "/pd/rpc"
	connectPDTimeout = time.Second * 3
	netIOTimeout     = time.Second
)

const (
	readBufferSize  = 8 * 1024
	writeBufferSize = 8 * 1024
)

const maxPipelineRequest = 10000

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

type clusterConfigRequest struct {
	pbReq  *pdpb.GetClusterConfigRequest
	done   chan error
	pbResp *pdpb.GetClusterConfigResponse
}

type rpcWorker struct {
	addr      string
	clusterID uint64
	requests  chan interface{}
	wg        sync.WaitGroup
	quit      chan struct{}
}

func newRPCWorker(addr string, clusterID uint64) *rpcWorker {
	w := &rpcWorker{
		addr:      addr,
		clusterID: clusterID,
		requests:  make(chan interface{}, maxPipelineRequest),
		quit:      make(chan struct{}),
	}
	w.wg.Add(1)
	go w.work()
	return w
}

func (w *rpcWorker) stop(err error) {
	close(w.quit)
	w.wg.Wait()

	for i := 0; i < len(w.requests); i++ {
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
	log.Infof("[pd] connect to pd server %v", w.addr)
	conn, err := rpcConnect(w.addr)
	if err != nil {
		log.Warnf("[pd] failed connect pd server: %v, will retry later", err)

		select {
		case <-time.After(netIOTimeout):
			goto RECONNECT
		case <-w.quit:
			return
		}
	}

	reader := bufio.NewReaderSize(deadline.NewDeadlineReader(conn, netIOTimeout), readBufferSize)
	writer := bufio.NewWriterSize(deadline.NewDeadlineWriter(conn, netIOTimeout), writeBufferSize)
	readwriter := bufio.NewReadWriter(reader, writer)

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
			if ok := w.handleRequests(pending, readwriter); !ok {
				conn.Close()
				goto RECONNECT
			}
		case <-w.quit:
			conn.Close()
			return
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

func (w *rpcWorker) callRPC(conn *bufio.ReadWriter, req *pdpb.Request) (*pdpb.Response, error) {
	msg := &msgpb.Message{
		MsgType: msgpb.MessageType_PdReq,
		PdReq:   req,
	}
	if err := util.WriteMessage(conn, newMsgID(), msg); err != nil {
		return nil, errors.Errorf("[pd] rpc failed: %v", err)
	}
	conn.Flush()
	if _, err := util.ReadMessage(conn, msg); err != nil {
		return nil, errors.Errorf("[pd] rpc failed: %v", err)
	}
	if msg.GetMsgType() != msgpb.MessageType_PdResp {
		return nil, errors.Trace(errInvalidResponse)
	}
	resp := msg.GetPdResp()
	if err := w.checkResponse(resp); err != nil {
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

func parseUrls(s string) ([]url.URL, error) {
	items := strings.Split(s, ",")
	urls := make([]url.URL, 0, len(items))
	for _, item := range items {
		u, err := url.Parse(item)
		if err != nil {
			return nil, errors.Trace(err)
		}

		urls = append(urls, *u)
	}

	return urls, nil
}

func rpcConnect(addr string) (net.Conn, error) {
	req, err := http.NewRequest("GET", pdRPCPrefix, nil)
	if err != nil {
		return nil, errors.Trace(err)
	}

	urls, err := parseUrls(addr)
	if err != nil {
		return nil, errors.Trace(err)
	}

	for _, url := range urls {
		var conn net.Conn
		switch url.Scheme {
		// used in tests
		case "unix", "unixs":
			conn, err = net.DialTimeout("unix", url.Host, connectPDTimeout)
		default:
			conn, err = net.DialTimeout("tcp", url.Host, connectPDTimeout)
		}

		if err != nil {
			continue
		}
		err = req.Write(conn)
		if err != nil {
			conn.Close()
			continue
		}
		return conn, nil
	}

	return nil, errors.Errorf("connect to %s failed", addr)
}
