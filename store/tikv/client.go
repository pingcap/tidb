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

// Package tikv provides tcp connection to kvserver.
package tikv

import (
	"sync/atomic"
	"time"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/kvproto/pkg/coprocessor"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/msgpb"
	"github.com/pingcap/kvproto/pkg/util"
)

// Client is a client that sends RPC.
// It should not be used after calling Close().
type Client interface {
	// Close should release all data.
	Close() error
	// SendKVReq sends kv request.
	SendKVReq(addr string, req *kvrpcpb.Request, timeout time.Duration) (*kvrpcpb.Response, error)
	// SendCopReq sends coprocessor request.
	SendCopReq(addr string, req *coprocessor.Request, timeout time.Duration) (*coprocessor.Response, error)
}

const (
	maxConnection     = 150
	dialTimeout       = 5 * time.Second
	writeTimeout      = 10 * time.Second
	readTimeoutShort  = 20 * time.Second  // For requests that read/write several key-values.
	readTimeoutMedium = 60 * time.Second  // For requests that may need scan region.
	readTimeoutLong   = 150 * time.Second // For requests that may need scan region multiple times.
)

type rpcClient struct {
	msgID uint64
	p     *Pools
}

func newRPCClient() *rpcClient {
	return &rpcClient{
		msgID: 0,
		p: NewPools(maxConnection, func(addr string) (*Conn, error) {
			return NewConnection(addr, dialTimeout)
		}),
	}
}

// SendCopReq sends a Request to co-processor and receives Response.
func (c *rpcClient) SendCopReq(addr string, req *coprocessor.Request, timeout time.Duration) (*coprocessor.Response, error) {
	start := time.Now()
	defer func() { sendReqHistogram.WithLabelValues("cop").Observe(time.Since(start).Seconds()) }()

	conn, err := c.p.GetConn(addr)
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer c.p.PutConn(conn)
	msg := msgpb.Message{
		MsgType: msgpb.MessageType_CopReq,
		CopReq:  req,
	}
	err = c.doSend(conn, &msg, writeTimeout, timeout)
	if err != nil {
		conn.Close()
		return nil, errors.Trace(err)
	}
	if msg.GetMsgType() != msgpb.MessageType_CopResp || msg.GetCopResp() == nil {
		conn.Close()
		return nil, errors.Trace(errInvalidResponse)
	}
	return msg.GetCopResp(), nil
}

// SendKVReq sends a Request to kv server and receives Response.
func (c *rpcClient) SendKVReq(addr string, req *kvrpcpb.Request, timeout time.Duration) (*kvrpcpb.Response, error) {
	start := time.Now()
	defer func() { sendReqHistogram.WithLabelValues("kv").Observe(time.Since(start).Seconds()) }()

	conn, err := c.p.GetConn(addr)
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer c.p.PutConn(conn)
	msg := msgpb.Message{
		MsgType: msgpb.MessageType_KvReq,
		KvReq:   req,
	}
	err = c.doSend(conn, &msg, writeTimeout, timeout)
	if err != nil {
		conn.Close()
		return nil, errors.Trace(err)
	}
	if msg.GetMsgType() != msgpb.MessageType_KvResp || msg.GetKvResp() == nil {
		conn.Close()
		return nil, errors.Trace(errInvalidResponse)
	}
	return msg.GetKvResp(), nil
}

func (c *rpcClient) doSend(conn *Conn, msg *msgpb.Message, writeTimeout time.Duration, readTimeout time.Duration) error {
	curMsgID := atomic.AddUint64(&c.msgID, 1)
	conn.SetWriteDeadline(time.Now().Add(writeTimeout))
	if err := util.WriteMessage(conn, curMsgID, msg); err != nil {
		return errors.Trace(err)
	}
	if err := conn.Flush(); err != nil {
		return errors.Trace(err)
	}
	conn.SetReadDeadline(time.Now().Add(readTimeout))
	msgID, err := util.ReadMessage(conn.BufioReader(), msg)
	if err != nil {
		return errors.Trace(err)
	}
	if curMsgID != msgID {
		log.Errorf("Sent msgID[%d] mismatches recv msgID[%d]", curMsgID, msgID)
		return errors.Trace(errInvalidResponse)
	}
	return nil
}

func (c *rpcClient) Close() error {
	c.p.Close()
	return nil
}
