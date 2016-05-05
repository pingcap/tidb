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
	"net"
	"sync/atomic"
	"time"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/kvproto/pkg/coprocessor"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/msgpb"
	"github.com/pingcap/kvproto/pkg/util"
	"gopkg.in/fatih/pool.v2"
)

// Client is a client that sends RPC.
// It should not be used after calling Close().
type Client interface {
	// Close should release all data.
	Close() error
	// SendKVReq sends kv request.
	SendKVReq(req *kvrpcpb.Request) (*kvrpcpb.Response, error)
	// SendCopReq sends coprocessor request.
	SendCopReq(req *coprocessor.Request) (*coprocessor.Response, error)
}

// ClientFactory is a function that creates a Client with server address.
type ClientFactory func(string) (Client, error)

// rpcClient connects kvserver to send Request by TCP.
type rpcClient struct {
	dst   string
	msgID uint64
	pool  pool.Pool
}

const (
	initConnection int           = 0
	maxConnecion   int           = 20
	readTimeout    time.Duration = 5 * time.Second // seconds
	writeTimeout   time.Duration = 5 * time.Second // seconds
	connectTimeout time.Duration = 5 * time.Second // seconds
)

// rpcBackoff is for RPC (with TiKV) retry.
// It is expected to sleep for about 10s(+/-3s) in total before abort.
func rpcBackoff() func() error {
	const (
		maxRetry  = 10
		sleepBase = 100
		sleepCap  = 2000
	)
	return NewBackoff(maxRetry, sleepBase, sleepCap, EqualJitter)
}

// NewRPCClient new client that sends protobuf to do RPC.
func NewRPCClient(srvHost string) (Client, error) {
	factory := func() (net.Conn, error) {
		conn, err := net.DialTimeout("tcp", srvHost, connectTimeout)
		if err != nil {
			return nil, errors.Trace(err)
		}
		return conn, nil
	}
	p, err := pool.NewChannelPool(initConnection, maxConnecion, factory)
	if err != nil {
		return nil, errors.Errorf("new channel pool failed err[%s]", err)
	}
	return &rpcClient{
		dst:   srvHost,
		msgID: 0,
		pool:  p,
	}, nil
}

// SendCopReq sends a Request to co-processor and receives Response.
func (c *rpcClient) SendCopReq(req *coprocessor.Request) (*coprocessor.Response, error) {
	conn, err := c.pool.Get()
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer conn.Close()
	msg := msgpb.Message{
		MsgType: msgpb.MessageType_CopReq.Enum(),
		CopReq:  req,
	}
	err = c.doSend(conn, &msg)
	if err == nil {
		if msg.GetMsgType() != msgpb.MessageType_CopResp || msg.GetCopResp() == nil {
			err = errors.Trace(errInvalidResponse)
		}
	}
	if err != nil {
		// This connection is not valid any more, so close its underlying conn.
		if poolConn, ok := conn.(*pool.PoolConn); ok {
			poolConn.MarkUnusable()
		}
		return nil, errors.Trace(err)
	}
	return msg.GetCopResp(), nil
}

// SendKVReq sends a Request to kv server and receives Response.
func (c *rpcClient) SendKVReq(req *kvrpcpb.Request) (*kvrpcpb.Response, error) {
	conn, err := c.pool.Get()
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer conn.Close()
	msg := msgpb.Message{
		MsgType: msgpb.MessageType_KvReq.Enum(),
		KvReq:   req,
	}
	err = c.doSend(conn, &msg)
	if err == nil {
		if msg.GetMsgType() != msgpb.MessageType_KvResp || msg.GetKvResp() == nil {
			err = errors.Trace(errInvalidResponse)
		}
	}
	if err != nil {
		// This connection is not valid any more, so close its underlying conn.
		if poolConn, ok := conn.(*pool.PoolConn); ok {
			poolConn.MarkUnusable()
		}
		return nil, errors.Trace(err)
	}
	return msg.GetKvResp(), nil
}

// Close close client.
func (c *rpcClient) Close() error {
	c.pool.Close()
	return nil
}

func (c *rpcClient) doSend(conn net.Conn, msg *msgpb.Message) error {
	curMsgID := atomic.AddUint64(&c.msgID, 1)
	log.Debugf("Send request msgID[%d] type[%v]", curMsgID, msg.GetMsgType())
	if err := conn.SetWriteDeadline(time.Now().Add(writeTimeout)); err != nil {
		log.Warn("Set write deadline failed, it may be blocked.")
	}
	if err := util.WriteMessage(conn, curMsgID, msg); err != nil {
		return errors.Trace(err)
	}
	if err := conn.SetReadDeadline(time.Now().Add(readTimeout)); err != nil {
		log.Warn("Set read deadline failed, it may be blocked.")
	}
	msgID, err := util.ReadMessage(conn, msg)
	if err != nil {
		return errors.Trace(err)
	}
	if curMsgID != msgID {
		log.Errorf("Sent msgID[%d] mismatches recv msgID[%d]", curMsgID, msgID)
	}
	log.Debugf("Receive response msgID[%d] type[%v]", msgID, msg.GetMsgType())
	return nil
}
