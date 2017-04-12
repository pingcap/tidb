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

package tikv

import (
	"net"
	"testing"

	"github.com/ngaut/log"
	. "github.com/pingcap/check"
	pb "github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/msgpb"
	"github.com/pingcap/kvproto/pkg/util"
	goctx "golang.org/x/net/context"
)

func TestT(t *testing.T) {
	CustomVerboseFlag = true
	TestingT(t)
}

var _ = Suite(&testClientSuite{})

type testClientSuite struct {
}

// handleRequest receive Request then send empty Response back fill with same Type.
func handleRequest(conn net.Conn, c *C) {
	c.Assert(conn, NotNil)
	defer conn.Close()
	var msg msgpb.Message
	msgID, err := util.ReadMessage(conn, &msg)
	c.Assert(err, IsNil)
	c.Assert(msgID, Greater, uint64(0))
	c.Assert(msg.GetMsgType(), Equals, msgpb.MessageType_KvReq)

	req := msg.GetKvReq()
	c.Assert(req, NotNil)
	var resp pb.Response
	resp.Type = req.Type
	msg = msgpb.Message{
		MsgType: msgpb.MessageType_KvResp,
		KvResp:  &resp,
	}
	err = util.WriteMessage(conn, msgID, &msg)
	c.Assert(err, IsNil)
}

// One normally `Send`.
func (s *testClientSuite) TestSendBySelf(c *C) {
	l := startServer(":61234", c, handleRequest)
	defer l.Close()
	cli := newRPCClient()
	req := new(pb.Request)
	req.Type = pb.MessageType_CmdGet
	getReq := new(pb.CmdGetRequest)
	getReq.Key = []byte("a")
	ver := uint64(0)
	getReq.Version = ver
	req.CmdGetReq = getReq
	resp, err := cli.SendKVReq(goctx.Background(), ":61234", req, readTimeoutShort)
	c.Assert(err, IsNil)
	c.Assert(req.GetType(), Equals, resp.GetType())
}

func closeRequest(conn net.Conn, c *C) {
	c.Assert(conn, NotNil)
	err := conn.Close()
	c.Assert(err, IsNil)
}

// Server close connection directly if new connection is coming.
func (s *testClientSuite) TestRetryClose(c *C) {
	l := startServer(":61235", c, closeRequest)
	defer l.Close()
	cli := newRPCClient()
	req := new(pb.Request)
	resp, err := cli.SendKVReq(goctx.Background(), ":61235", req, readTimeoutShort)
	c.Assert(err, NotNil)
	c.Assert(resp, IsNil)
}

func readThenCloseRequest(conn net.Conn, c *C) {
	c.Assert(conn, NotNil)
	defer conn.Close()
	var msg msgpb.Message
	msgID, err := util.ReadMessage(conn, &msg)
	c.Assert(err, IsNil)
	c.Assert(msg.GetKvReq(), NotNil)
	c.Assert(msgID, Greater, uint64(0))
}

// Server read message then close, so `Send` will return retry error.
func (s *testClientSuite) TestRetryReadThenClose(c *C) {
	l := startServer(":61236", c, readThenCloseRequest)
	defer l.Close()
	cli := newRPCClient()
	req := new(pb.Request)
	req.Type = pb.MessageType_CmdGet
	resp, err := cli.SendKVReq(goctx.Background(), ":61236", req, readTimeoutShort)
	c.Assert(err, NotNil)
	c.Assert(resp, IsNil)
}

func (s *testClientSuite) TestWrongMessageID(c *C) {
	l := startServer(":61237", c, func(conn net.Conn, c *C) {
		var msg msgpb.Message
		msgID, err := util.ReadMessage(conn, &msg)
		c.Assert(err, IsNil)
		resp := msgpb.Message{
			MsgType: msgpb.MessageType_KvResp,
			KvResp: &pb.Response{
				Type: msg.GetKvReq().GetType(),
			},
		}
		// Send the request back to client, set wrong msgID for the 1st
		// request.
		if msgID == 1 {
			err = util.WriteMessage(conn, msgID+100, &resp)
		} else {
			err = util.WriteMessage(conn, msgID, &resp)
		}
		c.Assert(err, IsNil)
	})
	defer l.Close()
	cli := newRPCClient()
	req := &pb.Request{
		Type: pb.MessageType_CmdGet,
	}
	// Wrong ID for the first request, correct for the rests.
	_, err := cli.SendKVReq(goctx.Background(), ":61237", req, readTimeoutShort)
	c.Assert(err, NotNil)
	resp, err := cli.SendKVReq(goctx.Background(), ":61237", req, readTimeoutShort)
	c.Assert(err, IsNil)
	c.Assert(resp.GetType(), Equals, req.GetType())
}

func startServer(host string, c *C, handleFunc func(net.Conn, *C)) net.Listener {
	l, err := net.Listen("tcp", host)
	c.Assert(err, IsNil)
	log.Debug("Start listenning on", host)
	go func() {
		for {
			conn, err := l.Accept()
			if err != nil {
				return
			}
			go handleFunc(conn, c)
		}
	}()
	return l
}
