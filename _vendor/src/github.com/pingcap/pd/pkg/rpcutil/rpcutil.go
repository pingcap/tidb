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

package rpcutil

import (
	"net"
	"net/url"
	"strings"

	"github.com/juju/errors"
	"github.com/pingcap/kvproto/pkg/msgpb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/kvproto/pkg/util"
)

// ParseUrls parses a string into multiple urls.
func ParseUrls(s string) ([]url.URL, error) {
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

// Call sends the request to conn and wait for the response.
func Call(conn net.Conn, reqID uint64, request *pdpb.Request) (*pdpb.Response, error) {
	req := &msgpb.Message{
		MsgType: msgpb.MessageType_PdReq,
		PdReq:   request,
	}
	if err := util.WriteMessage(conn, reqID, req); err != nil {
		return nil, errors.Trace(err)
	}
	resp := &msgpb.Message{}
	respID, err := util.ReadMessage(conn, resp)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if respID != reqID {
		return nil, errors.Errorf("message id mismatch: reqID %d respID %d", reqID, respID)
	}
	return resp.GetPdResp(), nil
}

// Request connects to urls, then sends the request and wait for the response.
func Request(urls string, reqID uint64, request *pdpb.Request) (*pdpb.Response, error) {
	conn, err := ConnectUrls(urls, 0)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return Call(conn, reqID, request)
}
