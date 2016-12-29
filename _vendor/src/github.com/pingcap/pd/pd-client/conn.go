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
	"strings"
	"sync"
	"time"

	"github.com/juju/errors"
	"github.com/ngaut/deadline"
	"github.com/ngaut/log"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/pd/pkg/apiutil"
	"github.com/pingcap/pd/pkg/rpcutil"
)

const (
	requestPDTimeout   = time.Second
	connectPDTimeout   = time.Second * 3
	reconnectPDTimeout = time.Second * 60
)

const (
	readBufferSize  = 8 * 1024
	writeBufferSize = 8 * 1024
)

type conn struct {
	net.Conn
	wg         sync.WaitGroup
	quit       chan struct{}
	ConnChan   chan *conn
	ReadWriter *bufio.ReadWriter
}

func newConn(c net.Conn) *conn {
	reader := bufio.NewReaderSize(deadline.NewDeadlineReader(c, requestPDTimeout), readBufferSize)
	writer := bufio.NewWriterSize(deadline.NewDeadlineWriter(c, requestPDTimeout), writeBufferSize)
	return &conn{
		Conn:       c,
		quit:       make(chan struct{}),
		ConnChan:   make(chan *conn, 1),
		ReadWriter: bufio.NewReadWriter(reader, writer),
	}
}

func mustNewConn(urls []string, quit chan struct{}) *conn {
	for {
		conn, err := rpcConnectLeader(urls)
		if err == nil {
			return newConn(conn)
		}
		log.Warn(err)

		conn, err = rpcConnect(urls)
		if err == nil {
			c := newConn(conn)
			c.wg.Add(1)
			go c.connectLeader(urls, reconnectPDTimeout)
			return c
		}
		log.Warn(err)

		select {
		case <-time.After(connectPDTimeout):
			break
		case <-quit:
			return nil
		}
	}
}

func (c *conn) Close() {
	c.Conn.Close()
	close(c.quit)
	c.wg.Wait()

	// Close the connection in case it is not used.
	select {
	case conn := <-c.ConnChan:
		conn.Close()
	default:
	}
}

func (c *conn) connectLeader(urls []string, interval time.Duration) {
	defer c.wg.Done()

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			conn, err := rpcConnectLeader(urls)
			if err == nil {
				c.ConnChan <- newConn(conn)
				return
			}
			log.Warn(err)
		case <-c.quit:
			return
		}
	}
}

func getLeader(urls []string) (*pdpb.Leader, error) {
	for _, u := range urls {
		client, err := apiutil.NewClient(u, connectPDTimeout)
		if err != nil {
			continue
		}
		leader, err := client.GetLeader()
		if err != nil {
			continue
		}
		return leader, nil
	}
	return nil, errors.Errorf("failed to get leader from %v", urls)
}

func rpcConnect(urls []string) (net.Conn, error) {
	s := strings.Join(urls, ",")
	return rpcutil.ConnectUrls(s, connectPDTimeout)
}

func rpcConnectLeader(urls []string) (net.Conn, error) {
	leader, err := getLeader(urls)
	if err != nil {
		return nil, errors.Trace(err)
	}
	conn, err := rpcutil.ConnectUrls(leader.GetAddr(), connectPDTimeout)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return conn, nil
}
