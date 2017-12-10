// Copyright (c) 2017 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package testutils

import (
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUDPTransport(t *testing.T) {
	server, err := NewTUDPServerTransport("127.0.0.1:0")
	require.NoError(t, err)
	defer server.Close()

	assert.NoError(t, server.Open())
	assert.True(t, server.IsOpen())
	assert.NotNil(t, server.Conn())

	c := make(chan []byte)
	defer close(c)

	go serveOnce(t, server, c)

	destAddr, err := net.ResolveUDPAddr("udp", server.Addr().String())
	require.NoError(t, err)

	connUDP, err := net.DialUDP(destAddr.Network(), nil, destAddr)
	require.NoError(t, err)
	defer connUDP.Close()

	n, err := connUDP.Write([]byte("test"))
	assert.NoError(t, err)
	assert.Equal(t, 4, n)

	select {
	case data := <-c:
		assert.Equal(t, "test", string(data))
	case <-time.After(time.Second * 1):
		t.Error("Server did not respond in time")
	}
}

func serveOnce(t *testing.T, transport *TUDPTransport, c chan []byte) {
	b := make([]byte, 65000, 65000)
	n, err := transport.Read(b)
	if err == nil {
		c <- b[:n]
	} else {
		panic("Server failed to read: " + err.Error())
	}
}
