// Copyright (c) 2016 Uber Technologies, Inc.
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

package client

import (
	"fmt"
	"net"
	"net/http"
	"sync"

	"github.com/crossdock/crossdock-go"

	"github.com/uber/jaeger-client-go/crossdock/common"
)

// Client is a controller for the tests
type Client struct {
	ClientHostPort     string
	ServerPortHTTP     string
	ServerPortTChannel string
	listener           net.Listener
	hostMapper         func(service string) string
}

// Start begins a blocking Crossdock client
func (c *Client) Start() error {
	if err := c.Listen(); err != nil {
		return err
	}
	return c.Serve()
}

// AsyncStart begins a Crossdock client in the background,
// but does not return until it started serving.
func (c *Client) AsyncStart() error {
	if err := c.Listen(); err != nil {
		return err
	}
	var started sync.WaitGroup
	started.Add(1)
	go func() {
		started.Done()
		c.Serve()
	}()
	started.Wait()
	return nil
}

// Listen initializes the server
func (c *Client) Listen() error {
	c.setDefaultPort(&c.ClientHostPort, ":"+common.DefaultClientPortHTTP)
	c.setDefaultPort(&c.ServerPortHTTP, common.DefaultServerPortHTTP)
	c.setDefaultPort(&c.ServerPortTChannel, common.DefaultServerPortTChannel)

	behaviors := crossdock.Behaviors{
		behaviorTrace: c.trace,
	}

	http.Handle("/", crossdock.Handler(behaviors, true))

	listener, err := net.Listen("tcp", c.ClientHostPort)
	if err != nil {
		return err
	}
	c.listener = listener
	c.ClientHostPort = listener.Addr().String()
	return nil
}

// Serve starts service crossdock traffic. This is a blocking call.
func (c *Client) Serve() error {
	return http.Serve(c.listener, nil)
}

// Close stops the client
func (c *Client) Close() error {
	return c.listener.Close()
}

// URL returns a URL that the client can be accessed on
func (c *Client) URL() string {
	return fmt.Sprintf("http://%s/", c.ClientHostPort)
}

func (c *Client) setDefaultPort(port *string, defaultPort string) {
	if *port == "" {
		*port = defaultPort
	}
}

func (c *Client) mapServiceToHost(service string) string {
	mapper := c.hostMapper
	if mapper == nil {
		return service
	}
	return mapper(service)
}
