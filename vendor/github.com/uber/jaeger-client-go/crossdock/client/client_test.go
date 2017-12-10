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

package client

import (
	"net/url"
	"testing"

	"github.com/crossdock/crossdock-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/uber/jaeger-client-go"
	"github.com/uber/jaeger-client-go/crossdock/common"
	"github.com/uber/jaeger-client-go/crossdock/log"
	"github.com/uber/jaeger-client-go/crossdock/server"
	jlog "github.com/uber/jaeger-client-go/log"
)

func TestCrossdock(t *testing.T) {
	log.Enabled = false // enable when debugging tests
	log.Printf("Starting crossdock test")

	var reporter jaeger.Reporter
	if log.Enabled {
		reporter = jaeger.NewLoggingReporter(jlog.StdLogger)
	} else {
		reporter = jaeger.NewNullReporter()
	}

	tracer, tCloser := jaeger.NewTracer(
		"crossdock",
		jaeger.NewConstSampler(false),
		reporter)
	defer tCloser.Close()

	s := &server.Server{
		HostPortHTTP:     "127.0.0.1:0",
		HostPortTChannel: "127.0.0.1:0",
		Tracer:           tracer,
	}
	err := s.Start()
	require.NoError(t, err)
	defer s.Close()

	c := &Client{
		ClientHostPort:     "127.0.0.1:0",
		ServerPortHTTP:     s.GetPortHTTP(),
		ServerPortTChannel: s.GetPortTChannel(),
		hostMapper:         func(server string) string { return "localhost" },
	}
	err = c.AsyncStart()
	require.NoError(t, err)
	defer c.Close()

	crossdock.Wait(t, s.URL(), 10)
	crossdock.Wait(t, c.URL(), 10)

	behaviors := []struct {
		name string
		axes map[string][]string
	}{
		{
			name: behaviorTrace,
			axes: map[string][]string{
				server1NameParam:      {common.DefaultServiceName},
				sampledParam:          {"true", "false"},
				server2NameParam:      {common.DefaultServiceName},
				server2TransportParam: {transportHTTP, transportTChannel, transportDummy},
				server3NameParam:      {common.DefaultServiceName},
				server3TransportParam: {transportHTTP, transportTChannel},
			},
		},
	}

	for _, bb := range behaviors {
		for _, entry := range crossdock.Combinations(bb.axes) {
			entryArgs := url.Values{}
			for k, v := range entry {
				entryArgs.Set(k, v)
			}
			// test via real HTTP call
			crossdock.Call(t, c.URL(), bb.name, entryArgs)
		}
	}
}

func TestHostMapper(t *testing.T) {
	c := &Client{
		ClientHostPort:     "127.0.0.1:0",
		ServerPortHTTP:     "8080",
		ServerPortTChannel: "8081",
	}
	assert.Equal(t, "go", c.mapServiceToHost("go"))
	c.hostMapper = func(server string) string { return "localhost" }
	assert.Equal(t, "localhost", c.mapServiceToHost("go"))
}
