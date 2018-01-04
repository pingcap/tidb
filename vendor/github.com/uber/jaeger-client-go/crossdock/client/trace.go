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
	"time"

	"github.com/crossdock/crossdock-go"
	"golang.org/x/net/context"

	"github.com/uber/jaeger-client-go/crossdock/common"
	"github.com/uber/jaeger-client-go/crossdock/log"
	"github.com/uber/jaeger-client-go/crossdock/thrift/tracetest"
	"github.com/uber/jaeger-client-go/utils"
)

func (c *Client) trace(t crossdock.T) {
	sampled := str2bool(t.Param(sampledParam))
	baggage := randomBaggage()

	level1 := tracetest.NewStartTraceRequest()
	level1.ServerRole = RoleS1
	level1.Sampled = sampled
	level1.Baggage = baggage
	server1 := t.Param(server1NameParam)

	level2 := tracetest.NewDownstream()
	level2.ServiceName = t.Param(server2NameParam)
	level2.ServerRole = RoleS2
	level2.Host = c.mapServiceToHost(level2.ServiceName)
	level2.Port = c.transport2port(t.Param(server2TransportParam))
	level2.Transport = transport2transport(t.Param(server2TransportParam))
	level1.Downstream = level2

	level3 := tracetest.NewDownstream()
	level3.ServiceName = t.Param(server3NameParam)
	level3.ServerRole = RoleS3
	level3.Host = c.mapServiceToHost(level3.ServiceName)
	level3.Port = c.transport2port(t.Param(server3TransportParam))
	level3.Transport = transport2transport(t.Param(server3TransportParam))
	level2.Downstream = level3

	server1host := c.mapServiceToHost(server1)
	url := fmt.Sprintf("http://%s:%s/start_trace", server1host, c.ServerPortHTTP)
	resp, err := common.PostJSON(context.Background(), url, level1)
	if err != nil {
		t.Errorf(err.Error())
		return
	}

	for r := resp; r != nil; r = r.Downstream {
		if r.NotImplementedError != "" {
			t.Skipf(r.NotImplementedError)
			log.Printf("SKIP: %s", r.NotImplementedError)
			return
		}
	}

	traceID := resp.Span.TraceId
	if traceID == "" {
		t.Errorf("Trace ID is empty in S1(%s)", server1)
		return
	}

	success := validateTrace(t, level1.Downstream, resp, server1, 1, traceID, sampled, baggage)
	if success {
		t.Successf("trace checks out")
		log.Printf("PASS")
	}
}

func validateTrace(
	t crossdock.T,
	target *tracetest.Downstream,
	resp *tracetest.TraceResponse,
	service string,
	level int,
	traceID string,
	sampled bool,
	baggage string) bool {

	success := true
	if traceID != resp.Span.TraceId {
		t.Errorf("Trace ID mismatch in S%d(%s): expected %s, received %s",
			level, service, traceID, resp.Span.TraceId)
		success = false
	}
	if baggage != resp.Span.Baggage {
		t.Errorf("Baggage mismatch in S%d(%s): expected %s, received %s",
			level, service, baggage, resp.Span.Baggage)
		success = false
	}
	if sampled != resp.Span.Sampled {
		t.Errorf("Sampled mismatch in S%d(%s): expected %t, received %t",
			level, service, sampled, resp.Span.Sampled)
		success = false
	}
	if target != nil {
		if resp.Downstream == nil {
			t.Errorf("Missing downstream in S%d(%s)", level, service)
			success = false
		} else {
			success = validateTrace(t, target.Downstream, resp.Downstream,
				target.Host, level+1, traceID, sampled, baggage) && success
		}
	} else if resp.Downstream != nil {
		t.Errorf("Unexpected downstream in S%d(%s)", level, service)
		success = false
	}
	return success
}

func randomBaggage() string {
	r := utils.NewRand(time.Now().UnixNano())
	n := uint64(r.Int63())
	return fmt.Sprintf("%x", n)
}

func str2bool(v string) bool {
	switch v {
	case "true":
		return true
	case "false":
		return false
	default:
		panic(v + " is not a Boolean")
	}
}

func (c *Client) transport2port(v string) string {
	switch v {
	case transportHTTP:
		return c.ServerPortHTTP
	case transportTChannel:
		return c.ServerPortTChannel
	case transportDummy:
		return "9999"
	default:
		panic("Unknown protocol " + v)
	}
}

func transport2transport(v string) tracetest.Transport {
	switch v {
	case transportHTTP:
		return tracetest.Transport_HTTP
	case transportTChannel:
		return tracetest.Transport_TCHANNEL
	case transportDummy:
		return tracetest.Transport_DUMMY
	default:
		panic("Unknown protocol " + v)
	}
}
