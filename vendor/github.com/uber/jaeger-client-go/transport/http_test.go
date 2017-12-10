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

package transport

import (
	"io/ioutil"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/apache/thrift/lib/go/thrift"
	"github.com/stretchr/testify/assert"

	"github.com/uber/jaeger-client-go"
	j "github.com/uber/jaeger-client-go/thrift-gen/jaeger"
)

func TestHTTPTransport(t *testing.T) {
	server := newHTTPServer(t)
	httpUsername := "Bender"
	httpPassword := "Rodriguez"
	sender := NewHTTPTransport(
		"http://localhost:10001/api/v1/spans",
		HTTPBatchSize(1),
		HTTPBasicAuth(httpUsername, httpPassword),
	)

	tracer, closer := jaeger.NewTracer(
		"test",
		jaeger.NewConstSampler(true),
		jaeger.NewRemoteReporter(sender),
	)
	defer closer.Close()

	span := tracer.StartSpan("root")
	span.Finish()

	// Need to yield to the select loop to accept the send request, and then
	// yield again to the send operation to write to the socket. I think the
	// best way to do that is just give it some time.

	deadline := time.Now().Add(2 * time.Second)
	for {
		if time.Now().After(deadline) {
			t.Fatal("never received a span")
		}
		if want, have := 1, len(server.getBatches()); want != have {
			time.Sleep(time.Millisecond)
			continue
		}
		break
	}

	srcSpanCtx := span.Context().(jaeger.SpanContext)
	gotBatch := server.getBatches()[0]
	assert.Len(t, gotBatch.Spans, 1)
	assert.Equal(t, "test", gotBatch.Process.ServiceName)
	gotSpan := gotBatch.Spans[0]
	assert.Equal(t, "root", gotSpan.OperationName)
	assert.EqualValues(t, srcSpanCtx.TraceID().High, gotSpan.TraceIdHigh)
	assert.EqualValues(t, srcSpanCtx.TraceID().Low, gotSpan.TraceIdLow)
	assert.EqualValues(t, srcSpanCtx.SpanID(), gotSpan.SpanId)
	assert.Equal(t,
		&HTTPBasicAuthCredentials{username: httpUsername, password: httpPassword},
		server.authCredentials[0],
	)
}

func TestHTTPOptions(t *testing.T) {
	sender := NewHTTPTransport(
		"some url",
		HTTPBatchSize(123),
		HTTPTimeout(123*time.Millisecond),
	)
	assert.Equal(t, 123, sender.batchSize)
	assert.Equal(t, 123*time.Millisecond, sender.client.Timeout)
}

type httpServer struct {
	t               *testing.T
	batches         []*j.Batch
	authCredentials []*HTTPBasicAuthCredentials
	mutex           sync.RWMutex
}

func (s *httpServer) getBatches() []*j.Batch {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	return s.batches
}

func (s *httpServer) credentials() []*HTTPBasicAuthCredentials {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	return s.authCredentials
}

// TODO this function and zipkin/http_test.go#newHTTPServer look like twins lost at birth
func newHTTPServer(t *testing.T) *httpServer {
	server := &httpServer{
		t:               t,
		batches:         make([]*j.Batch, 0),
		authCredentials: make([]*HTTPBasicAuthCredentials, 0),
		mutex:           sync.RWMutex{},
	}
	http.HandleFunc("/api/v1/spans", func(w http.ResponseWriter, r *http.Request) {
		contextType := r.Header.Get("Content-Type")
		if contextType != "application/x-thrift" {
			t.Errorf(
				"except Content-Type should be application/x-thrift, but is %s",
				contextType)
			return
		}

		body, err := ioutil.ReadAll(r.Body)
		if err != nil {
			t.Error(err)
			return
		}
		buffer := thrift.NewTMemoryBuffer()
		if _, err = buffer.Write(body); err != nil {
			t.Error(err)
			return
		}
		transport := thrift.NewTBinaryProtocolTransport(buffer)
		batch := &j.Batch{}
		if err = batch.Read(transport); err != nil {
			t.Error(err)
			return
		}
		server.mutex.Lock()
		defer server.mutex.Unlock()
		server.batches = append(server.batches, batch)
		u, p, _ := r.BasicAuth()
		server.authCredentials = append(server.authCredentials, &HTTPBasicAuthCredentials{username: u, password: p})
	})

	go func() {
		http.ListenAndServe(":10001", nil)
	}()

	return server
}
