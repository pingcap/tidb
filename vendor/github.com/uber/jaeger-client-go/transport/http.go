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
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"time"

	"github.com/apache/thrift/lib/go/thrift"

	"github.com/uber/jaeger-client-go"
	j "github.com/uber/jaeger-client-go/thrift-gen/jaeger"
)

// Default timeout for http request in seconds
const defaultHTTPTimeout = time.Second * 5

// HTTPTransport implements Transport by forwarding spans to a http server.
type HTTPTransport struct {
	url             string
	client          *http.Client
	batchSize       int
	spans           []*j.Span
	process         *j.Process
	httpCredentials *HTTPBasicAuthCredentials
}

// HTTPBasicAuthCredentials stores credentials for HTTP basic auth.
type HTTPBasicAuthCredentials struct {
	username string
	password string
}

// HTTPOption sets a parameter for the HttpCollector
type HTTPOption func(c *HTTPTransport)

// HTTPTimeout sets maximum timeout for http request.
func HTTPTimeout(duration time.Duration) HTTPOption {
	return func(c *HTTPTransport) { c.client.Timeout = duration }
}

// HTTPBatchSize sets the maximum batch size, after which a collect will be
// triggered. The default batch size is 100 spans.
func HTTPBatchSize(n int) HTTPOption {
	return func(c *HTTPTransport) { c.batchSize = n }
}

// HTTPBasicAuth sets the credentials required to perform HTTP basic auth
func HTTPBasicAuth(username string, password string) HTTPOption {
	return func(c *HTTPTransport) {
		c.httpCredentials = &HTTPBasicAuthCredentials{username: username, password: password}
	}
}

// NewHTTPTransport returns a new HTTP-backend transport. url should be an http
// url of the collector to handle POST request, typically something like:
//     http://hostname:14268/api/traces?format=jaeger.thrift
func NewHTTPTransport(url string, options ...HTTPOption) *HTTPTransport {
	c := &HTTPTransport{
		url:       url,
		client:    &http.Client{Timeout: defaultHTTPTimeout},
		batchSize: 100,
		spans:     []*j.Span{},
	}

	for _, option := range options {
		option(c)
	}
	return c
}

// Append implements Transport.
func (c *HTTPTransport) Append(span *jaeger.Span) (int, error) {
	if c.process == nil {
		c.process = jaeger.BuildJaegerProcessThrift(span)
	}
	jSpan := jaeger.BuildJaegerThrift(span)
	c.spans = append(c.spans, jSpan)
	if len(c.spans) >= c.batchSize {
		return c.Flush()
	}
	return 0, nil
}

// Flush implements Transport.
func (c *HTTPTransport) Flush() (int, error) {
	count := len(c.spans)
	if count == 0 {
		return 0, nil
	}
	err := c.send(c.spans)
	c.spans = c.spans[:0]
	return count, err
}

// Close implements Transport.
func (c *HTTPTransport) Close() error {
	return nil
}

func (c *HTTPTransport) send(spans []*j.Span) error {
	batch := &j.Batch{
		Spans:   spans,
		Process: c.process,
	}
	body, err := serializeThrift(batch)
	if err != nil {
		return err
	}
	req, err := http.NewRequest("POST", c.url, body)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/x-thrift")

	if c.httpCredentials != nil {
		req.SetBasicAuth(c.httpCredentials.username, c.httpCredentials.password)
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return err
	}
	io.Copy(ioutil.Discard, resp.Body)
	resp.Body.Close()
	if resp.StatusCode >= http.StatusBadRequest {
		return fmt.Errorf("error from collector: %d", resp.StatusCode)
	}
	return nil
}

func serializeThrift(obj thrift.TStruct) (*bytes.Buffer, error) {
	t := thrift.NewTMemoryBuffer()
	p := thrift.NewTBinaryProtocolTransport(t)
	if err := obj.Write(p); err != nil {
		return nil, err
	}
	return t.Buffer, nil
}
