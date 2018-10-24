/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package thrift

import (
	"bytes"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"time"
)

type THttpClient struct {
	// the most recent response, if any
	responseBuffer *bytes.Buffer
	url            *url.URL
	requestBuffer  *bytes.Buffer
	header         http.Header
	timeout        time.Duration
	// TODO what are these timeouts for?
	nsecConnectTimeout int64
	nsecReadTimeout    int64
	httpClient         *http.Client
}

type THttpClientTransportFactory struct {
	url     string
	isPost  bool
	timeout time.Duration
}

func (p *THttpClientTransportFactory) GetTransport(trans TTransport) TTransport {
	if trans != nil {
		t, ok := trans.(*THttpClient)
		if ok && t.url != nil {
			if t.requestBuffer != nil {
				t2, _ := NewTHttpPostClient(t.url.String(), t.timeout)
				return t2
			}
			t2, _ := NewTHttpClient(t.url.String(), t.timeout)
			return t2
		}
	}
	if p.isPost {
		s, _ := NewTHttpPostClient(p.url, p.timeout)
		return s
	}
	s, _ := NewTHttpClient(p.url, p.timeout)
	return s
}

func NewTHttpClientTransportFactory(url string, timeout time.Duration) *THttpClientTransportFactory {
	return &THttpClientTransportFactory{url: url, isPost: false, timeout: timeout}
}

func NewTHttpPostClientTransportFactory(url string, timeout time.Duration) *THttpClientTransportFactory {
	return &THttpClientTransportFactory{url: url, isPost: true, timeout: timeout}
}

func newHttpClient(timeout time.Duration) *http.Client {
	return &http.Client{
		// Setting a non-nil Transport means that each client will get its own
		// idle connection pool.  This is way more stable than sharing a single
		// pool for this entire server.
		Transport: &http.Transport{}, // TODO set additional (TLS?) timeouts?
		Timeout:   timeout,
	}
}

func NewTHttpClient(urlstr string, timeout time.Duration) (TTransport, error) {
	parsedURL, err := url.Parse(urlstr)
	if err != nil {
		return nil, err
	}
	response, err := http.Get(urlstr)
	if response != nil && response.Body != nil {
		defer response.Body.Close()
	}
	if err != nil {
		return nil, err
	}
	bs, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return nil, err
	}
	buf := bytes.NewBuffer(bs)

	return &THttpClient{
		responseBuffer: buf,
		url:            parsedURL,
		httpClient:     newHttpClient(timeout),
		timeout:        timeout,
	}, nil
}

// bufPool is a set of buffers used for the request bodies. Buffers should
// be Reset() before they are added to the pool.
var bufPool = sync.Pool{
	New: func() interface{} {
		return bytes.NewBuffer(make([]byte, 0, 1024))
	},
}

func getBuffer() *bytes.Buffer {
	buf := bufPool.Get().(*bytes.Buffer)
	if buf == nil {
		buf = bytes.NewBuffer(make([]byte, 0, 1024))
	}
	return buf
}

func NewTHttpPostClient(urlstr string, timeout time.Duration) (TTransport, error) {
	parsedURL, err := url.Parse(urlstr)
	if err != nil {
		return nil, err
	}
	return &THttpClient{
		url:           parsedURL,
		requestBuffer: getBuffer(),
		header:        http.Header{},
		httpClient:    newHttpClient(timeout),
	}, nil
}

// Set the HTTP Header for this specific Thrift Transport
// It is important that you first assert the TTransport as a THttpClient type
// like so:
//
// httpTrans := trans.(THttpClient)
// httpTrans.SetHeader("User-Agent","Thrift Client 1.0")
func (p *THttpClient) SetHeader(key string, value string) {
	p.header.Set(key, value)
}

// Get the HTTP Header represented by the supplied Header Key for this specific Thrift Transport
// It is important that you first assert the TTransport as a THttpClient type
// like so:
//
// httpTrans := trans.(THttpClient)
// hdrValue := httpTrans.GetHeader("User-Agent")
func (p *THttpClient) GetHeader(key string) string {
	return p.header.Get(key)
}

// Deletes the HTTP Header given a Header Key for this specific Thrift Transport
// It is important that you first assert the TTransport as a THttpClient type
// like so:
//
// httpTrans := trans.(THttpClient)
// httpTrans.DelHeader("User-Agent")
func (p *THttpClient) DelHeader(key string) {
	p.header.Del(key)
}

func (p *THttpClient) Open() error {
	// do nothing
	return nil
}

func (p *THttpClient) IsOpen() bool {
	return p.responseBuffer != nil || p.requestBuffer != nil
}

func (p *THttpClient) Peek() bool {
	return p.IsOpen()
}

func (p *THttpClient) Close() error {
	if p.responseBuffer != nil {
		p.responseBuffer = nil
	}
	if p.requestBuffer != nil {
		// Note: We do not return the buffer to the pool,
		// since there is a potential race here.  It will be
		// garbage collected.
		p.requestBuffer = nil
	}
	return nil
}

func (p *THttpClient) Read(buf []byte) (int, error) {
	if p.responseBuffer == nil {
		return 0, NewTTransportException(NOT_OPEN, "Response buffer is empty, no request.")
	}
	n, err := p.responseBuffer.Read(buf)
	if n > 0 && (err == nil || err == io.EOF) {
		return n, nil
	}
	return n, NewTTransportExceptionFromError(err)
}

func (p *THttpClient) ReadByte() (c byte, err error) {
	return readByte(p.responseBuffer)
}

func (p *THttpClient) Write(buf []byte) (int, error) {
	n, err := p.requestBuffer.Write(buf)
	return n, err
}

func (p *THttpClient) WriteByte(c byte) error {
	return p.requestBuffer.WriteByte(c)
}

func (p *THttpClient) WriteString(s string) (n int, err error) {
	return p.requestBuffer.WriteString(s)
}

// bufferCloser makes a bytes.Buffer into an io.Closer and returns unused
// buffers to the pool.
type bufferCloser struct {
	sync.Once
	*bytes.Buffer
}

// THttpClient reuses its buffer from one request to the next. If that
// buffer is not reset, well, it'll just keep getting extended from
// one request to the next. Make the bytes.Buffer into an io.Closer
// and rely on the http transport to call Close() (and therefore
// to call Reset() and return it to the pool).
func (b *bufferCloser) Close() error {
	b.Once.Do(func() {
		b.Reset()
		bufPool.Put(b.Buffer)
	})
	return nil
}

func (p *THttpClient) Flush() error {
	req, err := http.NewRequest("POST", p.url.String(), &bufferCloser{Buffer: p.requestBuffer})
	if err != nil {
		return NewTTransportExceptionFromError(err)
	}
	// Reset the request/response state
	p.requestBuffer = getBuffer()
	p.responseBuffer = nil
	// Do the call
	req.Header = p.header
	// http2 reads the header outside of the call to Do(), so make a new
	// header for next time here.
	p.header = make(http.Header, len(req.Header))
	// Also, I'm not sure why the headers are reused from request to request,
	// so make a copy of them in case anyone is expecting that.
	for k, v := range req.Header {
		p.header[k] = v
	}
	req.Header.Set("Content-Type", "application/x-thrift")
	response, err := p.httpClient.Do(req)
	if response != nil && response.Body != nil {
		defer response.Body.Close()
	}

	if err != nil {
		return NewTTransportExceptionFromError(err)
	}
	if response.StatusCode != http.StatusOK {
		// TODO(pomack) log bad response
		return NewTTransportException(UNKNOWN_TRANSPORT_EXCEPTION, "HTTP Response code: "+strconv.Itoa(response.StatusCode))
	}
	bs, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return err
	}
	p.responseBuffer = bytes.NewBuffer(bs)
	return nil
}
