// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package dumpservice

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"io"
	"maps"
	"net"
	"net/http"
	"net/url"
	"path/filepath"
	"slices"
	"strings"

	"github.com/pingcap/errors"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"golang.org/x/net/http2"
)

const (
	dataURL                  = "http://dump-service/data"
	shardsURL                = "http://dump-service/shards"
	metricsURL               = "http://dump-service/metrics"
	dumperScanStatusTrailer  = "x-dumper-scan-status"
	dumperScanErrorTrailer   = "x-dumper-scan-error"
	dumperScanStatusComplete = "complete"
	dumperScanStatusFailed   = "failed"
)

// Client multiplexes concurrent requests over HTTP/2 on an external Unix socket.
// Construct it with NewClient. It never manages the service process or listener.
type Client struct {
	httpClient *http.Client
	transport  *http2.Transport
}

// ParseSocketURL accepts only local Unix sockets with absolute paths.
func ParseSocketURL(address string) (string, error) {
	endpoint, err := url.Parse(address)
	if err != nil {
		return "", errors.Annotate(err, "parse dump service URL")
	}
	if endpoint.Scheme != "unix" || endpoint.Host != "" || endpoint.User != nil ||
		endpoint.Opaque != "" || endpoint.RawQuery != "" || endpoint.ForceQuery ||
		endpoint.Fragment != "" || strings.Contains(address, "#") ||
		!strings.HasPrefix(address, "unix:///") || !filepath.IsAbs(endpoint.Path) ||
		endpoint.Path == "/" || strings.ContainsRune(endpoint.Path, '\x00') {
		return "", errors.New("dump service URL requires unix:///absolute/path.sock without an authority, query, or fragment")
	}
	return endpoint.Path, nil
}

// Range is a half-open interval of logical keys, ordered by unsigned bytes.
type Range struct {
	Start []byte
	End   []byte
}

// NewClient validates the Unix URL and creates a client without connecting.
// The external service must be ready before the first request.
func NewClient(address string) (*Client, error) {
	socketPath, err := ParseSocketURL(address)
	if err != nil {
		return nil, err
	}
	return newClient(socketPath), nil
}

func newClient(socketPath string) *Client {
	dialer := &net.Dialer{}
	transport := &http2.Transport{
		AllowHTTP: true,
		DialTLSContext: func(ctx context.Context, _, _ string, _ *tls.Config) (net.Conn, error) {
			return dialer.DialContext(ctx, "unix", socketPath)
		},
	}
	return &Client{
		httpClient: &http.Client{Transport: transport},
		transport:  transport,
	}
}

type hexRange struct {
	StartKeyHex string `json:"start_key_hex"`
	EndKeyHex   string `json:"end_key_hex"`
}

type shardsResponse struct {
	Ranges []hexRange `json:"ranges"`
}

// rangeQueryURL encodes a half-open key range as hexadecimal query parameters.
func rangeQueryURL(baseURL string, startKey, endKey []byte) string {
	query := url.Values{}
	query.Set("start_key_hex", hex.EncodeToString(startKey))
	query.Set("end_key_hex", hex.EncodeToString(endKey))
	return baseURL + "?" + query.Encode()
}

// Scan opens a streaming range scan. The caller must read to completion or close it.
func (c *Client) Scan(
	ctx context.Context,
	startKey, endKey []byte,
) (*Scan, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, rangeQueryURL(dataURL, startKey, endKey), nil)
	if err != nil {
		return nil, errors.Annotate(err, "create dump service scan request")
	}
	response, err := c.httpClient.Do(request)
	if err != nil {
		return nil, errors.Annotate(err, "request dump service scan")
	}
	if response.StatusCode != http.StatusOK {
		defer response.Body.Close()
		detail, _ := io.ReadAll(io.LimitReader(response.Body, 64<<10))
		return nil, errors.Errorf("dump service scan returned %s: %s", response.Status, strings.TrimSpace(string(detail)))
	}
	return &Scan{
		response: response,
		input:    bufio.NewReaderSize(response.Body, 256*1024),
	}, nil
}

// Shards returns ordered, adjacent ranges that exactly cover the requested interval.
func (c *Client) Shards(
	ctx context.Context,
	startKey, endKey []byte,
) ([]Range, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, rangeQueryURL(shardsURL, startKey, endKey), nil)
	if err != nil {
		return nil, errors.Annotate(err, "create dump service shards request")
	}
	response, err := c.httpClient.Do(request)
	if err != nil {
		return nil, errors.Annotate(err, "request dump service shards")
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		detail, _ := io.ReadAll(io.LimitReader(response.Body, 64<<10))
		return nil, errors.Errorf("dump service shards returned %s: %s", response.Status, strings.TrimSpace(string(detail)))
	}
	var decoded shardsResponse
	if err := json.NewDecoder(response.Body).Decode(&decoded); err != nil {
		return nil, errors.Annotate(err, "decode dump service shards response")
	}
	return decodeShardRanges(startKey, endKey, decoded.Ranges)
}

func decodeShardRanges(
	requestedStart, requestedEnd []byte,
	encoded []hexRange,
) ([]Range, error) {
	if len(encoded) == 0 {
		return nil, errors.New("dump service shards returned no ranges")
	}
	ranges := make([]Range, 0, len(encoded))
	nextStart := requestedStart
	for index, shard := range encoded {
		start, err := hex.DecodeString(shard.StartKeyHex)
		if err != nil {
			return nil, errors.Annotatef(err, "decode dump service shard range %d start key", index)
		}
		end, err := hex.DecodeString(shard.EndKeyHex)
		if err != nil {
			return nil, errors.Annotatef(err, "decode dump service shard range %d end key", index)
		}
		if !bytes.Equal(start, nextStart) {
			return nil, errors.Errorf(
				"dump service shard range %d starts at %x instead of %x",
				index,
				start,
				nextStart,
			)
		}
		if bytes.Compare(start, end) >= 0 {
			return nil, errors.Errorf("dump service shard range %d is empty or reversed", index)
		}
		if bytes.Compare(end, requestedEnd) > 0 {
			return nil, errors.Errorf("dump service shard range %d ends outside the requested range", index)
		}
		ranges = append(ranges, Range{Start: start, End: end})
		nextStart = end
	}
	if !bytes.Equal(nextStart, requestedEnd) {
		return nil, errors.Errorf(
			"dump service shard ranges end at %x instead of %x",
			nextStart,
			requestedEnd,
		)
	}
	return ranges, nil
}

// Gather retrieves Prometheus metric families from the service.
// It implements prometheus.Gatherer and does not impose a request deadline.
func (c *Client) Gather() ([]*dto.MetricFamily, error) {
	response, err := c.httpClient.Get(metricsURL)
	if err != nil {
		return nil, errors.Annotate(err, "request dump service metrics")
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		detail, _ := io.ReadAll(io.LimitReader(response.Body, 64<<10))
		return nil, errors.Errorf("dump service metrics returned %s: %s", response.Status, strings.TrimSpace(string(detail)))
	}
	parser := expfmt.TextParser{}
	families, err := parser.TextToMetricFamilies(response.Body)
	if err != nil {
		return nil, errors.Annotate(err, "parse dump service metrics")
	}
	return slices.Collect(maps.Values(families)), nil
}

// Close releases idle connections. It does not cancel active requests or stop the service.
// Callers must finish or cancel their requests before calling Close.
func (c *Client) Close() {
	c.transport.CloseIdleConnections()
}

// Scan is a single response stream and is not safe for concurrent use.
type Scan struct {
	response *http.Response
	input    *bufio.Reader
	finished bool
}

// ReadRow reads one KV frame, reusing the supplied buffers where possible.
// Returned bytes may alias those buffers. end is true only after validating successful
// completion trailers. EOF without the completion trailer is an error.
func (s *Scan) ReadRow(keyBuffer, valueBuffer []byte) (key, value []byte, end bool, err error) {
	key, value, end, err = readKVRow(s.input, keyBuffer, valueBuffer)
	if err != nil {
		s.finished = true
		_ = s.response.Body.Close()
		return nil, nil, false, err
	}
	if !end {
		return key, value, false, nil
	}
	s.finished = true
	err = s.completionError()
	if closeErr := s.response.Body.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return nil, nil, false, err
	}
	return nil, nil, true, nil
}

func (s *Scan) completionError() error {
	status := s.response.Trailer.Get(dumperScanStatusTrailer)
	if status == dumperScanStatusComplete {
		return nil
	}
	if status == "" {
		return errors.New("dump service scan ended without a completion trailer")
	}
	if status != dumperScanStatusFailed {
		return errors.Errorf("dump service scan ended with unknown status %q", status)
	}
	detail, err := url.QueryUnescape(s.response.Trailer.Get(dumperScanErrorTrailer))
	if err != nil {
		return errors.Annotate(err, "decode dump service scan error")
	}
	if detail == "" {
		return errors.New("dump service scan failed")
	}
	return errors.Errorf("dump service scan failed: %s", detail)
}

// Close abandons the stream without asserting successful completion.
// It is idempotent and does not close the client's other streams.
func (s *Scan) Close() error {
	if s.finished {
		return nil
	}
	s.finished = true
	return s.response.Body.Close()
}

func readKVRow(input io.Reader, keyBuffer, valueBuffer []byte) (key, value []byte, end bool, err error) {
	keySize, err := readKVUint32(input)
	if err == io.EOF {
		return nil, nil, true, nil
	}
	if err != nil {
		return nil, nil, false, errors.Annotate(err, "read kv row key size")
	}
	valueSize, err := readKVUint32(input)
	if err != nil {
		return nil, nil, false, errors.Annotate(err, "read kv row value size")
	}
	if keySize == 0 {
		return nil, nil, false, errors.New("invalid kv row with empty key")
	}
	key = resizeKVBuffer(keyBuffer, int(keySize))
	value = resizeKVBuffer(valueBuffer, int(valueSize))
	if _, err := io.ReadFull(input, key); err != nil {
		return nil, nil, false, errors.Annotate(err, "read kv row key")
	}
	if _, err := io.ReadFull(input, value); err != nil {
		return nil, nil, false, errors.Annotate(err, "read kv row value")
	}
	return key, value, false, nil
}

func resizeKVBuffer(buffer []byte, size int) []byte {
	if cap(buffer) < size {
		return make([]byte, size)
	}
	return buffer[:size]
}

func readKVUint32(input io.Reader) (uint32, error) {
	var data [4]byte
	if _, err := io.ReadFull(input, data[:]); err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint32(data[:]), nil
}
