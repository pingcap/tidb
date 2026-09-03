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

package export

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
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/pingcap/errors"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"golang.org/x/net/http2"
)

const (
	cseScanURL            = "http://cse-ctl/scan"
	cseMetricsURL         = "http://cse-ctl/metrics"
	cseScanStatusTrailer  = "x-cse-scan-status"
	cseScanErrorTrailer   = "x-cse-scan-error"
	cseScanStatusComplete = "complete"
	cseScanStatusFailed   = "failed"
	maxCSEDiagnosticBytes = 64 << 10
)

type cseDumper struct {
	process *cseDumperProcess
	client  *cseDumperClient
}

type cseDumperProcess struct {
	cancel      context.CancelFunc
	temporary   string
	done        chan struct{}
	waitErr     error
	diagnostics string
	closeOnce   sync.Once
	closeErr    error
}

type cseDumperClient struct {
	httpClient *http.Client
	transport  *http2.Transport
}

func startCSEDumper(
	ctx context.Context,
	executable, metadataURL string,
	legacyEncryption bool,
	threads int,
	metrics *metrics,
) (dumper *cseDumper, resultErr error) {
	started := time.Now()
	defer func() {
		metrics.observePackedPhase(packedPhaseCSEStart, started, resultErr)
	}()
	temporary, err := os.MkdirTemp("", "dumpling-cse-")
	if err != nil {
		return nil, errors.Annotate(err, "create cse-ctl temporary directory")
	}
	socketPath := filepath.Join(temporary, "dumper.sock")
	childCtx, cancel := context.WithCancel(ctx)
	// #nosec G204 -- executable is the explicit user-provided --cse.ctl-path.
	cmd := exec.CommandContext(childCtx, executable, cseDumperArgs(
		metadataURL,
		socketPath,
		legacyEncryption,
		threads,
	)...)
	stderr, err := cmd.StderrPipe()
	if err != nil {
		cancel()
		_ = os.RemoveAll(temporary)
		return nil, errors.Annotate(err, "open cse-ctl dumper stderr")
	}
	if err := cmd.Start(); err != nil {
		cancel()
		_ = os.RemoveAll(temporary)
		return nil, errors.Annotatef(err, "start %q dumper", executable)
	}

	process := &cseDumperProcess{
		cancel:    cancel,
		temporary: temporary,
		done:      make(chan struct{}),
	}
	stderrResult := make(chan string, 1)
	go func() {
		stderrResult <- readCSEDumperDiagnostics(stderr)
	}()
	go func() {
		process.diagnostics = <-stderrResult
		process.waitErr = cmd.Wait()
		close(process.done)
	}()

	if err := process.waitForSocket(ctx, socketPath); err != nil {
		_ = process.close()
		return nil, err
	}
	return &cseDumper{
		process: process,
		client:  newCSEDumperClient(socketPath),
	}, nil
}

func readCSEDumperDiagnostics(input io.Reader) string {
	data, err := io.ReadAll(io.LimitReader(input, maxCSEDiagnosticBytes+1))
	truncated := len(data) > maxCSEDiagnosticBytes
	if truncated {
		data = data[:maxCSEDiagnosticBytes]
		if _, drainErr := io.Copy(io.Discard, input); err == nil {
			err = drainErr
		}
	}
	diagnostics := strings.TrimSpace(string(data))
	if truncated {
		diagnostics += "\ncse-ctl stderr truncated"
	}
	if err != nil {
		diagnostics += "\nread cse-ctl stderr: " + err.Error()
	}
	return strings.TrimSpace(diagnostics)
}

func newCSEDumperClient(socketPath string) *cseDumperClient {
	dialer := &net.Dialer{}
	transport := &http2.Transport{
		AllowHTTP: true,
		DialTLSContext: func(ctx context.Context, _, _ string, _ *tls.Config) (net.Conn, error) {
			return dialer.DialContext(ctx, "unix", socketPath)
		},
	}
	return &cseDumperClient{
		httpClient: &http.Client{Transport: transport},
		transport:  transport,
	}
}

func cseDumperArgs(metadataURL, socketPath string, legacyEncryption bool, threads int) []string {
	args := []string{
		"dumper",
		"--metadata-url", metadataURL,
		"--unix-socket", socketPath,
		"--scan-concurrency", strconv.Itoa(threads),
	}
	if legacyEncryption {
		args = append(args, "--legacy-encryption")
	}
	return args
}

func (p *cseDumperProcess) waitForSocket(ctx context.Context, socketPath string) error {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		info, err := os.Stat(socketPath)
		if err == nil {
			if info.Mode()&os.ModeSocket == 0 {
				return errors.Errorf("cse-ctl dumper created non-socket path %q", socketPath)
			}
			return nil
		}
		if !os.IsNotExist(err) {
			return errors.Annotate(err, "inspect cse-ctl dumper socket")
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-p.done:
			return p.exitError("start")
		case <-ticker.C:
		}
	}
}

type cseDumperScanRequest struct {
	StartKeyHex string `json:"start_key_hex"`
	EndKeyHex   string `json:"end_key_hex"`
}

func (d *cseDumper) scan(
	ctx context.Context,
	startKey, endKey []byte,
) (*cseDumperScan, error) {
	scan, err := d.client.scan(ctx, startKey, endKey)
	if err == nil {
		return scan, nil
	}
	select {
	case <-d.process.done:
		return nil, d.process.exitError("serve scan")
	default:
		return nil, err
	}
}

func (c *cseDumperClient) scan(
	ctx context.Context,
	startKey, endKey []byte,
) (*cseDumperScan, error) {
	payload, err := json.Marshal(cseDumperScanRequest{
		StartKeyHex: hex.EncodeToString(startKey),
		EndKeyHex:   hex.EncodeToString(endKey),
	})
	if err != nil {
		return nil, errors.Annotate(err, "encode cse-ctl dumper scan request")
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, cseScanURL, bytes.NewReader(payload))
	if err != nil {
		return nil, errors.Annotate(err, "create cse-ctl dumper scan request")
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := c.httpClient.Do(request)
	if err != nil {
		return nil, errors.Annotate(err, "request cse-ctl dumper scan")
	}
	if response.StatusCode != http.StatusOK {
		defer response.Body.Close()
		detail, _ := io.ReadAll(io.LimitReader(response.Body, 64<<10))
		return nil, errors.Errorf("cse-ctl dumper scan returned %s: %s", response.Status, strings.TrimSpace(string(detail)))
	}
	return &cseDumperScan{
		response: response,
		input:    bufio.NewReaderSize(response.Body, 256*1024),
	}, nil
}

type cseMetricsGatherer struct {
	owner *Dumper
}

func (g cseMetricsGatherer) Gather() ([]*dto.MetricFamily, error) {
	client := g.owner.packedService.Load()
	if client == nil {
		return nil, nil
	}
	response, err := client.httpClient.Get(cseMetricsURL)
	if err != nil {
		return nil, errors.Annotate(err, "request cse-ctl dumper metrics")
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		detail, _ := io.ReadAll(io.LimitReader(response.Body, 64<<10))
		return nil, errors.Errorf("cse-ctl dumper metrics returned %s: %s", response.Status, strings.TrimSpace(string(detail)))
	}
	parser := expfmt.TextParser{}
	families, err := parser.TextToMetricFamilies(response.Body)
	if err != nil {
		return nil, errors.Annotate(err, "parse cse-ctl dumper metrics")
	}
	return slices.Collect(maps.Values(families)), nil
}

func (p *cseDumperProcess) exitError(action string) error {
	if p.diagnostics != "" {
		return errors.Errorf("cse-ctl dumper exited while trying to %s: %v; stderr: %s", action, p.waitErr, p.diagnostics)
	}
	return errors.Errorf("cse-ctl dumper exited while trying to %s: %v", action, p.waitErr)
}

func (p *cseDumperProcess) close() error {
	p.closeOnce.Do(func() {
		p.cancel()
		<-p.done
		p.closeErr = os.RemoveAll(p.temporary)
	})
	return p.closeErr
}

func (c *cseDumperClient) close() {
	c.transport.CloseIdleConnections()
}

func (d *cseDumper) close() error {
	d.client.close()
	return d.process.close()
}

type cseDumperScan struct {
	response *http.Response
	input    *bufio.Reader
	finished bool
}

func (s *cseDumperScan) readRow(keyBuffer, valueBuffer []byte) (key, value []byte, end bool, err error) {
	key, value, end, err = readPackedRow(s.input, keyBuffer, valueBuffer)
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

func (s *cseDumperScan) completionError() error {
	status := s.response.Trailer.Get(cseScanStatusTrailer)
	if status == cseScanStatusComplete {
		return nil
	}
	if status == "" {
		return errors.New("cse-ctl dumper scan ended without a completion trailer")
	}
	if status != cseScanStatusFailed {
		return errors.Errorf("cse-ctl dumper scan ended with unknown status %q", status)
	}
	detail, err := url.QueryUnescape(s.response.Trailer.Get(cseScanErrorTrailer))
	if err != nil {
		return errors.Annotate(err, "decode cse-ctl dumper scan error")
	}
	if detail == "" {
		return errors.New("cse-ctl dumper scan failed")
	}
	return errors.Errorf("cse-ctl dumper scan failed: %s", detail)
}

func (s *cseDumperScan) close() error {
	if s.finished {
		return nil
	}
	s.finished = true
	return s.response.Body.Close()
}

func scanPackedRange(
	ctx context.Context,
	scanner packedScanner,
	startKey, endKey []byte,
	emit func(key, value []byte) error,
) error {
	scan, err := scanner.scan(ctx, startKey, endKey)
	if err != nil {
		return err
	}
	defer func() { _ = scan.close() }()
	var keyBuffer, valueBuffer []byte
	for {
		key, value, end, err := scan.readRow(keyBuffer, valueBuffer)
		if err != nil {
			return err
		}
		if end {
			return nil
		}
		if err := emit(key, value); err != nil {
			return err
		}
		keyBuffer = key
		valueBuffer = value
	}
}

func readPackedRow(input io.Reader, keyBuffer, valueBuffer []byte) (key, value []byte, end bool, err error) {
	keySize, err := readPackedUint32(input)
	if err == io.EOF {
		return nil, nil, true, nil
	}
	if err != nil {
		return nil, nil, false, errors.Annotate(err, "read packed row key size")
	}
	valueSize, err := readPackedUint32(input)
	if err != nil {
		return nil, nil, false, errors.Annotate(err, "read packed row value size")
	}
	if keySize == 0 {
		return nil, nil, false, errors.New("invalid packed row with empty key")
	}
	key = resizePackedBuffer(keyBuffer, int(keySize))
	value = resizePackedBuffer(valueBuffer, int(valueSize))
	if _, err := io.ReadFull(input, key); err != nil {
		return nil, nil, false, errors.Annotate(err, "read packed row key")
	}
	if _, err := io.ReadFull(input, value); err != nil {
		return nil, nil, false, errors.Annotate(err, "read packed row value")
	}
	return key, value, false, nil
}

func resizePackedBuffer(buffer []byte, size int) []byte {
	if cap(buffer) < size {
		return make([]byte, size)
	}
	return buffer[:size]
}

func readPackedUint32(input io.Reader) (uint32, error) {
	var data [4]byte
	if _, err := io.ReadFull(input, data[:]); err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint32(data[:]), nil
}
