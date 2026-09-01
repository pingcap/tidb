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
)

type cseDumper struct {
	client      *http.Client
	transport   *http2.Transport
	cancel      context.CancelFunc
	temporary   string
	done        chan struct{}
	waitErr     error
	diagnostics string
	closeOnce   sync.Once
	closeErr    error
}

func startCSEDumper(
	ctx context.Context,
	executable, metadataURL string,
	legacyEncryption bool,
	threads int,
	observation *packedExportObservation,
) (*cseDumper, error) {
	temporary, err := os.MkdirTemp("", "dumpling-cse-")
	if err != nil {
		return nil, errors.Annotate(err, "create cse-ctl temporary directory")
	}
	socketPath := filepath.Join(temporary, "dumper.sock")
	childCtx, cancel := context.WithCancel(ctx)
	// #nosec G204 -- executable is the explicit user-provided --cse-ctl-path.
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

	dumper := &cseDumper{
		cancel:    cancel,
		temporary: temporary,
		done:      make(chan struct{}),
	}
	stderrResult := make(chan string, 1)
	go func() {
		stderrResult <- readCSEDumperStderr(stderr, observation)
	}()
	go func() {
		dumper.diagnostics = <-stderrResult
		dumper.waitErr = cmd.Wait()
		close(dumper.done)
	}()

	if err := dumper.waitForSocket(ctx, socketPath); err != nil {
		_ = dumper.close()
		return nil, err
	}
	dumper.client, dumper.transport = newCSEDumperHTTPClient(socketPath)
	return dumper, nil
}

func newCSEDumperHTTPClient(socketPath string) (*http.Client, *http2.Transport) {
	dialer := &net.Dialer{}
	transport := &http2.Transport{
		AllowHTTP: true,
		DialTLSContext: func(ctx context.Context, _, _ string, _ *tls.Config) (net.Conn, error) {
			return dialer.DialContext(ctx, "unix", socketPath)
		},
	}
	return &http.Client{Transport: transport}, transport
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

func (d *cseDumper) waitForSocket(ctx context.Context, socketPath string) error {
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
		case <-d.done:
			return d.exitError("start")
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
	observation *packedExportObservation,
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
	response, err := d.client.Do(request)
	if err != nil {
		select {
		case <-d.done:
			return nil, d.exitError("serve scan")
		default:
		}
		return nil, errors.Annotate(err, "request cse-ctl dumper scan")
	}
	if response.StatusCode != http.StatusOK {
		defer response.Body.Close()
		detail, _ := io.ReadAll(io.LimitReader(response.Body, 64<<10))
		return nil, errors.Errorf("cse-ctl dumper scan returned %s: %s", response.Status, strings.TrimSpace(string(detail)))
	}
	return &cseDumperScan{
		response:    response,
		input:       bufio.NewReaderSize(response.Body, 256*1024),
		observation: newPackedScanContext(observation),
	}, nil
}

type cseMetricsGatherer struct {
	owner *Dumper
}

func (g cseMetricsGatherer) Gather() ([]*dto.MetricFamily, error) {
	dumper := g.owner.packedService.Load()
	if dumper == nil {
		return nil, nil
	}
	response, err := dumper.client.Get(cseMetricsURL)
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

func (d *cseDumper) exitError(action string) error {
	if d.diagnostics != "" {
		return errors.Errorf("cse-ctl dumper exited while trying to %s: %v; stderr: %s", action, d.waitErr, d.diagnostics)
	}
	return errors.Errorf("cse-ctl dumper exited while trying to %s: %v", action, d.waitErr)
}

func (d *cseDumper) close() error {
	d.closeOnce.Do(func() {
		if d.transport != nil {
			d.transport.CloseIdleConnections()
		}
		d.cancel()
		<-d.done
		d.closeErr = os.RemoveAll(d.temporary)
	})
	return d.closeErr
}

type cseDumperScan struct {
	response    *http.Response
	input       *bufio.Reader
	observation *packedScanContext
	finished    bool
}

func (s *cseDumperScan) readRow(keyBuffer, valueBuffer []byte) (key, value []byte, end bool, err error) {
	key, value, end, err = s.observation.readRow(s.input, keyBuffer, valueBuffer)
	if err != nil {
		s.finished = true
		_ = s.response.Body.Close()
		s.observation.finish(err)
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
	s.observation.finish(err)
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
	err := s.response.Body.Close()
	s.observation.finish(err)
	return err
}

func scanCSEDumperRange(
	ctx context.Context,
	dumper *cseDumper,
	startKey, endKey []byte,
	emit func(key, value []byte) error,
	observation *packedExportObservation,
) error {
	scan, err := dumper.scan(ctx, startKey, endKey, observation)
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
