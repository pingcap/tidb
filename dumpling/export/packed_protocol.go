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
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"os/exec"
	"sync"
	"sync/atomic"

	"github.com/pingcap/errors"
)

var cseDumperMagic = [...]byte{'C', 'S', 'E', 'D', 'U', 'M', 'P', 0, 1}

const (
	cseRequestClose  = byte(0)
	cseRequestSchema = byte(1)
	cseRequestScan   = byte(2)

	maxPackedManifestSize  = 64 << 20
	maxCSEDumperStderrSize = 64 << 10
)

type cseDumperStderr struct {
	data []byte
}

func (w *cseDumperStderr) Write(data []byte) (int, error) {
	written := len(data)
	if len(data) >= maxCSEDumperStderrSize {
		w.data = append(w.data[:0], data[len(data)-maxCSEDumperStderrSize:]...)
		return written, nil
	}
	overflow := len(w.data) + len(data) - maxCSEDumperStderrSize
	if overflow > 0 {
		copy(w.data, w.data[overflow:])
		w.data = w.data[:len(w.data)-overflow]
	}
	w.data = append(w.data, data...)
	return written, nil
}

func (w *cseDumperStderr) Bytes() []byte { return w.data }

type packedManifest struct {
	Version    uint32                   `json:"version"`
	ClusterID  uint64                   `json:"cluster_id"`
	KeyspaceID uint32                   `json:"keyspace_id"`
	ReadTS     uint64                   `json:"read_ts"`
	Databases  []packedManifestDatabase `json:"databases"`
}

type packedManifestDatabase struct {
	Database json.RawMessage   `json:"database"`
	Tables   []json.RawMessage `json:"tables"`
}

type cseDumperClient struct {
	cmd    *exec.Cmd
	stdin  io.WriteCloser
	input  *bufio.Reader
	output *bufio.Writer
	stderr cseDumperStderr

	waitOnce sync.Once
	waitErr  error
}

func startCSEDumperClient(ctx context.Context, executable, metadataURL string) (*cseDumperClient, error) {
	cmd := exec.CommandContext(ctx, executable, "dumper", "--metadata-url", metadataURL)
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, errors.Annotate(err, "open cse-ctl dumper stdin")
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		_ = stdin.Close()
		return nil, errors.Annotate(err, "open cse-ctl dumper stdout")
	}
	client := &cseDumperClient{
		cmd:    cmd,
		stdin:  stdin,
		input:  bufio.NewReaderSize(stdout, 256*1024),
		output: bufio.NewWriterSize(stdin, 4096),
	}
	cmd.Stderr = &client.stderr
	if err := cmd.Start(); err != nil {
		_ = stdin.Close()
		return nil, errors.Annotatef(err, "start %q dumper", executable)
	}
	var magic [len(cseDumperMagic)]byte
	if _, err := io.ReadFull(client.input, magic[:]); err != nil {
		return nil, client.fail(errors.Annotate(err, "read cse-ctl dumper protocol magic"))
	}
	if magic != cseDumperMagic {
		return nil, client.fail(errors.Errorf("unsupported cse-ctl dumper protocol magic %q", magic))
	}
	return client, nil
}

func (c *cseDumperClient) schema() (*packedManifest, error) {
	if err := c.writeRequest(cseRequestSchema); err != nil {
		return nil, err
	}
	size, err := readPackedUint32(c.input)
	if err != nil {
		return nil, c.fail(errors.Annotate(err, "read packed schema manifest size"))
	}
	if size > maxPackedManifestSize {
		return nil, c.fail(errors.Errorf("packed schema manifest is too large: %d bytes", size))
	}
	data := make([]byte, int(size))
	if _, err := io.ReadFull(c.input, data); err != nil {
		return nil, c.fail(errors.Annotate(err, "read packed schema manifest"))
	}
	manifest := &packedManifest{}
	if err := json.Unmarshal(data, manifest); err != nil {
		return nil, c.fail(errors.Annotate(err, "decode packed schema manifest"))
	}
	if manifest.Version != 1 {
		return nil, c.fail(errors.Errorf("unsupported packed schema manifest version %d", manifest.Version))
	}
	return manifest, nil
}

func (c *cseDumperClient) startScan(tableIDs []int64) error {
	if len(tableIDs) == 0 {
		return errors.New("packed table scan has no physical table IDs")
	}
	if uint64(len(tableIDs)) > uint64(^uint32(0)) {
		return errors.Errorf("packed table scan has too many physical table IDs: %d", len(tableIDs))
	}
	if err := c.output.WriteByte(cseRequestScan); err != nil {
		return c.fail(errors.Annotate(err, "write packed scan request"))
	}
	if err := writePackedUint32(c.output, uint32(len(tableIDs))); err != nil {
		return c.fail(errors.Annotate(err, "write packed table ID count"))
	}
	for _, tableID := range tableIDs {
		if err := binary.Write(c.output, binary.LittleEndian, tableID); err != nil {
			return c.fail(errors.Annotate(err, "write packed physical table ID"))
		}
	}
	if err := c.output.Flush(); err != nil {
		return c.fail(errors.Annotate(err, "flush packed scan request"))
	}
	return nil
}

func (c *cseDumperClient) readRow(keyBuffer, valueBuffer []byte) (key, value []byte, end bool, err error) {
	key, value, end, err = readPackedRow(c.input, keyBuffer, valueBuffer)
	if err != nil {
		return nil, nil, false, c.fail(err)
	}
	return key, value, end, nil
}

func readPackedRow(input io.Reader, keyBuffer, valueBuffer []byte) (key, value []byte, end bool, err error) {
	keySize, err := readPackedUint32(input)
	if err != nil {
		return nil, nil, false, errors.Annotate(err, "read packed row key size")
	}
	valueSize, err := readPackedUint32(input)
	if err != nil {
		return nil, nil, false, errors.Annotate(err, "read packed row value size")
	}
	if keySize == 0 {
		if valueSize != 0 {
			return nil, nil, false, errors.Errorf("invalid packed row terminator with value size %d", valueSize)
		}
		return nil, nil, true, nil
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

func (c *cseDumperClient) writeRequest(request byte) error {
	if err := c.output.WriteByte(request); err != nil {
		return c.fail(errors.Annotate(err, "write cse-ctl dumper request"))
	}
	if err := c.output.Flush(); err != nil {
		return c.fail(errors.Annotate(err, "flush cse-ctl dumper request"))
	}
	return nil
}

func (c *cseDumperClient) fail(protocolErr error) error {
	if c.cmd.Process != nil {
		_ = c.cmd.Process.Kill()
	}
	waitErr := c.wait()
	detail := bytes.TrimSpace(c.stderr.Bytes())
	if len(detail) > 0 {
		return errors.Annotatef(protocolErr, "cse-ctl dumper stderr: %s", detail)
	}
	if waitErr != nil {
		return errors.Annotatef(protocolErr, "cse-ctl dumper exited: %v", waitErr)
	}
	return protocolErr
}

func (c *cseDumperClient) wait() error {
	c.waitOnce.Do(func() {
		_ = c.stdin.Close()
		c.waitErr = c.cmd.Wait()
	})
	return c.waitErr
}

func (c *cseDumperClient) close() error {
	_ = c.writeRequest(cseRequestClose)
	return c.wait()
}

type cseDumperPool struct {
	ctx        context.Context
	cancel     context.CancelFunc
	executable string
	metadata   string
	clients    chan *cseDumperClient
	closed     atomic.Bool
}

func newCSEDumperPool(ctx context.Context, size int, executable, metadataURL string) (*cseDumperPool, error) {
	poolCtx, cancel := context.WithCancel(ctx)
	pool := &cseDumperPool{
		ctx:        poolCtx,
		cancel:     cancel,
		executable: executable,
		metadata:   metadataURL,
		clients:    make(chan *cseDumperClient, size),
	}
	for range size {
		client, err := startCSEDumperClient(poolCtx, executable, metadataURL)
		if err != nil {
			pool.close()
			return nil, err
		}
		pool.clients <- client
	}
	return pool, nil
}

func (p *cseDumperPool) acquire(ctx context.Context) (*cseDumperClient, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-p.ctx.Done():
		return nil, p.ctx.Err()
	case client := <-p.clients:
		return client, nil
	}
}

func (p *cseDumperPool) release(client *cseDumperClient, healthy bool) error {
	if client == nil {
		return nil
	}
	if p.closed.Load() {
		return client.close()
	}
	if !healthy {
		_ = client.close()
		var err error
		client, err = startCSEDumperClient(p.ctx, p.executable, p.metadata)
		if err != nil {
			return err
		}
	}
	select {
	case <-p.ctx.Done():
		return client.close()
	case p.clients <- client:
		return nil
	}
}

func (p *cseDumperPool) schema(ctx context.Context) (*packedManifest, error) {
	client, err := p.acquire(ctx)
	if err != nil {
		return nil, err
	}
	manifest, err := client.schema()
	releaseErr := p.release(client, err == nil)
	if err != nil {
		return nil, err
	}
	return manifest, releaseErr
}

func (p *cseDumperPool) close() {
	if !p.closed.CompareAndSwap(false, true) {
		return
	}
	p.cancel()
	for {
		select {
		case client := <-p.clients:
			_ = client.close()
		default:
			return
		}
	}
}

func readPackedUint32(input io.Reader) (uint32, error) {
	var data [4]byte
	if _, err := io.ReadFull(input, data[:]); err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint32(data[:]), nil
}

func writePackedUint32(output io.Writer, value uint32) error {
	var data [4]byte
	binary.LittleEndian.PutUint32(data[:], value)
	_, err := output.Write(data[:])
	return err
}

func (m *packedManifest) String() string {
	return fmt.Sprintf("CSE packed backup cluster=%d keyspace=%d read-ts=%d", m.ClusterID, m.KeyspaceID, m.ReadTS)
}
