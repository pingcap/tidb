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
	"context"
	"encoding/binary"
	"encoding/hex"
	"io"
	"os/exec"
	"sync"
	"sync/atomic"

	"github.com/pingcap/errors"
)

type cseDumperScan struct {
	cmd         *exec.Cmd
	input       *bufio.Reader
	stderr      cseDumperStderr
	observation cseDumperProcessObservation
	finished    atomic.Bool
	done        chan struct{}
	waitOnce    sync.Once
	waitErr     error
}

func startCSEDumperScan(
	ctx context.Context,
	executable, metadataURL string,
	legacyEncryption bool,
	startKey, endKey []byte,
) (*cseDumperScan, error) {
	args := cseDumperArgs(metadataURL, legacyEncryption, startKey, endKey)
	cmd := exec.CommandContext(ctx, executable, args...)
	cmd.Env = append(cmd.Environ(), cseDumperObservabilityEnv+"=1")
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, errors.Annotate(err, "open cse-ctl dumper stdout")
	}
	scan := &cseDumperScan{
		cmd:         cmd,
		input:       bufio.NewReaderSize(stdout, 256*1024),
		observation: newCSEDumperProcessObservation(),
		done:        make(chan struct{}),
	}
	cmd.Stderr = &scan.stderr
	if err := cmd.Start(); err != nil {
		return nil, errors.Annotatef(err, "start %q dumper", executable)
	}
	scan.observation.recordSpawn()
	return scan, nil
}

func cseDumperArgs(
	metadataURL string,
	legacyEncryption bool,
	startKey, endKey []byte,
) []string {
	args := []string{
		"dumper",
		"--metadata-url", metadataURL,
		"--start-key-hex", hex.EncodeToString(startKey),
		"--end-key-hex", hex.EncodeToString(endKey),
	}
	if legacyEncryption {
		args = append(args, "--legacy-encryption")
	}
	return args
}

func (s *cseDumperScan) readRow(keyBuffer, valueBuffer []byte) (key, value []byte, end bool, err error) {
	key, value, end, err = s.observation.readRow(s.input, keyBuffer, valueBuffer)
	if err != nil {
		return nil, nil, false, s.fail(err)
	}
	if !end {
		return key, value, false, nil
	}
	s.finished.Store(true)
	if err := s.wait(); err != nil {
		return nil, nil, false, s.exitError(err)
	}
	return nil, nil, true, nil
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

func (s *cseDumperScan) fail(streamErr error) error {
	if s.cmd.Process != nil {
		_ = s.cmd.Process.Kill()
	}
	waitErr := s.wait()
	stderr, _ := s.stderr.snapshot()
	detail := cseDumperHumanStderr(stderr)
	if len(detail) > 0 {
		return errors.Annotatef(streamErr, "cse-ctl dumper stderr: %s", detail)
	}
	if waitErr != nil {
		return errors.Annotatef(streamErr, "cse-ctl dumper exited: %v", waitErr)
	}
	return streamErr
}

func (s *cseDumperScan) exitError(waitErr error) error {
	stderr, _ := s.stderr.snapshot()
	detail := cseDumperHumanStderr(stderr)
	if len(detail) > 0 {
		return errors.Annotatef(waitErr, "cse-ctl dumper stderr: %s", detail)
	}
	return errors.Annotate(waitErr, "cse-ctl dumper exited")
}

func (s *cseDumperScan) wait() error {
	s.waitOnce.Do(func() {
		defer close(s.done)
		s.waitErr = s.observation.wait(s.cmd.Wait)
	})
	return s.waitErr
}

func (s *cseDumperScan) close() error {
	if !s.finished.Load() && s.cmd.Process != nil {
		_ = s.cmd.Process.Kill()
	}
	waitErr := s.wait()
	if s.finished.Load() && waitErr != nil {
		return s.exitError(waitErr)
	}
	return nil
}

func scanCSEDumperRange(
	ctx context.Context,
	executable, metadataURL string,
	legacyEncryption bool,
	startKey, endKey []byte,
	emit func(key, value []byte) error,
	observer *cseDumperScanObserver,
) (resultErr error) {
	scan, err := startCSEDumperScan(
		ctx,
		executable,
		metadataURL,
		legacyEncryption,
		startKey,
		endKey,
	)
	if err != nil {
		return err
	}
	if observer != nil && observer.started != nil {
		observer.started(scan)
	}
	defer func() {
		if closeErr := scan.close(); resultErr == nil && closeErr != nil {
			resultErr = closeErr
		}
		if observer != nil && observer.finished != nil {
			observer.finished(scan, resultErr)
		}
	}()
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

func readPackedUint32(input io.Reader) (uint32, error) {
	var data [4]byte
	if _, err := io.ReadFull(input, data[:]); err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint32(data[:]), nil
}
