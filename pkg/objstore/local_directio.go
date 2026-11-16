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

package objstore

import (
	"context"
	"io"
	"os"

	"github.com/ncw/directio"
	"github.com/pingcap/errors"
)

// localDirectWriter always copies into aligned memory. bufio.Writer cannot be
// used here because it may pass a large, unaligned caller buffer to the file.
type localDirectWriter struct {
	file   *os.File
	buf    []byte
	used   int
	size   int64
	err    error
	closed bool
}

func newLocalDirectWriter(filename string, partSize int64) (*localDirectWriter, error) {
	const blockSize = int64(directio.BlockSize)
	maxInt := int64(int(^uint(0) >> 1))
	// Reserve space for both rounding and AlignedBlock's alignment allocation.
	if partSize > maxInt-int64(directio.AlignSize)-(blockSize-1) {
		return nil, errors.New("local direct I/O part size is too large")
	}
	bufferSize := int64(1 << 20)
	if partSize > 0 {
		bufferSize = (partSize + blockSize - 1) / blockSize * blockSize
	}
	file, err := directio.OpenFile(filename, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o666)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &localDirectWriter{file: file, buf: directio.AlignedBlock(int(bufferSize))}, nil
}

func (w *localDirectWriter) Write(_ context.Context, data []byte) (int, error) {
	if w.err != nil {
		return 0, w.err
	}
	if w.closed {
		return 0, errors.Trace(os.ErrClosed)
	}
	written := 0
	for len(data) > 0 {
		n := copy(w.buf[w.used:], data)
		w.used += n
		w.size += int64(n)
		written += n
		data = data[n:]
		if w.used == len(w.buf) {
			if err := w.flush(w.buf); err != nil {
				return written, err
			}
			w.used = 0
		}
	}
	return written, nil
}

func (w *localDirectWriter) flush(data []byte) error {
	n, err := w.file.Write(data)
	if err == nil && n != len(data) {
		err = io.ErrShortWrite
	}
	if err != nil {
		w.err = errors.Trace(err)
		// Compression wrappers may stop without calling Close after a failed
		// Write. Release the descriptor here and keep the original write error.
		_ = w.file.Close()
		w.closed = true
		w.buf = nil
	}
	return w.err
}

func (w *localDirectWriter) Close(_ context.Context) (err error) {
	if w.err != nil {
		return w.err
	}
	if w.closed {
		return errors.Trace(os.ErrClosed)
	}
	defer func() {
		if !w.closed {
			if closeErr := w.file.Close(); err == nil {
				err = errors.Trace(closeErr)
			}
		}
		w.closed = true
		w.buf = nil
		w.err = err
	}()
	if w.used > 0 {
		padded := (w.used + directio.BlockSize - 1) / directio.BlockSize * directio.BlockSize
		clear(w.buf[w.used:padded])
		if err := w.flush(w.buf[:padded]); err != nil {
			return err
		}
	}
	// Padding is only an I/O alignment detail, never part of the stored object.
	return errors.Trace(w.file.Truncate(w.size))
}
