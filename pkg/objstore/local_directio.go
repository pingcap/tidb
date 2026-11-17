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
	"math"
	"os"

	"github.com/docker/go-units"
	"github.com/ncw/directio"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/objstore/objectio"
	"go.uber.org/zap"
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

func newLocalDirectWriter(filename string, partSize int64) (objectio.Writer, error) {
	const blockSize = int64(directio.BlockSize)
	// Reserve space for both rounding and AlignedBlock's alignment allocation.
	if partSize > math.MaxInt-int64(directio.AlignSize)-(blockSize-1) {
		return nil, errors.New("local direct I/O part size is too large")
	}
	bufferSize := int64(units.MiB)
	if partSize > 0 {
		bufferSize = (partSize + blockSize - 1) / blockSize * blockSize
	}
	openFile := directio.OpenFile
	failpoint.InjectCall("openLocalDirectIO", &openFile)
	file, err := openFile(filename, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o666)
	if err != nil {
		// Some platforms do not preserve errno when enabling direct I/O fails.
		// Retry a normal open, which still reports genuine file-creation errors.
		if pathErr, ok := err.(*os.PathError); ok {
			// A local filename can contain sensitive user-supplied information.
			err = pathErr.Err
		}
		log.Warn("failed to open local file with direct I/O, falling back to buffered I/O", zap.Error(err))
		return newLocalBufferedWriter(filename, partSize)
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
		// Close here despite the usual Write/Close separation: BufferedWriter.Close
		// returns after a failed final upload without closing its underlying writer.
		// Making that wrapper always close is unsafe for multipart backends, whose
		// Close commits uploaded parts and could publish a truncated object after
		// an upload failure. Release this local descriptor eagerly and preserve the
		// write error; our Close can then return it without closing the file again.
		// TODO: Refactor wrapper error handling to safely propagate Close, then move
		// descriptor cleanup from Write to Close.
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
			closeErr := w.file.Close()
			if err == nil {
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
