// Copyright 2020 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package chunk

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"sync"
)

const (
	checksumBlockSize   = 1024
	checksumSize        = 4
	checksumPayloadSize = checksumBlockSize - checksumSize
)

var checksumReaderBufPool = sync.Pool{
	New: func() interface{} { return make([]byte, checksumBlockSize) },
}

// checksumWriter implements an io.WriteCloser, it calculates and stores a CRC-32 checksum for the payload before
// writing to the underlying object.
type checksumWriter struct {
	err         error
	w           io.WriteCloser
	buf         []byte
	payload     []byte
	payloadUsed int
}

func newChecksumWriter(w io.WriteCloser) *checksumWriter {
	checksumWriter := &checksumWriter{w: w}
	checksumWriter.buf = make([]byte, checksumBlockSize)
	checksumWriter.payload = checksumWriter.buf[checksumSize:]
	checksumWriter.payloadUsed = 0
	return checksumWriter
}

// Available returns how many bytes are unused in the buffer.
func (w *checksumWriter) Available() int { return checksumPayloadSize - w.payloadUsed }

// Write implements the io.Writer interface.
func (w *checksumWriter) Write(p []byte) (nn int, err error) {
	for len(p) > w.Available() && w.err == nil {
		n := copy(w.payload[w.payloadUsed:], p)
		w.payloadUsed += n
		err = w.Flush()
		if err != nil {
			return
		}
		nn += n
		p = p[n:]
	}
	if w.err != nil {
		return nn, w.err
	}
	n := copy(w.payload[w.payloadUsed:], p)
	w.payloadUsed += n
	nn += n
	return
}

// Buffered returns the number of bytes that have been written into the current buffer.
func (w *checksumWriter) Buffered() int { return w.payloadUsed }

// Flush writes any buffered data to the disk.
func (w *checksumWriter) Flush() error {
	if w.err != nil {
		return w.err
	}
	if w.payloadUsed == 0 {
		return nil
	}
	checksum := crc32.Checksum(w.payload[:w.payloadUsed], crc32.MakeTable(crc32.IEEE))
	binary.LittleEndian.PutUint32(w.buf, checksum)
	n, err := w.w.Write(w.buf[:w.payloadUsed+checksumSize])
	if n < w.payloadUsed && err == nil {
		err = io.ErrShortWrite
	}
	if err != nil {
		w.err = err
		return err
	}
	w.payloadUsed = 0
	return nil
}

// Close implements the io.Closer interface.
func (w *checksumWriter) Close() (err error) {
	err = w.Flush()
	if err != nil {
		return
	}
	return w.w.Close()
}

// checksumReader implements an io.ReadAt, reading from the input source after verifying the checksum.
type checksumReader struct {
	r   io.ReaderAt
	err error
}

func newChecksumReader(r io.ReaderAt) *checksumReader {
	checksumReader := &checksumReader{r: r}
	return checksumReader
}

func (r *checksumReader) readErr() error {
	err := r.err
	r.err = nil
	return err
}

// Read implements the io.ReadAt interface.
func (r *checksumReader) ReadAt(p []byte, off int64) (nn int, err error) {
	if len(p) == 0 {
		return 0, nil
	}
	offsetInPayload := off % checksumPayloadSize
	cursor := off / checksumPayloadSize * checksumBlockSize

	buf := checksumReaderBufPool.Get().([]byte)
	defer checksumReaderBufPool.Put(buf)

	var n int
	for len(p) > 0 && r.err == nil {
		n, r.err = r.r.ReadAt(buf, cursor)
		if r.err != nil {
			if n == 0 {
				return nn, r.readErr()
			}
			// continue if n > 0
		}
		cursor += int64(n)
		originChecksum := binary.LittleEndian.Uint32(buf)
		checksum := crc32.Checksum(buf[checksumSize:n], crc32.MakeTable(crc32.IEEE))
		if originChecksum != checksum {
			return nn, errors.New("error checksum")
		}
		n1 := copy(p, buf[checksumSize+offsetInPayload:n])
		nn += n1
		p = p[n1:]
		offsetInPayload = 0
	}
	return nn, r.readErr()
}
