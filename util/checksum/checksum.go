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

package checksum

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"sync"
)

const (
	// the size of whole checksum block
	checksumBlockSize = 1024
	// the size of checksum field, we use CRC-32 algorithm to generate a 4 bytes checksum
	checksumSize = 4
	// the size of the payload of a checksum block
	checksumPayloadSize = checksumBlockSize - checksumSize
)

var checksumReaderBufPool = sync.Pool{
	New: func() interface{} { return make([]byte, checksumBlockSize) },
}

// Writer implements an io.WriteCloser, it calculates and stores a CRC-32 checksum for the payload before
// writing to the underlying object.
//
// For example, a layout of the checksum block which payload is 2100 bytes is as follow:
//
// | --    4B    -- | --  1020B  -- || --    4B    -- | --  1020B  -- || --    4B    -- | --   60B   -- |
// | -- checksum -- | -- payload -- || -- checksum -- | -- payload -- || -- checksum -- | -- payload -- |
type Writer struct {
	err         error
	w           io.WriteCloser
	buf         []byte
	payload     []byte
	payloadUsed int
}

// NewWriter returns a new Writer which calculates and stores a CRC-32 checksum for the payload before
// writing to the underlying object.
func NewWriter(w io.WriteCloser) *Writer {
	checksumWriter := &Writer{w: w}
	checksumWriter.buf = make([]byte, checksumBlockSize)
	checksumWriter.payload = checksumWriter.buf[checksumSize:]
	checksumWriter.payloadUsed = 0
	return checksumWriter
}

// AvailableSize returns how many bytes are unused in the buffer.
func (w *Writer) AvailableSize() int { return checksumPayloadSize - w.payloadUsed }

// Write implements the io.Writer interface.
func (w *Writer) Write(p []byte) (n int, err error) {
	for len(p) > w.AvailableSize() && w.err == nil {
		copiedNum := copy(w.payload[w.payloadUsed:], p)
		w.payloadUsed += copiedNum
		err = w.Flush()
		if err != nil {
			return
		}
		n += copiedNum
		p = p[copiedNum:]
	}
	if w.err != nil {
		return n, w.err
	}
	copiedNum := copy(w.payload[w.payloadUsed:], p)
	w.payloadUsed += copiedNum
	n += copiedNum
	return
}

// Buffered returns the number of bytes that have been written into the current buffer.
func (w *Writer) Buffered() int { return w.payloadUsed }

// Flush writes all the buffered data to the underlying object.
func (w *Writer) Flush() error {
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
func (w *Writer) Close() (err error) {
	err = w.Flush()
	if err != nil {
		return
	}
	return w.w.Close()
}

// Reader implements an io.ReadAt, reading from the input source after verifying the checksum.
type Reader struct {
	r io.ReaderAt
}

// NewReader returns a new Reader which can read from the input source after verifying the checksum.
func NewReader(r io.ReaderAt) *Reader {
	checksumReader := &Reader{r: r}
	return checksumReader
}

var errChecksumFail = errors.New("error checksum")

// ReadAt implements the io.ReadAt interface.
func (r *Reader) ReadAt(p []byte, off int64) (nn int, err error) {
	if len(p) == 0 {
		return 0, nil
	}
	offsetInPayload := off % checksumPayloadSize
	cursor := off / checksumPayloadSize * checksumBlockSize

	buf := checksumReaderBufPool.Get().([]byte)
	defer checksumReaderBufPool.Put(buf)

	var n int
	for len(p) > 0 && err == nil {
		n, err = r.r.ReadAt(buf, cursor)
		if err != nil {
			if n == 0 || err != io.EOF {
				return nn, err
			}
			err = nil
			// continue if n > 0 and r.err is io.EOF
		}
		if n < checksumSize {
			return nn, errChecksumFail
		}
		cursor += int64(n)
		originChecksum := binary.LittleEndian.Uint32(buf)
		checksum := crc32.Checksum(buf[checksumSize:n], crc32.MakeTable(crc32.IEEE))
		if originChecksum != checksum {
			return nn, errChecksumFail
		}
		n1 := copy(p, buf[checksumSize+offsetInPayload:n])
		nn += n1
		p = p[n1:]
		offsetInPayload = 0
	}
	return nn, err
}
