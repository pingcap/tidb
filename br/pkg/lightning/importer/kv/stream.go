// Copyright 2022 PingCAP, Inc.
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

package kv

import (
	"bufio"
	"encoding/binary"
	"io"
)

// StreamReader reads key-value pairs from a stream.
// The stream is expected to be in the format of:
//
//	<key-len><key><val-len><val>
//
// The key-len and val-len are encoded as varints.
// See binary.Varint for details.
type StreamReader struct {
	r    io.ReadCloser
	bufr *bufio.Reader
	key  []byte
	val  []byte
}

// NewStreamReader creates a new StreamReader.
func NewStreamReader(r io.ReadCloser) *StreamReader {
	return &StreamReader{
		r:    r,
		bufr: bufio.NewReaderSize(r, 1<<20),
	}
}

func (sr *StreamReader) Read() (key, val []byte, err error) {
	keyLen, err := binary.ReadUvarint(sr.bufr)
	if err != nil {
		if err == io.EOF {
			err = nil
		}
		return nil, nil, err
	}
	sr.key = resizeBytes(sr.key, int(keyLen))
	if _, err := io.ReadFull(sr.bufr, sr.key); err != nil {
		return nil, nil, err
	}
	valLen, err := binary.ReadUvarint(sr.bufr)
	if err != nil {
		return nil, nil, err
	}
	sr.val = resizeBytes(sr.val, int(valLen))
	if _, err := io.ReadFull(sr.bufr, sr.val); err != nil {
		return nil, nil, err
	}
	return sr.key, sr.val, nil
}

func (sr *StreamReader) Close() error {
	return sr.r.Close()
}

// StreamWriter is a writer that writes key-value pairs to a stream.
// The stream is in the format of:
//
//	<key-len><key><val-len><val>
//
// The key-len and val-len are encoded as varints.
// See binary.Varint for details.
type StreamWriter struct {
	w         io.WriteCloser
	bufw      *bufio.Writer
	varintBuf [binary.MaxVarintLen64]byte
}

// NewStreamWriter creates a new StreamWriter.
func NewStreamWriter(w io.WriteCloser) *StreamWriter {
	return &StreamWriter{
		w:    w,
		bufw: bufio.NewWriterSize(w, 1<<20),
	}
}

func (sw *StreamWriter) Write(key, val []byte) error {
	keyLen := binary.PutUvarint(sw.varintBuf[:], uint64(len(key)))
	if _, err := sw.bufw.Write(sw.varintBuf[:keyLen]); err != nil {
		return err
	}
	if _, err := sw.bufw.Write(key); err != nil {
		return err
	}
	valLen := binary.PutUvarint(sw.varintBuf[:], uint64(len(val)))
	if _, err := sw.bufw.Write(sw.varintBuf[:valLen]); err != nil {
		return err
	}
	if _, err := sw.bufw.Write(val); err != nil {
		return err
	}
	return nil
}

func (sw *StreamWriter) Close() error {
	err := sw.bufw.Flush()
	if closeErr := sw.w.Close(); err == nil {
		err = closeErr
	}
	return err
}

func resizeBytes(b []byte, n int) []byte {
	if cap(b) < n {
		return make([]byte, n)
	}
	return b[:n]
}
