// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package compressedio

import (
	"bytes"

	"github.com/pingcap/errors"
)

// Buffer a compressed buffer.
type Buffer struct {
	*bytes.Buffer
	compressWriter Writer
	cap            int
}

// Write implements objectio.interceptBuffer.
func (b *Buffer) Write(p []byte) (int, error) {
	written, err := b.compressWriter.Write(p)
	return written, errors.Trace(err)
}

// Len implements objectio.interceptBuffer.
func (b *Buffer) Len() int {
	return b.Buffer.Len()
}

// Cap implements objectio.interceptBuffer.
func (b *Buffer) Cap() int {
	return b.cap
}

// Reset implements objectio.interceptBuffer.
func (b *Buffer) Reset() {
	b.Buffer.Reset()
}

// Flush implements objectio.interceptBuffer.
func (b *Buffer) Flush() error {
	return b.compressWriter.Flush()
}

// Close implements objectio.interceptBuffer.
func (b *Buffer) Close() error {
	return b.compressWriter.Close()
}

// Compressed implements objectio.interceptBuffer.
func (*Buffer) Compressed() bool {
	return true
}

// NewBuffer creates a new Buffer.
func NewBuffer(chunkSize int, compressType CompressType) *Buffer {
	bf := bytes.NewBuffer(make([]byte, 0, chunkSize))
	return &Buffer{
		Buffer:         bf,
		cap:            chunkSize,
		compressWriter: NewWriter(compressType, bf),
	}
}
