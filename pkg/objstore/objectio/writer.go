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

package objectio

import (
	"bytes"
	"context"
	"io"

	"github.com/klauspost/compress/gzip"
	"github.com/klauspost/compress/snappy"
	"github.com/klauspost/compress/zstd"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/objstore/recording"
	"go.uber.org/zap"
)

// CompressType represents the type of compression.
type CompressType uint8

const (
	// NoCompression won't compress given bytes.
	NoCompression CompressType = iota
	// Gzip will compress given bytes in gzip format.
	Gzip
	// Snappy will compress given bytes in snappy format.
	Snappy
	// Zstd will compress given bytes in zstd format.
	Zstd
)

// DecompressConfig is the config used for decompression.
type DecompressConfig struct {
	// ZStdDecodeConcurrency only used for ZStd decompress, see WithDecoderConcurrency.
	// if not 1, ZStd will decode file asynchronously.
	ZStdDecodeConcurrency int
}

type Flusher interface {
	Flush() error
}

// EmptyFlusher empty Flusher.
type EmptyFlusher struct{}

// Flush do flush.
func (*EmptyFlusher) Flush() error {
	return nil
}

type interceptBuffer interface {
	io.WriteCloser
	Flusher
	Len() int
	Cap() int
	Bytes() []byte
	Reset()
	Compressed() bool
}

func newInterceptBuffer(chunkSize int, compressType CompressType) interceptBuffer {
	if compressType == NoCompression {
		return newNoCompressionBuffer(chunkSize)
	}
	return newSimpleCompressBuffer(chunkSize, compressType)
}

// NewCompressWriter creates a compress writer
func NewCompressWriter(compressType CompressType, w io.Writer) SimpleCompressWriter {
	switch compressType {
	case Gzip:
		return gzip.NewWriter(w)
	case Snappy:
		return snappy.NewBufferedWriter(w)
	case Zstd:
		newWriter, err := zstd.NewWriter(w)
		if err != nil {
			log.Warn("Met error when creating new writer for Zstd type file", zap.Error(err))
		}
		return newWriter
	default:
		return nil
	}
}

// NewCompressReader read compressed data.
// only for test now.
func NewCompressReader(compressType CompressType, cfg DecompressConfig, r io.Reader) (io.Reader, error) {
	switch compressType {
	case Gzip:
		return gzip.NewReader(r)
	case Snappy:
		return snappy.NewReader(r), nil
	case Zstd:
		options := []zstd.DOption{}
		if cfg.ZStdDecodeConcurrency > 0 {
			options = append(options, zstd.WithDecoderConcurrency(cfg.ZStdDecodeConcurrency))
		}
		return zstd.NewReader(r, options...)
	default:
		return nil, nil
	}
}

type noCompressionBuffer struct {
	*bytes.Buffer
}

func (*noCompressionBuffer) Flush() error {
	return nil
}

func (*noCompressionBuffer) Close() error {
	return nil
}

func (*noCompressionBuffer) Compressed() bool {
	return false
}

func newNoCompressionBuffer(chunkSize int) *noCompressionBuffer {
	return &noCompressionBuffer{bytes.NewBuffer(make([]byte, 0, chunkSize))}
}

type SimpleCompressWriter interface {
	io.WriteCloser
	Flusher
}

type simpleCompressBuffer struct {
	*bytes.Buffer
	compressWriter SimpleCompressWriter
	cap            int
}

func (b *simpleCompressBuffer) Write(p []byte) (int, error) {
	written, err := b.compressWriter.Write(p)
	return written, errors.Trace(err)
}

func (b *simpleCompressBuffer) Len() int {
	return b.Buffer.Len()
}

func (b *simpleCompressBuffer) Cap() int {
	return b.cap
}

func (b *simpleCompressBuffer) Reset() {
	b.Buffer.Reset()
}

func (b *simpleCompressBuffer) Flush() error {
	return b.compressWriter.Flush()
}

func (b *simpleCompressBuffer) Close() error {
	return b.compressWriter.Close()
}

func (*simpleCompressBuffer) Compressed() bool {
	return true
}

func newSimpleCompressBuffer(chunkSize int, compressType CompressType) *simpleCompressBuffer {
	bf := bytes.NewBuffer(make([]byte, 0, chunkSize))
	return &simpleCompressBuffer{
		Buffer:         bf,
		cap:            chunkSize,
		compressWriter: NewCompressWriter(compressType, bf),
	}
}

// BufferedWriter is a buffered writer
type BufferedWriter struct {
	buf       interceptBuffer
	writer    Writer
	accessRec *recording.AccessStats
}

// Write implements objstoreapi.Writer.
func (u *BufferedWriter) Write(ctx context.Context, p []byte) (int, error) {
	n, err := u.write0(ctx, p)
	u.accessRec.RecWrite(n)
	return n, errors.Trace(err)
}

func (u *BufferedWriter) write0(ctx context.Context, p []byte) (int, error) {
	bytesWritten := 0
	for u.buf.Len()+len(p) > u.buf.Cap() {
		// We won't fit p in this chunk

		// Is this chunk full?
		chunkToFill := u.buf.Cap() - u.buf.Len()
		if chunkToFill > 0 {
			// It's not full so we write enough of p to fill it
			prewrite := p[0:chunkToFill]
			w, err := u.buf.Write(prewrite)
			bytesWritten += w
			if err != nil {
				return bytesWritten, errors.Trace(err)
			}
			p = p[w:]
			// continue buf because compressed data size may be less than Cap - Len
			if u.buf.Compressed() {
				continue
			}
		}
		_ = u.buf.Flush()
		err := u.uploadChunk(ctx)
		if err != nil {
			return 0, errors.Trace(err)
		}
	}
	w, err := u.buf.Write(p)
	bytesWritten += w
	return bytesWritten, errors.Trace(err)
}

func (u *BufferedWriter) uploadChunk(ctx context.Context) error {
	if u.buf.Len() == 0 {
		return nil
	}
	b := u.buf.Bytes()
	u.buf.Reset()
	_, err := u.writer.Write(ctx, b)
	return errors.Trace(err)
}

// Close implements objstoreapi.Writer.
func (u *BufferedWriter) Close(ctx context.Context) error {
	u.buf.Close()
	err := u.uploadChunk(ctx)
	if err != nil {
		return errors.Trace(err)
	}
	return u.writer.Close(ctx)
}

// GetWriter get the underlying writer.
func (u *BufferedWriter) GetWriter() Writer {
	return u.writer
}

// NewUploaderWriter wraps the Writer interface over an uploader.
func NewUploaderWriter(writer Writer, chunkSize int, compressType CompressType) Writer {
	return NewBufferedWriter(writer, chunkSize, compressType, nil)
}

// NewBufferedWriter is used to build a buffered writer.
func NewBufferedWriter(writer Writer, chunkSize int, compressType CompressType, accessRec *recording.AccessStats) *BufferedWriter {
	return &BufferedWriter{
		writer:    writer,
		buf:       newInterceptBuffer(chunkSize, compressType),
		accessRec: accessRec,
	}
}

// BytesWriter is a Writer implementation on top of bytes.Buffer that is useful for testing.
type BytesWriter struct {
	buf *bytes.Buffer
}

// Write delegates to bytes.Buffer.
func (u *BytesWriter) Write(_ context.Context, p []byte) (int, error) {
	return u.buf.Write(p)
}

// Close delegates to bytes.Buffer.
func (*BytesWriter) Close(_ context.Context) error {
	// noop
	return nil
}

// Bytes delegates to bytes.Buffer.
func (u *BytesWriter) Bytes() []byte {
	return u.buf.Bytes()
}

// String delegates to bytes.Buffer.
func (u *BytesWriter) String() string {
	return u.buf.String()
}

// Reset delegates to bytes.Buffer.
func (u *BytesWriter) Reset() {
	u.buf.Reset()
}

// NewBufferWriter creates a Writer that simply writes to a buffer (useful for testing).
func NewBufferWriter() *BytesWriter {
	return &BytesWriter{buf: &bytes.Buffer{}}
}
