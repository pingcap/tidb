package storage

import (
	"bytes"
	"context"
	"io"

	"github.com/klauspost/compress/gzip"
	"github.com/klauspost/compress/snappy"
	"github.com/klauspost/compress/zstd"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
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

type flusher interface {
	Flush() error
}

type emptyFlusher struct{}

func (*emptyFlusher) Flush() error {
	return nil
}

type interceptBuffer interface {
	io.WriteCloser
	flusher
	Len() int
	Cap() int
	Bytes() []byte
	Reset()
	Compressed() bool
}

func createSuffixString(compressType CompressType) string {
	txtSuffix := ".txt"
	switch compressType {
	case Gzip:
		txtSuffix += ".gz"
	case Snappy:
		txtSuffix += ".snappy"
	case Zstd:
		txtSuffix += ".zst"
	default:
		return ""
	}
	return txtSuffix
}

func newInterceptBuffer(chunkSize int, compressType CompressType) interceptBuffer {
	if compressType == NoCompression {
		return newNoCompressionBuffer(chunkSize)
	}
	return newSimpleCompressBuffer(chunkSize, compressType)
}

func newCompressWriter(compressType CompressType, w io.Writer) simpleCompressWriter {
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

func newCompressReader(compressType CompressType, cfg DecompressConfig, r io.Reader) (io.Reader, error) {
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

type simpleCompressWriter interface {
	io.WriteCloser
	flusher
}

type simpleCompressBuffer struct {
	*bytes.Buffer
	compressWriter simpleCompressWriter
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
		compressWriter: newCompressWriter(compressType, bf),
	}
}

type bufferedWriter struct {
	buf    interceptBuffer
	writer ExternalFileWriter
}

func (u *bufferedWriter) Write(ctx context.Context, p []byte) (int, error) {
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

func (u *bufferedWriter) uploadChunk(ctx context.Context) error {
	if u.buf.Len() == 0 {
		return nil
	}
	b := u.buf.Bytes()
	u.buf.Reset()
	_, err := u.writer.Write(ctx, b)
	return errors.Trace(err)
}

func (u *bufferedWriter) Close(ctx context.Context) error {
	u.buf.Close()
	err := u.uploadChunk(ctx)
	if err != nil {
		return errors.Trace(err)
	}
	return u.writer.Close(ctx)
}

// NewUploaderWriter wraps the Writer interface over an uploader.
func NewUploaderWriter(writer ExternalFileWriter, chunkSize int, compressType CompressType) ExternalFileWriter {
	return newBufferedWriter(writer, chunkSize, compressType)
}

// newBufferedWriter is used to build a buffered writer.
func newBufferedWriter(writer ExternalFileWriter, chunkSize int, compressType CompressType) *bufferedWriter {
	return &bufferedWriter{
		writer: writer,
		buf:    newInterceptBuffer(chunkSize, compressType),
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
