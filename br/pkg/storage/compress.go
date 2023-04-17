// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package storage

import (
	"bytes"
	"context"
	"io"

	"github.com/pingcap/errors"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
)

type withCompression struct {
	ExternalStorage
	compressType CompressType
}

// WithCompression returns an ExternalStorage with compress option
func WithCompression(inner ExternalStorage, compressionType CompressType) ExternalStorage {
	if compressionType == NoCompression {
		return inner
	}
	return &withCompression{ExternalStorage: inner, compressType: compressionType}
}

func (w *withCompression) Create(ctx context.Context, name string) (ExternalFileWriter, *Error) {
	var (
		writer ExternalFileWriter
		err    error
	)
	if s3Storage, ok := w.ExternalStorage.(*S3Storage); ok {
		writer, err = s3Storage.CreateUploader(ctx, name)
	} else {
		writer, err = w.ExternalStorage.Create(ctx, name)
	}
	if err != nil {
		return nil, NewSimpleError(err)
	}
	compressedWriter := newBufferedWriter(writer, hardcodedS3ChunkSize, w.compressType)
	return compressedWriter, nil
}

func (w *withCompression) Open(ctx context.Context, path string) (ExternalFileReader, *Error) {
	fileReader, err := w.ExternalStorage.Open(ctx, path)
	if err != nil {
		return nil, NewSimpleError(err)
	}
	uncompressReader, err2 := InterceptDecompressReader(fileReader, w.compressType)
	if err2 != nil {
		return nil, NewSimpleError(err2)
	}
	return uncompressReader, nil
}

func (w *withCompression) WriteFile(ctx context.Context, name string, data []byte) *Error {
	bf := bytes.NewBuffer(make([]byte, 0, len(data)))
	compressBf := newCompressWriter(w.compressType, bf)
	_, err := compressBf.Write(data)
	if err != nil {
		return NewSimpleError(err)
	}
	err = compressBf.Close()
	if err != nil {
		return NewSimpleError(err)
	}
	return w.ExternalStorage.WriteFile(ctx, name, bf.Bytes())
}

func (w *withCompression) ReadFile(ctx context.Context, name string) ([]byte, *Error) {
	data, err := w.ExternalStorage.ReadFile(ctx, name)
	if err != nil {
		return data, NewSimpleError(err)
	}
	bf := bytes.NewBuffer(data)
	compressBf, err2 := newCompressReader(w.compressType, bf)
	if err2 != nil {
		return nil, NewSimpleError(err2)
	}
	bs, err3 := io.ReadAll(compressBf)
	if err3 != nil {
		return nil, NewSimpleError(err3)
	}
	return bs, nil
}

// compressReader is a wrapper for compress.Reader
type compressReader struct {
	io.Reader
	io.Seeker
	io.Closer
}

// InterceptDecompressReader intercepts the reader and wraps it with a decompress
// reader on the given io.ReadSeekCloser. Note that the returned
// io.ReadSeekCloser does not have the property that Seek(0, io.SeekCurrent)
// equals total bytes Read() if the decompress reader is used.
func InterceptDecompressReader(fileReader io.ReadSeekCloser, compressType CompressType) (io.ReadSeekCloser, error) {
	if compressType == NoCompression {
		return fileReader, nil
	}
	r, err := newCompressReader(compressType, fileReader)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &compressReader{
		Reader: r,
		Closer: fileReader,
		Seeker: fileReader,
	}, nil
}

func NewLimitedInterceptReader(fileReader ExternalFileReader, compressType CompressType, n int64) (ExternalFileReader, error) {
	newFileReader := fileReader
	if n < 0 {
		return nil, NewErrorWithMessage(berrors.ErrStorageInvalidConfig, "compressReader doesn't support negative limit, n: %d", n)
	} else if n > 0 {
		newFileReader = &compressReader{
			Reader: io.LimitReader(fileReader, n),
			Seeker: fileReader,
			Closer: fileReader,
		}
	}
	return InterceptDecompressReader(newFileReader, compressType)
}

func (c *compressReader) Seek(offset int64, whence int) (int64, error) {
	// only support get original reader's current offset
	if offset == 0 && whence == io.SeekCurrent {
		return c.Seeker.Seek(offset, whence)
	}
	return int64(0), NewErrorWithMessage(berrors.ErrStorageInvalidConfig, "compressReader doesn't support Seek now, offset %d, whence %d", offset, whence)
}

func (c *compressReader) Close() error {
	err := c.Closer.Close()
	return err
}

type flushStorageWriter struct {
	writer  io.Writer
	flusher flusher
	closer  io.Closer
}

func (w *flushStorageWriter) Write(_ context.Context, data []byte) (int, error) {
	n, err := w.writer.Write(data)
	return n, errors.Trace(err)
}

func (w *flushStorageWriter) Close(_ context.Context) error {
	err := w.flusher.Flush()
	if err != nil {
		return errors.Trace(err)
	}
	return w.closer.Close()
}

func newFlushStorageWriter(writer io.Writer, flusher2 flusher, closer io.Closer) *flushStorageWriter {
	return &flushStorageWriter{
		writer:  writer,
		flusher: flusher2,
		closer:  closer,
	}
}
