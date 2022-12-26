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

func (w *withCompression) Create(ctx context.Context, name string) (ExternalFileWriter, error) {
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
		return nil, errors.Trace(err)
	}
	compressedWriter := newBufferedWriter(writer, hardcodedS3ChunkSize, w.compressType)
	return compressedWriter, nil
}

func (w *withCompression) Open(ctx context.Context, path string) (ExternalFileReader, error) {
	fileReader, err := w.ExternalStorage.Open(ctx, path)
	if err != nil {
		return nil, errors.Trace(err)
	}
	uncompressReader, err := newInterceptReader(fileReader, w.compressType)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return uncompressReader, nil
}

func (w *withCompression) WriteFile(ctx context.Context, name string, data []byte) error {
	bf := bytes.NewBuffer(make([]byte, 0, len(data)))
	compressBf := newCompressWriter(w.compressType, bf)
	_, err := compressBf.Write(data)
	if err != nil {
		return errors.Trace(err)
	}
	err = compressBf.Close()
	if err != nil {
		return errors.Trace(err)
	}
	return w.ExternalStorage.WriteFile(ctx, name, bf.Bytes())
}

func (w *withCompression) ReadFile(ctx context.Context, name string) ([]byte, error) {
	data, err := w.ExternalStorage.ReadFile(ctx, name)
	if err != nil {
		return data, errors.Trace(err)
	}
	bf := bytes.NewBuffer(data)
	compressBf, err := newCompressReader(w.compressType, bf)
	if err != nil {
		return nil, err
	}
	return io.ReadAll(compressBf)
}

// compressReader is a wrapper for compress.Reader
type compressReader struct {
	io.Reader
	io.Seeker
	io.Closer
}

// nolint:interfacer
func newInterceptReader(fileReader ExternalFileReader, compressType CompressType) (ExternalFileReader, error) {
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
		return nil, errors.Annotatef(berrors.ErrStorageInvalidConfig, "compressReader doesn't support negative limit, n: %d", n)
	} else if n > 0 {
		newFileReader = &compressReader{
			Reader: io.LimitReader(fileReader, n),
			Seeker: fileReader,
			Closer: fileReader,
		}
	}
	return newInterceptReader(newFileReader, compressType)
}

func (c *compressReader) Seek(offset int64, whence int) (int64, error) {
	// only support get original reader's current offset
	if offset == 0 && whence == io.SeekCurrent {
		return c.Seeker.Seek(offset, whence)
	}
	return int64(0), errors.Annotatef(berrors.ErrStorageInvalidConfig, "compressReader doesn't support Seek now, offset %d, whence %d", offset, whence)
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
