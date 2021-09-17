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

type compressReader struct {
	io.ReadCloser
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
		ReadCloser: r,
	}, nil
}

func (r *compressReader) Seek(_ int64, _ int) (int64, error) {
	return int64(0), errors.Annotatef(berrors.ErrStorageInvalidConfig, "compressReader doesn't support Seek now")
}

type flushStorageWriter struct {
	writer  io.Writer
	flusher flusher
	closer  io.Closer
}

func (w *flushStorageWriter) Write(ctx context.Context, data []byte) (int, error) {
	n, err := w.writer.Write(data)
	return n, errors.Trace(err)
}

func (w *flushStorageWriter) Close(ctx context.Context) error {
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
