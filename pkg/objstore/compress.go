// Copyright 2020 PingCAP, Inc.
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

package objstore

import (
	"bytes"
	"context"
	"io"

	"github.com/pingcap/errors"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
	"github.com/pingcap/tidb/pkg/objstore/recording"
)

type withCompression struct {
	Storage
	compressType  CompressType
	decompressCfg DecompressConfig
}

// WithCompression returns an Storage with compress option
func WithCompression(inner Storage, compressionType CompressType, cfg DecompressConfig) Storage {
	if compressionType == NoCompression {
		return inner
	}
	return &withCompression{
		Storage:       inner,
		compressType:  compressionType,
		decompressCfg: cfg,
	}
}

func (w *withCompression) Create(ctx context.Context, name string, o *WriterOption) (FileWriter, error) {
	writer, err := w.Storage.Create(ctx, name, o)
	if err != nil {
		return nil, errors.Trace(err)
	}
	// some implementation already wrap the writer, so we need to unwrap it
	if bw, ok := writer.(*bufferedWriter); ok {
		writer = bw.writer
	}
	// the external storage will do access recording, so no need to pass it again.
	compressedWriter := newBufferedWriter(writer, hardcodedS3ChunkSize, w.compressType, nil)
	return compressedWriter, nil
}

func (w *withCompression) Open(ctx context.Context, path string, o *ReaderOption) (FileReader, error) {
	fileReader, err := w.Storage.Open(ctx, path, o)
	if err != nil {
		return nil, errors.Trace(err)
	}
	uncompressReader, err := InterceptDecompressReader(fileReader, w.compressType, w.decompressCfg)
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
	return w.Storage.WriteFile(ctx, name, bf.Bytes())
}

func (w *withCompression) ReadFile(ctx context.Context, name string) ([]byte, error) {
	data, err := w.Storage.ReadFile(ctx, name)
	if err != nil {
		return data, errors.Trace(err)
	}
	bf := bytes.NewBuffer(data)
	compressBf, err := newCompressReader(w.compressType, w.decompressCfg, bf)
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

// InterceptDecompressReader intercepts the reader and wraps it with a decompress
// reader on the given FileReader. Note that the returned
// FileReader does not have the property that Seek(0, io.SeekCurrent)
// equals total bytes Read() if the decompress reader is used.
func InterceptDecompressReader(
	fileReader FileReader,
	compressType CompressType,
	cfg DecompressConfig,
) (FileReader, error) {
	if compressType == NoCompression {
		return fileReader, nil
	}
	r, err := newCompressReader(compressType, cfg, fileReader)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &compressReader{
		Reader: r,
		Closer: fileReader,
		Seeker: fileReader,
	}, nil
}

// NewLimitedInterceptReader creates a decompress reader with limit n.
func NewLimitedInterceptReader(
	fileReader FileReader,
	compressType CompressType,
	cfg DecompressConfig,
	n int64,
) (FileReader, error) {
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
	return InterceptDecompressReader(newFileReader, compressType, cfg)
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

func (c *compressReader) GetFileSize() (int64, error) {
	return 0, errors.Annotatef(berrors.ErrUnsupportedOperation, "compressReader doesn't support GetFileSize now")
}

type flushStorageWriter struct {
	writer    io.Writer
	flusher   flusher
	closer    io.Closer
	accessRec *recording.AccessStats
}

func (w *flushStorageWriter) Write(_ context.Context, data []byte) (int, error) {
	n, err := w.writer.Write(data)
	w.accessRec.RecWrite(n)
	return n, errors.Trace(err)
}

func (w *flushStorageWriter) Close(_ context.Context) error {
	err := w.flusher.Flush()
	if err != nil {
		return errors.Trace(err)
	}
	return w.closer.Close()
}

func newFlushStorageWriter(writer io.Writer, flusher2 flusher, closer io.Closer, accessRec *recording.AccessStats) *flushStorageWriter {
	return &flushStorageWriter{
		writer:    writer,
		flusher:   flusher2,
		closer:    closer,
		accessRec: accessRec,
	}
}
