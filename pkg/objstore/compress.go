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
	"time"

	"github.com/pingcap/errors"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
	"github.com/pingcap/tidb/pkg/objstore/compressedio"
	"github.com/pingcap/tidb/pkg/objstore/objectio"
	"github.com/pingcap/tidb/pkg/objstore/recording"
	"github.com/pingcap/tidb/pkg/objstore/s3like"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
	"go.uber.org/multierr"
)

type withCompression struct {
	storeapi.Storage
	compressType  compressedio.CompressType
	decompressCfg compressedio.DecompressConfig
}

// WithCompression returns a Storage with compression enabled. Closing a compressed
// reader cancels its context, stops the decoder, and then closes the source.
// With asynchronous decoding, inner must unblock pending reads when the context
// passed to Open is canceled.
func WithCompression(inner storeapi.Storage, compressionType compressedio.CompressType, cfg compressedio.DecompressConfig) storeapi.Storage {
	if compressionType == compressedio.NoCompression {
		return inner
	}
	return &withCompression{
		Storage:       inner,
		compressType:  compressionType,
		decompressCfg: cfg,
	}
}

func (w *withCompression) Create(ctx context.Context, name string, o *storeapi.WriterOption) (objectio.Writer, error) {
	writer, err := w.Storage.Create(ctx, name, o)
	if err != nil {
		return nil, errors.Trace(err)
	}
	// some implementation already wrap the writer, so we need to unwrap it
	if bw, ok := writer.(*objectio.BufferedWriter); ok {
		writer = bw.GetWriter()
	}
	// the external storage will do access recording, so no need to pass it again.
	compressedWriter := objectio.NewBufferedWriter(writer, s3like.HardcodedChunkSize, w.compressType, nil)
	return compressedWriter, nil
}

func (w *withCompression) Open(ctx context.Context, path string, o *storeapi.ReaderOption) (objectio.Reader, error) {
	ctx, cancel := context.WithCancel(ctx)
	fileReader, err := w.Storage.Open(ctx, path, o)
	if err != nil {
		cancel()
		return nil, errors.Trace(err)
	}
	uncompressReader, err := InterceptDecompressReader(fileReader, w.compressType, w.decompressCfg)
	if err != nil {
		cancel()
		return nil, errors.Trace(multierr.Append(err, fileReader.Close()))
	}
	return &readerWithCancel{Reader: uncompressReader, cancel: cancel}, nil
}

func (w *withCompression) WriteFile(ctx context.Context, name string, data []byte) error {
	bf := bytes.NewBuffer(make([]byte, 0, len(data)))
	compressBf := compressedio.NewWriter(w.compressType, bf)
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
	compressBf, err := compressedio.NewReader(w.compressType, w.decompressCfg, bf)
	if err != nil {
		return nil, err
	}
	defer compressBf.Close()
	return io.ReadAll(compressBf)
}

func (w *withCompression) PresignFile(ctx context.Context, fileName string, expire time.Duration) (string, error) {
	return w.Storage.PresignFile(ctx, fileName, expire)
}

type readerWithCancel struct {
	objectio.Reader
	cancel context.CancelFunc
}

func (r *readerWithCancel) Close() error {
	// Interrupt source reads and retries before waiting for the decoder to stop.
	r.cancel()
	return r.Reader.Close()
}

// compressReader owns both the source reader and, when present, its decoder.
type compressReader struct {
	io.Reader
	io.Seeker
	io.Closer
	decoder io.Closer
}

// InterceptDecompressReader intercepts the reader and wraps it with a decompress
// reader on the given Reader. Note that the returned
// Reader does not have the property that Seek(0, io.SeekCurrent)
// equals total bytes Read() if the decompress reader is used.
// On success, closing the returned reader closes both the decoder and fileReader.
// On failure, fileReader remains the caller's responsibility.
// With asynchronous decoding, the caller must interrupt pending source reads
// (for example, by canceling their context) before closing the returned reader.
// Close waits for the decoder to stop before closing fileReader.
func InterceptDecompressReader(
	fileReader objectio.Reader,
	compressType compressedio.CompressType,
	cfg compressedio.DecompressConfig,
) (objectio.Reader, error) {
	if compressType == compressedio.NoCompression {
		return fileReader, nil
	}
	r, err := compressedio.NewReader(compressType, cfg, fileReader)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &compressReader{
		Reader:  r,
		Closer:  fileReader,
		Seeker:  fileReader,
		decoder: r,
	}, nil
}

// NewLimitedInterceptReader creates a decompress reader with limit n.
// With asynchronous decoding, the caller must interrupt pending source reads
// before Close, as required by InterceptDecompressReader.
func NewLimitedInterceptReader(
	fileReader objectio.Reader,
	compressType compressedio.CompressType,
	cfg compressedio.DecompressConfig,
	n int64,
) (objectio.Reader, error) {
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
	// The decoder exclusively owns source reads until its workers have stopped.
	var err error
	if c.decoder != nil {
		err = c.decoder.Close()
	}
	return multierr.Append(err, c.Closer.Close())
}

func (c *compressReader) GetFileSize() (int64, error) {
	return 0, errors.Annotatef(berrors.ErrUnsupportedOperation, "compressReader doesn't support GetFileSize now")
}

type flushStorageWriter struct {
	writer    io.Writer
	flusher   compressedio.Flusher
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

func newFlushStorageWriter(writer io.Writer, flusher2 compressedio.Flusher, closer io.Closer, accessRec *recording.AccessStats) *flushStorageWriter {
	return &flushStorageWriter{
		writer:    writer,
		flusher:   flusher2,
		closer:    closer,
		accessRec: accessRec,
	}
}
