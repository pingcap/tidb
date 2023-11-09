// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package storage

import (
	"bytes"
	"context"
	"io"
	"strconv"
	"time"

	"cloud.google.com/go/storage"
	"github.com/pingcap/errors"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
	"github.com/pingcap/tidb/br/pkg/lightning/log"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

type withCompression struct {
	ExternalStorage
	compressType  CompressType
	decompressCfg DecompressConfig
}

// WithCompression returns an ExternalStorage with compress option
func WithCompression(inner ExternalStorage, compressionType CompressType, cfg DecompressConfig) ExternalStorage {
	if compressionType == NoCompression {
		return inner
	}
	return &withCompression{
		ExternalStorage: inner,
		compressType:    compressionType,
		decompressCfg:   cfg,
	}
}

func (w *withCompression) Create(ctx context.Context, name string, o *WriterOption) (ExternalFileWriter, error) {
	writer, err := w.ExternalStorage.Create(ctx, name, o)
	if err != nil {
		return nil, errors.Trace(err)
	}
	// some implementation already wrap the writer, so we need to unwrap it
	if bw, ok := writer.(*bufferedWriter); ok {
		writer = bw.writer
	}
	compressedWriter := newBufferedWriter(writer, hardcodedS3ChunkSize, w.compressType)
	return compressedWriter, nil
}

func (w *withCompression) Open(ctx context.Context, path string, o *ReaderOption) (ExternalFileReader, error) {
	fileReader, err := w.ExternalStorage.Open(ctx, path, o)
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
	return w.ExternalStorage.WriteFile(ctx, name, bf.Bytes())
}

func (w *withCompression) ReadFile(ctx context.Context, name string) ([]byte, error) {
	data, err := w.ExternalStorage.ReadFile(ctx, name)
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
// reader on the given ExternalFileReader. Note that the returned
// ExternalFileReader does not have the property that Seek(0, io.SeekCurrent)
// equals total bytes Read() if the decompress reader is used.
func InterceptDecompressReader(
	fileReader ExternalFileReader,
	compressType CompressType,
	cfg DecompressConfig,
) (ExternalFileReader, error) {
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

func NewLimitedInterceptReader(
	fileReader ExternalFileReader,
	compressType CompressType,
	cfg DecompressConfig,
	n int64,
) (ExternalFileReader, error) {
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

type gcsMultiWriter struct {
	eg     errgroup.Group
	prefix string
	seq    int
	s      *GCSStorage
}

// Write writes data to the GCS object.
func (w *gcsMultiWriter) Write(ctx context.Context, data []byte) (int, error) {
	object := w.s.objectName(w.prefix + strconv.Itoa(w.seq))
	w.seq++
	w.eg.Go(
		func() error {
			wc := w.s.bucket.Object(object).NewWriter(ctx)
			wc.StorageClass = w.s.gcs.StorageClass
			wc.PredefinedACL = w.s.gcs.PredefinedAcl
			_, _ = wc.Write(data)
			return wc.Close()
		})
	return 0, nil
}

// Close closes the writer and returns the error if exists.
func (w *gcsMultiWriter) Close(ctx context.Context) error {
	err := w.eg.Wait()
	if err != nil {
		return errors.Trace(err)
	}

	ts := time.Now()
	batch := 30
	headObject := w.s.bucket.Object(w.s.objectName(w.prefix))
	var tempObject *storage.ObjectHandle
	objectsToConcat := make([]*storage.ObjectHandle, 0, batch)
	allTempObjects := make([]*storage.ObjectHandle, 0, batch)
	for i := 0; i < w.seq; i += batch {
		if i+batch < w.seq {
			tempObject = w.s.bucket.Object(w.s.objectName(w.prefix + "temp" + "_" + strconv.Itoa(i)))
		} else {
			tempObject = headObject
		}
		for j := i; j < i+batch && j < w.seq; j++ {
			objectsToConcat = append(objectsToConcat, w.s.bucket.Object(w.s.objectName(w.prefix+strconv.Itoa(j))))
			allTempObjects = append(allTempObjects, w.s.bucket.Object(w.s.objectName(w.prefix+strconv.Itoa(j))))
		}
		_, err := tempObject.ComposerFrom(objectsToConcat...).Run(ctx)
		if err != nil {
			return errors.Trace(err)
		}
		if i+batch < w.seq {
			objectsToConcat = objectsToConcat[:0]
			objectsToConcat = append(objectsToConcat, tempObject)
			allTempObjects = append(allTempObjects, tempObject)
		}
	}

	log.FromContext(ctx).Info("concat files", zap.Int("count", w.seq), zap.Duration("cost", time.Since(ts)))
	ts = time.Now()

	for _, oj := range allTempObjects {
		err := oj.Delete(ctx)
		if err != nil {
			return errors.Trace(err)
		}
	}
	log.FromContext(ctx).Info("delete temporary files", zap.Int("count", w.seq), zap.Duration("cost", time.Since(ts)))

	return nil
}

func newGcsMultiWriter(prefix string, s *GCSStorage) *gcsMultiWriter {
	w := &gcsMultiWriter{
		prefix: prefix,
		s:      s,
	}
	// Limit the max concurrency.
	w.eg.SetLimit(20)
	return w
}

type flushStorageWriter struct {
	writer  io.Writer
	flusher flusher
	closer  io.Closer
}

// Write writes data to the underlying writer.
func (w *flushStorageWriter) Write(_ context.Context, data []byte) (int, error) {
	n, err := w.writer.Write(data)
	return n, errors.Trace(err)
}

// Close closes the underlying writer.
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
