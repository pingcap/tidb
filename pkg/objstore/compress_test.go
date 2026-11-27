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
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"testing/synctest"

	"github.com/klauspost/compress/gzip"
	"github.com/klauspost/compress/zstd"
	"github.com/pingcap/tidb/pkg/objstore/compressedio"
	"github.com/pingcap/tidb/pkg/objstore/objectio"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
	"github.com/pingcap/tidb/pkg/util/prefetch"
	"github.com/stretchr/testify/require"
)

func TestWithCompressReadWriteFile(t *testing.T) {
	dir := t.TempDir()
	backend, err := ParseBackend("local://"+filepath.ToSlash(dir), nil)
	require.NoError(t, err)
	ctx := context.Background()
	storage, err := Create(ctx, backend, true)
	require.NoError(t, err)
	storage = WithCompression(storage, compressedio.Gzip, compressedio.DecompressConfig{})
	name := "with compress test"
	content := "hello,world!"
	fileName := strings.ReplaceAll(name, " ", "-") + ".txt.gz"
	err = storage.WriteFile(ctx, fileName, []byte(content))
	require.NoError(t, err)

	// make sure compressed file is written correctly
	file, err := os.Open(filepath.Join(dir, fileName))
	require.NoError(t, err)
	defer file.Close()
	uncompressedFile, err := compressedio.NewReader(compressedio.Gzip, compressedio.DecompressConfig{}, file)
	require.NoError(t, err)
	defer uncompressedFile.Close()
	newContent, err := io.ReadAll(uncompressedFile)
	require.NoError(t, err)
	require.Equal(t, content, string(newContent))

	// test withCompression ReadFile
	newContent, err = storage.ReadFile(ctx, fileName)
	require.NoError(t, err)
	require.Equal(t, content, string(newContent))
}

type compressionTestReader struct {
	*bytes.Reader
	closeCount int
	closeErr   error
	onRead     func([]byte) (int, error)
	onClose    func()
}

func (r *compressionTestReader) Read(p []byte) (int, error) {
	if r.onRead != nil {
		return r.onRead(p)
	}
	return r.Reader.Read(p)
}

func (r *compressionTestReader) Close() error {
	r.closeCount++
	if r.onClose != nil {
		r.onClose()
	}
	return r.closeErr
}

func (r *compressionTestReader) GetFileSize() (int64, error) {
	return r.Size(), nil
}

type compressionTestStorage struct {
	storeapi.Storage
	reader  objectio.Reader
	openCtx context.Context
	openErr error
}

func (s *compressionTestStorage) Open(ctx context.Context, _ string, _ *storeapi.ReaderOption) (objectio.Reader, error) {
	s.openCtx = ctx
	return s.reader, s.openErr
}

func TestWithCompressionOpenCleanup(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	closeErr := errors.New("close source failed")
	for _, sourceErr := range []error{nil, closeErr} {
		source := &compressionTestReader{
			Reader:   bytes.NewReader([]byte("invalid gzip header")),
			closeErr: sourceErr,
		}
		inner := &compressionTestStorage{reader: source}
		var closeCtxErr error
		source.onClose = func() { closeCtxErr = inner.openCtx.Err() }
		storage := WithCompression(inner, compressedio.Gzip, compressedio.DecompressConfig{})
		reader, err := storage.Open(ctx, "invalid.gz", nil)
		require.Nil(t, reader)
		require.ErrorIs(t, err, gzip.ErrHeader)
		require.Equal(t, 1, source.closeCount)
		require.ErrorIs(t, closeCtxErr, context.Canceled)
		require.ErrorIs(t, inner.openCtx.Err(), context.Canceled)
		require.NoError(t, ctx.Err())
		if sourceErr != nil {
			require.ErrorIs(t, err, sourceErr)
		}
	}
}

func TestWithCompressionOpenCancellation(t *testing.T) {
	t.Run("close waits for canceled read", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			releaseRead := make(chan struct{})
			source := &compressionTestReader{}
			inner := &compressionTestStorage{reader: source}
			source.onRead = func([]byte) (int, error) {
				<-inner.openCtx.Done()
				<-releaseRead
				return 0, inner.openCtx.Err()
			}
			storage := WithCompression(inner, compressedio.Zstd, compressedio.DecompressConfig{ZStdDecodeConcurrency: 2})
			reader, err := storage.Open(context.Background(), "test.zst", nil)
			require.NoError(t, err)
			// Let the decoder block in the source Read before initiating shutdown.
			synctest.Wait()
			closeDone := make(chan error, 1)
			go func() { closeDone <- reader.Close() }()
			defer func() {
				close(releaseRead)
				require.NoError(t, <-closeDone)
				require.Equal(t, 1, source.closeCount)
			}()
			synctest.Wait()
			require.ErrorIs(t, inner.openCtx.Err(), context.Canceled)
			require.Zero(t, source.closeCount, "source closed before the canceled decoder read returned")
		})
	})

	t.Run("open failure", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		openErr := errors.New("open source failed")
		inner := &compressionTestStorage{openErr: openErr}
		storage := WithCompression(inner, compressedio.Gzip, compressedio.DecompressConfig{})
		reader, err := storage.Open(ctx, "test.gz", nil)
		require.Nil(t, reader)
		require.ErrorIs(t, err, openErr)
		require.ErrorIs(t, inner.openCtx.Err(), context.Canceled)
		require.NoError(t, ctx.Err())
	})

	t.Run("reader contexts", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		var compressed bytes.Buffer
		writer := compressedio.NewWriter(compressedio.Gzip, &compressed)
		_, err := writer.Write([]byte("hello,world!"))
		require.NoError(t, err)
		require.NoError(t, writer.Close())
		inner := &compressionTestStorage{}
		storage := WithCompression(inner, compressedio.Gzip, compressedio.DecompressConfig{})
		var readers [2]objectio.Reader
		var sources [2]*compressionTestReader
		var requestCtxs [2]context.Context
		var closeCtxErrs [2]error
		for i := range readers {
			sources[i] = &compressionTestReader{
				Reader:  bytes.NewReader(compressed.Bytes()),
				onClose: func() { closeCtxErrs[i] = requestCtxs[i].Err() },
			}
			inner.reader = sources[i]
			readers[i], err = storage.Open(ctx, "test.gz", nil)
			require.NoError(t, err)
			requestCtxs[i] = inner.openCtx
			t.Cleanup(func() {
				if sources[i].closeCount == 0 {
					require.NoError(t, readers[i].Close())
				}
			})
			require.NoError(t, requestCtxs[i].Err())
		}

		require.NoError(t, readers[0].Close())
		require.ErrorIs(t, closeCtxErrs[0], context.Canceled)
		require.ErrorIs(t, requestCtxs[0].Err(), context.Canceled)
		require.Equal(t, 1, sources[0].closeCount)
		require.NoError(t, requestCtxs[1].Err())
		require.NoError(t, ctx.Err())

		cancel()
		require.ErrorIs(t, requestCtxs[1].Err(), context.Canceled)
		require.NoError(t, readers[1].Close())
		require.ErrorIs(t, closeCtxErrs[1], context.Canceled)
		require.Equal(t, 1, sources[1].closeCount)
	})
}

func TestInterceptDecompressReaderClose(t *testing.T) {
	content := bytes.Repeat([]byte("hello,world!"), 256*1024)
	for _, test := range []struct {
		name        string
		compression compressedio.CompressType
	}{
		{name: "none", compression: compressedio.NoCompression},
		{name: "gzip", compression: compressedio.Gzip},
		{name: "snappy", compression: compressedio.Snappy},
		{name: "zstd", compression: compressedio.Zstd},
	} {
		t.Run(test.name, func(t *testing.T) {
			var compressed bytes.Buffer
			if test.compression == compressedio.NoCompression {
				compressed.Write(content)
			} else {
				writer := compressedio.NewWriter(test.compression, &compressed)
				_, err := writer.Write(content)
				require.NoError(t, err)
				require.NoError(t, writer.Close())
			}
			for _, limited := range []bool{false, true} {
				for _, sourceErr := range []error{nil, errors.New("close source failed")} {
					source := &compressionTestReader{Reader: bytes.NewReader(compressed.Bytes()), closeErr: sourceErr}
					cfg := compressedio.DecompressConfig{ZStdDecodeConcurrency: 2}
					var reader objectio.Reader
					var err error
					if limited {
						reader, err = NewLimitedInterceptReader(source, test.compression, cfg, int64(compressed.Len()))
					} else {
						reader, err = InterceptDecompressReader(source, test.compression, cfg)
					}
					require.NoError(t, err)
					// Drain after a failed assertion so the regression test itself does not leak workers.
					t.Cleanup(func() { _, _ = io.Copy(io.Discard, reader) })
					buf := make([]byte, 1)
					_, err = io.ReadFull(reader, buf)
					require.NoError(t, err)
					require.Equal(t, content[:1], buf)
					if test.compression != compressedio.Zstd {
						offset, err := reader.Seek(0, io.SeekCurrent)
						require.NoError(t, err)
						require.Positive(t, offset)
					}
					require.ErrorIs(t, reader.Close(), sourceErr)
					require.Equal(t, 1, source.closeCount)
					if test.compression == compressedio.Zstd {
						_, err = reader.Read(buf)
						require.ErrorIs(t, err, zstd.ErrDecoderClosed)
					}
				}
			}
		})
	}

	for _, prefetched := range []bool{false, true} {
		name := "blocked zstd read"
		if prefetched {
			name = "blocked prefetched zstd read"
		}
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				pipeReader, pipeWriter := io.Pipe()
				defer pipeReader.Close()
				defer pipeWriter.Close()
				var input io.ReadCloser = pipeReader
				if prefetched {
					input = prefetch.NewReader(input, 1024, 16)
				}
				source := &compressReader{Reader: input, Closer: input}
				reader, err := InterceptDecompressReader(source, compressedio.Zstd, compressedio.DecompressConfig{ZStdDecodeConcurrency: 2})
				require.NoError(t, err)
				// Wait until the decoder worker is blocked reading from the open pipe.
				synctest.Wait()
				// Direct wrapping does not own a cancellation function. Interrupt the
				// source before Close waits for the decoder to finish reading it.
				require.NoError(t, pipeWriter.CloseWithError(context.Canceled))
				require.NoError(t, reader.Close())
				_, err = reader.Read(make([]byte, 1))
				require.ErrorIs(t, err, zstd.ErrDecoderClosed)
			})
		})
	}
}

func TestCompressReaderCloseErrors(t *testing.T) {
	var compressed bytes.Buffer
	writer := compressedio.NewWriter(compressedio.Gzip, &compressed)
	_, err := writer.Write([]byte("hello,world!"))
	require.NoError(t, err)
	require.NoError(t, writer.Close())
	// Set an invalid DEFLATE block type after the ten-byte gzip header.
	compressed.Bytes()[10] = 0x07
	sourceErr := errors.New("close source failed")
	source := &compressionTestReader{Reader: bytes.NewReader(compressed.Bytes()), closeErr: sourceErr}
	reader, err := InterceptDecompressReader(source, compressedio.Gzip, compressedio.DecompressConfig{})
	require.NoError(t, err)
	_, readErr := io.ReadAll(reader)
	require.Error(t, readErr)
	closeErr := reader.Close()
	require.ErrorIs(t, closeErr, sourceErr)
	require.ErrorIs(t, closeErr, readErr)
	require.Equal(t, 1, source.closeCount)
}

func TestInterceptDecompressReaderInitError(t *testing.T) {
	source := &compressionTestReader{Reader: bytes.NewReader([]byte("invalid gzip header"))}
	reader, err := InterceptDecompressReader(source, compressedio.Gzip, compressedio.DecompressConfig{})
	require.Nil(t, reader)
	require.ErrorIs(t, err, gzip.ErrHeader)
	// A failed interception leaves ownership with the caller.
	require.Zero(t, source.closeCount)
	require.NoError(t, source.Close())
}
