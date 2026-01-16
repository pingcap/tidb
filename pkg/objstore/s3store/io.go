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

package s3store

import (
	"bytes"
	"context"
	"io"
	"sync"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	errors2 "github.com/pingcap/tidb/br/pkg/errors"
	"github.com/pingcap/tidb/br/pkg/logutil"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/objstore/s3like"
	"github.com/pingcap/tidb/pkg/util/injectfailpoint"
	"github.com/pingcap/tidb/pkg/util/prefetch"
	"go.uber.org/zap"
)

// s3ObjectReader wrap GetObjectOutput.Body and add the `Seek` method.
type s3ObjectReader struct {
	storage   *S3Storage
	name      string
	reader    io.ReadCloser
	pos       int64
	rangeInfo RangeInfo
	// reader context used for implement `io.Seek`
	ctx          context.Context
	prefetchSize int
}

// Read implement the io.Reader interface.
func (r *s3ObjectReader) Read(p []byte) (n int, err error) {
	retryCnt := 0
	maxCnt := r.rangeInfo.End + 1 - r.pos
	if maxCnt == 0 {
		return 0, io.EOF
	}
	if maxCnt > int64(len(p)) {
		maxCnt = int64(len(p))
	}
	n, err = r.reader.Read(p[:maxCnt])
	n, err = injectfailpoint.RandomErrorForReadWithOnePerPercent(n, err)
	// TODO: maybe we should use !errors.Is(err, io.EOF) here to avoid error lint, but currently, pingcap/errors
	// doesn't implement this method yet.
	for err != nil && errors.Cause(err) != io.EOF && r.ctx.Err() == nil && retryCnt < maxErrorRetries { //nolint:errorlint
		metrics.RetryableErrorCount.WithLabelValues(err.Error()).Inc()
		log.L().Warn(
			"read s3 object failed, will retry",
			zap.String("file", r.name),
			zap.Int("retryCnt", retryCnt),
			zap.Error(err),
		)
		// if can retry, reopen a new reader and try read again
		end := r.rangeInfo.End + 1
		if end == r.rangeInfo.Size {
			end = 0
		}
		_ = r.reader.Close()

		newReader, rangeInfo, err1 := r.storage.open(r.ctx, r.name, r.pos, end)
		if err1 != nil {
			log.Warn("open new s3 reader failed", zap.String("file", r.name), zap.Error(err1))
			return
		}
		r.reader = newReader
		if r.prefetchSize > 0 {
			r.reader = prefetch.NewReader(r.reader, rangeInfo.RangeSize(), r.prefetchSize)
		}
		retryCnt++
		n, err = r.reader.Read(p[:maxCnt])
	}

	r.storage.accessRec.RecRead(n)
	r.pos += int64(n)
	return
}

// Close implement the io.Closer interface.
func (r *s3ObjectReader) Close() error {
	return r.reader.Close()
}

// Seek implement the io.Seeker interface.
//
// Currently, tidb-lightning depends on this method to read parquet file for s3 storage.
func (r *s3ObjectReader) Seek(offset int64, whence int) (int64, error) {
	var realOffset int64
	switch whence {
	case io.SeekStart:
		realOffset = offset
	case io.SeekCurrent:
		realOffset = r.pos + offset
	case io.SeekEnd:
		realOffset = r.rangeInfo.Size + offset
	default:
		return 0, errors.Annotatef(errors2.ErrStorageUnknown, "Seek: invalid whence '%d'", whence)
	}
	if realOffset < 0 {
		return 0, errors.Annotatef(errors2.ErrStorageUnknown, "Seek in '%s': invalid offset to seek '%d'.", r.name, realOffset)
	}

	if realOffset == r.pos {
		return realOffset, nil
	} else if realOffset >= r.rangeInfo.Size {
		// See: https://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.35
		// because s3's GetObject interface doesn't allow get a range that matches zero length data,
		// so if the position is out of range, we need to always return io.EOF after the seek operation.

		// close current read and open a new one which target offset
		if err := r.reader.Close(); err != nil {
			log.L().Warn("close s3 reader failed, will ignore this error", logutil.ShortError(err))
		}

		r.reader = io.NopCloser(bytes.NewReader(nil))
		r.pos = r.rangeInfo.Size
		return r.pos, nil
	}

	// if seek ahead no more than 64k, we discard these data
	if realOffset > r.pos && realOffset-r.pos <= maxSkipOffsetByRead {
		_, err := io.CopyN(io.Discard, r, realOffset-r.pos)
		if err != nil {
			return r.pos, errors.Trace(err)
		}
		return realOffset, nil
	}

	// close current read and open a new one which target offset
	err := r.reader.Close()
	if err != nil {
		return 0, errors.Trace(err)
	}

	newReader, info, err := r.storage.open(r.ctx, r.name, realOffset, 0)
	if err != nil {
		return 0, errors.Trace(err)
	}
	r.reader = newReader
	if r.prefetchSize > 0 {
		r.reader = prefetch.NewReader(r.reader, info.RangeSize(), r.prefetchSize)
	}
	r.rangeInfo = info
	r.pos = realOffset
	return realOffset, nil
}

func (r *s3ObjectReader) GetFileSize() (int64, error) {
	return r.rangeInfo.Size, nil
}

type asyncWriter struct {
	rd       *io.PipeReader
	wd       *io.PipeWriter
	wg       *sync.WaitGroup
	uploader s3like.Uploader
	err      error
	name     string
}

func (s *asyncWriter) start(ctx context.Context) {
	s.wg.Add(1)
	go func() {
		err := s.uploader.Upload(ctx, s.rd)
		// like a channel we only let sender close the pipe in happy path
		if err != nil {
			log.Warn("upload to s3 failed", zap.String("filename", s.name), zap.Error(err))
			_ = s.rd.CloseWithError(err)
		}
		s.err = err
		s.wg.Done()
	}()
}

// Write implement the objectio.Writer interface.
func (s *asyncWriter) Write(_ context.Context, p []byte) (int, error) {
	return s.wd.Write(p)
}

// Close implement the objectio.Writer interface.
func (s *asyncWriter) Close(_ context.Context) error {
	err := s.wd.Close()
	if err != nil {
		return err
	}
	s.wg.Wait()
	return s.err
}
