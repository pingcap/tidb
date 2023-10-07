// Copyright 2023 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package external

import (
	"context"
	"io"

	"github.com/pingcap/tidb/br/pkg/lightning/log"
	"github.com/pingcap/tidb/br/pkg/storage"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

// concurrentFileReader reads a file with multiple chunks concurrently.
type concurrentFileReader struct {
	ctx            context.Context
	concurrency    int
	readBufferSize int

	storage storage.ExternalStorage
	name    string

	offset   int64
	fileSize int64
}

// newConcurrentFileReader creates a new concurrentFileReader.
func newConcurrentFileReader(
	ctx context.Context,
	st storage.ExternalStorage,
	name string,
	offset int64,
	fileSize int64,
	concurrency int,
	readBufferSize int,
) (*concurrentFileReader, error) {
	return &concurrentFileReader{
		ctx:            ctx,
		concurrency:    concurrency,
		readBufferSize: readBufferSize,
		offset:         offset,
		fileSize:       fileSize,
		name:           name,
		storage:        st,
	}, nil
}

// read loads the file content concurrently into the buffer.
func (r *concurrentFileReader) read(buf []byte) (int64, error) {
	if r.offset >= r.fileSize {
		return 0, io.EOF
	}

	bufSize := len(buf)
	fileSizeRemain := r.fileSize - r.offset
	readBatchTotal := r.readBufferSize * r.concurrency
	bytesRead := min(bufSize, int(fileSizeRemain), readBatchTotal)
	eg := errgroup.Group{}
	for i := 0; i < r.concurrency; i++ {
		i := i
		eg.Go(func() error {
			bufStart := i * r.readBufferSize
			bufEnd := bufStart + r.readBufferSize
			if bufEnd > bytesRead {
				bufEnd = bytesRead
			}
			if bufStart >= bufEnd {
				return nil
			}

			fileStart := r.offset + int64(bufStart)
			_, err := storage.ReadDataInRange(
				r.ctx,
				r.storage,
				r.name,
				fileStart,
				buf[bufStart:bufEnd],
			)
			if err != nil {
				log.FromContext(r.ctx).Error(
					"concurrent read meet error",
					zap.Int64("fileStart", fileStart),
					zap.Int("readSize", bufEnd-bufStart),
					zap.Error(err),
				)
				return err
			}
			return nil
		})
	}
	err := eg.Wait()
	if err != nil {
		return 0, err
	}

	r.offset += int64(bytesRead)
	return int64(bytesRead), nil
}
