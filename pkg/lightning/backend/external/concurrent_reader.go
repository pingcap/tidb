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

	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/pkg/lightning/log"
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
func (r *concurrentFileReader) read(bufs [][]byte) ([][]byte, error) {
	if r.offset >= r.fileSize {
		return nil, io.EOF
	}

	ret := make([][]byte, 0, r.concurrency)
	eg := errgroup.Group{}
	for i := 0; i < r.concurrency; i++ {
		if r.offset >= r.fileSize {
			break
		}
		end := r.readBufferSize
		if r.offset+int64(end) > r.fileSize {
			end = int(r.fileSize - r.offset)
		}
		buf := bufs[i][:end]
		ret = append(ret, buf)
		offset := r.offset
		r.offset += int64(end)
		eg.Go(func() error {
			_, err := storage.ReadDataInRange(
				r.ctx,
				r.storage,
				r.name,
				offset,
				buf,
			)
			if err != nil {
				log.FromContext(r.ctx).Error(
					"concurrent read meet error",
					zap.Int64("offset", offset),
					zap.Int("readSize", len(buf)),
					zap.Error(err),
				)
				return err
			}
			return nil
		})
	}
	err := eg.Wait()
	if err != nil {
		return nil, err
	}

	return ret, nil
}
