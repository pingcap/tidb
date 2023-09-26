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

// singeFileReader is a concurrent reader for a single file.
type singeFileReader struct {
	ctx               context.Context
	concurrency       int
	readBufferSize    int
	currentFileOffset int64
	bufferReadOffset  int64
	bufferMaxOffset   int64

	fileSize int64
	name     string

	storage storage.ExternalStorage
	buffer  []byte
}

// newSingeFileReader creates a new singeFileReader.
func newSingeFileReader(
	ctx context.Context,
	st storage.ExternalStorage,
	name string,
	fileSize int64,
	concurrency int,
	readBufferSize int,
) (*singeFileReader, error) {
	if st == nil {
		return nil, nil
	}
	return &singeFileReader{
		ctx:               ctx,
		concurrency:       concurrency,
		readBufferSize:    readBufferSize,
		currentFileOffset: 0,
		bufferReadOffset:  0,
		fileSize:          fileSize,
		name:              name,
		storage:           st,
		buffer:            nil,
	}, nil
}

// reload reloads the buffer.
func (r *singeFileReader) reload() error {
	if r.currentFileOffset >= r.fileSize {
		return io.EOF
	}

	eg := errgroup.Group{}
	for i := 0; i < r.concurrency; i++ {
		i := i
		eg.Go(func() error {
			bufStart := i * r.readBufferSize
			fileStart := r.currentFileOffset + int64(bufStart)
			fileEnd := fileStart + int64(r.readBufferSize)
			if fileEnd > r.fileSize {
				fileEnd = r.fileSize
			}
			if fileStart > fileEnd {
				return nil
			}

			_, err := storage.ReadDataInRange(
				r.ctx,
				r.storage,
				r.name,
				fileStart,
				r.buffer[bufStart:bufStart+int(fileEnd-fileStart)],
			)
			if err != nil {
				log.FromContext(r.ctx).Warn(
					"read meet error",
					zap.Int64("fileStart", fileStart),
					zap.Int64("fileEnd", fileEnd),
					zap.Error(err),
				)
				return err
			}
			return nil
		})
	}
	err := eg.Wait()
	if err != nil {
		return err
	}

	if r.currentFileOffset+int64(r.readBufferSize*r.concurrency) > r.fileSize {
		r.bufferMaxOffset = r.fileSize - r.currentFileOffset
		r.currentFileOffset = r.fileSize
	} else {
		r.bufferMaxOffset = int64(r.readBufferSize * r.concurrency)
		r.currentFileOffset += int64(r.readBufferSize * r.concurrency)
	}
	r.bufferReadOffset = 0

	return nil
}

// next returns the next n bytes.
func (r *singeFileReader) next(n int) []byte {
	end := min(r.bufferReadOffset+int64(n), r.bufferMaxOffset)
	ret := r.buffer[r.bufferReadOffset:end]
	r.bufferReadOffset += int64(len(ret))

	return ret
}
