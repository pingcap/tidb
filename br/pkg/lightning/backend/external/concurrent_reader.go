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

	maxFileOffset int64
	name          string

	storage storage.ExternalStorage
	buffer  []byte
}

// newSingeFileReader creates a new singeFileReader.
func newSingeFileReader(ctx context.Context, st storage.ExternalStorage, name string, concurrency int, readBufferSize int) (*singeFileReader, error) {
	if st == nil {
		return nil, nil
	}
	if _, ok := st.(*storage.S3Storage); !ok {
		return nil, nil
	}
	maxOffset, err := storage.GetMaxOffset(ctx, st, name)
	if err != nil {
		return nil, err
	}
	return &singeFileReader{
		ctx:               ctx,
		concurrency:       concurrency,
		readBufferSize:    readBufferSize,
		currentFileOffset: 0,
		bufferReadOffset:  0,
		maxFileOffset:     maxOffset,
		name:              name,
		storage:           st,
		buffer:            nil,
	}, nil
}

// reload reloads the buffer.
func (r *singeFileReader) reload() error {
	if r.currentFileOffset >= r.maxFileOffset {
		return io.EOF
	}

	eg := errgroup.Group{}
	for i := 0; i < r.concurrency; i++ {
		i := i
		eg.Go(func() error {
			startOffset := r.currentFileOffset + int64(i*r.readBufferSize)
			endOffset := startOffset + int64(r.readBufferSize)
			if endOffset > r.maxFileOffset {
				endOffset = r.maxFileOffset
			}
			if startOffset > endOffset {
				return nil
			}

			_, err := storage.ReadDataInRange(r.ctx, r.storage, r.name, startOffset, r.buffer[i*r.readBufferSize:i*r.readBufferSize+int(endOffset-startOffset)])
			if err != nil {
				log.FromContext(r.ctx).Warn("read meet error", zap.Any("startOffset", startOffset), zap.Any("endOffset", endOffset), zap.Error(err))
				return err
			}
			return nil
		})
	}
	err := eg.Wait()
	if err != nil {
		return err
	}

	if r.currentFileOffset+int64(r.readBufferSize*r.concurrency) > r.maxFileOffset {
		r.bufferMaxOffset = r.maxFileOffset - r.currentFileOffset
		r.currentFileOffset = r.maxFileOffset
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
