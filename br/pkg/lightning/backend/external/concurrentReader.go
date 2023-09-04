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
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/util/mathutil"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"io"
)

type SingeFileReader struct {
	ctx               context.Context
	concurrency       int
	readBufferSize    int
	currentFileOffset int64
	bufferReadOffset  int64
	bufferMaxOffset   int64

	BufferTotalOffset int64

	MaxFileOffset int64
	name          string

	storage storage.ExternalStorage
	buffer  []byte
}

func NewSingeFileReader(ctx context.Context, st storage.ExternalStorage, name string, concurrency int, readBufferSize int) (*SingeFileReader, error) {
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
	return &SingeFileReader{
		ctx:               ctx,
		concurrency:       concurrency,
		readBufferSize:    readBufferSize,
		currentFileOffset: 0,
		bufferReadOffset:  0,
		MaxFileOffset:     int64(maxOffset),
		name:              name,
		storage:           st,
		buffer:            nil,
	}, nil
}

func (r *SingeFileReader) Reload() error {
	if r.currentFileOffset >= r.MaxFileOffset {
		return io.EOF
	}

	eg := errgroup.Group{}
	for i := 0; i < r.concurrency; i++ {
		i := i
		eg.Go(func() error {
			startOffset := r.currentFileOffset + int64(i*r.readBufferSize)
			endOffset := startOffset + int64(r.readBufferSize)
			if endOffset > r.MaxFileOffset {
				endOffset = r.MaxFileOffset
			}
			if startOffset > endOffset {
				return nil
			}

			_, err := storage.ReadDataInRange(r.ctx, r.storage, r.name, startOffset, endOffset, r.buffer[i*r.readBufferSize:(i+1)*r.readBufferSize])
			if err != nil && err != io.ErrUnexpectedEOF {
				log.Warn("read meet error", zap.Any("startOffset", startOffset), zap.Any("endOffset", endOffset), zap.Error(err))
				return err
			}
			return nil
		})
	}
	err := eg.Wait()
	if err != nil {
		return err
	}

	if r.currentFileOffset+int64(r.readBufferSize*r.concurrency) > r.MaxFileOffset {
		r.bufferMaxOffset = r.MaxFileOffset - r.currentFileOffset
		r.currentFileOffset = r.MaxFileOffset
	} else {
		r.bufferMaxOffset = int64(r.readBufferSize * r.concurrency)
		r.currentFileOffset += int64(r.readBufferSize * r.concurrency)
	}
	r.BufferTotalOffset += r.bufferMaxOffset
	r.bufferReadOffset = 0

	return nil
}

func (r *SingeFileReader) Next(n int) []byte {
	end := mathutil.Min(r.bufferReadOffset+int64(n), r.bufferMaxOffset)
	ret := r.buffer[r.bufferReadOffset:end]
	r.bufferReadOffset += int64(len(ret))

	return ret
}
