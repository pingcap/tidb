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

package simplesst

import (
	"context"
	"encoding/binary"

	"github.com/pingcap/tidb/pkg/objstore/storeapi"
)

// StatsReader is used to read RangeProperty from the file.
type StatsReader struct {
	byteReader *byteReader
}

// NewStatsReader creates a new StatsReader to read RangeProperty.
func NewStatsReader(ctx context.Context, store storeapi.Storage, name string, bufSize int) (*StatsReader, error) {
	sr, err := openStoreReaderAndSeek(ctx, store, name, 0, 250*1024)
	if err != nil {
		return nil, err
	}
	br, err := newByteReader(ctx, sr, bufSize)
	if err != nil {
		return nil, err
	}
	return &StatsReader{
		byteReader: br,
	}, nil
}

// NextProp returns the next RangeProperty.
func (r *StatsReader) NextProp() (*RangeProperty, error) {
	lenBytes, err := r.byteReader.readNBytes(4)
	if err != nil {
		return nil, err
	}
	propLen := int(binary.BigEndian.Uint32(lenBytes))
	propBytes, err := r.byteReader.readNBytes(propLen)
	if err != nil {
		return nil, noEOF(err)
	}
	return decodeProp(propBytes), nil
}

// Close the StatsReader.
func (r *StatsReader) Close() error {
	return r.byteReader.Close()
}
