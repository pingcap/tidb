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
	"bytes"
	"context"
	"encoding/binary"

	"github.com/pingcap/tidb/br/pkg/storage"
)

type statsReader struct {
	byteReader *byteReader
	kvBuffer   bytes.Buffer
}

func newStatsReader(ctx context.Context, store storage.ExternalStorage, name string, bufSize int) (*statsReader, error) {
	sr, err := openStoreReaderAndSeek(ctx, store, name, 0, 250*1024)
	if err != nil {
		return nil, err
	}
	br, err := newByteReader(ctx, sr, bufSize)
	if err != nil {
		return nil, err
	}
	return &statsReader{
		byteReader: br,
	}, nil
}

func (r *statsReader) nextProp() (*rangeProperty, error) {
	r.kvBuffer.Grow(4)
	b := r.kvBuffer.Bytes()[:4]
	if err := r.byteReader.readNBytes(b); err != nil {
		return nil, err
	}
	propLen := int(binary.BigEndian.Uint32(b))

	b = make([]byte, propLen)
	if err := r.byteReader.readNBytes(b); err != nil {
		return nil, noEOF(err)
	}
	return decodeProp(b), nil
}

func (r *statsReader) Close() error {
	return r.byteReader.Close()
}
