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
	"encoding/binary"

	"github.com/pingcap/tidb/br/pkg/storage"
)

type kvReader struct {
	byteReader *byteReader
}

func newKVReader(ctx context.Context, name string, store storage.ExternalStorage, initFileOffset uint64, bufSize int) (*kvReader, error) {
	sr, err := openStoreReaderAndSeek(ctx, store, name, initFileOffset)
	if err != nil {
		return nil, err
	}
	br, err := newByteReader(ctx, sr, bufSize)
	return &kvReader{
		byteReader: br,
	}, nil
}

func (r *kvReader) nextKV() (key, val []byte, err error) {
	r.byteReader.reset()
	lenBytes, err := r.byteReader.readNBytes(8)
	if err != nil {
		return nil, nil, err
	}
	keyLen := int(binary.BigEndian.Uint64(*lenBytes))
	keyPtr, err := r.byteReader.readNBytes(keyLen)
	if err != nil {
		return nil, nil, err
	}
	lenBytes, err = r.byteReader.readNBytes(8)
	if err != nil {
		return nil, nil, err
	}
	valLen := int(binary.BigEndian.Uint64(*lenBytes))
	valPtr, err := r.byteReader.readNBytes(valLen)
	if err != nil {
		return nil, nil, err
	}
	return *keyPtr, *valPtr, nil
}

func (r *kvReader) Close() error {
	return r.byteReader.Close()
}
