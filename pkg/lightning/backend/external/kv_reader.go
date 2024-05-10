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
	"io"

	"github.com/docker/go-units"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

var (
	// default read buf size of kvReader, this buf is split into 3 parts, 2 for prefetch
	// from storage, 1 for read by user.
	defaultReadBufferSize = 64 * units.KiB
)

type kvReader struct {
	byteReader *byteReader
}

func newKVReader(
	ctx context.Context,
	name string,
	store storage.ExternalStorage,
	initFileOffset uint64,
	bufSize int,
) (*kvReader, error) {
	oneThird := bufSize / 3
	sr, err := openStoreReaderAndSeek(ctx, store, name, initFileOffset, oneThird*2)
	if err != nil {
		return nil, err
	}
	br, err := newByteReader(ctx, sr, oneThird)
	if err != nil {
		return nil, err
	}
	return &kvReader{
		byteReader: br,
	}, nil
}

func (r *kvReader) nextKV() (key, val []byte, err error) {
	lenBytes, err := r.byteReader.readNBytes(8)
	if err != nil {
		return nil, nil, err
	}
	keyLen := int(binary.BigEndian.Uint64(lenBytes))
	lenBytes, err = r.byteReader.readNBytes(8)
	if err != nil {
		return nil, nil, noEOF(err)
	}
	valLen := int(binary.BigEndian.Uint64(lenBytes))
	keyAndValue, err := r.byteReader.readNBytes(keyLen + valLen)
	if err != nil {
		return nil, nil, noEOF(err)
	}
	return keyAndValue[:keyLen], keyAndValue[keyLen:], nil
}

// noEOF converts the EOF error to io.ErrUnexpectedEOF.
func noEOF(err error) error {
	if err == io.EOF {
		logutil.BgLogger().Warn("unexpected EOF", zap.Error(errors.Trace(err)))
		return io.ErrUnexpectedEOF
	}
	return err
}

func (r *kvReader) Close() error {
	if p := r.byteReader.concurrentReader.largeBufferPool; p != nil {
		p.Destroy()
	}
	return r.byteReader.Close()
}
