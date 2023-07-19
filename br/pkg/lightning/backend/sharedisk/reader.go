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

package sharedisk

import (
	"context"
	"encoding/binary"
	"io"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/mathutil"
	"go.uber.org/zap"
)

type kvReader struct {
	byteReader *byteReader
	key        []byte
	val        []byte
}

func newKVReader(ctx context.Context, name string, store storage.ExternalStorage, initFileOffset uint64, bufSize int) (*kvReader, error) {
	br, err := newByteReader(ctx, store, name, initFileOffset, bufSize)
	if err != nil {
		return nil, err
	}
	return &kvReader{
		byteReader: br,
		key:        nil,
		val:        nil,
	}, nil
}

func (r *kvReader) nextKV() (key, val []byte, err error) {
	if r.byteReader.eof() {
		return nil, nil, nil
	}
	r.byteReader.reset()
	lenBuf, err := r.byteReader.sliceNext(8)
	if err != nil {
		return nil, nil, err
	}
	keyLen := int(binary.BigEndian.Uint64(lenBuf.get()))
	kBuf, err := r.byteReader.sliceNext(keyLen)
	if err != nil {
		return nil, nil, err
	}
	lenBuf, err = r.byteReader.sliceNext(8)
	if err != nil {
		return nil, nil, err
	}
	valLen := int(binary.BigEndian.Uint64(lenBuf.get()))
	valBuf, err := r.byteReader.sliceNext(valLen)
	if err != nil {
		return nil, nil, err
	}
	return kBuf.get(), valBuf.get(), nil
}

func (r *kvReader) Close() error {
	return r.byteReader.Close()
}

type statsReader struct {
	byteReader *byteReader
	propBytes  []byte
}

func newStatsReader(ctx context.Context, store storage.ExternalStorage, name string, bufSize int) (*statsReader, error) {
	br, err := newByteReader(ctx, store, name, 0, bufSize)
	if err != nil {
		return nil, err
	}
	return &statsReader{
		byteReader: br,
		propBytes:  nil,
	}, nil
}

func (r *statsReader) nextProp() (*RangeProperty, error) {
	if r.byteReader.eof() {
		return nil, nil
	}
	r.byteReader.reset()
	lenBuf, err := r.byteReader.sliceNext(4)
	if err != nil {
		return nil, err
	}
	propLen := int(binary.BigEndian.Uint32(lenBuf.get()))
	if cap(r.propBytes) < propLen {
		r.propBytes = make([]byte, propLen)
	}
	propBytes, err := r.byteReader.sliceNext(propLen)
	if err != nil {
		return nil, err
	}
	return decodeProp(propBytes.get())
}

func (r *statsReader) Close() error {
	return r.byteReader.Close()
}

func decodeProp(data []byte) (*RangeProperty, error) {
	rp := &RangeProperty{}
	keyLen := binary.BigEndian.Uint32(data[0:4])
	rp.Key = data[4 : 4+keyLen]
	rp.Size = binary.BigEndian.Uint64(data[4+keyLen : 12+keyLen])
	rp.Keys = binary.BigEndian.Uint64(data[12+keyLen : 20+keyLen])
	rp.offset = binary.BigEndian.Uint64(data[20+keyLen : 28+keyLen])
	rp.WriterID = int(binary.BigEndian.Uint32(data[28+keyLen : 32+keyLen]))
	rp.DataSeq = int(binary.BigEndian.Uint32(data[32+keyLen : 36+keyLen]))
	return rp, nil
}

type byteReader struct {
	ctx           context.Context
	name          string
	storageReader storage.ReadSeekCloser

	buf       []byte
	bufOffset int

	fileStart uint64

	auxBuf   []byte
	cowSlice [][]byte
	isEOF    bool
}

func newByteReader(ctx context.Context, store storage.ExternalStorage, name string, initFileOffset uint64, bufSize int) (*byteReader, error) {
	storageReader, err := store.Open(ctx, name)
	if err != nil {
		return nil, err
	}
	_, err = storageReader.Seek(int64(initFileOffset), io.SeekStart)
	if err != nil {
		return nil, err
	}
	br := &byteReader{
		ctx:           ctx,
		name:          name,
		storageReader: storageReader,
		buf:           make([]byte, bufSize),
		bufOffset:     bufSize,
	}
	return br, err
}

type slice interface {
	get() []byte
}

type commonSlice []byte

func (c commonSlice) get() []byte {
	return c
}

type lazySlice struct {
	cowIdx     int
	byteReader *byteReader
}

func (s lazySlice) get() []byte {
	return s.byteReader.cowSlice[s.cowIdx]
}

// sliceNext reads the next n bytes from the reader and returns a buffer slice containing those bytes.
// If the reader has fewer than n bytes remaining in current buffer, `auxBuf` is used as a container instead.
func (r *byteReader) sliceNext(n int) (slice, error) {
	b := r.next(n)
	readLen := len(b)
	if readLen == n {
		return mkLazySlice(r, b), nil
	}
	if cap(r.auxBuf) < n {
		r.auxBuf = make([]byte, n)
	}
	r.auxBuf = r.auxBuf[:n]
	copy(r.auxBuf, b)
	for readLen < n {
		r.cloneSlices()
		err := r.reload()
		if err != nil {
			return nil, err
		}
		b = r.next(n - readLen)
		copy(r.auxBuf[readLen:], b)
		readLen += len(b)
	}
	return commonSlice(r.auxBuf), nil
}

func mkLazySlice(r *byteReader, s []byte) lazySlice {
	r.cowSlice = append(r.cowSlice, s)
	return lazySlice{
		cowIdx:     len(r.cowSlice) - 1,
		byteReader: r,
	}
}

func (r *byteReader) reset() {
	r.cowSlice = r.cowSlice[:0]
}

func (r *byteReader) cloneSlices() {
	for i := range r.cowSlice {
		r.cowSlice[i] = append([]byte(nil), r.cowSlice[i]...)
	}
}

func (r *byteReader) eof() bool {
	return r.isEOF && len(r.buf) == r.bufOffset
}

func (r *byteReader) next(n int) []byte {
	end := mathutil.Min(r.bufOffset+n, len(r.buf))
	ret := r.buf[r.bufOffset:end]
	r.bufOffset += len(ret)
	return ret
}

func (r *byteReader) reload() error {
	startTime := time.Now()
	nBytes, err := io.ReadFull(r.storageReader, r.buf[0:])
	if err == io.EOF {
		logutil.BgLogger().Error("unexpected EOF", zap.String("file", r.name), zap.Uint64("start", r.fileStart))
		r.isEOF = true
		return err
	} else if err != nil && err == io.ErrUnexpectedEOF {
		r.isEOF = true
	} else if err != nil {
		logutil.BgLogger().Warn("Other error during reading from external storage", zap.String("file", r.name), zap.Uint64("start", r.fileStart), zap.Error(err))
		return err
	}
	elapsed := time.Since(startTime).Microseconds()
	ReadByteForTest.Add(uint64(nBytes))
	ReadTimeForTest.Add(uint64(elapsed))
	readRate := float64(nBytes) / 1024.0 / 1024.0 / (float64(time.Since(startTime).Microseconds()) / 1000000.0)
	log.Info("s3 read rate", zap.Any("res", readRate))
	metrics.GlobalSortSharedDiskRate.WithLabelValues("read").Observe(readRate)
	ReadIOCnt.Add(1)
	r.bufOffset = 0
	if nBytes < len(r.buf) {
		// The last batch.
		r.buf = r.buf[:nBytes]
	}
	return nil
}

func (r *byteReader) Close() error {
	return r.storageReader.Close()
}
