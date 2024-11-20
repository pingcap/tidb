// Copyright 2024 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tikv

import (
	"bytes"
	"encoding/binary"
	"slices"

	rocks "github.com/lance6716/pebble"
	rocksbloom "github.com/lance6716/pebble/bloom"
	rockssst "github.com/lance6716/pebble/sstable"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/util/codec"
)

type WritableBytesChan chan []byte

func (w WritableBytesChan) Write(p []byte) error {
	w <- slices.Clone(p)
	// TODO(lance6716): add context to avoid blocking
	return nil
}

func (w WritableBytesChan) Finish() error {
	close(w)
	return nil
}

func (w WritableBytesChan) Abort() {
	close(w)
}

var fixedSuffixSliceTransform = &rockssst.Comparer{
	Name: "leveldb.BytewiseComparator",
	Compare: func(a, b []byte) int {
		return bytes.Compare(a, b)
	},
	SplitterName: "FixedSuffixSliceTransform",
	Split: func(a []byte) int {
		return len(a) - 8
	},
}

// newWriteCFWriter creates a new writeCFWriter.
func newWriteCFWriter(
	ts uint64,
) (*rockssst.Writer, WritableBytesChan, error) {
	writable := WritableBytesChan(make(chan []byte, 16))
	writer := rockssst.NewWriter(writable, rockssst.WriterOptions{
		// TODO(lance6716): should read TiKV config to know these values.
		BlockSize:   32 * 1024,
		Compression: rocks.ZstdCompression,
		// TODO(lance6716): should check the behaviour is the exactly same.
		FilterPolicy: rocksbloom.FilterPolicy(10),
		FilterType:   rockssst.TableFilter,
		Comparer:     fixedSuffixSliceTransform,
		MergerName:   "nullptr",
		TablePropertyCollectors: []func() rockssst.TablePropertyCollector{
			func() rockssst.TablePropertyCollector {
				return newMVCCPropCollector(ts)
			},
			func() rockssst.TablePropertyCollector {
				return newRangePropertiesCollector()
			},
			// titan is only triggered when SST compaction at TiKV side.
			func() rockssst.TablePropertyCollector {
				return mockCollector{name: "BlobFileSizeCollector"}
			},
		},
	})
	return writer, writable, nil
}

// newDefaultCFWriter creates a new defaultCFWriter.
func newDefaultCFWriter() (*rockssst.Writer, WritableBytesChan, error) {
	writable := WritableBytesChan(make(chan []byte, 16))
	writer := rockssst.NewWriter(writable, rockssst.WriterOptions{
		// TODO(lance6716): should read TiKV config to know these values.
		BlockSize:   32 * 1024,
		Compression: rocks.ZstdCompression,
		// TODO(lance6716): should check the behaviour is the exactly same.
		FilterPolicy: rocksbloom.FilterPolicy(10),
		MergerName:   "nullptr",
		TablePropertyCollectors: []func() rockssst.TablePropertyCollector{
			func() rockssst.TablePropertyCollector {
				return newRangePropertiesCollector()
			},
			// titan is only triggered when SST compaction at TiKV side.
			func() rockssst.TablePropertyCollector {
				return mockCollector{name: "BlobFileSizeCollector"}
			},
		},
	})
	return writer, writable, nil
}

func encodeKey4SST(key []byte, ts uint64) []byte {
	// key layout in this case:
	// z{mem-comparable encoded key}{bit-wise reversed TS}
	actualKey := make([]byte, 0, 1+codec.EncodedBytesLength(len(key))+8)
	// keys::data_key will add the 'z' prefix [1] at `TxnSstWriter.put` [2].
	//
	// [1] https://github.com/tikv/tikv/blob/7793f1d5dc40206fe406ca001be1e0d7f1b83a8f/components/keys/src/lib.rs#L206
	// [2] https://github.com/tikv/tikv/blob/7793f1d5dc40206fe406ca001be1e0d7f1b83a8f/components/sst_importer/src/sst_writer.rs#L92
	actualKey = append(actualKey, 'z')
	// Key::from_raw [3] will encode the key as bytes at `TxnSstWriter.write` [4],
	// which is the caller of `TxnSstWriter.put` [2].
	//
	// [3] https://github.com/tikv/tikv/blob/7793f1d5dc40206fe406ca001be1e0d7f1b83a8f/components/txn_types/src/types.rs#L55
	// [4] https://github.com/tikv/tikv/blob/7793f1d5dc40206fe406ca001be1e0d7f1b83a8f/components/sst_importer/src/sst_writer.rs#L74
	actualKey = codec.EncodeBytes(actualKey, key)
	// Key::append_ts [5] will append the bit-wise reverted ts at
	// `TxnSstWriter.write` [4].
	//
	// [5] https://github.com/tikv/tikv/blob/7793f1d5dc40206fe406ca001be1e0d7f1b83a8f/components/txn_types/src/types.rs#L118
	actualKey = binary.BigEndian.AppendUint64(actualKey, ^ts)
	return actualKey
}

func isShortValue(val []byte) bool {
	return len(val) <= 255
}

func encodeShortValue4SST(value []byte, ts uint64) []byte {
	// value layout in this case:
	// P{varint-encoded TS}v{value length}{value}
	actualValue := make([]byte, 0, 1+binary.MaxVarintLen64+1+1+len(value))
	// below logic can be found at `WriteRef.to_bytes` [6]. This function is called
	// at `TxnSstWriter.put` [2].
	//
	// [6] https://github.com/tikv/tikv/blob/7793f1d5dc40206fe406ca001be1e0d7f1b83a8f/components/txn_types/src/write.rs#L362
	actualValue = append(actualValue, 'P')
	actualValue = binary.AppendUvarint(actualValue, ts)
	actualValue = append(actualValue, 'v')
	actualValue = append(actualValue, byte(len(value)))
	actualValue = append(actualValue, value...)
	return actualValue
}

func encodeLongValue4SST(ts uint64) []byte {
	// value layout in this case:
	// P{varint-encoded TS}
	actualValue := make([]byte, 0, 1+binary.MaxVarintLen64)
	// below logic can be found at `WriteRef.to_bytes` [6].
	actualValue = append(actualValue, 'P')
	actualValue = binary.AppendUvarint(actualValue, ts)
	return actualValue
}

type LocalSSTWriter struct {
	ts             uint64
	defaultCF      *rockssst.Writer
	defaultCFHasKV bool
	defaultCFChan  WritableBytesChan
	writeCF        *rockssst.Writer
}

func NewLocalSSTWriter(
	ts uint64,
) (
	sstWriter *LocalSSTWriter,
	defaultCFChan WritableBytesChan,
	writeCFChan WritableBytesChan,
	err error,
) {
	ret := &LocalSSTWriter{ts: ts}

	ret.defaultCF, ret.defaultCFChan, err = newDefaultCFWriter()
	if err != nil {
		return nil, nil, nil, errors.Trace(err)
	}
	ret.writeCF, writeCFChan, err = newWriteCFWriter(ts)
	if err != nil {
		return nil, nil, nil, errors.Trace(err)
	}
	return ret, ret.defaultCFChan, writeCFChan, nil
}

func (w *LocalSSTWriter) Set(key, value []byte) error {
	actualKey := encodeKey4SST(key, w.ts)
	if isShortValue(value) {
		actualValue := encodeShortValue4SST(value, w.ts)
		return errors.Trace(w.writeCF.Set(actualKey, actualValue))
	}

	if err := w.defaultCF.Set(actualKey, value); err != nil {
		return errors.Trace(err)
	}
	w.defaultCFHasKV = true
	return errors.Trace(w.writeCF.Set(actualKey, encodeLongValue4SST(w.ts)))
}

// Close flushes the SST files to disk and return the SST file paths that can be
// ingested into default / write column family.
func (w *LocalSSTWriter) Close() (
	errRet error,
) {
	var err error
	if w.defaultCFHasKV {
		err = w.defaultCF.Close()
	} else {
		w.defaultCFChan.Abort()
	}
	err2 := w.writeCF.Close()
	if err != nil {
		return errors.Trace(err)
	}
	if err2 != nil {
		return errors.Trace(err2)
	}
	return nil
}
