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
	"context"
	"encoding/binary"
	"encoding/json"
	"slices"
	"strings"
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/objstorage/objstorageprovider"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/google/uuid"
	rocks "github.com/lance6716/pebble"
	rocksbloom "github.com/lance6716/pebble/bloom"
	rockssst "github.com/lance6716/pebble/sstable"
	"github.com/stretchr/testify/require"
)

var (
	sortedKVs = [][2][]byte{
		{[]byte("a"), []byte("1")},
	}
	ts uint64 = 1
)

func TestGRPCWriteToTiKV(t *testing.T) {
	t.Skip("this is a manual test")

	ctx := context.Background()
	pdAddrs := []string{"127.0.0.1:2379"}

	metas, err := write2ImportService4Test(ctx, pdAddrs, sortedKVs, ts)
	require.NoError(t, err)
	for _, meta := range metas {
		t.Logf("meta UUID: %v", uuid.UUID(meta.Uuid).String())
	}
}

type mockCollector struct {
	name string
}

func (m mockCollector) Add(key rockssst.InternalKey, value []byte) error {
	return nil
}

func (m mockCollector) Finish(userProps map[string]string) error {
	return nil
}

func (m mockCollector) Name() string {
	return m.name
}

type indexHandleKV struct {
	key    []byte
	size   uint64
	offset uint64
}

type mvccPropCollector struct {
	props struct {
		minTS          uint64
		maxTS          uint64
		numRows        uint64
		numPuts        uint64
		numDeletes     uint64
		numVersions    uint64
		maxRowVersions uint64
		ttl            struct {
			maxExpireTS *uint64
			minExpireTS *uint64
		}
	}
	curIndexSize    uint64
	curIndexOffset  uint64
	rowIndexHandles []indexHandleKV
}

func newMVCCPropCollector(ts uint64) *mvccPropCollector {
	ret := &mvccPropCollector{}
	ret.props.minTS = ts
	ret.props.maxTS = ts
	ret.props.maxRowVersions = 1
	return ret
}

func (m *mvccPropCollector) Add(key rockssst.InternalKey, _ []byte) error {
	m.props.numVersions++
	m.props.numRows++
	m.props.numPuts++

	m.curIndexSize++
	m.curIndexOffset++
	if m.curIndexOffset == 1 || m.curIndexSize >= 10000 {
		// trim TS
		keyWithoutTS := key.UserKey[:len(key.UserKey)-8]
		m.rowIndexHandles = append(m.rowIndexHandles, indexHandleKV{
			key:    keyWithoutTS,
			size:   m.curIndexSize,
			offset: m.curIndexOffset,
		})
		m.curIndexSize = 0
	}

	return nil
}

func (m *mvccPropCollector) Finish(userProps map[string]string) error {
	userProps["tikv.min_ts"] = string(binary.BigEndian.AppendUint64(nil, m.props.minTS))
	userProps["tikv.max_ts"] = string(binary.BigEndian.AppendUint64(nil, m.props.maxTS))
	userProps["tikv.num_rows"] = string(binary.BigEndian.AppendUint64(nil, m.props.numRows))
	userProps["tikv.num_puts"] = string(binary.BigEndian.AppendUint64(nil, m.props.numPuts))
	userProps["tikv.num_deletes"] = string(binary.BigEndian.AppendUint64(nil, m.props.numDeletes))
	userProps["tikv.num_versions"] = string(binary.BigEndian.AppendUint64(nil, m.props.numVersions))
	userProps["tikv.max_row_versions"] = string(binary.BigEndian.AppendUint64(nil, m.props.maxRowVersions))
	if m.props.ttl.maxExpireTS != nil {
		userProps["tikv.max_expire_ts"] = string(binary.BigEndian.AppendUint64(nil, *m.props.ttl.maxExpireTS))
	}
	if m.props.ttl.minExpireTS != nil {
		userProps["tikv.min_expire_ts"] = string(binary.BigEndian.AppendUint64(nil, *m.props.ttl.minExpireTS))
	}
	userProps["tikv.num_errors"] = string(binary.BigEndian.AppendUint64(nil, 0))

	slices.SortFunc(m.rowIndexHandles, func(i, j indexHandleKV) int {
		return bytes.Compare(i.key, j.key)
	})
	buf := make([]byte, 0, 1024)
	for _, handle := range m.rowIndexHandles {
		buf = binary.BigEndian.AppendUint64(buf, uint64(len(handle.key)))
		buf = append(buf, handle.key...)
		buf = binary.BigEndian.AppendUint64(buf, handle.size)
		buf = binary.BigEndian.AppendUint64(buf, handle.offset)
	}
	userProps["tikv.rows_index"] = string(buf)
	return nil
}

func (m *mvccPropCollector) Name() string {
	return "tikv.mvcc-properties-collector"
}

type rangeOffsets struct {
	Size uint64
	Keys uint64
}

type rangeProperty struct {
	Key []byte
	rangeOffsets
}

type rangeProperties []rangeProperty

// Encode encodes the range properties into a byte slice.
func (r rangeProperties) Encode() []byte {
	b := make([]byte, 0, 1024)
	for _, p := range r {
		b = binary.BigEndian.AppendUint64(b, uint64(len(p.Key)))
		b = append(b, p.Key...)
		b = binary.BigEndian.AppendUint64(b, p.Size)
		b = binary.BigEndian.AppendUint64(b, p.Keys)
	}
	return b
}

// RangePropertiesCollector collects range properties for each range.
type RangePropertiesCollector struct {
	props               rangeProperties
	lastOffsets         rangeOffsets
	lastKey             []byte
	currentOffsets      rangeOffsets
	propSizeIdxDistance uint64
	propKeysIdxDistance uint64
}

func newRangePropertiesCollector() *RangePropertiesCollector {
	return &RangePropertiesCollector{
		props:               make([]rangeProperty, 0, 1024),
		propSizeIdxDistance: 4 * 1024 * 1024,
		propKeysIdxDistance: 40 * 1024,
	}
}

func (c *RangePropertiesCollector) sizeInLastRange() uint64 {
	return c.currentOffsets.Size - c.lastOffsets.Size
}

func (c *RangePropertiesCollector) keysInLastRange() uint64 {
	return c.currentOffsets.Keys - c.lastOffsets.Keys
}

func (c *RangePropertiesCollector) insertNewPoint(key []byte) {
	c.lastOffsets = c.currentOffsets
	c.props = append(c.props, rangeProperty{Key: append([]byte{}, key...), rangeOffsets: c.currentOffsets})
}

// Add implements `pebble.TablePropertyCollector`.
func (c *RangePropertiesCollector) Add(key rockssst.InternalKey, value []byte) error {
	c.currentOffsets.Size += uint64(len(value)) + uint64(len(key.UserKey))
	c.currentOffsets.Keys++
	if len(c.lastKey) == 0 || c.sizeInLastRange() >= c.propSizeIdxDistance ||
		c.keysInLastRange() >= c.propKeysIdxDistance {
		c.insertNewPoint(key.UserKey)
	}
	c.lastKey = append(c.lastKey[:0], key.UserKey...)
	return nil
}

// Finish implements `pebble.TablePropertyCollector`.
func (c *RangePropertiesCollector) Finish(userProps map[string]string) error {
	if c.sizeInLastRange() > 0 || c.keysInLastRange() > 0 {
		c.insertNewPoint(c.lastKey)
	}

	userProps["tikv.range_index"] = string(c.props.Encode())
	return nil
}

// Name implements `pebble.TablePropertyCollector`.
func (*RangePropertiesCollector) Name() string {
	return "tikv.range-properties-collector"
}

func TestPebbleWriteSST(t *testing.T) {
	//t.Skip("this is a manual test")

	kvs := encodeKVs4Test(sortedKVs, ts)

	sstPath := "/tmp/test-write.sst"
	f, err := vfs.Default.Create(sstPath)
	require.NoError(t, err)
	writable := objstorageprovider.NewFileWritable(f)

	writer := rockssst.NewWriter(writable, rockssst.WriterOptions{
		Compression:  rocks.ZstdCompression, // should read TiKV config, and CF differs
		FilterPolicy: rocksbloom.FilterPolicy(10),
		MergerName:   "nullptr",
		TablePropertyCollectors: []func() rockssst.TablePropertyCollector{
			func() rockssst.TablePropertyCollector {
				return newMVCCPropCollector(ts)
			},
			func() rockssst.TablePropertyCollector {
				return newRangePropertiesCollector()
			},
			func() rockssst.TablePropertyCollector {
				return mockCollector{name: "BlobFileSizeCollector"}
			},
		},
	}, &rockssst.Identity{
		DB:                 "SST Writer",
		Host:               "lance6716-nuc10i7fnh",
		Session:            "DS38NDUWK5HLG8SSL5M7",
		OriginalFileNumber: 1,
	})
	for _, kv := range kvs {
		t.Logf("key: %X\nvalue: %X", kv[0], kv[1])
		err = writer.Set(kv[0], kv[1])
		require.NoError(t, err)
	}
	err = writer.Close()
	require.NoError(t, err)

	f, err = vfs.Default.Open(sstPath)
	require.NoError(t, err)
	readable, err := rockssst.NewSimpleReadable(f)
	require.NoError(t, err)
	reader, err := rockssst.NewReader(readable, rockssst.ReaderOptions{})
	require.NoError(t, err)
	defer reader.Close()

	layout, err := reader.Layout()
	require.NoError(t, err)

	infos := layout.BlockInfos(reader)
	expected := `
[
	{"Offset":0,"Length":42,"Name":"data","Compression":0,"Checksum":2258416982},
	{"Offset":121,"Length":39,"Name":"index","Compression":0,"Checksum":3727189474},
	{"Offset":165,"Length":1253,"Name":"properties","Compression":0,"Checksum":561778464},
	{"Offset":1423,"Length":79,"Name":"meta-index","Compression":0,"Checksum":955781521},
	{"Offset":1507,"Length":53,"Name":"footer","Compression":0,"Checksum":0}
]`
	var expectedInfos []*rockssst.BlockInfo
	err = json.Unmarshal([]byte(expected), &expectedInfos)
	require.NoError(t, err)
	require.Equal(t, expectedInfos, infos)
}

func TestDebugReadSST(t *testing.T) {
	t.Skip("this is a manual test")

	sstPath := "/tmp/test-write.sst"
	t.Logf("read sst: %s", sstPath)
	f, err := vfs.Default.Open(sstPath)
	require.NoError(t, err)
	readable, err := sstable.NewSimpleReadable(f)
	require.NoError(t, err)
	reader, err := sstable.NewReader(readable, sstable.ReaderOptions{})
	require.NoError(t, err)
	defer reader.Close()

	layout, err := reader.Layout()
	require.NoError(t, err)

	content := &strings.Builder{}
	layout.Describe(content, true, reader, nil)

	t.Logf("layout:\n %s", content.String())
	t.Logf("properties:\n %s", reader.Properties.String())

	iter, err := reader.NewIter(nil, nil)
	require.NoError(t, err)
	defer iter.Close()

	k, v := iter.First()
	if k == nil {
		return
	}
	getValue := func(v pebble.LazyValue) []byte {
		realV, _, err2 := v.Value(nil)
		require.NoError(t, err2)
		return realV
	}
	t.Logf("key: %X\nvalue: %X", k.UserKey, getValue(v))
	for {
		k, v = iter.Next()
		if k == nil {
			break
		}
		t.Logf("key: %X\nvalue: %X", k.UserKey, getValue(v))
	}
}
