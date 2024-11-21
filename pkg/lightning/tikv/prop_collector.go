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

	rockssst "github.com/cockroachdb/pebble/sstable"
)

type mockCollector struct {
	name string
}

var _ rockssst.TablePropertyCollector = mockCollector{}

// Add implements the TablePropertyCollector interface.
func (mockCollector) Add(rockssst.InternalKey, []byte) error {
	return nil
}

// Finish implements the TablePropertyCollector interface.
func (mockCollector) Finish(map[string]string) error {
	return nil
}

// Name implements the TablePropertyCollector interface.
func (m mockCollector) Name() string {
	return m.name
}

type indexHandleKV struct {
	key    []byte
	size   uint64
	offset uint64
}

// mvccPropCollector is a specialized version of TiKV's `MvccPropertiesCollector`.
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
	lastRow         []byte
	curIndexSize    uint64
	curIndexOffset  uint64
	rowIndexHandles []indexHandleKV
}

var _ rockssst.TablePropertyCollector = (*mvccPropCollector)(nil)

func newMVCCPropCollector(ts uint64) *mvccPropCollector {
	ret := &mvccPropCollector{}
	ret.props.minTS = ts
	ret.props.maxTS = ts
	ret.props.maxRowVersions = 1
	return ret
}

// Add implements the TablePropertyCollector interface. It mimics
// https://github.com/tikv/tikv/blob/7793f1d5dc40206fe406ca001be1e0d7f1b83a8f/components/engine_rocks/src/properties.rs#L407.
func (m *mvccPropCollector) Add(key rockssst.InternalKey, _ []byte) error {
	m.props.numVersions++
	m.props.numRows++
	m.props.numPuts++

	m.curIndexSize++
	m.curIndexOffset++
	m.lastRow = key.UserKey[:len(key.UserKey)-8]
	if m.curIndexOffset == 1 || m.curIndexSize >= 10000 {
		m.rowIndexHandles = append(m.rowIndexHandles, indexHandleKV{
			key:    m.lastRow,
			size:   m.curIndexSize,
			offset: m.curIndexOffset,
		})
		m.curIndexSize = 0
	}

	return nil
}

// Finish implements the TablePropertyCollector interface. It mimics
// https://github.com/tikv/tikv/blob/7793f1d5dc40206fe406ca001be1e0d7f1b83a8f/components/engine_rocks/src/properties.rs#L505.
func (m *mvccPropCollector) Finish(userProps map[string]string) error {
	if m.curIndexSize > 0 {
		m.rowIndexHandles = append(m.rowIndexHandles, indexHandleKV{
			key:    m.lastRow,
			size:   m.curIndexSize,
			offset: m.curIndexOffset,
		})
	}

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

// Name implements the TablePropertyCollector interface.
func (*mvccPropCollector) Name() string {
	return "tikv.mvcc-properties-collector"
}

type rangeOffsets struct {
	size     uint64
	keyCount uint64
}

type rangeProperty struct {
	key []byte
	rangeOffsets
}

type rangeProperties []rangeProperty

// encode encodes the range properties into a byte slice.
func (r rangeProperties) encode() []byte {
	b := make([]byte, 0, 1024)
	for _, p := range r {
		b = binary.BigEndian.AppendUint64(b, uint64(len(p.key)))
		b = append(b, p.key...)
		b = binary.BigEndian.AppendUint64(b, p.size)
		b = binary.BigEndian.AppendUint64(b, p.keyCount)
	}
	return b
}

// rangePropertiesCollector is a specialized version of TiKV's `RangePropertiesCollector`.
type rangePropertiesCollector struct {
	props               rangeProperties
	lastOffsets         rangeOffsets
	lastKey             []byte
	currentOffsets      rangeOffsets
	propSizeIdxDistance uint64
	propKeysIdxDistance uint64
}

func newRangePropertiesCollector() *rangePropertiesCollector {
	return &rangePropertiesCollector{
		props:               make([]rangeProperty, 0, 1024),
		propSizeIdxDistance: 4 * 1024 * 1024,
		propKeysIdxDistance: 40 * 1024,
	}
}

func (c *rangePropertiesCollector) sizeInLastRange() uint64 {
	return c.currentOffsets.size - c.lastOffsets.size
}

func (c *rangePropertiesCollector) keysInLastRange() uint64 {
	return c.currentOffsets.keyCount - c.lastOffsets.keyCount
}

func (c *rangePropertiesCollector) insertNewPoint(key []byte) {
	c.lastOffsets = c.currentOffsets
	c.props = append(c.props, rangeProperty{key: append([]byte{}, key...), rangeOffsets: c.currentOffsets})
}

// Add implements the TablePropertyCollector interface. It mimics
// https://github.com/tikv/tikv/blob/7793f1d5dc40206fe406ca001be1e0d7f1b83a8f/components/engine_rocks/src/properties.rs#L329.
func (c *rangePropertiesCollector) Add(key rockssst.InternalKey, value []byte) error {
	c.currentOffsets.size += uint64(len(value)) + uint64(len(key.UserKey))
	c.currentOffsets.keyCount++
	if len(c.lastKey) == 0 || c.sizeInLastRange() >= c.propSizeIdxDistance ||
		c.keysInLastRange() >= c.propKeysIdxDistance {
		c.insertNewPoint(key.UserKey)
	}
	c.lastKey = append(c.lastKey[:0], key.UserKey...)
	return nil
}

// Finish implements the TablePropertyCollector interface. It mimics
// https://github.com/tikv/tikv/blob/7793f1d5dc40206fe406ca001be1e0d7f1b83a8f/components/engine_rocks/src/properties.rs#L349.
func (c *rangePropertiesCollector) Finish(userProps map[string]string) error {
	if c.sizeInLastRange() > 0 || c.keysInLastRange() > 0 {
		c.insertNewPoint(c.lastKey)
	}

	userProps["tikv.range_index"] = string(c.props.encode())
	return nil
}

// Name implements the TablePropertyCollector interface.
func (*rangePropertiesCollector) Name() string {
	return "tikv.range-properties-collector"
}
