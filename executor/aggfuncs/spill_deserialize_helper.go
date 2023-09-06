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

package aggfuncs

import (
	"unsafe"

	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/spill"
)

const boolLen = int64(unsafe.Sizeof(true))
const uint32Len = int64(unsafe.Sizeof(uint32(0)))
const uint64Len = int64(unsafe.Sizeof(uint64(0)))
const int8Len = int64(unsafe.Sizeof(int8(0)))
const int32Len = int64(unsafe.Sizeof(int32(0)))
const int64Len = int64(unsafe.Sizeof(int64(0)))
const float32Len = int64(unsafe.Sizeof(float32(0)))
const float64Len = int64(unsafe.Sizeof(float64(0)))
const timeLen = int64(unsafe.Sizeof(types.Time{}))

type strSizeType uint16

type spillDeserializeHelper struct {
	buf     []byte
	bufLen  int64
	readPos int64
}

func newDeserializeHelper(buf []byte) spillDeserializeHelper {
	return spillDeserializeHelper{
		buf:     buf,
		bufLen:  int64(len(buf)),
		readPos: 0,
	}
}

func (d *spillDeserializeHelper) readBool(dst *bool) bool {
	if d.readPos+boolLen <= d.bufLen {
		*dst = spill.DeserializeBool(d.buf, d.readPos)
		d.readPos += boolLen
		return true
	}
	return false
}

func (d *spillDeserializeHelper) readUint32(dst *uint32) bool {
	if d.readPos+uint32Len <= d.bufLen {
		*dst = spill.DeserializeUint32(d.buf, d.readPos)
		d.readPos += uint32Len
		return true
	}
	return false
}

func (d *spillDeserializeHelper) readUint64(dst *uint64) bool {
	if d.readPos+uint64Len <= d.bufLen {
		*dst = spill.DeserializeUint64(d.buf, d.readPos)
		d.readPos += uint64Len
		return true
	}
	return false
}

func (d *spillDeserializeHelper) readInt64(dst *int64) bool {
	if d.readPos+int64Len <= d.bufLen {
		*dst = spill.DeserializeInt64(d.buf, d.readPos)
		d.readPos += int64Len
		return true
	}
	return false
}

func (d *spillDeserializeHelper) readFloat32(dst *float32) bool {
	if d.readPos+float32Len <= d.bufLen {
		*dst = spill.DeserializeFloat32(d.buf, d.readPos)
		d.readPos += float32Len
		return true
	}
	return false
}

func (d *spillDeserializeHelper) readFloat64(dst *float64) bool {
	if d.readPos+float64Len <= d.bufLen {
		*dst = spill.DeserializeFloat64(d.buf, d.readPos)
		d.readPos += float64Len
		return true
	}
	return false
}

func (d *spillDeserializeHelper) readTime(dst *types.Time) bool {
	if d.readPos+timeLen <= d.bufLen {
		coreTime := *(*types.CoreTime)(unsafe.Pointer(&d.buf[d.readPos]))
		dst.SetCoreTime(coreTime)
		d.readPos += timeLen
		return true
	}
	return false
}

func (d *spillDeserializeHelper) readMyDecimal(dst *types.MyDecimal) (bool, error) {
	if d.readPos < d.bufLen {
		readByteNum, err := dst.DeserializeForSpill(d.buf[d.readPos:])
		if err != nil {
			return false, err
		}
		d.readPos += readByteNum
		return true, err
	}
	return false, nil
}

func (d *spillDeserializeHelper) readDuration(dst *types.Duration) (bool, error) {
	if d.readPos < d.bufLen {
		readByteNum, err := dst.DeserializeForSpill(d.buf[d.readPos:])
		if err != nil {
			return false, err
		}
		d.readPos += readByteNum
		return true, nil
	}
	return false, nil
}

// TODO if DefRowSize and DefInterfaceSize need to be deserialized?
