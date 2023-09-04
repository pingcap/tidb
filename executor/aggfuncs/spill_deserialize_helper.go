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
	"github.com/pingcap/tidb/util/agg_spill"
)

// This flag means that the length of deserialized data is not fixed
const varLenFlag = -1

type strSizeType uint16

type spillDeserializeHelper struct {
	buf        []byte
	bufLen     int
	typeLen    int
	readPos    int
	readPosEnd int
}

func newDeserializeHelper(buf []byte, typeLen int) spillDeserializeHelper {
	readPosEnd := len(buf)
	if typeLen != varLenFlag {
		readPosEnd -= typeLen + 1
	}

	return spillDeserializeHelper{
		buf:        buf,
		bufLen:     len(buf),
		typeLen:    typeLen,
		readPos:    0,
		readPosEnd: readPosEnd,
	}
}

func (d *spillDeserializeHelper) readBool(dst *bool) bool {
	if d.readPos < d.readPosEnd {
		*dst = agg_spill.DeserializeBool(d.buf, d.readPos)
		d.readPos += d.typeLen
		return true
	}
	return false
}

func (d *spillDeserializeHelper) readUint32(dst *uint32) bool {
	if d.readPos < d.readPosEnd {
		*dst = agg_spill.DeserializeUint32(d.buf, d.readPos)
		d.readPos += d.typeLen
		return true
	}
	return false
}

func (d *spillDeserializeHelper) readUint64(dst *uint64) bool {
	if d.readPos < d.readPosEnd {
		*dst = agg_spill.DeserializeUint64(d.buf, d.readPos)
		d.readPos += d.typeLen
		return true
	}
	return false
}

func (d *spillDeserializeHelper) readInt64(dst *int64) bool {
	if d.readPos < d.readPosEnd {
		*dst = agg_spill.DeserializeInt64(d.buf, d.readPos)
		d.readPos += d.typeLen
		return true
	}
	return false
}

func (d *spillDeserializeHelper) readFloat64(dst *float64) bool {
	if d.readPos < d.readPosEnd {
		*dst = agg_spill.DeserializeFloat64(d.buf, d.readPos)
		d.readPos += d.typeLen
		return true
	}
	return false
}

func (d *spillDeserializeHelper) readTime(dst *types.Time) bool {
	if d.readPos < d.readPosEnd {
		coreTime := *(*types.CoreTime)(unsafe.Pointer(&d.buf[d.readPos]))
		dst.SetCoreTime(coreTime)
		d.readPos += d.typeLen
		return true
	}
	return false
}

func (d *spillDeserializeHelper) readMyDecimal(dst *types.MyDecimal) (bool, error) {
	if d.readPos < d.readPosEnd {
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
	if d.readPos < d.readPosEnd {
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
