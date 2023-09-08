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
	"time"
	"unsafe"

	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/hack"
	"github.com/pingcap/tidb/util/spill"
)

const intLen = int64(unsafe.Sizeof(int(0)))
const boolLen = int64(unsafe.Sizeof(true))
const uint32Len = int64(unsafe.Sizeof(uint32(0)))
const uint64Len = int64(unsafe.Sizeof(uint64(0)))
const int8Len = int64(unsafe.Sizeof(int8(0)))
const int32Len = int64(unsafe.Sizeof(int32(0)))
const int64Len = int64(unsafe.Sizeof(int64(0)))
const float32Len = int64(unsafe.Sizeof(float32(0)))
const float64Len = int64(unsafe.Sizeof(float64(0)))
const timeLen = int64(unsafe.Sizeof(types.Time{}))
const durationLen = int64(unsafe.Sizeof(time.Duration(0)))

type strSizeType uint16

type spillDeserializeHelper struct {
	column       *chunk.Column
	readRowIndex int
	rowNum       int
}

func newDeserializeHelper(column *chunk.Column, rowNum int) spillDeserializeHelper {
	return spillDeserializeHelper{
		column:       column,
		readRowIndex: 0,
		rowNum:       rowNum,
	}
}

func (s *spillDeserializeHelper) deserializePartialResult4Count(dst *partialResult4Count) bool {
	if s.readRowIndex < s.rowNum {
		bytes := s.column.GetBytes(s.readRowIndex)
		*dst = spill.DeserializeInt64(bytes, 0)
		s.readRowIndex++
		return true
	}
	return false
}

func (s *spillDeserializeHelper) deserializePartialResult4MaxMinInt(dst *partialResult4MaxMinInt) bool {
	if s.readRowIndex < s.rowNum {
		bytes := s.column.GetBytes(s.readRowIndex)
		dst.val = spill.DeserializeInt64(bytes, 0)
		dst.isNull = spill.DeserializeBool(bytes, int64Len)
		s.readRowIndex++
		return true
	}
	return false
}

func (s *spillDeserializeHelper) deserializePartialResult4MaxMinUint(dst *partialResult4MaxMinUint) bool {
	if s.readRowIndex < s.rowNum {
		bytes := s.column.GetBytes(s.readRowIndex)
		dst.val = spill.DeserializeUint64(bytes, 0)
		dst.isNull = spill.DeserializeBool(bytes, uint64Len)
		s.readRowIndex++
		return true
	}
	return false
}

func (s *spillDeserializeHelper) deserializePartialResult4MaxMinDecimal(dst *partialResult4MaxMinDecimal) bool {
	if s.readRowIndex < s.rowNum {
		bytes := s.column.GetBytes(s.readRowIndex)
		dst.val = *(*types.MyDecimal)(unsafe.Pointer(&bytes[0]))
		dst.isNull = spill.DeserializeBool(bytes, types.MyDecimalStructSize)
		s.readRowIndex++
		return true
	}
	return false
}

func (s *spillDeserializeHelper) deserializePartialResult4MaxMinFloat32(dst *partialResult4MaxMinFloat32) bool {
	if s.readRowIndex < s.rowNum {
		bytes := s.column.GetBytes(s.readRowIndex)
		dst.val = spill.DeserializeFloat32(bytes, 0)
		dst.isNull = spill.DeserializeBool(bytes, float32Len)
		s.readRowIndex++
		return true
	}
	return false
}

func (s *spillDeserializeHelper) deserializePartialResult4MaxMinFloat64(dst *partialResult4MaxMinFloat64) bool {
	if s.readRowIndex < s.rowNum {
		bytes := s.column.GetBytes(s.readRowIndex)
		dst.val = spill.DeserializeFloat64(bytes, 0)
		dst.isNull = spill.DeserializeBool(bytes, float64Len)
		s.readRowIndex++
		return true
	}
	return false
}

func (s *spillDeserializeHelper) deserializePartialResult4MaxMinTime(dst *partialResult4MaxMinTime) bool {
	if s.readRowIndex < s.rowNum {
		bytes := s.column.GetBytes(s.readRowIndex)
		dst.val = *(*types.Time)(unsafe.Pointer(&bytes[0]))
		dst.isNull = spill.DeserializeBool(bytes, timeLen)
		s.readRowIndex++
		return true
	}
	return false
}

func (s *spillDeserializeHelper) deserializePartialResult4MaxMinDuration(dst *partialResult4MaxMinDuration) bool {
	if s.readRowIndex < s.rowNum {
		bytes := s.column.GetBytes(s.readRowIndex)
		dst.val.Duration = *(*time.Duration)(unsafe.Pointer(&bytes[0]))
		dst.isNull = spill.DeserializeBool(bytes, durationLen)
		s.readRowIndex++
		return true
	}
	return false
}

func (s *spillDeserializeHelper) deserializePartialResult4MaxMinString(dst *partialResult4MaxMinString) bool {
	if s.readRowIndex < s.rowNum {
		bytes := s.column.GetBytes(s.readRowIndex)
		dst.isNull = spill.DeserializeBool(bytes, 0)
		dst.val = string(hack.String(bytes[boolLen:]))
		s.readRowIndex++
		return true
	}
	return false
}

func (s *spillDeserializeHelper) deserializePartialResult4MaxMinJSON(dst *partialResult4MaxMinJSON) bool {
	if s.readRowIndex < s.rowNum {
		bytes := s.column.GetBytes(s.readRowIndex)
		dst.val.TypeCode = bytes[0]
		dst.isNull = spill.DeserializeBool(bytes, 1)
		copy(dst.val.Value, bytes[1+boolLen:])
		s.readRowIndex++
		return true
	}
	return false
}

func (s *spillDeserializeHelper) deserializePartialResult4MaxMinEnum(dst *partialResult4MaxMinEnum) bool {
	if s.readRowIndex < s.rowNum {
		bytes := s.column.GetBytes(s.readRowIndex)
		dst.val.Value = spill.DeserializeUint64(bytes, 0)
		dst.isNull = spill.DeserializeBool(bytes, uint64Len)
		dst.val.Name = string(hack.String(bytes[boolLen+uint64Len:]))
		s.readRowIndex++
		return true
	}
	return false
}

func (s *spillDeserializeHelper) deserializePartialResult4MaxMinSet(dst *partialResult4MaxMinSet) bool {
	if s.readRowIndex < s.rowNum {
		bytes := s.column.GetBytes(s.readRowIndex)
		dst.val.Value = spill.DeserializeUint64(bytes, 0)
		dst.isNull = spill.DeserializeBool(bytes, uint64Len)
		dst.val.Name = string(hack.String(bytes[boolLen+uint64Len:]))
		s.readRowIndex++
		return true
	}
	return false
}

func (s *spillDeserializeHelper) deserializePartialResult4AvgDecimal(dst *partialResult4AvgDecimal) bool {
	if s.readRowIndex < s.rowNum {
		bytes := s.column.GetBytes(s.readRowIndex)
		dst.sum = *(*types.MyDecimal)(unsafe.Pointer(&bytes[0]))
		dst.count = spill.DeserializeInt64(bytes, types.MyDecimalStructSize)
		s.readRowIndex++
		return true
	}
	return false
}

func (s *spillDeserializeHelper) deserializePartialResult4AvgFloat64(dst *partialResult4AvgFloat64) bool {
	if s.readRowIndex < s.rowNum {
		bytes := s.column.GetBytes(s.readRowIndex)
		dst.sum = spill.DeserializeFloat64(bytes, 0)
		dst.count = spill.DeserializeInt64(bytes, float64Len)
		s.readRowIndex++
		return true
	}
	return false
}

func (s *spillDeserializeHelper) deserializePartialResult4SumDecimal(dst *partialResult4SumDecimal) bool {
	if s.readRowIndex < s.rowNum {
		bytes := s.column.GetBytes(s.readRowIndex)
		dst.val = *(*types.MyDecimal)(unsafe.Pointer(&bytes[0]))
		dst.notNullRowCount = spill.DeserializeInt64(bytes, types.MyDecimalStructSize)
		s.readRowIndex++
		return true
	}
	return false
}

func (s *spillDeserializeHelper) deserializePartialResult4SumFloat64(dst *partialResult4SumFloat64) bool {
	if s.readRowIndex < s.rowNum {
		bytes := s.column.GetBytes(s.readRowIndex)
		dst.val = spill.DeserializeFloat64(bytes, 0)
		dst.notNullRowCount = spill.DeserializeInt64(bytes, float64Len)
		s.readRowIndex++
		return true
	}
	return false
}
