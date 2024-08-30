// Copyright 2024 PingCAP, Inc.
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

package types

import (
	"encoding/binary"
	"math"
	"strconv"
	"unsafe"

	jsoniter "github.com/json-iterator/go"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/parser/types"
)

func init() {
	var buf [4]byte
	binary.NativeEndian.PutUint32(buf[:], 0x2)
	if buf[0] == 0x02 && buf[1] == 0x00 && buf[2] == 0x00 && buf[3] == 0x00 {
		return
	}
	panic("VectorFloat32 only supports little endian")
}

// VectorFloat32 represents a vector of float32.
//
// Memory Format:
// 4 byte       - Length
// 4 byte * N   - Data in Float32
//
// Normally, the data layout in storage (i.e. after serialization) is identical
// to the memory layout. However, in BigEndian machines, we have BigEndian in
// memory and always have LittleEndian in storage or during data exchange.
type VectorFloat32 struct {
	data []byte // Note: data must be a well-formatted byte slice (len >= 4)
}

// ZeroVectorFloat32 is a zero value of VectorFloat32.
var ZeroVectorFloat32 = InitVectorFloat32( /* dims= */ 0)

// InitVectorFloat32 initializes a vector with the given dimension. The values are initialized to zero.
func InitVectorFloat32(dims int) VectorFloat32 {
	data := make([]byte, 4+dims*4)
	binary.LittleEndian.PutUint32(data, uint32(dims))
	return VectorFloat32{data: data}
}

// CheckVectorDimValid checks if the vector's dimension is valid.
func CheckVectorDimValid(dim int) error {
	const (
		maxVectorDimension = 16383
	)
	if dim < 0 {
		return errors.Errorf("dimensions for type vector must be at least 0")
	}
	if dim > maxVectorDimension {
		return errors.Errorf("vector cannot have more than %d dimensions", maxVectorDimension)
	}
	return nil
}

// CheckDimsFitColumn checks if the vector has the expected dimension, which is defined by the column type or cast type.
func (v VectorFloat32) CheckDimsFitColumn(expectedFlen int) error {
	if expectedFlen != types.UnspecifiedLength && v.Len() != expectedFlen {
		return errors.Errorf("vector has %d dimensions, does not fit VECTOR(%d)", v.Len(), expectedFlen)
	}
	return nil
}

// Len returns the length (dimension) of the vector.
func (v VectorFloat32) Len() int {
	return int(binary.LittleEndian.Uint32(v.data))
}

// Elements returns a mutable typed slice of the elements.
func (v VectorFloat32) Elements() []float32 {
	l := v.Len()
	if l == 0 {
		return nil
	}
	return unsafe.Slice((*float32)(unsafe.Pointer(&v.data[4])), l)
}

// String returns a string representation of the vector, which can be parsed later.
func (v VectorFloat32) String() string {
	elements := v.Elements()

	buf := make([]byte, 0, 2+v.Len()*2)
	buf = append(buf, '[')
	for i, v := range elements {
		if i > 0 {
			buf = append(buf, ',')
		}
		buf = strconv.AppendFloat(buf, float64(v), 'f', -1, 32)
	}
	buf = append(buf, ']')

	// buf is not used elsewhere, so it's safe to just cast to String
	return unsafe.String(unsafe.SliceData(buf), len(buf))
}

// ZeroCopySerialize serializes the vector into a new byte slice, without memory copy.
func (v VectorFloat32) ZeroCopySerialize() []byte {
	return v.data
}

// SerializeTo serializes the vector into the byte slice.
func (v VectorFloat32) SerializeTo(b []byte) []byte {
	return append(b, v.data...)
}

// SerializedSize returns the size of the serialized data.
func (v VectorFloat32) SerializedSize() int {
	return len(v.data)
}

// EstimatedMemUsage returns the estimated memory usage.
func (v VectorFloat32) EstimatedMemUsage() int {
	return int(unsafe.Sizeof(v)) + len(v.data)
}

// PeekBytesAsVectorFloat32 trys to peek some bytes from b, until
// we can deserialize a VectorFloat32 from those bytes.
func PeekBytesAsVectorFloat32(b []byte) (n int, err error) {
	if len(b) < 4 {
		err = errors.Errorf("bad VectorFloat32 value header (len=%d)", len(b))
		return
	}

	elements := binary.LittleEndian.Uint32(b)
	totalDataSize := elements*4 + 4
	if len(b) < int(totalDataSize) {
		err = errors.Errorf("bad VectorFloat32 value (len=%d, expected=%d)", len(b), totalDataSize)
		return
	}
	return int(totalDataSize), nil
}

// ZeroCopyDeserializeVectorFloat32 deserializes the byte slice into a vector, without memory copy.
// Note: b must not be mutated, because this function does zero copy.
func ZeroCopyDeserializeVectorFloat32(b []byte) (VectorFloat32, []byte, error) {
	length, err := PeekBytesAsVectorFloat32(b)
	if err != nil {
		return ZeroVectorFloat32, b, err
	}
	return VectorFloat32{data: b[:length]}, b[length:], nil
}

// ParseVectorFloat32 parses a string into a vector.
func ParseVectorFloat32(s string) (VectorFloat32, error) {
	var values []float32
	var valueError error
	// We explicitly use a JSON float parser to reject other JSON types.
	parser := jsoniter.ParseString(jsoniter.ConfigDefault, s)
	parser.ReadArrayCB(func(parser *jsoniter.Iterator) bool {
		v := parser.ReadFloat64()
		if math.IsNaN(v) {
			valueError = errors.Errorf("NaN not allowed in vector")
			return false
		}
		if math.IsInf(v, 0) {
			valueError = errors.Errorf("infinite value not allowed in vector")
			return false
		}
		values = append(values, float32(v))
		return true
	})
	if parser.Error != nil {
		return ZeroVectorFloat32, errors.Errorf("Invalid vector text: %s", s)
	}
	if valueError != nil {
		return ZeroVectorFloat32, valueError
	}

	dim := len(values)
	if err := CheckVectorDimValid(dim); err != nil {
		return ZeroVectorFloat32, err
	}

	vec := InitVectorFloat32(dim)
	copy(vec.Elements(), values)
	return vec, nil
}

// Clone returns a deep copy of the vector.
func (v VectorFloat32) Clone() VectorFloat32 {
	data := make([]byte, len(v.data))
	copy(data, v.data)
	return VectorFloat32{data: data}
}

// IsZeroValue returns true if the vector is a zero value (which length is zero).
func (v VectorFloat32) IsZeroValue() bool {
	return v.Len() == 0
}
