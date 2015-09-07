// Copyright 2014 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Tobias Schottdorf (tobias.schottdorf@gmail.com)

// Copyright 2015 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package codec

import (
	"encoding/binary"
	"fmt"
	"math"

	"github.com/juju/errors"
)

// EncodeInt encodes the int64 value with variable length format.
// The encoded bytes format is: length flag(1 byte) + encoded data.
// The length flag is calculated with following way
//  flag  Value Range
//  8  -> [MinInt64, MinInt32)
//  12 -> [MinInt32, MinInt16)
//  14 -> [MinInt16, MinInt8)
//  15 -> [MinInt8, 0)
//  16 -> 0
//  17 -> (0, MaxInt8]
//  18 -> (MaxInt8, MaxInt16]
//  20 -> (MaxInt16, MaxInt32]
//  24 -> (MaxInt32, MaxInt64]
//
// EncodeInt appends the encoded value to slice b and returns the appended slice.
// EncodeInt guarantees that the encoded value is in ascending order for comparison.
func EncodeInt(b []byte, v int64) []byte {
	if v < 0 {
		switch {
		case v >= math.MinInt8:
			return append(b, 15, byte(v))
		case v >= math.MinInt16:
			return append(b, 14, byte(v>>8), byte(v))
		case v >= math.MinInt32:
			return append(b, 12, byte(v>>24), byte(v>>16), byte(v>>8), byte(v))
		default:
			return append(b, 8, byte(v>>56), byte(v>>48), byte(v>>40), byte(v>>32),
				byte(v>>24), byte(v>>16), byte(v>>8), byte(v))
		}
	}

	return EncodeUint(b, uint64(v))
}

// EncodeIntDesc encodes the int64 value with variable length format.
// The encoded bytes format is: length flag(1 byte) + encoded data.
// The length flag is calculated with following way
//  flag  Value Range
//  24 -> [MinInt64, MinInt32)
//  20 -> [MinInt32, MinInt16)
//  18 -> [MinInt16, MinInt8)
//  17 -> [MinInt8, 0)
//  16 -> 0
//  15 -> (0, MaxInt8]
//  14 -> (MaxInt8, MaxInt16]
//  12 -> (MaxInt16, MaxInt32]
//  8  -> (MaxInt32, MaxInt64]
//
// EncodeIntDesc appends the encoded value to slice b and returns the appended slice.
// EncodeIntDesc guarantees that the encoded value is in descending order for comparison.
func EncodeIntDesc(b []byte, v int64) []byte {
	if v < 0 {
		switch {
		case v >= math.MinInt8:
			v = ^v
			return append(b, 17, byte(v))
		case v >= math.MinInt16:
			v = ^v
			return append(b, 18, byte(v>>8), byte(v))
		case v >= math.MinInt32:
			v = ^v
			return append(b, 20, byte(v>>24), byte(v>>16), byte(v>>8), byte(v))
		default:
			v = ^v
			return append(b, 24, byte(v>>56), byte(v>>48), byte(v>>40), byte(v>>32),
				byte(v>>24), byte(v>>16), byte(v>>8), byte(v))
		}
	}

	return EncodeUintDesc(b, uint64(v))
}

// DecodeInt decodes value encoded by EncodeInt before.
// It returns the leftover un-decoded slice, decoded value if no error.
func DecodeInt(b []byte) ([]byte, int64, error) {
	if len(b) == 0 {
		return nil, 0, errors.Errorf("empty bytes to decode value")
	}

	flag := b[0]
	length := int(flag) - 16
	if length < 0 {
		length = -length
		leftover := b[1:]
		if len(leftover) < length {
			return nil, 0, errors.Errorf("insufficient bytes to decode value, need %d, but only %d", length, len(leftover))
		}
		var v int64
		switch length {
		case 1:
			v = int64(int8(leftover[0]))
		case 2:
			v = int64(int16(binary.BigEndian.Uint16(leftover[:length])))
		case 4:
			v = int64(int32(binary.BigEndian.Uint32(leftover[:length])))
		case 8:
			v = int64(binary.BigEndian.Uint64(leftover[:length]))
		default:
			return nil, 0, errors.Errorf("invalid encoded length flag %d", flag)
		}

		return leftover[length:], v, nil
	}

	leftover, v, err := DecodeUint(b)
	if v > math.MaxInt64 {
		return nil, 0, fmt.Errorf("decoded value %d overflow int64", v)
	}
	return leftover, int64(v), err
}

// DecodeIntDesc decodes value encoded by EncodeInt before.
// It returns the leftover un-decoded slice, decoded value if no error.
func DecodeIntDesc(b []byte) ([]byte, int64, error) {
	if len(b) == 0 {
		return nil, 0, errors.Errorf("empty bytes to decode value")
	}

	flag := b[0]
	length := int(flag) - 16
	if length > 0 {
		leftover := b[1:]
		if len(leftover) < length {
			return nil, 0, errors.Errorf("insufficient bytes to decode value, need %d, but only %d", length, len(leftover))
		}
		var v int64
		switch length {
		case 1:
			v = int64(int8(leftover[0]))
		case 2:
			v = int64(int16(binary.BigEndian.Uint16(leftover[:length])))
		case 4:
			v = int64(int32(binary.BigEndian.Uint32(leftover[:length])))
		case 8:
			v = int64(binary.BigEndian.Uint64(leftover[:length]))
		default:
			return nil, 0, errors.Errorf("invalid encoded length flag %d", flag)
		}

		return leftover[length:], ^v, nil
	}

	leftover, v, err := DecodeUintDesc(b)
	if v > math.MaxInt64 {
		return nil, 0, fmt.Errorf("decoded value %d overflow int64", v)
	}

	return leftover, int64(v), err
}

// EncodeUint encodes the uint64 value with variable length format.
// The encoded bytes format is: length flag(1 byte) + encoded data.
// The length flag is calculated with following way:
//  flag  Value Range
//  16 -> 0
//  17 -> (0, MaxUint8]
//  18 -> (MaxUint8, MaxUint16]
//  20 -> (MaxUint16, MaxUint32]
//  24 -> (MaxUint32, MaxUint64]
//
// EncodeUint appends the encoded value to slice b and returns the appended slice.
// EncodeUint guarantees that the encoded value is in ascending order for comparison.
func EncodeUint(b []byte, v uint64) []byte {
	switch {
	case v == 0:
		return append(b, 16)
	case v <= math.MaxUint8:
		return append(b, 17, byte(v))
	case v <= math.MaxUint16:
		return append(b, 18, byte(v>>8), byte(v))
	case v <= math.MaxUint32:
		return append(b, 20, byte(v>>24), byte(v>>16), byte(v>>8), byte(v))
	default:
		return append(b, 24, byte(v>>56), byte(v>>48), byte(v>>40), byte(v>>32),
			byte(v>>24), byte(v>>16), byte(v>>8), byte(v))
	}
}

// EncodeUintDesc encodes the int64 value with variable length format.
// The encoded bytes format is: length flag(1 byte) + encoded data.
// The length flag is calculated with following way
//  flag  Value Range
//  16 -> 0
//  15 -> (0, MaxUint8]
//  14 -> (MaxUint8, MaxUint16]
//  12 -> (MaxUint16, MaxUint32]
//  8  -> (MaxUint32, MaxUint64]
//
// EncodeUintDesc appends the encoded value to slice b and returns the appended slice.
// EncodeUintDesc guarantees that the encoded value is in descending order for comparison.
func EncodeUintDesc(b []byte, v uint64) []byte {
	switch {
	case v == 0:
		return append(b, 16)
	case v <= math.MaxInt8:
		v = ^v
		return append(b, 15, byte(v))
	case v <= math.MaxUint16:
		v = ^v
		return append(b, 14, byte(v>>8), byte(v))
	case v <= math.MaxUint32:
		v = ^v
		return append(b, 12, byte(v>>24), byte(v>>16), byte(v>>8), byte(v))
	default:
		v = ^v
		return append(b, 8, byte(v>>56), byte(v>>48), byte(v>>40), byte(v>>32),
			byte(v>>24), byte(v>>16), byte(v>>8), byte(v))
	}
}

// DecodeUint decodes value encoded by EncodeUint before.
// It returns the leftover un-decoded slice, decoded value if no error.
func DecodeUint(b []byte) ([]byte, uint64, error) {
	if len(b) == 0 {
		return nil, 0, errors.Errorf("empty bytes to decode value")
	}

	flag := b[0]
	length := int(flag) - 16

	leftover := b[1:]
	if len(leftover) < length {
		return nil, 0, errors.Errorf("insufficient bytes to decode value, need %d, but only %d", length, len(leftover))
	}

	var v uint64
	switch length {
	case 0:
		v = 0
	case 1:
		v = uint64(leftover[0])
	case 2:
		v = uint64(binary.BigEndian.Uint16(leftover[:length]))
	case 4:
		v = uint64(binary.BigEndian.Uint32(leftover[:length]))
	case 8:
		v = uint64(binary.BigEndian.Uint64(leftover[:length]))
	default:
		return nil, 0, errors.Errorf("invalid encoded length flag %d", flag)
	}

	return leftover[length:], v, nil
}

// DecodeUintDesc decodes value encoded by EncodeInt before.
// It returns the leftover un-decoded slice, decoded value if no error.
func DecodeUintDesc(b []byte) ([]byte, uint64, error) {
	if len(b) == 0 {
		return nil, 0, errors.Errorf("empty bytes to decode value")
	}

	flag := b[0]
	length := 16 - int(flag)

	leftover := b[1:]
	if len(leftover) < length {
		return nil, 0, errors.Errorf("insufficient bytes to decode value, need %d, but only %d", length, len(leftover))
	}

	var v uint64
	switch length {
	case 0:
		v = 0
	case 1:
		v = uint64(^leftover[0])
	case 2:
		v = uint64(^binary.BigEndian.Uint16(leftover[:length]))
	case 4:
		v = uint64(^binary.BigEndian.Uint32(leftover[:length]))
	case 8:
		v = uint64(^binary.BigEndian.Uint64(leftover[:length]))
	default:
		return nil, 0, errors.Errorf("invalid encoded length flag %d", flag)
	}

	return leftover[length:], v, nil
}
