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

package types

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/juju/errors"
)

// BinString is the internal type for storing bit / hex literal type.
type BinString struct {
	// Value holds the raw value for literal in BigEndian.
	Value []byte
}

// BitLiteral is the bit literal type.
type BitLiteral struct {
	BinString
}

// HexLiteral is the hex literal type.
type HexLiteral struct {
	BinString
}

// ZeroBinString is a BinString literal with zero value.
var ZeroBinString = BinString{[]byte{}}

func trimLeadingZeroBytes(bytes []byte) []byte {
	if len(bytes) == 0 {
		return bytes
	}
	var pos int
	posMax := len(bytes) - 1
	for pos = 0; pos < posMax; pos++ {
		if bytes[pos] != 0 {
			break
		}
	}
	return bytes[pos:]
}

// NewBinStringFromBytes creates a new BinString instance by the given bytes.
// The leading zero in bytes are required to be trimmed before feeding to this function.
func NewBinStringFromBytes(bytes []byte) BinString {
	return BinString{bytes}
}

// NewBinStringFromUint creates a new BinString instance by the given uint value in BitEndian.
// byteSize will be used as the length of the new BinString, with leading bytes filled to zero.
// If byteSize is -1, the leading zeros in new BinString will be trimmed.
func NewBinStringFromUint(value uint64, byteSize int) BinString {
	if byteSize != -1 && (byteSize < 1 || byteSize > 8) {
		panic("Invalid byteSize")
	}
	bytes := make([]byte, 8)
	binary.BigEndian.PutUint64(bytes, value)
	if byteSize == -1 {
		bytes = trimLeadingZeroBytes(bytes)
	} else {
		bytes = bytes[8-byteSize:]
	}
	return NewBinStringFromBytes(bytes)
}

// String implements fmt.Stringer interface.
func (b BinString) String() string {
	if len(b.Value) == 0 {
		return ""
	}
	return "0x" + hex.EncodeToString(b.Value)
}

// ToString returns the string representation for the literal.
func (b BinString) ToString() string {
	return string(b.Value)
}

// ToBitLiteralString returns the bit literal representation for the literal.
func (b BinString) ToBitLiteralString(trimLeadingZero bool) string {
	if len(b.Value) == 0 {
		return "b''"
	}
	var buf bytes.Buffer
	for _, data := range b.Value {
		fmt.Fprintf(&buf, "%08b", data)
	}
	ret := buf.Bytes()
	if trimLeadingZero {
		ret = bytes.TrimLeft(ret, "0")
		if len(ret) == 0 {
			ret = []byte{'0'}
		}
	}
	return fmt.Sprintf("b'%s'", string(ret))
}

// ToInt returns the int value for the literal.
func (b BinString) ToInt() (uint64, error) {
	bytes := trimLeadingZeroBytes(b.Value)
	length := len(bytes)
	if length == 0 {
		return 0, nil
	}
	if length > 8 {
		return math.MaxUint64, ErrTruncated
	}
	// Note: the byte-order is BigEndian.
	val := uint64(bytes[0])
	for i := 1; i < length; i++ {
		val = (val << 8) | uint64(bytes[i])
	}
	return val, nil
}

// ParseBitStr parses bit string.
// The string format can be b'val', B'val' or 0bval, val must be 0 or 1.
// See https://dev.mysql.com/doc/refman/5.7/en/bit-value-literals.html
func ParseBitStr(s string) (BinString, error) {
	if len(s) == 0 {
		return NewBinStringFromBytes(nil), errors.Errorf("invalid empty string for parsing bit type")
	}

	if s[0] == 'b' || s[0] == 'B' {
		// format is b'val' or B'val'
		s = strings.Trim(s[1:], "'")
	} else if strings.HasPrefix(s, "0b") {
		s = s[2:]
	} else {
		// here means format is not b'val', B'val' or 0bval.
		return NewBinStringFromBytes(nil), errors.Errorf("invalid bit type format %s", s)
	}

	if len(s) == 0 {
		return ZeroBinString, nil
	}

	alignedLength := (len(s) + 7) &^ 7
	s = ("00000000" + s)[len(s)+8-alignedLength:] // Pad with zero (slice from `-alignedLength`)
	byteLength := len(s) >> 3
	bytes := make([]byte, byteLength)

	for i := 0; i < byteLength; i++ {
		strPosition := i << 3
		val, err := strconv.ParseUint(s[strPosition:strPosition+8], 2, 8)
		if err != nil {
			return NewBinStringFromBytes(nil), errors.Trace(err)
		}
		bytes[i] = byte(val)
	}

	return NewBinStringFromBytes(bytes), nil
}

// NewBitLiteral parses bit string as BitLiteral type.
func NewBitLiteral(s string) (BitLiteral, error) {
	b, err := ParseBitStr(s)
	if err != nil {
		return BitLiteral{}, err
	}
	return BitLiteral{b}, nil
}

// ParseHexStr parses hexadecimal string literal.
// See https://dev.mysql.com/doc/refman/5.7/en/hexadecimal-literals.html
func ParseHexStr(s string) (BinString, error) {
	if len(s) == 0 {
		return NewBinStringFromBytes(nil), errors.Errorf("invalid empty string for parsing hexadecimal literal")
	}

	if s[0] == 'x' || s[0] == 'X' {
		// format is x'val' or X'val'
		s = strings.Trim(s[1:], "'")
		if len(s)%2 != 0 {
			return NewBinStringFromBytes(nil), errors.Errorf("invalid hexadecimal format, must even numbers, but %d", len(s))
		}
	} else if strings.HasPrefix(s, "0x") {
		s = s[2:]
	} else {
		// here means format is not x'val', X'val' or 0xval.
		return NewBinStringFromBytes(nil), errors.Errorf("invalid hexadecimal format %s", s)
	}

	if len(s) == 0 {
		return ZeroBinString, nil
	}

	if len(s)%2 != 0 {
		s = "0" + s
	}
	bytes, err := hex.DecodeString(s)
	if err != nil {
		return NewBinStringFromBytes(nil), errors.Trace(err)
	}
	return NewBinStringFromBytes(bytes), nil
}

// NewHexLiteral parses hexadecimal string as HexLiteral type.
func NewHexLiteral(s string) (HexLiteral, error) {
	h, err := ParseHexStr(s)
	if err != nil {
		return HexLiteral{}, err
	}
	return HexLiteral{h}, nil
}
