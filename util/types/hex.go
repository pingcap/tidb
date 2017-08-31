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
	"encoding/hex"
	"math"
	"strings"

	"github.com/juju/errors"
)

// Hex is for mysql hexadecimal literal type.
type Hex struct {
	// Value holds the raw value for hexadecimal literal.
	Value []byte
}

// ZeroHex is a Hex literal with zero value.
var ZeroHex = Hex{[]byte{}}

// String implements fmt.Stringer interface.
func (h Hex) String() string {
	if len(h.Value) == 0 {
		return ""
	}
	return "0x" + hex.EncodeToString(h.Value)
}

// ToString returns the string representation for hexadecimal literal.
func (h Hex) ToString() string {
	return string(h.Value)
}

// ToInt returns the int value for hexadecimal literal.
func (h Hex) ToInt() (uint64, error) {
	length := len(h.Value)
	if length == 0 {
		return 0, nil
	}
	if length > 8 {
		return math.MaxUint64, ErrTruncated
	}
	// Note: the byte-order is BigEndian.
	val := uint64(h.Value[0])
	for i := 1; i < length; i++ {
		val = (val << 8) | uint64(h.Value[i])
	}
	return val, nil
}

// ParseHexStr parses hexadecimal literal as string.
// See https://dev.mysql.com/doc/refman/5.7/en/hexadecimal-literals.html
func ParseHexStr(s string) (Hex, error) {
	if len(s) == 0 {
		return Hex{}, errors.Errorf("invalid empty string for parsing hexadecimal literal")
	}

	if s[0] == 'x' || s[0] == 'X' {
		// format is x'val' or X'val'
		s = strings.Trim(s[1:], "'")
		if len(s)%2 != 0 {
			return Hex{}, errors.Errorf("invalid hexadecimal format, must even numbers, but %d", len(s))
		}
	} else if strings.HasPrefix(s, "0x") {
		s = s[2:]
	} else {
		// here means format is not x'val', X'val' or 0xval.
		return Hex{}, errors.Errorf("invalid hexadecimal format %s", s)
	}

	if len(s) == 0 {
		return ZeroHex, nil
	}

	if len(s)%2 != 0 {
		s = "0" + s
	}
	bytes, err := hex.DecodeString(s)
	if err != nil {
		return Hex{}, errors.Trace(err)
	}
	return Hex{bytes}, nil
}
