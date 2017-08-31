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
	"fmt"
	"strconv"
	"strings"

	"github.com/juju/errors"
)

// Bit is for mysql bit literal type, inherited from Hex.
type Bit struct {
	Hex
}

// ZeroBit is a Bit literal with zero value.
var ZeroBit = Bit{Hex{[]byte{}}}

// String implements fmt.Stringer interface.
func (b Bit) String() string {
	if len(b.Value) == 0 {
		return ""
	}
	var outputBuffer bytes.Buffer
	outputBuffer.WriteString("0b")
	for _, b := range b.Value {
		fmt.Fprintf(&outputBuffer, "%08b", b)
	}
	return outputBuffer.String()
}

// ParseBitStr parses bit string.
// The string format can be b'val', B'val' or 0bval, val must be 0 or 1.
// See https://dev.mysql.com/doc/refman/5.7/en/bit-value-literals.html
func ParseBitStr(s string) (Bit, error) {
	if len(s) == 0 {
		return Bit{}, errors.Errorf("invalid empty string for parsing bit type")
	}

	if s[0] == 'b' || s[0] == 'B' {
		// format is b'val' or B'val'
		s = strings.Trim(s[1:], "'")
	} else if strings.HasPrefix(s, "0b") {
		s = s[2:]
	} else {
		// here means format is not b'val', B'val' or 0bval.
		return Bit{}, errors.Errorf("invalid bit type format %s", s)
	}

	if len(s) == 0 {
		return ZeroBit, nil
	}

	alignedLength := (len(s) + 7) &^ 7
	s = ("00000000" + s)[len(s)+8-alignedLength:] // Pad with zero (slice from `-alignedLength`)
	byteLength := len(s) >> 3
	bytes := make([]byte, byteLength)

	for i := 0; i < byteLength; i++ {
		strPosition := i << 3
		val, err := strconv.ParseUint(s[strPosition:strPosition+8], 2, 8)
		if err != nil {
			return Bit{}, errors.Trace(err)
		}
		bytes[i] = byte(val)
	}

	return Bit{Hex{bytes}}, nil
}
