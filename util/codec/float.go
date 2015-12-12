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
	"bytes"
	"encoding/binary"
	"math"

	"github.com/juju/errors"
)

func encodeFloatToCmpUint64(f float64) uint64 {
	u := math.Float64bits(f)
	if f >= 0 {
		u |= signMask
	} else {
		u = ^u
	}
	return u
}

func decodeCmpUintToFloat(u uint64) float64 {
	if u&signMask > 0 {
		u &= ^signMask
	} else {
		u = ^u
	}
	return math.Float64frombits(u)
}

func (e ascEncoder) WriteFloat(b *bytes.Buffer, v float64) {
	e.WriteUint(b, encodeFloatToCmpUint64(v))
}

func (e ascEncoder) ReadFloat(b *bytes.Buffer) (float64, error) {
	u, err := e.ReadUint(b)
	return decodeCmpUintToFloat(u), errors.Trace(err)
}

func (e descEncoder) WriteFloat(b *bytes.Buffer, v float64) {
	e.WriteUint(b, encodeFloatToCmpUint64(v))
}

func (e descEncoder) ReadFloat(b *bytes.Buffer) (float64, error) {
	u, err := e.ReadUint(b)
	return decodeCmpUintToFloat(u), errors.Trace(err)
}

func (compactEncoder) WriteFloat(b *bytes.Buffer, v float64) {
	binary.Write(b, binary.BigEndian, v)
}

func (compactEncoder) ReadFloat(b *bytes.Buffer) (float64, error) {
	var v float64
	err := binary.Read(b, binary.BigEndian, &v)
	return v, errors.Trace(err)
}
