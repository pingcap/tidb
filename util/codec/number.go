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

	"bytes"

	"github.com/juju/errors"
)

const signMask uint64 = 0x8000000000000000

func encodeIntToCmpUint(v int64) uint64 {
	u := uint64(v)
	if u&signMask > 0 {
		u &= ^signMask
	} else {
		u |= signMask
	}

	return u
}

func decodeCmpUintToInt(u uint64) int64 {
	if u&signMask > 0 {
		u &= ^signMask
	} else {
		u |= signMask
	}

	return int64(u)
}

func (ascEncoder) WriteInt(b *bytes.Buffer, v int64) {
	u := encodeIntToCmpUint(v)
	binary.Write(b, binary.BigEndian, u)
}

func (ascEncoder) ReadInt(b *bytes.Buffer) (int64, error) {
	var u uint64
	err := binary.Read(b, binary.BigEndian, &u)
	return decodeCmpUintToInt(u), errors.Trace(err)
}

func (descEncoder) WriteInt(b *bytes.Buffer, v int64) {
	u := encodeIntToCmpUint(v)
	binary.Write(b, binary.BigEndian, ^u)
}

func (descEncoder) ReadInt(b *bytes.Buffer) (int64, error) {
	var u uint64
	err := binary.Read(b, binary.BigEndian, &u)
	return decodeCmpUintToInt(^u), errors.Trace(err)
}

func (compactEncoder) WriteInt(b *bytes.Buffer, v int64) {
	var data [binary.MaxVarintLen64]byte
	n := binary.PutVarint(data[:], v)
	b.Write(data[:n])
}

func (compactEncoder) ReadInt(b *bytes.Buffer) (int64, error) {
	n, err := binary.ReadVarint(b)
	return n, errors.Trace(err)
}

func (ascEncoder) WriteUint(b *bytes.Buffer, v uint64) {
	binary.Write(b, binary.BigEndian, v)
}

func (ascEncoder) ReadUint(b *bytes.Buffer) (uint64, error) {
	var u uint64
	err := binary.Read(b, binary.BigEndian, &u)
	return u, errors.Trace(err)
}

func (descEncoder) WriteUint(b *bytes.Buffer, v uint64) {
	binary.Write(b, binary.BigEndian, ^v)
}

func (descEncoder) ReadUint(b *bytes.Buffer) (uint64, error) {
	var u uint64
	err := binary.Read(b, binary.BigEndian, &u)
	return ^u, errors.Trace(err)
}

func (compactEncoder) WriteUint(b *bytes.Buffer, v uint64) {
	var data [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(data[:], v)
	b.Write(data[:n])
}

func (compactEncoder) ReadUint(b *bytes.Buffer) (uint64, error) {
	n, err := binary.ReadUvarint(b)
	return n, errors.Trace(err)
}
