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

	"github.com/pingcap/tidb/mysql"
)

// An Encoder specifies how to write values into byte buffer, and how to read
// values back.
type Encoder interface {
	WriteSingleByte(b *bytes.Buffer, v byte)
	ReadSingleByte(b *bytes.Buffer) (byte, error)
	WriteBytes(b *bytes.Buffer, v []byte)
	ReadBytes(b *bytes.Buffer) ([]byte, error)
	WriteInt(b *bytes.Buffer, v int64)
	ReadInt(b *bytes.Buffer) (int64, error)
	WriteUint(b *bytes.Buffer, v uint64)
	ReadUint(b *bytes.Buffer) (uint64, error)
	WriteFloat(b *bytes.Buffer, v float64)
	ReadFloat(b *bytes.Buffer) (float64, error)
	WriteDecimal(b *bytes.Buffer, v mysql.Decimal)
	ReadDecimal(b *bytes.Buffer) (mysql.Decimal, error)
	Write(b *bytes.Buffer, v ...interface{}) error
	Read(b *bytes.Buffer) ([]interface{}, error)
}

type ascEncoder struct{}
type descEncoder struct{}
type compactEncoder struct{}

// AscEncoder is an encoder that guarantees the encoded value is in ascending
// order for comparison.
var AscEncoder ascEncoder

// DescEncoder is an encoder that guarantees the encoded value is in descending
// order for comparison.
var DescEncoder descEncoder

// CompactEncoder is an encoder that encode values in a more compact way. It does
// NOT guarantee the order for comparison.
var CompactEncoder compactEncoder
