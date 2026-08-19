// Copyright 2026 PingCAP, Inc.
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

package csvfile

import "github.com/pingcap/tidb/pkg/dumpformat"

// FieldKind re-exports dumpformat.FieldKind so CSV callers keep a local name.
type FieldKind = dumpformat.FieldKind

// Column kinds, re-exported from dumpformat.
const (
	KindNumber = dumpformat.KindNumber
	KindString = dumpformat.KindString
	KindBytes  = dumpformat.KindBytes
)

// BinaryFormat controls how KindBytes values are rendered.
type BinaryFormat uint8

const (
	// BinaryFormatUTF8 writes the raw bytes (escaped like a string).
	BinaryFormatUTF8 BinaryFormat = iota
	// BinaryFormatHEX writes lowercase hex.
	BinaryFormatHEX
	// BinaryFormatBase64 writes standard base64.
	BinaryFormatBase64
)

// Config holds the CSV framing knobs. NewWriter applies no defaults, so the
// caller must set every field.
type Config struct {
	// FieldsTerminatedBy separates fields.
	FieldsTerminatedBy string
	// FieldsEnclosedBy quotes string/bytes fields; empty means unquoted.
	FieldsEnclosedBy string
	// FieldsEscapedBy is the escape character (length <= 1); empty selects
	// enclosure-doubling instead.
	FieldsEscapedBy string
	// LinesTerminatedBy is written after each row.
	LinesTerminatedBy string
	// NullValue is written for NULL fields.
	NullValue []byte
	// BinaryFormat selects how KindBytes is rendered.
	BinaryFormat BinaryFormat
}
