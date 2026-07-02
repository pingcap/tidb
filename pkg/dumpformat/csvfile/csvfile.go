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

// Package csvfile holds the CSV writer shared by Dumpling and the exporter,
// the sibling of parquetfile and sqlfile.
package csvfile

// FieldKind classifies a column for CSV framing, mirroring dumpling's
// Number/String/Bytes RowReceiver split.
type FieldKind uint8

const (
	// KindNumber is written unquoted.
	KindNumber FieldKind = iota
	// KindString is delimiter-quoted and escaped.
	KindString
	// KindBytes is a binary value, rendered per Config.BinaryFormat.
	KindBytes
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

// Config holds the CSV framing knobs, mirroring dumpling's csvOption.
type Config struct {
	// Separator is written between fields.
	Separator []byte
	// Delimiter quotes string/bytes fields (default `"`); empty means unquoted.
	Delimiter []byte
	// NullValue is written for NULL fields (default `\N`).
	NullValue []byte
	// LineTerminator is written after each row (default "\n").
	LineTerminator []byte
	// BinaryFormat selects how KindBytes is rendered.
	BinaryFormat BinaryFormat
	// EscapeBackslash selects backslash escaping instead of delimiter doubling.
	EscapeBackslash bool
}

// DefaultConfig returns the CSV defaults matching Dumpling.
func DefaultConfig() *Config {
	return &Config{
		Separator:      []byte(","),
		Delimiter:      []byte(`"`),
		NullValue:      []byte(`\N`),
		LineTerminator: []byte("\n"),
	}
}
