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

package dumpformat

// FieldKind classifies a column so a format writer can render its value. The
// classification is format-agnostic; each format renders a kind its own way.
type FieldKind uint8

const (
	// KindNumber is a numeric column.
	KindNumber FieldKind = iota
	// KindString is a string column, or any type that needs quoting/escaping.
	KindString
	// KindBytes is a binary column.
	KindBytes
)

// MaxBufferedValueSize bounds how much of a row the SQL and CSV writers hold in
// their buffers. They encode each quoted value in pieces of about this many
// input bytes and write the buffer out whenever it reaches this size, so the
// buffer stays at a few MiB however wide the row is. Without it the buffer
// grows to the encoded size of the widest row, which is several times the raw
// size for values of hundreds of MiB.
const MaxBufferedValueSize = 1 << 20
