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

// Package sqlfile holds the SQL (INSERT statement) writer.
package sqlfile

import "github.com/pingcap/tidb/pkg/dumpformat"

// FieldKind re-exports dumpformat.FieldKind so SQL callers keep a local name.
type FieldKind = dumpformat.FieldKind

// Column kinds, re-exported from dumpformat.
const (
	KindNumber = dumpformat.KindNumber
	KindString = dumpformat.KindString
	KindBytes  = dumpformat.KindBytes
)

// Config holds the SQL framing knobs.
type Config struct {
	// StatementSize splits the INSERT statement once the bytes written for the
	// current statement reach it; 0 means a single statement per file.
	StatementSize int64
	// EscapeBackslash selects backslash escaping instead of single-quote doubling
	// for string values.
	EscapeBackslash bool
}
