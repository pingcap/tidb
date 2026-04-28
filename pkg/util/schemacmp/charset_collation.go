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

package schemacmp

import (
	"strings"

	tidbcharset "github.com/pingcap/tidb/pkg/parser/charset"
)

type charsetLattice struct {
	value string
}

// Charset is a lattice for comparing/joining character sets. Currently it
// supports the ordering: latin1 < utf8mb4 and utf8(utf8mb3) < utf8mb4. Other
// charsets are only comparable when identical.
func Charset(cs string) charsetLattice {
	normalized := strings.ToLower(cs)
	if normalized == tidbcharset.CharsetUTF8MB3 {
		normalized = tidbcharset.CharsetUTF8
	}
	return charsetLattice{value: normalized}
}

func (a charsetLattice) Unwrap() any {
	return a.value
}

func (a charsetLattice) Compare(other Lattice) (int, error) {
	b, ok := other.(charsetLattice)
	if !ok {
		return 0, typeMismatchError(a, other)
	}

	switch {
	case a.value == b.value:
		return 0, nil
	// This must be compatible with ddl.checkModifyCharsetAndCollation().
	case a.value == tidbcharset.CharsetUTF8MB4 &&
		(b.value == tidbcharset.CharsetUTF8 || b.value == tidbcharset.CharsetLatin1):
		return 1, nil
	case b.value == tidbcharset.CharsetUTF8MB4 &&
		(a.value == tidbcharset.CharsetUTF8 || a.value == tidbcharset.CharsetLatin1):
		return -1, nil
	default:
		return 0, incompatibleCharsetError(a.value, b.value)
	}
}

func (a charsetLattice) Join(other Lattice) (Lattice, error) {
	b, ok := other.(charsetLattice)
	if !ok {
		return nil, typeMismatchError(a, other)
	}

	cmp, err := a.Compare(b)
	if err == nil {
		if cmp >= 0 {
			return a, nil
		}
		return b, nil
	}

	// Currently the only special case is Join(latin1, utf8) = utf8mb4
	if a.value == tidbcharset.CharsetUTF8 && b.value == tidbcharset.CharsetLatin1 ||
		a.value == tidbcharset.CharsetLatin1 && b.value == tidbcharset.CharsetUTF8 {
		return Charset(tidbcharset.CharsetUTF8MB4), nil
	}

	return nil, err
}

type collationLattice struct {
	charset charsetLattice
	// suffix is the part after "<charset>_" in the normalized collation name.
	// For collations without an underscore, suffix is empty and charset==collation.
	suffix string
}

// Collation is a lattice for comparing/joining collations.
//
// It supports the ordering:
//   - latin1_<suffix> < utf8mb4_<suffix>
//   - utf8_<suffix> < utf8mb4_<suffix>
//
// (same suffix only).
//
// Other collations are only comparable when identical.
func Collation(co string) collationLattice {
	charsetName, suffix, _ := strings.Cut(co, "_")
	return collationLattice{charset: Charset(charsetName), suffix: strings.ToLower(suffix)}
}

func (a collationLattice) Unwrap() any {
	return a.unwrapString()
}

func (a collationLattice) unwrapString() string {
	if a.suffix == "" {
		return a.charset.value
	}
	return a.charset.value + "_" + a.suffix
}

func (a collationLattice) Compare(other Lattice) (int, error) {
	b, ok := other.(collationLattice)
	if !ok {
		return 0, typeMismatchError(a, other)
	}

	if a.suffix != b.suffix {
		return 0, incompatibleCollationError(a.unwrapString(), b.unwrapString())
	}

	return a.charset.Compare(b.charset)
}

func (a collationLattice) Join(other Lattice) (Lattice, error) {
	b, ok := other.(collationLattice)
	if !ok {
		return nil, typeMismatchError(a, other)
	}

	cmp, err := a.Compare(b)
	if err == nil {
		if cmp >= 0 {
			return a, nil
		}
		return b, nil
	}

	// If suffix differs, the join doesn't exist (keep the original error).
	if a.suffix != b.suffix {
		return nil, err
	}

	// When suffix matches, delegate to charset join to handle incomparable-but-joinable pairs.
	joinCharset, joinErr := a.charset.Join(b.charset)
	if joinErr != nil {
		return nil, err
	}
	return collationLattice{charset: joinCharset.(charsetLattice), suffix: a.suffix}, nil
}
