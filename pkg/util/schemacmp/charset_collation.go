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
	value  string
	family charsetFamily
}

type charsetFamily int

const (
	charsetFamilyOther charsetFamily = iota
	charsetFamilyLatin1
	charsetFamilyUTF8
	charsetFamilyUTF8MB4
)

// Charset is a lattice for comparing/joining character sets. Currently it
// supports the ordering: latin1 < utf8mb4 and utf8(utf8mb3) < utf8mb4. Other
// charsets are only comparable when identical.
func Charset(cs string) Lattice {
	ret := charsetLattice{value: cs}

	normalized := strings.ToLower(cs)
	if normalized == tidbcharset.CharsetUTF8MB3 {
		normalized = tidbcharset.CharsetUTF8
	}

	switch normalized {
	case tidbcharset.CharsetLatin1:
		ret.family = charsetFamilyLatin1
	case tidbcharset.CharsetUTF8:
		ret.family = charsetFamilyUTF8
	case tidbcharset.CharsetUTF8MB4:
		ret.family = charsetFamilyUTF8MB4
	default:
		// Caller should always pass an explicit charset. Unrecognized values are treated as "other".
		ret.family = charsetFamilyOther
	}
	return ret
}

func (a charsetLattice) Unwrap() any {
	return a.value
}

func (a charsetLattice) Compare(other Lattice) (int, error) {
	b, ok := other.(charsetLattice)
	if !ok {
		return 0, typeMismatchError(a, other)
	}

	if a.family == b.family {
		if a.family == charsetFamilyOther && a.value != b.value {
			return 0, incompatibleCharsetError(a.value, b.value)
		}
		return 0, nil
	}

	switch {
	case a.family == charsetFamilyUTF8 && b.family == charsetFamilyUTF8MB4:
		return -1, nil
	case a.family == charsetFamilyUTF8MB4 && b.family == charsetFamilyUTF8:
		return 1, nil
	case a.family == charsetFamilyLatin1 && b.family == charsetFamilyUTF8MB4:
		return -1, nil
	case a.family == charsetFamilyUTF8MB4 && b.family == charsetFamilyLatin1:
		return 1, nil
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
	if err != nil {
		return nil, err
	}
	if cmp >= 0 {
		return a, nil
	}
	return b, nil
}

type collationLattice struct {
	value string
	// suffix will be set to normalized value when family is collationFamilyOther for
	// implementation simplicity.
	suffix string
	family collationFamily
}

type collationFamily int

const (
	collationFamilyOther collationFamily = iota
	collationFamilyLatin1
	collationFamilyUTF8
	collationFamilyUTF8MB4
)

// Collation is a lattice for comparing/joining collations.
// It supports the ordering:
//   - latin1_<suffix> < utf8mb4_<suffix>
//   - utf8_<suffix> < utf8mb4_<suffix>
//
// (same suffix only).
// Other collations are only comparable when identical.
func Collation(co string) Lattice {
	ret := collationLattice{value: co}

	const (
		utf8mb3Prefix = tidbcharset.CharsetUTF8MB3 + "_"
		utf8mb4Prefix = tidbcharset.CharsetUTF8MB4 + "_"
		utf8Prefix    = tidbcharset.CharsetUTF8 + "_"
		latin1Prefix  = tidbcharset.CharsetLatin1 + "_"
	)

	normalized := strings.ToLower(co)
	if after, ok := strings.CutPrefix(normalized, utf8mb3Prefix); ok {
		normalized = utf8Prefix + after
	}

	if after, ok := strings.CutPrefix(normalized, utf8mb4Prefix); ok {
		ret.suffix = after
		ret.family = collationFamilyUTF8MB4
	} else if after, ok := strings.CutPrefix(normalized, utf8Prefix); ok {
		ret.suffix = after
		ret.family = collationFamilyUTF8
	} else if after, ok := strings.CutPrefix(normalized, latin1Prefix); ok {
		ret.suffix = after
		ret.family = collationFamilyLatin1
	} else {
		ret.family = collationFamilyOther
		ret.suffix = normalized
	}
	return ret
}

func (a collationLattice) Unwrap() any {
	return a.value
}

func (a collationLattice) Compare(other Lattice) (int, error) {
	b, ok := other.(collationLattice)
	if !ok {
		return 0, typeMismatchError(a, other)
	}

	if a.suffix != b.suffix {
		return 0, incompatibleCollationError(a.value, b.value)
	}

	if a.family == b.family {
		return 0, nil
	}

	switch {
	case a.family == collationFamilyUTF8 && b.family == collationFamilyUTF8MB4:
		return -1, nil
	case a.family == collationFamilyUTF8MB4 && b.family == collationFamilyUTF8:
		return 1, nil
	case a.family == collationFamilyLatin1 && b.family == collationFamilyUTF8MB4:
		return -1, nil
	case a.family == collationFamilyUTF8MB4 && b.family == collationFamilyLatin1:
		return 1, nil
	default:
		return 0, incompatibleCollationError(a.value, b.value)
	}
}

func (a collationLattice) Join(other Lattice) (Lattice, error) {
	b, ok := other.(collationLattice)
	if !ok {
		return nil, typeMismatchError(a, other)
	}

	cmp, err := a.Compare(b)
	if err != nil {
		return nil, err
	}
	if cmp >= 0 {
		return a, nil
	}
	return b, nil
}
