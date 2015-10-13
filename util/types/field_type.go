// Copyright 2014 The ql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSES/QL-LICENSE file.

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
	"fmt"
	"strings"

	mysql "github.com/pingcap/tidb/mysqldef"
	"github.com/pingcap/tidb/util/charset"
)

// UnspecifiedLength is unspecified length.
const (
	UnspecifiedLength int = -1
)

// FieldType records field type information.
type FieldType struct {
	Tp      byte
	Flag    uint
	Flen    int
	Decimal int
	Charset string
	Collate string
	// Elems is the element list for enum and set type.
	Elems []string
}

// NewFieldType returns a FieldType,
// with a type and other information about field type.
func NewFieldType(tp byte) *FieldType {
	return &FieldType{
		Tp:      tp,
		Flen:    UnspecifiedLength,
		Decimal: UnspecifiedLength,
	}
}

// String joins the information of FieldType and
// returns a string.
func (ft *FieldType) String() string {
	ts := FieldTypeToStr(ft.Tp, ft.Charset)
	ans := []string{ts}
	switch ft.Tp {
	case mysql.TypeEnum, mysql.TypeSet:
		// Format is ENUM ('e1', 'e2') or SET ('e1', 'e2')
		ans = append(ans, fmt.Sprintf("('%s')", strings.Join(ft.Elems, "','")))
	default:
		if ft.Flen != UnspecifiedLength {
			if ft.Decimal == UnspecifiedLength {
				ans = append(ans, fmt.Sprintf("(%d)", ft.Flen))
			} else {
				ans = append(ans, fmt.Sprintf("(%d, %d)", ft.Flen, ft.Decimal))
			}
		} else if ft.Decimal != UnspecifiedLength {
			ans = append(ans, fmt.Sprintf("(%d)", ft.Decimal))
		}
	}

	if mysql.HasUnsignedFlag(ft.Flag) {
		ans = append(ans, "UNSIGNED")
	}
	if mysql.HasZerofillFlag(ft.Flag) {
		ans = append(ans, "ZEROFILL")
	}
	if mysql.HasBinaryFlag(ft.Flag) {
		ans = append(ans, "BINARY")
	}

	if IsTypeChar(ft.Tp) || IsTypeBlob(ft.Tp) {
		if ft.Charset != "" && ft.Charset != charset.CharsetBin {
			ans = append(ans, fmt.Sprintf("CHARACTER SET %s", ft.Charset))
		}
		if ft.Collate != "" && ft.Collate != charset.CharsetBin {
			ans = append(ans, fmt.Sprintf("COLLATE %s", ft.Collate))
		}
	}

	return strings.Join(ans, " ")
}
