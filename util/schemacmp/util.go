// Copyright 2022 PingCAP, Inc.
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

import "github.com/pingcap/tidb/parser/mysql"

// compareMySQLIntegerType compares two MySQL integer types,
// return -1 if a < b, 0 if a == b and 1 if a > b.
func compareMySQLIntegerType(a, b byte) int {
	if a == b {
		return 0
	}

	// TypeTiny(1) < TypeShort(2) < TypeInt24(9) < TypeLong(3) < TypeLonglong(8)
	switch {
	case a == mysql.TypeInt24:
		if b <= mysql.TypeShort {
			return 1
		}
		return -1
	case b == mysql.TypeInt24:
		if a <= mysql.TypeShort {
			return -1
		}
		return 1
	case a < b:
		return -1
	default:
		return 1
	}
}

// compareMySQLBlobType compares two MySQL blob types,
// return -1 if a < b, 0 if a == b and 1 if a > b.
func compareMySQLBlobType(a, b byte) int {
	if a == b {
		return 0
	}

	// TypeTinyBlob(0xf9, 249) < TypeBlob(0xfc, 252) < TypeMediumBlob(0xfa, 250) < TypeLongBlob(0xfb, 251)
	switch {
	case a == mysql.TypeBlob:
		if b == mysql.TypeTinyBlob {
			return 1
		}
		return -1
	case b == mysql.TypeBlob:
		if a == mysql.TypeTinyBlob {
			return -1
		}
		return 1
	case a < b:
		return -1
	default:
		return 1
	}
}
