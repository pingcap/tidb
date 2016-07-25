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

package mysql

import "testing"

func TestGetFieldLength(t *testing.T) {
	tbl := []struct {
		Type   byte
		Length int
	}{
		{TypeTiny, 4},
		{TypeShort, 6},
		{TypeInt24, 9},
		{TypeLong, 11},
		{TypeLonglong, 21},
		{TypeBit, -1},
		{TypeBlob, -1},
		{TypeNull, -1},
		{TypeNewDecimal, 10},
	}

	for _, test := range tbl {
		l := GetDefaultFieldLength(test.Type)
		if l != test.Length {
			t.Fatalf("invalid field length %d != %d", l, test.Length)
		}
		if test.Type == TypeNewDecimal {
			if dec := GetDefaultDecimal(test.Type); dec != 0 {
				t.Fatalf("invalid field decimal %d", dec)
			}
		}
	}
}
