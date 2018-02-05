// Copyright 2018 PingCAP, Inc.
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

package main

import (
	"testing"
)

func TestIntToDecimalString(t *testing.T) {
	tests := []struct {
		intValue int64
		decimal  int
		expect   string
	}{
		{100, 3, "0.100"},
		{100, 1, "10.0"},
		{100, 0, "100"},
		{1, 3, "0.001"},
		{0, 1, "0.0"},
		{0, 5, "0.00000"},
		{12, 0, "12"},
		{999, 1, "99.9"},
		{1234, 1, "123.4"},
		{12345678, 2, "123456.78"},
	}
	for _, s := range tests {
		if u := intToDecimalString(s.intValue, s.decimal); u != s.expect {
			t.Errorf("test failed on (%d, %d): expected %s, but we got %s\n", s.intValue, s.decimal, s.expect, u)
		}
	}
}
