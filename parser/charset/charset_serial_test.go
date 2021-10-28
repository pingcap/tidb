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

package charset

import (
	"testing"
)

func TestValidCustomCharset(t *testing.T) {
	AddCharset(&Charset{"custom", "custom_collation", make(map[string]*Collation), "Custom", 4})
	defer RemoveCharset("custom")
	AddCollation(&Collation{99999, "custom", "custom_collation", true})

	tests := []struct {
		cs   string
		co   string
		succ bool
	}{
		{"custom", "custom_collation", true},
		{"utf8", "utf8_invalid_ci", false},
	}
	for _, tt := range tests {
		testValidCharset(t, tt.cs, tt.co, tt.succ)
	}
}
