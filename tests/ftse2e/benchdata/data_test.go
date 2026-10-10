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

package benchdata

import (
	"testing"
	"unicode/utf8"
)

func TestDocument(t *testing.T) {
	for _, size := range []int{0, 1, 31, 128, 4096, 262144} {
		for row := range 8 {
			text := Document(row, size)
			if len(text) != size || !utf8.ValidString(text) || text != Document(row+4, size) {
				t.Fatalf("invalid corpus row=%d size=%d", row, size)
			}
		}
	}
}
