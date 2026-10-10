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

// Package benchdata provides a deterministic Local MATCH benchmark corpus.
package benchdata

import "strings"

// Document returns exactly size bytes. Rows cycle through a match, an excluded
// match, a case variant and a miss. Padding never splits a UTF-8 code point.
// Keep these units identical to bench_local_match_against.cpp in TiFlash.
func Document(row, size int) string {
	units := [...]string{
		"quick brown fox prefix 数据库 ",
		"quick slow fox prefix 数据库 ",
		"QUICK brown FOX PREFIX 数据库 ",
		"other words unrelated sample 文档 ",
	}
	unit := units[row%len(units)]
	return strings.Repeat(unit, size/len(unit)) + strings.Repeat(" ", size%len(unit))
}
