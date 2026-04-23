// Copyright 2023 PingCAP, Inc.
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

package test

// GetWindowFuncTokenMap returns the window function token map keys for testing.
// This is a local copy to avoid accessing unexported parser symbols.
func GetWindowFuncTokenMap() map[string]int {
	return map[string]int{
		"CUME_DIST":    0,
		"DENSE_RANK":   0,
		"FIRST_VALUE":  0,
		"GROUPS":       0,
		"LAG":          0,
		"LAST_VALUE":   0,
		"LEAD":         0,
		"NTH_VALUE":    0,
		"NTILE":        0,
		"OVER":         0,
		"PERCENT_RANK": 0,
		"RANK":         0,
		"ROW_NUMBER":   0,
		"WINDOW":       0,
	}
}
