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

package fixcontrol

import (
	"strconv"
	"strings"
)

const (
	// Fix33031 controls whether to disallow plan cache for partitioned
	// tables (both prepared statments and non-prepared statements)
	// See #33031
	Fix33031 uint64 = 33031
	// Fix44262 controls whether to allow to use dynamic-mode to access partitioning tables without global-stats (#44262).
	Fix44262 uint64 = 44262
	// Fix44389 controls whether to consider non-point ranges of some CNF item when building ranges.
	Fix44389 uint64 = 44389
	// Fix44830 controls whether to allow to cache Batch/PointGet from some complex scenarios.
	// See #44830 for more details.
	Fix44830 uint64 = 44830
	// Fix44823 controls the maximum number of parameters for a query that can be cached in the Plan Cache.
	Fix44823 uint64 = 44823
	// Fix44855 controls whether to use a more accurate upper bound when estimating row count of index
	// range scan under inner side of index join.
	Fix44855 uint64 = 44855
	// Fix45132 controls whether to use access range row count to determine access path on the Skyline pruning.
	Fix45132 uint64 = 45132
	// Fix45822 controls whether to eliminate apply operator.
	Fix45822 uint64 = 45822
	// Fix45798 controls whether to cache plans that access generated columns.
	Fix45798 uint64 = 45798
	// Fix46177 controls whether to explore enforced plans for DataSource if it has already found an unenforced plan.
	Fix46177 uint64 = 46177
	// Fix49736 controls whether to force the optimizer to use plan cache even if there is risky optimization.
	// This fix-control is test-only.
	Fix49736 uint64 = 49736
	// Fix52869 controls whether to disable the limitation that index merge path won't be generated automatically when
	// there exist other single-index access paths that do range scan.
	Fix52869 uint64 = 52869
)

// GetStr fetches the given key from the fix control map as a string type.
func GetStr(fixControlMap map[uint64]string, key uint64) (value string, exists bool) {
	if fixControlMap == nil {
		return "", false
	}
	rawValue, ok := fixControlMap[key]
	if !ok {
		return "", false
	}
	return rawValue, true
}

// GetStrWithDefault fetches the given key from the fix control map as a string type,
// and a default value would be returned when fail to fetch the expected key.
func GetStrWithDefault(fixControlMap map[uint64]string, key uint64, defaultVal string) string {
	value, exists := GetStr(fixControlMap, key)
	if !exists {
		return defaultVal
	}
	return value
}

// GetBool fetches the given key from the fix control map as a boolean type.
func GetBool(fixControlMap map[uint64]string, key uint64) (value bool, exists bool) {
	if fixControlMap == nil {
		return false, false
	}
	rawValue, ok := fixControlMap[key]
	if !ok {
		return false, false
	}
	// The same as TiDBOptOn in sessionctx/variable.
	value = strings.EqualFold(rawValue, "ON") || rawValue == "1"
	return value, true
}

// GetBoolWithDefault fetches the given key from the fix control map as a boolean type,
// and a default value would be returned when fail to fetch the expected key.
func GetBoolWithDefault(fixControlMap map[uint64]string, key uint64, defaultVal bool) bool {
	value, exists := GetBool(fixControlMap, key)
	if !exists {
		return defaultVal
	}
	return value
}

// GetInt fetches the given key from the fix control map as an uint64 type.
func GetInt(fixControlMap map[uint64]string, key uint64) (value int64, exists bool, parseErr error) {
	if fixControlMap == nil {
		return 0, false, nil
	}
	rawValue, ok := fixControlMap[key]
	if !ok {
		return 0, false, nil
	}
	// The same as TidbOptInt64 in sessionctx/variable.
	value, parseErr = strconv.ParseInt(rawValue, 10, 64)
	return value, true, parseErr
}

// GetIntWithDefault fetches the given key from the fix control map as an uint64 type,
// // and a default value would be returned when fail to fetch the expected key.
func GetIntWithDefault(fixControlMap map[uint64]string, key uint64, defaultVal int64) int64 {
	value, exists, err := GetInt(fixControlMap, key)
	if !exists || err != nil {
		return defaultVal
	}
	return value
}

// GetFloat fetches the given key from the fix control map as a float64 type.
func GetFloat(fixControlMap map[uint64]string, key uint64) (value float64, exists bool, parseErr error) {
	if fixControlMap == nil {
		return 0, false, nil
	}
	rawValue, ok := fixControlMap[key]
	if !ok {
		return 0, false, nil
	}
	// The same as tidbOptFloat64 in sessionctx/variable.
	value, parseErr = strconv.ParseFloat(rawValue, 64)
	return value, true, parseErr
}

// GetFloatWithDefault fetches the given key from the fix control map as a float64 type,
// // and a default value would be returned when fail to fetch the expected key.
func GetFloatWithDefault(fixControlMap map[uint64]string, key uint64, defaultVal float64) float64 {
	value, exists, err := GetFloat(fixControlMap, key)
	if !exists || err != nil {
		return defaultVal
	}
	return value
}
