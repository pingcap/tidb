// Copyright 2020 PingCAP, Inc.
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

package collate

import (
	"sort"
	"sync/atomic"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/charset"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

var (
	newCollatorMap      map[string]Collator
	newCollatorIDMap    map[int]Collator
	newCollationEnabled int32

	// binCollatorInstance is a singleton used for all collations when newCollationEnabled is false.
	binCollatorInstance = &binCollator{}

	// ErrUnsupportedCollation is returned when an unsupported collation is specified.
	ErrUnsupportedCollation = terror.ClassDDL.New(mysql.ErrUnknownCollation, "Unsupported collation when new collation is enabled: '%-.64s'")
	// ErrIllegalMixCollation is returned when illegal mix of collations.
	ErrIllegalMixCollation = terror.ClassExpression.New(mysql.ErrCantAggregate2collations, mysql.MySQLErrName[mysql.ErrCantAggregate2collations])
)

// DefaultLen is set for datum if the string datum don't know its length.
const (
	DefaultLen = 0
)

// Collator provides functionality for comparing strings for a given
// collation order.
type Collator interface {
	// Compare returns an integer comparing the two strings. The result will be 0 if a == b, -1 if a < b, and +1 if a > b.
	Compare(a, b string) int
	// Key returns the collate key for str. If the collation is padding, make sure the PadLen >= len(rune[]str) in opt.
	Key(str string) []byte
	// Pattern get a collation-aware WildcardPattern.
	Pattern() WildcardPattern
}

// WildcardPattern is the interface used for wildcard pattern match.
type WildcardPattern interface {
	// Compile compiles the patternStr with specified escape character.
	Compile(patternStr string, escape byte)
	// DoMatch tries to match the str with compiled pattern, `Compile()` must be called before calling it.
	DoMatch(str string) bool
}

// EnableNewCollations enables the new collation.
func EnableNewCollations() {
	SetNewCollationEnabledForTest(true)
}

// SetNewCollationEnabledForTest sets if the new collation are enabled in test.
// Note: Be careful to use this function, if this functions is used in tests, make sure the tests are serial.
func SetNewCollationEnabledForTest(flag bool) {
	if flag {
		atomic.StoreInt32(&newCollationEnabled, 1)
		return
	}
	atomic.StoreInt32(&newCollationEnabled, 0)
}

// NewCollationEnabled returns if the new collations are enabled.
func NewCollationEnabled() bool {
	return atomic.LoadInt32(&newCollationEnabled) == 1
}

// CompatibleCollate checks whether the two collate are the same.
func CompatibleCollate(collate1, collate2 string) bool {
	if (collate1 == "utf8mb4_general_ci" || collate1 == "utf8_general_ci") && (collate2 == "utf8mb4_general_ci" || collate2 == "utf8_general_ci") {
		return true
	} else if (collate1 == "utf8mb4_bin" || collate1 == "utf8_bin") && (collate2 == "utf8mb4_bin" || collate2 == "utf8_bin") {
		return true
	} else {
		return collate1 == collate2
	}
}

// RewriteNewCollationIDIfNeeded rewrites a collation id if the new collations are enabled.
// When new collations are enabled, we turn the collation id to negative so that other the
// components of the cluster(for example, TiKV) is able to aware of it without any change to
// the protocol definition.
// When new collations are not enabled, collation id remains the same.
func RewriteNewCollationIDIfNeeded(id int32) int32 {
	if atomic.LoadInt32(&newCollationEnabled) == 1 {
		if id < 0 {
			logutil.BgLogger().Warn("Unexpected negative collation ID for rewrite.", zap.Int32("ID", id))
		} else {
			return -id
		}
	}
	return id
}

// RestoreCollationIDIfNeeded restores a collation id if the new collations are enabled.
func RestoreCollationIDIfNeeded(id int32) int32 {
	if atomic.LoadInt32(&newCollationEnabled) == 1 {
		if id > 0 {
			logutil.BgLogger().Warn("Unexpected positive collation ID for restore.", zap.Int32("ID", id))
		} else {
			return -id
		}
	}
	return id
}

// GetCollator get the collator according to collate, it will return the binary collator if the corresponding collator doesn't exist.
func GetCollator(collate string) Collator {
	if atomic.LoadInt32(&newCollationEnabled) == 1 {
		ctor, ok := newCollatorMap[collate]
		if !ok {
			logutil.BgLogger().Warn(
				"Unable to get collator by name, use binCollator instead.",
				zap.String("name", collate),
				zap.Stack("stack"))
			return newCollatorMap["utf8mb4_bin"]
		}
		return ctor
	}
	return binCollatorInstance
}

// GetCollatorByID get the collator according to id, it will return the binary collator if the corresponding collator doesn't exist.
func GetCollatorByID(id int) Collator {
	if atomic.LoadInt32(&newCollationEnabled) == 1 {
		ctor, ok := newCollatorIDMap[id]
		if !ok {
			logutil.BgLogger().Warn(
				"Unable to get collator by ID, use binCollator instead.",
				zap.Int("ID", id),
				zap.Stack("stack"))
			return newCollatorMap["utf8mb4_bin"]
		}
		return ctor
	}
	return binCollatorInstance
}

// CollationID2Name return the collation name by the given id.
// If the id is not found in the map, the default collation is returned.
func CollationID2Name(id int32) string {
	name, ok := mysql.Collations[uint8(id)]
	if !ok {
		// TODO(bb7133): fix repeating logs when the following code is uncommented.
		//logutil.BgLogger().Warn(
		//	"Unable to get collation name from ID, use default collation instead.",
		//	zap.Int32("ID", id),
		//	zap.Stack("stack"))
		return mysql.DefaultCollationName
	}
	return name
}

// GetCollationByName wraps charset.GetCollationByName, it checks the collation.
func GetCollationByName(name string) (coll *charset.Collation, err error) {
	if coll, err = charset.GetCollationByName(name); err != nil {
		return nil, errors.Trace(err)
	}
	if atomic.LoadInt32(&newCollationEnabled) == 1 {
		if _, ok := newCollatorIDMap[coll.ID]; !ok {
			return nil, ErrUnsupportedCollation.GenWithStackByArgs(name)
		}
	}
	return
}

// GetSupportedCollations gets information for all collations supported so far.
func GetSupportedCollations() []*charset.Collation {
	if atomic.LoadInt32(&newCollationEnabled) == 1 {
		newSupportedCollations := make([]*charset.Collation, 0, len(newCollatorMap))
		for name := range newCollatorMap {
			if coll, err := charset.GetCollationByName(name); err != nil {
				// Should never happens.
				terror.Log(err)
			} else {
				newSupportedCollations = append(newSupportedCollations, coll)
			}
		}
		sort.Slice(newSupportedCollations, func(i int, j int) bool {
			return newSupportedCollations[i].Name < newSupportedCollations[j].Name
		})
		return newSupportedCollations
	}
	return charset.GetSupportedCollations()
}

func truncateTailingSpace(str string) string {
	byteLen := len(str)
	i := byteLen - 1
	for ; i >= 0; i-- {
		if str[i] != ' ' {
			break
		}
	}
	str = str[:i+1]
	return str
}

// IsCICollation returns if the collation is case-sensitive
func IsCICollation(collate string) bool {
	return collate == "utf8_general_ci" || collate == "utf8mb4_general_ci"
}

func init() {
	newCollationEnabled = 1

	newCollatorMap = make(map[string]Collator)
	newCollatorIDMap = make(map[int]Collator)

	newCollatorMap["binary"] = &binCollator{}
	newCollatorIDMap[int(mysql.CollationNames["binary"])] = &binCollator{}
	newCollatorMap["ascii_bin"] = &binPaddingCollator{}
	newCollatorIDMap[int(mysql.CollationNames["ascii_bin"])] = &binPaddingCollator{}
	newCollatorMap["latin1_bin"] = &binPaddingCollator{}
	newCollatorIDMap[int(mysql.CollationNames["latin1_bin"])] = &binPaddingCollator{}
	newCollatorMap["utf8mb4_bin"] = &binPaddingCollator{}
	newCollatorIDMap[int(mysql.CollationNames["utf8mb4_bin"])] = &binPaddingCollator{}
	newCollatorMap["utf8_bin"] = &binPaddingCollator{}
	newCollatorIDMap[int(mysql.CollationNames["utf8_bin"])] = &binPaddingCollator{}
	newCollatorMap["utf8mb4_general_ci"] = &generalCICollator{}
	newCollatorIDMap[int(mysql.CollationNames["utf8mb4_general_ci"])] = &generalCICollator{}
	newCollatorMap["utf8_general_ci"] = &generalCICollator{}
	newCollatorIDMap[int(mysql.CollationNames["utf8_general_ci"])] = &generalCICollator{}
}
