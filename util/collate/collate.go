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
	"strings"
	"sync/atomic"

	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

var (
	collatorMap         map[string]Collator
	collatorIDMap       map[int]Collator
	newCollatorMap      map[string]Collator
	newCollatorIDMap    map[int]Collator
	newCollationEnabled int32
)

// DefaultLen is set for datum if the string datum don't know its length.
const (
	DefaultLen = 0
)

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

// CollatorOption is the option of collator.
type CollatorOption struct {
	PadLen int
}

// NewCollatorOption creates a new CollatorOption with the specified arguments.
func NewCollatorOption(padLen int) CollatorOption {
	return CollatorOption{padLen}
}

// Collator provides functionality for comparing strings for a given
// collation order.
type Collator interface {
	// Compare returns an integer comparing the two strings. The result will be 0 if a == b, -1 if a < b, and +1 if a > b.
	Compare(a, b string, opt CollatorOption) int
	// Key returns the collate key for str. If the collation is padding, make sure the PadLen >= len(rune[]str) in opt.
	Key(str string, opt CollatorOption) []byte
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
			return newCollatorMap["utf8mb4_bin"]
		}
		return ctor
	}
	ctor, ok := collatorMap[collate]
	if !ok {
		return collatorMap["utf8mb4_bin"]
	}
	return ctor
}

// GetCollatorByID get the collator according to id, it will return the binary collator if the corresponding collator doesn't exist.
func GetCollatorByID(id int) Collator {
	if atomic.LoadInt32(&newCollationEnabled) == 1 {
		ctor, ok := newCollatorIDMap[id]
		if !ok {
			return newCollatorMap["utf8mb4_bin"]
		}
		return ctor
	}
	ctor, ok := collatorIDMap[id]
	if !ok {
		return collatorMap["utf8mb4_bin"]
	}
	return ctor
}

type binCollator struct {
}

// Compare implement Collator interface.
func (bc *binCollator) Compare(a, b string, opt CollatorOption) int {
	return strings.Compare(a, b)
}

// Key implement Collator interface.
func (bc *binCollator) Key(str string, opt CollatorOption) []byte {
	return []byte(str)
}

// CollationID2Name return the collation name by the given id.
// If the id is not found in the map, we reutrn the default one directly.
func CollationID2Name(id int32) string {
	name, ok := mysql.Collations[uint8(id)]
	if !ok {
		return mysql.DefaultCollationName
	}
	return name
}

type binPaddingCollator struct {
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

func (bpc *binPaddingCollator) Compare(a, b string, opt CollatorOption) int {
	return strings.Compare(truncateTailingSpace(a), truncateTailingSpace(b))
}

func (bpc *binPaddingCollator) Key(str string, opt CollatorOption) []byte {
	return []byte(truncateTailingSpace(str))
}

func init() {
	collatorMap = make(map[string]Collator)
	collatorIDMap = make(map[int]Collator)
	newCollatorMap = make(map[string]Collator)
	newCollatorIDMap = make(map[int]Collator)

	collatorMap["binary"] = &binCollator{}
	collatorMap["utf8mb4_bin"] = &binCollator{}
	collatorMap["utf8_bin"] = &binCollator{}
	collatorMap["utf8mb4_general_ci"] = &binCollator{}
	collatorMap["utf8_general_ci"] = &binCollator{}

	// See https://github.com/pingcap/parser/blob/master/charset/charset.go for more information about the IDs.
	collatorIDMap[63] = &binCollator{}
	collatorIDMap[46] = &binCollator{}
	collatorIDMap[83] = &binCollator{}
	collatorIDMap[45] = &binCollator{}
	collatorIDMap[33] = &binCollator{}

	newCollatorMap["binary"] = &binCollator{}
	newCollatorMap["utf8mb4_bin"] = &binPaddingCollator{}
	newCollatorMap["utf8_bin"] = &binPaddingCollator{}
	newCollatorMap["utf8mb4_general_ci"] = &generalCICollator{}
	newCollatorMap["utf8_general_ci"] = &generalCICollator{}

	newCollatorIDMap[63] = &binCollator{}
	newCollatorIDMap[46] = &binPaddingCollator{}
	newCollatorIDMap[83] = &binPaddingCollator{}
	newCollatorIDMap[45] = &generalCICollator{}
	newCollatorIDMap[33] = &generalCICollator{}
}
