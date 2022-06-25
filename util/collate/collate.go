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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package collate

import (
	"sort"
	"sync/atomic"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/parser/charset"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/util/dbterror"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

var (
	newCollatorMap      map[string]Collator
	newCollatorIDMap    map[int]Collator
	newCollationEnabled int32

	// binCollatorInstance is a singleton used for all collations when newCollationEnabled is false.
	binCollatorInstance              = &binCollator{}
	binCollatorInstanceSliceWithLen1 = []Collator{binCollatorInstance}

	// ErrUnsupportedCollation is returned when an unsupported collation is specified.
	ErrUnsupportedCollation = dbterror.ClassDDL.NewStdErr(mysql.ErrUnknownCollation, mysql.Message("Unsupported collation when new collation is enabled: '%-.64s'", nil))
	// ErrIllegalMixCollation is returned when illegal mix of collations.
	ErrIllegalMixCollation = dbterror.ClassExpression.NewStd(mysql.ErrCantAggregateNcollations)
	// ErrIllegalMix2Collation is returned when illegal mix of 2 collations.
	ErrIllegalMix2Collation = dbterror.ClassExpression.NewStd(mysql.ErrCantAggregate2collations)
	// ErrIllegalMix3Collation is returned when illegal mix of 3 collations.
	ErrIllegalMix3Collation = dbterror.ClassExpression.NewStd(mysql.ErrCantAggregate3collations)
)

const (
	// DefaultLen is set for datum if the string datum don't know its length.
	DefaultLen = 0
	// first byte of a 2-byte encoding starts 110 and carries 5 bits of data
	b2Mask = 0x1F // 0001 1111
	// first byte of a 3-byte encoding starts 1110 and carries 4 bits of data
	b3Mask = 0x0F // 0000 1111
	// first byte of a 4-byte encoding starts 11110 and carries 3 bits of data
	b4Mask = 0x07 // 0000 0111
	// non-first bytes start 10 and carry 6 bits of data
	mbMask = 0x3F // 0011 1111
)

// Collator provides functionality for comparing strings for a given
// collation order.
type Collator interface {
	// Compare returns an integer comparing the two strings. The result will be 0 if a == b, -1 if a < b, and +1 if a > b.
	Compare(a, b string) int
	// Key returns the collate key for str. If the collation is padding, make sure the PadLen >= len(rune[]str) in opt.
	Key(str string) []byte
	// KeyWithoutTrimRightSpace returns the collate key for str. The difference with Key is str will not be trimed.
	KeyWithoutTrimRightSpace(str string) []byte
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

// SetNewCollationEnabledForTest sets if the new collation are enabled in test.
// Note: Be careful to use this function, if this functions is used in tests, make sure the tests are serial.
func SetNewCollationEnabledForTest(flag bool) {
	switchDefaultCollation(flag)
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
	} else if (collate1 == "utf8mb4_bin" || collate1 == "utf8_bin" || collate1 == "latin1_bin") && (collate2 == "utf8mb4_bin" || collate2 == "utf8_bin") {
		return true
	} else if (collate1 == "utf8mb4_unicode_ci" || collate1 == "utf8_unicode_ci") && (collate2 == "utf8mb4_unicode_ci" || collate2 == "utf8_unicode_ci") {
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
			if collate != "" {
				logutil.BgLogger().Warn(
					"Unable to get collator by name, use binCollator instead.",
					zap.String("name", collate),
					zap.Stack("stack"))
			}
			return newCollatorMap[charset.CollationUTF8MB4]
		}
		return ctor
	}
	return binCollatorInstance
}

// GetBinaryCollator gets the binary collator, it is often used when we want to apply binary compare.
func GetBinaryCollator() Collator {
	return binCollatorInstance
}

// GetBinaryCollatorSlice gets the binary collator slice with len n.
func GetBinaryCollatorSlice(n int) []Collator {
	if n == 1 {
		return binCollatorInstanceSliceWithLen1
	}
	collators := make([]Collator, n)
	for i := 0; i < n; i++ {
		collators[i] = binCollatorInstance
	}
	return collators
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
	collation, err := charset.GetCollationByID(int(id))
	if err != nil {
		// TODO(bb7133): fix repeating logs when the following code is uncommented.
		// logutil.BgLogger().Warn(
		// 	"Unable to get collation name from ID, use default collation instead.",
		// 	zap.Int32("ID", id),
		// 	zap.Stack("stack"))
		return mysql.DefaultCollationName
	}
	return collation.Name
}

// CollationName2ID return the collation id by the given name.
// If the name is not found in the map, the default collation id is returned
func CollationName2ID(name string) int {
	if coll, err := charset.GetCollationByName(name); err == nil {
		return coll.ID
	}
	return mysql.DefaultCollationID
}

// SubstituteMissingCollationToDefault will switch to the default collation if
// new collations are enabled and the specified collation is not supported.
func SubstituteMissingCollationToDefault(co string) string {
	var err error
	if _, err = GetCollationByName(co); err == nil {
		return co
	}
	logutil.BgLogger().Warn(err.Error())
	var coll *charset.Collation
	if coll, err = GetCollationByName(charset.CollationUTF8MB4); err != nil {
		logutil.BgLogger().Warn(err.Error())
	}
	return coll.Name
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
			// utf8mb4_zh_pinyin_tidb_as_cs is under developing, should not be shown to user.
			if name == "utf8mb4_zh_pinyin_tidb_as_cs" {
				continue
			}
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

func sign(i int) int {
	if i < 0 {
		return -1
	} else if i > 0 {
		return 1
	}
	return 0
}

// decode rune by hand
func decodeRune(s string, si int) (r rune, newIndex int) {
	b := s[si]
	switch runeLen(b) {
	case 1:
		r = rune(b)
		newIndex = si + 1
	case 2:
		r = rune(b&b2Mask)<<6 |
			rune(s[1+si]&mbMask)
		newIndex = si + 2
	case 3:
		r = rune(b&b3Mask)<<12 |
			rune(s[si+1]&mbMask)<<6 |
			rune(s[si+2]&mbMask)
		newIndex = si + 3
	default:
		r = rune(b&b4Mask)<<18 |
			rune(s[si+1]&mbMask)<<12 |
			rune(s[si+2]&mbMask)<<6 |
			rune(s[si+3]&mbMask)
		newIndex = si + 4
	}
	return
}

func runeLen(b byte) int {
	if b < 0x80 {
		return 1
	} else if b < 0xE0 {
		return 2
	} else if b < 0xF0 {
		return 3
	}
	return 4
}

// IsCICollation returns if the collation is case-sensitive
func IsCICollation(collate string) bool {
	return collate == "utf8_general_ci" || collate == "utf8mb4_general_ci" ||
		collate == "utf8_unicode_ci" || collate == "utf8mb4_unicode_ci"
}

// IsBinCollation returns if the collation is 'xx_bin' or 'bin'.
// The function is to determine whether the sortkey of a char type of data under the collation is equal to the data itself,
// and both xx_bin and collationBin are satisfied.
func IsBinCollation(collate string) bool {
	return collate == charset.CollationASCII || collate == charset.CollationLatin1 ||
		collate == charset.CollationUTF8 || collate == charset.CollationUTF8MB4 ||
		collate == charset.CollationBin
}

// CollationToProto converts collation from string to int32(used by protocol).
func CollationToProto(c string) int32 {
	if coll, err := charset.GetCollationByName(c); err == nil {
		return RewriteNewCollationIDIfNeeded(int32(coll.ID))
	}
	v := RewriteNewCollationIDIfNeeded(int32(mysql.DefaultCollationID))
	logutil.BgLogger().Warn(
		"Unable to get collation ID by name, use ID of the default collation instead",
		zap.String("name", c),
		zap.Int32("default collation ID", v),
		zap.String("default collation", mysql.DefaultCollationName),
	)
	return v
}

// ProtoToCollation converts collation from int32(used by protocol) to string.
func ProtoToCollation(c int32) string {
	coll, err := charset.GetCollationByID(int(RestoreCollationIDIfNeeded(c)))
	if err == nil {
		return coll.Name
	}
	logutil.BgLogger().Warn(
		"Unable to get collation name from ID, use name of the default collation instead",
		zap.Int32("id", c),
		zap.Int("default collation ID", mysql.DefaultCollationID),
		zap.String("default collation", mysql.DefaultCollationName),
	)
	return mysql.DefaultCollationName
}

func init() {
	// Set it to 1 in init() to make sure the tests enable the new collation, it would be covered in bootstrap().
	newCollationEnabled = 1

	newCollatorMap = make(map[string]Collator)
	newCollatorIDMap = make(map[int]Collator)

	newCollatorMap["binary"] = &binCollator{}
	newCollatorIDMap[CollationName2ID("binary")] = &binCollator{}
	newCollatorMap["ascii_bin"] = &binPaddingCollator{}
	newCollatorIDMap[CollationName2ID("ascii_bin")] = &binPaddingCollator{}
	newCollatorMap["latin1_bin"] = &binPaddingCollator{}
	newCollatorIDMap[CollationName2ID("latin1_bin")] = &binPaddingCollator{}
	newCollatorMap["utf8mb4_bin"] = &binPaddingCollator{}
	newCollatorIDMap[CollationName2ID("utf8mb4_bin")] = &binPaddingCollator{}
	newCollatorMap["utf8_bin"] = &binPaddingCollator{}
	newCollatorIDMap[CollationName2ID("utf8_bin")] = &binPaddingCollator{}
	newCollatorMap["utf8mb4_general_ci"] = &generalCICollator{}
	newCollatorIDMap[CollationName2ID("utf8mb4_general_ci")] = &generalCICollator{}
	newCollatorMap["utf8_general_ci"] = &generalCICollator{}
	newCollatorIDMap[CollationName2ID("utf8_general_ci")] = &generalCICollator{}
	newCollatorMap["utf8mb4_unicode_ci"] = &unicodeCICollator{}
	newCollatorIDMap[CollationName2ID("utf8mb4_unicode_ci")] = &unicodeCICollator{}
	newCollatorMap["utf8_unicode_ci"] = &unicodeCICollator{}
	newCollatorIDMap[CollationName2ID("utf8_unicode_ci")] = &unicodeCICollator{}
	newCollatorMap["utf8mb4_zh_pinyin_tidb_as_cs"] = &zhPinyinTiDBASCSCollator{}
	newCollatorIDMap[CollationName2ID("utf8mb4_zh_pinyin_tidb_as_cs")] = &zhPinyinTiDBASCSCollator{}
	newCollatorMap[charset.CollationGBKBin] = &gbkBinCollator{charset.NewCustomGBKEncoder()}
	newCollatorIDMap[CollationName2ID(charset.CollationGBKBin)] = &gbkBinCollator{charset.NewCustomGBKEncoder()}
	newCollatorMap[charset.CollationGBKChineseCI] = &gbkChineseCICollator{}
	newCollatorIDMap[CollationName2ID(charset.CollationGBKChineseCI)] = &gbkChineseCICollator{}
}
