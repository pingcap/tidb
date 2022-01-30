// Copyright 2021 PingCAP, Inc.
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
	"strings"

	"github.com/pingcap/tidb/parser/charset"
)

var (
	enableCharsetFeat bool
)

// EnableNewCharset enables the charset feature.
func EnableNewCharset() {
	enableCharsetFeat = true
	addCharset()
}

// SetCharsetFeatEnabledForTest set charset feature enabled. Only used in test.
// It will also enable or disable new collation.
func SetCharsetFeatEnabledForTest(flag bool) {
	enableCharsetFeat = flag
	SetNewCollationEnabledForTest(flag)
	if flag {
		addCharset()
	} else {
		removeCharset()
	}
}

// CharsetFeatEnabled return true if charset feature is enabled.
func CharsetFeatEnabled() bool {
	return enableCharsetFeat
}

func addCharset() {
	for _, c := range experimentalCharsetInfo {
		charset.AddCharset(c)
		for _, coll := range charset.GetCollations() {
			if strings.EqualFold(coll.CharsetName, c.Name) {
				charset.AddCollation(coll)
			}
		}
	}

	for name, collator := range experimentalCollation {
		newCollatorMap[name] = collator
		newCollatorIDMap[CollationName2ID(name)] = collator
	}
}

func removeCharset() {
	for _, c := range experimentalCharsetInfo {
		charset.RemoveCharset(c.Name)
	}

	for name := range experimentalCollation {
		delete(newCollatorMap, name)
		delete(newCollatorIDMap, CollationName2ID(name))
	}
}

var (
	// All the experimental supported charset should be in the following table, only used when charset feature is enable.
	experimentalCharsetInfo []*charset.Charset

	experimentalCollation map[string]Collator
)

func init() {
	experimentalCollation = make(map[string]Collator)
	experimentalCharsetInfo = append(experimentalCharsetInfo,
		&charset.Charset{Name: charset.CharsetGBK, DefaultCollation: "gbk_chinese_ci", Collations: make(map[string]*charset.Collation), Desc: "Chinese Internal Code Specification", Maxlen: 2},
	)
	e, _ := charset.Lookup(charset.CharsetGBK)
	experimentalCollation[charset.CollationGBKBin] = &gbkBinCollator{e.NewEncoder()}
	experimentalCollation["gbk_chinese_ci"] = &gbkChineseCICollator{}
}
