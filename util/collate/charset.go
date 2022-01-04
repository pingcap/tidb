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
	"github.com/pingcap/tidb/parser/charset"
)

// EnableNewCharset enables the charset feature.
func EnableNewCharset() {
	addCharset()
}

func addCharset() {
	if NewCollationEnabled() {
		charset.AddCharset(&charset.Charset{Name: charset.CharsetGBK, DefaultCollation: charset.CollationGBKChineseCI, Collations: make(map[string]*charset.Collation), Desc: "Chinese Internal Code Specification", Maxlen: 2})
		charset.AddCollation(&charset.Collation{ID: 28, CharsetName: charset.CharsetGBK, Name: charset.CollationGBKChineseCI, IsDefault: true})
		charset.AddCollation(&charset.Collation{ID: 87, CharsetName: charset.CharsetGBK, Name: charset.CollationGBKBin, IsDefault: false})
	} else {
		charset.AddCharset(&charset.Charset{Name: charset.CharsetGBK, DefaultCollation: charset.CollationGBKBin, Collations: make(map[string]*charset.Collation), Desc: "Chinese Internal Code Specification", Maxlen: 2})
		charset.AddCollation(&charset.Collation{ID: 87, CharsetName: charset.CharsetGBK, Name: charset.CollationGBKBin, IsDefault: true})
		charset.AddSupportedCollation(&charset.Collation{ID: 87, CharsetName: charset.CharsetGBK, Name: charset.CollationGBKBin, IsDefault: true})
	}
}
