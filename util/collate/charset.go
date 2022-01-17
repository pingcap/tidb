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
	"sort"
	"strings"

	"github.com/pingcap/tidb/parser/charset"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

// switchDefaultCollation switch the default collation for charset according to the new collation config.
func switchDefaultCollation(flag bool) {
	if flag {
		charset.SetDefaultCollation(charset.CharsetGBK, charset.CollationGBKChineseCI)
	} else {
		charset.SetDefaultCollation(charset.CharsetGBK, charset.CollationGBKBin)
	}
}

// GetAllSupportedCharsets gets descriptions for all charsets supported so far.
func GetAllSupportedCharsets() []*charset.Charset {
	charsets := make([]*charset.Charset, 0, len(supportCharsets))
	for ch := range supportCharsets {
		charsets = append(charsets, charset.FindCharsetByName(ch))
	}

	// sort charset by name.
	sort.Slice(charsets, func(i, j int) bool {
		return charsets[i].Name < charsets[j].Name
	})
	return charsets
}

// GetCharsetByName gets descriptions for all charsets supported so far.
func GetCharsetByName(name string) (*charset.Charset, error) {
	name = strings.ToLower(name)
	if _, supported := supportCharsets[name]; !supported {
		return nil, charset.ErrUnknownCharacterSet.GenWithStackByArgs(name)
	}
	return charset.FindCharsetByName(name), nil
}

// GetDefaultCharsetAndCollate returns the default charset and collation.
func GetDefaultCharsetAndCollate() (string, string) {
	return mysql.DefaultCharset, mysql.DefaultCollationName
}

// GetDefaultCollation returns the default collation for charset.
func GetDefaultCollation(name string) (string, error) {
	charsetByName, err := GetCharsetByName(name)
	if err != nil {
		return "", err
	}
	return charsetByName.DefaultCollation, nil
}

// GetCharsetByCollationID returns charset and collation for id as cs_number.
func GetCharsetByCollationID(coID int) (string, string, error) {
	if coID == mysql.DefaultCollationID {
		return mysql.DefaultCharset, mysql.DefaultCollationName, nil
	}
	collation, err := charset.FindCollationByID(coID)
	if err != nil {
		logutil.BgLogger().Warn(
			"unable to get collation name from collation ID, return default charset and collation instead",
			zap.Int("ID", coID),
			zap.Stack("stack"))
		return mysql.DefaultCharset, mysql.DefaultCollationName, err
	}
	return collation.CharsetName, collation.Name, nil
}

var (
	supportCharsets = map[string]struct{}{
		charset.CharsetUTF8:    {},
		charset.CharsetUTF8MB4: {},
		charset.CharsetASCII:   {},
		charset.CharsetLatin1:  {},
		charset.CharsetBin:     {},
		charset.CharsetGBK:     {},
	}

	// TiFlashSupportedCharsets is a map which contains TiFlash supports charsets.
	TiFlashSupportedCharsets = map[string]struct{}{
		charset.CharsetUTF8:    {},
		charset.CharsetUTF8MB4: {},
		charset.CharsetASCII:   {},
		charset.CharsetLatin1:  {},
		charset.CharsetBin:     {},
	}
)
