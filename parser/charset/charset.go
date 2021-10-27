// Copyright 2015 PingCAP, Inc.
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

package charset

import (
	"sort"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/parser/terror"
)

var (
	ErrUnknownCollation         = terror.ClassDDL.NewStd(mysql.ErrUnknownCollation)
	ErrCollationCharsetMismatch = terror.ClassDDL.NewStd(mysql.ErrCollationCharsetMismatch)
)

// Charset is a charset.
// Now we only support MySQL.
type Charset struct {
	Name             string
	DefaultCollation string
	Collations       map[string]*Collation
	Desc             string
	Maxlen           int
}

// Collation is a collation.
// Now we only support MySQL.
type Collation struct {
	ID          int
	CharsetName string
	Name        string
	IsDefault   bool
}

var collationsIDMap = make(map[int]*Collation)
var collationsNameMap = make(map[string]*Collation)
var supportedCollations = make([]*Collation, 0, len(supportedCollationNames))

// All the supported charsets should be in the following table.
var charsetInfos = map[string]*Charset{
	CharsetUTF8:     {CharsetUTF8, CollationUTF8, make(map[string]*Collation), "UTF-8 Unicode", 3},
	CharsetUTF16:    {CharsetUTF16, CollationUTF16Default, make(map[string]*Collation), "UTF-16 Unicode", 4},
	CharsetUTF32:    {CharsetUTF32, CollationUTF32Default, make(map[string]*Collation), "UTF-32 Unicode", 4},
	CharsetUTF8MB3:  {CharsetUTF8MB3, CollationUTF8MB3Bin, make(map[string]*Collation), "UTF-8 Unicode", 3},
	CharsetUTF8MB4:  {CharsetUTF8MB4, CollationUTF8MB4, make(map[string]*Collation), "UTF-8 Unicode", 4},
	CharsetASCII:    {CharsetASCII, CollationASCII, make(map[string]*Collation), "US ASCII", 1},
	CharsetLatin1:   {CharsetLatin1, CollationLatin1, make(map[string]*Collation), "cp1252 West European", 1},
	CharsetLatin2:   {CharsetLatin2, CollationLatin2Default, make(map[string]*Collation), "ISO 8859-2 Central European", 1},
	CharsetLatin5:   {CharsetLatin5, CollationLatin5Default, make(map[string]*Collation), "ISO 8859-9 Turkish", 1},
	CharsetLatin7:   {CharsetLatin7, CollationLatin7Default, make(map[string]*Collation), "ISO 8859-13 Baltic", 1},
	CharsetBin:      {CharsetBin, CollationBin, make(map[string]*Collation), "Binary pseudo charset", 1},
	CharsetGBK:      {CharsetGBK, CollationGBKDefault, make(map[string]*Collation), "GBK Simplified Chinese", 2},
	CharsetARMSCII8: {CharsetARMSCII8, CollationARMSCII8Default, make(map[string]*Collation), "ARMSCII-8 Armenian", 1},
	CharsetBig5:     {CharsetBig5, CollationBig5Default, make(map[string]*Collation), "Big5 Traditional Chinese", 2},
	CharsetCP1250:   {CharsetCP1250, CollationCP1250Default, make(map[string]*Collation), "Windows Central European", 1},
	CharsetCP1251:   {CharsetCP1251, CollationCP1251Default, make(map[string]*Collation), "Windows Cyrillic", 1},
	CharsetCP1256:   {CharsetCP1256, CollationCP1256Default, make(map[string]*Collation), "Windows Arabic", 1},
	CharsetCP1257:   {CharsetCP1257, CollationCP1257Default, make(map[string]*Collation), "Windows Baltic", 1},
	CharsetCP850:    {CharsetCP850, CollationCP850Default, make(map[string]*Collation), "DOS West European", 1},
	CharsetCP852:    {CharsetCP852, CollationCP852Default, make(map[string]*Collation), "DOS Central European", 1},
	CharsetCP866:    {CharsetCP866, CollationCP866Default, make(map[string]*Collation), "DOS Russian", 1},
	CharsetCP932:    {CharsetCP932, CollationCP932Default, make(map[string]*Collation), "SJIS for Windows Japanese", 2},
	CharsetDEC8:     {CharsetDEC8, CollationDEC8Default, make(map[string]*Collation), "DEC West European", 1},
	CharsetEUCJPMS:  {CharsetEUCJPMS, CollationEUCJPMSDefault, make(map[string]*Collation), "UJIS for Windows Japanese", 3},
	CharsetEUCKR:    {CharsetEUCKR, CollationEUCKRDefault, make(map[string]*Collation), "EUC-KR Korean", 2},
	CharsetGB18030:  {CharsetGB18030, CollationGB18030Default, make(map[string]*Collation), "China National Standard GB18030", 4},
	CharsetGB2312:   {CharsetGB2312, CollationGB2312Default, make(map[string]*Collation), "GB2312 Simplified Chinesegbk", 2},
	CharsetGEOSTD8:  {CharsetGEOSTD8, CollationGEOSTD8Default, make(map[string]*Collation), "GEOSTD8 Georgian", 1},
	CharsetGreek:    {CharsetGreek, CollationGreekDefault, make(map[string]*Collation), "ISO 8859-7 Greek", 1},
	CharsetHebrew:   {CharsetHebrew, CollationHebrewDefault, make(map[string]*Collation), "ISO 8859-8 Hebrewhp8", 1},
	CharsetHP8:      {CharsetHP8, CollationHP8Default, make(map[string]*Collation), "HP West European", 1},
	CharsetKEYBCS2:  {CharsetKEYBCS2, CollationKEYBCS2Default, make(map[string]*Collation), "DOS Kamenicky Czech-Slovak", 1},
	CharsetKOI8R:    {CharsetKOI8R, CollationKOI8RDefault, make(map[string]*Collation), "KOI8-R Relcom Russian", 1},
	CharsetKOI8U:    {CharsetKOI8U, CollationKOI8UDefault, make(map[string]*Collation), "KOI8-U Ukrainian", 1},
	CharsetMacCE:    {CharsetMacCE, CollationMacCEDefault, make(map[string]*Collation), "Mac Central European", 1},
	CharsetMacRoman: {CharsetMacRoman, CollationMacRomanDefault, make(map[string]*Collation), "Mac West European", 1},
	CharsetSJIS:     {CharsetSJIS, CollationSJISDefault, make(map[string]*Collation), "Shift-JIS Japanese", 2},
	CharsetSWE7:     {CharsetSWE7, CollationSWE7Default, make(map[string]*Collation), "7bit Swedish", 1},
	CharsetTIS620:   {CharsetTIS620, CollationTIS620Default, make(map[string]*Collation), "TIS620 Thaiucs2", 1},
	CharsetUCS2:     {CharsetUCS2, CollationUCS2Default, make(map[string]*Collation), "UCS-2 Unicode", 2},
	CharsetUJIS:     {CharsetUJIS, CollationUJISDefault, make(map[string]*Collation), "EUC-JP Japanese", 3},
	CharsetUTF16LE:  {CharsetUTF16LE, CollationUTF16LEDefault, make(map[string]*Collation), "UTF-16LE Unicode", 4},
}

// All the names supported collations should be in the following table.
var supportedCollationNames = map[string]struct{}{
	CollationUTF8:            {},
	CollationUTF16Bin:        {},
	CollationUTF32Bin:        {},
	CollationUTF8MB3Bin:      {},
	CollationUTF8MB4:         {},
	CollationASCII:           {},
	CollationLatin1:          {},
	CollationBin:             {},
	CollationARMSCII8Bin:     {},
	CollationBig5Bin:         {},
	CollationCP1250Bin:       {},
	CollationCP1251Bin:       {},
	CollationCP1256Bin:       {},
	CollationCP1257Bin:       {},
	CollationCP850Bin:        {},
	CollationCP852Bin:        {},
	CollationCP866Bin:        {},
	CollationCP932Bin:        {},
	CollationDEC8Bin:         {},
	CollationEUCJPMSBin:      {},
	CollationEUCKRBin:        {},
	CollationGB18030Bin:      {},
	CollationGB2312Bin:       {},
	CollationGEOSTD8Bin:      {},
	CollationGBKBin:          {},
	CollationSWE7Bin:         {},
	CollationGreekBin:        {},
	CollationHebrewBin:       {},
	CollationHP8Bin:          {},
	CollationKEYBCS2Bin:      {},
	CollationKOI8RBin:        {},
	CollationKOI8UBin:        {},
	CollationLatin5Bin:       {},
	CollationLatin7Bin:       {},
	CollationMacCEBin:        {},
	CollationMacRomanBin:     {},
	CollationSJISBin:         {},
	CollationTIS620Bin:       {},
	CollationUCS2Bin:         {},
	CollationUJISBin:         {},
	CollationUTF16Default:    {},
	CollationUTF32Default:    {},
	CollationLatin2Default:   {},
	CollationLatin5Default:   {},
	CollationLatin7Default:   {},
	CollationGBKDefault:      {},
	CollationARMSCII8Default: {},
	CollationBig5Default:     {},
	CollationCP1250Default:   {},
	CollationCP1251Default:   {},
	CollationCP1256Default:   {},
	CollationCP1257Default:   {},
	CollationCP850Default:    {},
	CollationCP852Default:    {},
	CollationCP866Default:    {},
	CollationCP932Default:    {},
	CollationDEC8Default:     {},
	CollationEUCJPMSDefault:  {},
	CollationEUCKRDefault:    {},
	CollationGB18030Default:  {},
	CollationGB2312Default:   {},
	CollationGEOSTD8Default:  {},
	CollationMacCEDefault:    {},
	CollationSJISDefault:     {},
	CollationSWE7Default:     {},
	CollationTIS620Default:   {},
	CollationUCS2Default:     {},
	CollationUJISDefault:     {},
	CollationUTF16LEDefault:  {},
	CollationGreekDefault:    {},
	CollationHebrewDefault:   {},
	CollationHP8Default:      {},
	CollationKEYBCS2Default:  {},
	CollationKOI8RDefault:    {},
	CollationKOI8UDefault:    {},
	CollationMacRomanDefault: {},
}

// GetSupportedCharsets gets descriptions for all charsets supported so far.
func GetSupportedCharsets() []*Charset {
	charsets := make([]*Charset, 0, len(charsetInfos))
	for _, ch := range charsetInfos {
		charsets = append(charsets, ch)
	}

	// sort charset by name.
	sort.Slice(charsets, func(i, j int) bool {
		return charsets[i].Name < charsets[j].Name
	})
	return charsets
}

// GetSupportedCollations gets information for all collations supported so far.
func GetSupportedCollations() []*Collation {
	return supportedCollations
}

// ValidCharsetAndCollation checks the charset and the collation validity
// and returns a boolean.
func ValidCharsetAndCollation(cs string, co string) bool {
	// We will use utf8 as a default charset.
	if cs == "" {
		cs = "utf8"
	}
	chs, err := GetCharsetInfo(cs)
	if err != nil {
		return false
	}

	if co == "" {
		return true
	}
	co = strings.ToLower(co)
	_, ok := chs.Collations[co]
	return ok
}

// GetDefaultCollationLegacy is compatible with the charset support in old version parser.
func GetDefaultCollationLegacy(charset string) (string, error) {
	switch strings.ToLower(charset) {
	case CharsetUTF8, CharsetUTF16, CharsetUTF32, CharsetUTF8MB3, CharsetUTF8MB4, CharsetASCII, CharsetLatin1, CharsetBin,
		CollationGBKBin, CollationARMSCII8Bin, CollationBig5Bin, CollationCP1250Bin, CollationCP1251Bin, CollationCP1256Bin, CollationCP1257Bin,
		CollationCP850Bin, CollationCP852Bin, CollationCP866Bin, CollationCP932Bin, CollationDEC8Bin, CollationEUCJPMSBin, CollationEUCKRBin,
		CollationGB18030Bin, CollationGB2312Bin, CollationGEOSTD8Bin, CollationGreekBin, CollationHebrewBin, CollationHP8Bin, CollationKEYBCS2Bin,
		CollationKOI8RBin, CollationKOI8UBin, CollationLatin5Bin, CollationLatin7Bin, CollationMacCEBin, CollationMacRomanBin, CollationSJISBin,
		CollationSWE7Bin, CollationTIS620Bin, CollationUCS2Bin, CollationUJISBin, CollationUTF16LEBin:
		return GetDefaultCollation(charset)
	default:
		return "", errors.Errorf("Unknown charset %s", charset)
	}
}

// GetDefaultCollation returns the default collation for charset.
func GetDefaultCollation(charset string) (string, error) {
	cs, err := GetCharsetInfo(charset)
	if err != nil {
		return "", err
	}
	return cs.DefaultCollation, nil
}

// GetDefaultCharsetAndCollate returns the default charset and collation.
func GetDefaultCharsetAndCollate() (string, string) {
	return mysql.DefaultCharset, mysql.DefaultCollationName
}

// GetCharsetInfo returns charset and collation for cs as name.
func GetCharsetInfo(cs string) (*Charset, error) {
	if c, ok := charsetInfos[strings.ToLower(cs)]; ok {
		return c, nil
	}

	return nil, errors.Errorf("Unknown charset %s", cs)
}

// GetCharsetInfoByID returns charset and collation for id as cs_number.
func GetCharsetInfoByID(coID int) (string, string, error) {
	if coID == mysql.DefaultCollationID {
		return mysql.DefaultCharset, mysql.DefaultCollationName, nil
	}
	if collation, ok := collationsIDMap[coID]; ok {
		return collation.CharsetName, collation.Name, nil
	}
	return "", "", errors.Errorf("Unknown charset id %d", coID)
}

// GetCollations returns a list for all collations.
func GetCollations() []*Collation {
	return collations
}

func GetCollationByName(name string) (*Collation, error) {
	collation, ok := collationsNameMap[strings.ToLower(name)]
	if !ok {
		return nil, ErrUnknownCollation.GenWithStackByArgs(name)
	}
	return collation, nil
}

// GetCollationByID returns collations by given id.
func GetCollationByID(id int) (*Collation, error) {
	collation, ok := collationsIDMap[id]
	if !ok {
		return nil, errors.Errorf("Unknown collation id %d", id)
	}

	return collation, nil
}

const (
	// CharsetBin is used for marking binary charset.
	CharsetBin = "binary"
	// CollationBin is the default collation for CharsetBin.
	CollationBin  = "binary"
	CharsetBinary = "binary"
	// CharsetUTF8 is the default charset for string types.
	CharsetUTF8 = "utf8"
	// CollationUTF8 is the default collation for CharsetUTF8.
	CollationUTF8 = "utf8_bin"
	// CharsetUTF8MB3 represents 3 bytes utf8, which works the same way as utf8 in Go.
	CharsetUTF8MB3 = "utf8mb3"
	// CollationUTF8MB3Bin is the default collation for CharsetUTF8MB3.
	CollationUTF8MB3Bin = "utf8mb3_bin"
	// CharsetUTF8MB4 represents 4 bytes utf8, which works the same way as utf8 in Go.
	CharsetUTF8MB4 = "utf8mb4"
	// CollationUTF8MB4 is the default collation for CharsetUTF8MB4.
	CollationUTF8MB4 = "utf8mb4_bin"
	// CharsetASCII is a subset of UTF8.
	CharsetASCII = "ascii"
	// CollationASCII is the default collation for CharsetACSII.
	CollationASCII = "ascii_bin"
	// CharsetLatin1 is a single byte charset.
	CharsetLatin1 = "latin1"
	// CollationLatin1 is the default collation for CharsetLatin1.
	CollationLatin1     = "latin1_bin"
	CharsetGBK          = "gbk"
	CollationGBKBin     = "gbk_bin"
	CollationGBKDefault = "gbk_chinese_ci"

	CharsetARMSCII8          = "armscii8"
	CollationARMSCII8Bin     = "armscii8_bin"
	CollationARMSCII8Default = "armscii8_general_ci"
	CharsetBig5              = "big5"
	CollationBig5Bin         = "big5_bin"
	CollationBig5Default     = "big5_chinese_ci"
	CharsetCP1250            = "cp1250"
	CollationCP1250Bin       = "cp1250_bin"
	CollationCP1250Default   = "cp1250_general_ci"
	CharsetCP1251            = "cp1251"
	CollationCP1251Bin       = "cp1251_bin"
	CollationCP1251Default   = "cp1251_general_ci"
	CharsetCP1256            = "cp1256"
	CollationCP1256Bin       = "cp1256_bin"
	CollationCP1256Default   = "cp1256_general_ci"
	CharsetCP1257            = "cp1257"
	CollationCP1257Bin       = "cp1257_bin"
	CollationCP1257Default   = "cp1257_general_ci"
	CharsetCP850             = "cp850"
	CollationCP850Bin        = "cp850_bin"
	CollationCP850Default    = "cp850_general_ci"
	CharsetCP852             = "cp852"
	CollationCP852Bin        = "cp852_bin"
	CollationCP852Default    = "cp852_general_ci"
	CharsetCP866             = "cp866"
	CollationCP866Bin        = "cp866_bin"
	CollationCP866Default    = "cp866_general_ci"
	CharsetCP932             = "cp932"
	CollationCP932Bin        = "cp932_bin"
	CollationCP932Default    = "cp932_japanese_ci"
	CharsetDEC8              = "dec8"
	CollationDEC8Bin         = "dec8_bin"
	CollationDEC8Default     = "dec8_swedish_ci"
	CharsetEUCJPMS           = "eucjpms"
	CollationEUCJPMSBin      = "eucjpms_bin"
	CollationEUCJPMSDefault  = "eucjpms_japanese_ci"
	CharsetEUCKR             = "euckr"
	CollationEUCKRBin        = "euckr_bin"
	CollationEUCKRDefault    = "euckr_korean_ci"
	CharsetGB18030           = "gb18030"
	CollationGB18030Bin      = "gb18030_bin"
	CollationGB18030Default  = "gb18030_chinese_ci"
	CharsetGB2312            = "gb2312"
	CollationGB2312Bin       = "gb2312_bin"
	CollationGB2312Default   = "gb2312_chinese_ci"
	CharsetGEOSTD8           = "geostd8"
	CollationGEOSTD8Bin      = "geostd8_bin"
	CollationGEOSTD8Default  = "geostd8_general_ci"
	CharsetGreek             = "greek"
	CollationGreekBin        = "greek_bin"
	CollationGreekDefault    = "greek_general_ci"
	CharsetHebrew            = "hebrew"
	CollationHebrewBin       = "hebrew_bin"
	CollationHebrewDefault   = "hebrew_general_ci"
	CharsetHP8               = "hp8"
	CollationHP8Bin          = "hp8_bin"
	CollationHP8Default      = "hp8_english_ci"
	CharsetKEYBCS2           = "keybcs2"
	CollationKEYBCS2Bin      = "keybcs2_bin"
	CollationKEYBCS2Default  = "keybcs2_general_ci"
	CharsetKOI8R             = "koi8r"
	CollationKOI8RBin        = "koi8r_bin"
	CollationKOI8RDefault    = "koi8r_general_ci"
	CharsetKOI8U             = "koi8u"
	CollationKOI8UBin        = "koi8u_bin"
	CollationKOI8UDefault    = "koi8u_general_ci"
	CharsetLatin2            = "latin2"
	CollationLatin2Bin       = "latin2_bin"
	CollationLatin2Default   = "latin2_general_ci"
	CharsetLatin5            = "latin5"
	CollationLatin5Bin       = "latin5_bin"
	CollationLatin5Default   = "latin5_turkish_ci"
	CharsetLatin7            = "latin7"
	CollationLatin7Bin       = "latin7_bin"
	CollationLatin7Default   = "latin7_general_ci"
	CharsetMacCE             = "macce"
	CollationMacCEBin        = "macce_bin"
	CollationMacCEDefault    = "macce_general_ci"
	CharsetMacRoman          = "macroman"
	CollationMacRomanBin     = "macroman_bin"
	CollationMacRomanDefault = "macroman_general_ci"
	CharsetSJIS              = "sjis"
	CollationSJISBin         = "sjis_bin"
	CollationSJISDefault     = "sjis_japanese_ci"
	CharsetSWE7              = "swe7"
	CollationSWE7Bin         = "swe7_bin"
	CollationSWE7Default     = "swe7_swedish_ci"
	CharsetTIS620            = "tis620"
	CollationTIS620Bin       = "tis620_bin"
	CollationTIS620Default   = "tis620_thai_ci"
	CharsetUCS2              = "ucs2"
	CollationUCS2Bin         = "ucs2_bin"
	CollationUCS2Default     = "ucs2_general_ci"
	CharsetUJIS              = "ujis"
	CollationUJISBin         = "ujis_bin"
	CollationUJISDefault     = "ujis_japanese_ci"
	CharsetUTF16             = "utf16"
	CollationUTF16Bin        = "utf16_bin"
	CollationUTF16Default    = "utf16_general_ci"
	CharsetUTF16LE           = "utf16le"
	CollationUTF16LEBin      = "utf16le_bin"
	CollationUTF16LEDefault  = "utf16le_general_ci"
	CharsetUTF32             = "utf32"
	CollationUTF32Bin        = "utf32_bin"
	CollationUTF32Default    = "utf32_general_ci"
)

var collations = []*Collation{
	{1, "big5", "big5_chinese_ci", true},
	{2, "latin2", "latin2_czech_cs", false},
	{3, "dec8", "dec8_swedish_ci", true},
	{4, "cp850", "cp850_general_ci", true},
	{5, "latin1", "latin1_german1_ci", false},
	{6, "hp8", "hp8_english_ci", true},
	{7, "koi8r", "koi8r_general_ci", true},
	{8, "latin1", "latin1_swedish_ci", false},
	{9, "latin2", "latin2_general_ci", true},
	{10, "swe7", "swe7_swedish_ci", true},
	{11, "ascii", "ascii_general_ci", false},
	{12, "ujis", "ujis_japanese_ci", true},
	{13, "sjis", "sjis_japanese_ci", true},
	{14, "cp1251", "cp1251_bulgarian_ci", false},
	{15, "latin1", "latin1_danish_ci", false},
	{16, "hebrew", "hebrew_general_ci", true},
	{18, "tis620", "tis620_thai_ci", true},
	{19, "euckr", "euckr_korean_ci", true},
	{20, "latin7", "latin7_estonian_cs", false},
	{21, "latin2", "latin2_hungarian_ci", false},
	{22, "koi8u", "koi8u_general_ci", true},
	{23, "cp1251", "cp1251_ukrainian_ci", false},
	{24, "gb2312", "gb2312_chinese_ci", true},
	{25, "greek", "greek_general_ci", true},
	{26, "cp1250", "cp1250_general_ci", true},
	{27, "latin2", "latin2_croatian_ci", false},
	{28, "gbk", "gbk_chinese_ci", true},
	{29, "cp1257", "cp1257_lithuanian_ci", false},
	{30, "latin5", "latin5_turkish_ci", true},
	{31, "latin1", "latin1_german2_ci", false},
	{32, "armscii8", "armscii8_general_ci", true},
	{33, "utf8", "utf8_general_ci", false},
	{34, "cp1250", "cp1250_czech_cs", false},
	{35, "ucs2", "ucs2_general_ci", true},
	{36, "cp866", "cp866_general_ci", true},
	{37, "keybcs2", "keybcs2_general_ci", true},
	{38, "macce", "macce_general_ci", true},
	{39, "macroman", "macroman_general_ci", true},
	{40, "cp852", "cp852_general_ci", true},
	{41, "latin7", "latin7_general_ci", true},
	{42, "latin7", "latin7_general_cs", false},
	{43, "macce", "macce_bin", false},
	{44, "cp1250", "cp1250_croatian_ci", false},
	{45, "utf8mb4", "utf8mb4_general_ci", false},
	{46, "utf8mb4", "utf8mb4_bin", true},
	{47, "latin1", "latin1_bin", true},
	{48, "latin1", "latin1_general_ci", false},
	{49, "latin1", "latin1_general_cs", false},
	{50, "cp1251", "cp1251_bin", false},
	{51, "cp1251", "cp1251_general_ci", true},
	{52, "cp1251", "cp1251_general_cs", false},
	{53, "macroman", "macroman_bin", false},
	{54, "utf16", "utf16_general_ci", true},
	{55, "utf16", "utf16_bin", false},
	{56, "utf16le", "utf16le_general_ci", true},
	{57, "cp1256", "cp1256_general_ci", true},
	{58, "cp1257", "cp1257_bin", false},
	{59, "cp1257", "cp1257_general_ci", true},
	{60, "utf32", "utf32_general_ci", true},
	{61, "utf32", "utf32_bin", false},
	{62, "utf16le", "utf16le_bin", false},
	{63, "binary", "binary", true},
	{64, "armscii8", "armscii8_bin", false},
	{65, "ascii", "ascii_bin", true},
	{66, "cp1250", "cp1250_bin", false},
	{67, "cp1256", "cp1256_bin", false},
	{68, "cp866", "cp866_bin", false},
	{69, "dec8", "dec8_bin", false},
	{70, "greek", "greek_bin", false},
	{71, "hebrew", "hebrew_bin", false},
	{72, "hp8", "hp8_bin", false},
	{73, "keybcs2", "keybcs2_bin", false},
	{74, "koi8r", "koi8r_bin", false},
	{75, "koi8u", "koi8u_bin", false},
	{77, "latin2", "latin2_bin", false},
	{78, "latin5", "latin5_bin", false},
	{79, "latin7", "latin7_bin", false},
	{80, "cp850", "cp850_bin", false},
	{81, "cp852", "cp852_bin", false},
	{82, "swe7", "swe7_bin", false},
	{83, "utf8", "utf8_bin", true},
	{84, "big5", "big5_bin", false},
	{85, "euckr", "euckr_bin", false},
	{86, "gb2312", "gb2312_bin", false},
	{87, "gbk", "gbk_bin", false},
	{88, "sjis", "sjis_bin", false},
	{89, "tis620", "tis620_bin", false},
	{90, "ucs2", "ucs2_bin", false},
	{91, "ujis", "ujis_bin", false},
	{92, "geostd8", "geostd8_general_ci", true},
	{93, "geostd8", "geostd8_bin", false},
	{94, "latin1", "latin1_spanish_ci", false},
	{95, "cp932", "cp932_japanese_ci", true},
	{96, "cp932", "cp932_bin", false},
	{97, "eucjpms", "eucjpms_japanese_ci", true},
	{98, "eucjpms", "eucjpms_bin", false},
	{99, "cp1250", "cp1250_polish_ci", false},
	{101, "utf16", "utf16_unicode_ci", false},
	{102, "utf16", "utf16_icelandic_ci", false},
	{103, "utf16", "utf16_latvian_ci", false},
	{104, "utf16", "utf16_romanian_ci", false},
	{105, "utf16", "utf16_slovenian_ci", false},
	{106, "utf16", "utf16_polish_ci", false},
	{107, "utf16", "utf16_estonian_ci", false},
	{108, "utf16", "utf16_spanish_ci", false},
	{109, "utf16", "utf16_swedish_ci", false},
	{110, "utf16", "utf16_turkish_ci", false},
	{111, "utf16", "utf16_czech_ci", false},
	{112, "utf16", "utf16_danish_ci", false},
	{113, "utf16", "utf16_lithuanian_ci", false},
	{114, "utf16", "utf16_slovak_ci", false},
	{115, "utf16", "utf16_spanish2_ci", false},
	{116, "utf16", "utf16_roman_ci", false},
	{117, "utf16", "utf16_persian_ci", false},
	{118, "utf16", "utf16_esperanto_ci", false},
	{119, "utf16", "utf16_hungarian_ci", false},
	{120, "utf16", "utf16_sinhala_ci", false},
	{121, "utf16", "utf16_german2_ci", false},
	{122, "utf16", "utf16_croatian_ci", false},
	{123, "utf16", "utf16_unicode_520_ci", false},
	{124, "utf16", "utf16_vietnamese_ci", false},
	{128, "ucs2", "ucs2_unicode_ci", false},
	{129, "ucs2", "ucs2_icelandic_ci", false},
	{130, "ucs2", "ucs2_latvian_ci", false},
	{131, "ucs2", "ucs2_romanian_ci", false},
	{132, "ucs2", "ucs2_slovenian_ci", false},
	{133, "ucs2", "ucs2_polish_ci", false},
	{134, "ucs2", "ucs2_estonian_ci", false},
	{135, "ucs2", "ucs2_spanish_ci", false},
	{136, "ucs2", "ucs2_swedish_ci", false},
	{137, "ucs2", "ucs2_turkish_ci", false},
	{138, "ucs2", "ucs2_czech_ci", false},
	{139, "ucs2", "ucs2_danish_ci", false},
	{140, "ucs2", "ucs2_lithuanian_ci", false},
	{141, "ucs2", "ucs2_slovak_ci", false},
	{142, "ucs2", "ucs2_spanish2_ci", false},
	{143, "ucs2", "ucs2_roman_ci", false},
	{144, "ucs2", "ucs2_persian_ci", false},
	{145, "ucs2", "ucs2_esperanto_ci", false},
	{146, "ucs2", "ucs2_hungarian_ci", false},
	{147, "ucs2", "ucs2_sinhala_ci", false},
	{148, "ucs2", "ucs2_german2_ci", false},
	{149, "ucs2", "ucs2_croatian_ci", false},
	{150, "ucs2", "ucs2_unicode_520_ci", false},
	{151, "ucs2", "ucs2_vietnamese_ci", false},
	{159, "ucs2", "ucs2_general_mysql500_ci", false},
	{160, "utf32", "utf32_unicode_ci", false},
	{161, "utf32", "utf32_icelandic_ci", false},
	{162, "utf32", "utf32_latvian_ci", false},
	{163, "utf32", "utf32_romanian_ci", false},
	{164, "utf32", "utf32_slovenian_ci", false},
	{165, "utf32", "utf32_polish_ci", false},
	{166, "utf32", "utf32_estonian_ci", false},
	{167, "utf32", "utf32_spanish_ci", false},
	{168, "utf32", "utf32_swedish_ci", false},
	{169, "utf32", "utf32_turkish_ci", false},
	{170, "utf32", "utf32_czech_ci", false},
	{171, "utf32", "utf32_danish_ci", false},
	{172, "utf32", "utf32_lithuanian_ci", false},
	{173, "utf32", "utf32_slovak_ci", false},
	{174, "utf32", "utf32_spanish2_ci", false},
	{175, "utf32", "utf32_roman_ci", false},
	{176, "utf32", "utf32_persian_ci", false},
	{177, "utf32", "utf32_esperanto_ci", false},
	{178, "utf32", "utf32_hungarian_ci", false},
	{179, "utf32", "utf32_sinhala_ci", false},
	{180, "utf32", "utf32_german2_ci", false},
	{181, "utf32", "utf32_croatian_ci", false},
	{182, "utf32", "utf32_unicode_520_ci", false},
	{183, "utf32", "utf32_vietnamese_ci", false},
	{192, "utf8", "utf8_unicode_ci", false},
	{193, "utf8", "utf8_icelandic_ci", false},
	{194, "utf8", "utf8_latvian_ci", false},
	{195, "utf8", "utf8_romanian_ci", false},
	{196, "utf8", "utf8_slovenian_ci", false},
	{197, "utf8", "utf8_polish_ci", false},
	{198, "utf8", "utf8_estonian_ci", false},
	{199, "utf8", "utf8_spanish_ci", false},
	{200, "utf8", "utf8_swedish_ci", false},
	{201, "utf8", "utf8_turkish_ci", false},
	{202, "utf8", "utf8_czech_ci", false},
	{203, "utf8", "utf8_danish_ci", false},
	{204, "utf8", "utf8_lithuanian_ci", false},
	{205, "utf8", "utf8_slovak_ci", false},
	{206, "utf8", "utf8_spanish2_ci", false},
	{207, "utf8", "utf8_roman_ci", false},
	{208, "utf8", "utf8_persian_ci", false},
	{209, "utf8", "utf8_esperanto_ci", false},
	{210, "utf8", "utf8_hungarian_ci", false},
	{211, "utf8", "utf8_sinhala_ci", false},
	{212, "utf8", "utf8_german2_ci", false},
	{213, "utf8", "utf8_croatian_ci", false},
	{214, "utf8", "utf8_unicode_520_ci", false},
	{215, "utf8", "utf8_vietnamese_ci", false},
	{223, "utf8", "utf8_general_mysql500_ci", false},
	{224, "utf8mb4", "utf8mb4_unicode_ci", false},
	{225, "utf8mb4", "utf8mb4_icelandic_ci", false},
	{226, "utf8mb4", "utf8mb4_latvian_ci", false},
	{227, "utf8mb4", "utf8mb4_romanian_ci", false},
	{228, "utf8mb4", "utf8mb4_slovenian_ci", false},
	{229, "utf8mb4", "utf8mb4_polish_ci", false},
	{230, "utf8mb4", "utf8mb4_estonian_ci", false},
	{231, "utf8mb4", "utf8mb4_spanish_ci", false},
	{232, "utf8mb4", "utf8mb4_swedish_ci", false},
	{233, "utf8mb4", "utf8mb4_turkish_ci", false},
	{234, "utf8mb4", "utf8mb4_czech_ci", false},
	{235, "utf8mb4", "utf8mb4_danish_ci", false},
	{236, "utf8mb4", "utf8mb4_lithuanian_ci", false},
	{237, "utf8mb4", "utf8mb4_slovak_ci", false},
	{238, "utf8mb4", "utf8mb4_spanish2_ci", false},
	{239, "utf8mb4", "utf8mb4_roman_ci", false},
	{240, "utf8mb4", "utf8mb4_persian_ci", false},
	{241, "utf8mb4", "utf8mb4_esperanto_ci", false},
	{242, "utf8mb4", "utf8mb4_hungarian_ci", false},
	{243, "utf8mb4", "utf8mb4_sinhala_ci", false},
	{244, "utf8mb4", "utf8mb4_german2_ci", false},
	{245, "utf8mb4", "utf8mb4_croatian_ci", false},
	{246, "utf8mb4", "utf8mb4_unicode_520_ci", false},
	{247, "utf8mb4", "utf8mb4_vietnamese_ci", false},
	{248, "gb18030", "gb18030_chinese_ci", true},
	{249, "gb18030", "gb18030_bin", false},
	{255, "utf8mb4", "utf8mb4_0900_ai_ci", false},
	{2047, "utf8mb3", "utf8mb3_bin", true},
	{2048, "utf8mb4", "utf8mb4_zh_pinyin_tidb_as_cs", false},
}

// AddCharset adds a new charset.
// Use only when adding a custom charset to the parser.
func AddCharset(c *Charset) {
	charsetInfos[c.Name] = c
}

// RemoveCharset remove a charset.
// Use only when adding a custom charset to the parser.
func RemoveCharset(c string) {
	delete(charsetInfos, c)
}

// AddCollation adds a new collation.
// Use only when adding a custom collation to the parser.
func AddCollation(c *Collation) {
	collationsIDMap[c.ID] = c
	collationsNameMap[c.Name] = c

	if _, ok := supportedCollationNames[c.Name]; ok {
		supportedCollations = append(supportedCollations, c)
	}

	if charset, ok := charsetInfos[c.CharsetName]; ok {
		charset.Collations[c.Name] = c
	}
}

// init method always puts to the end of file.
func init() {
	for _, c := range collations {
		AddCollation(c)
	}
}
