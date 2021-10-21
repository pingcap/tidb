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
// See the License for the specific language governing permissions and
// limitations under the License.

package charset

import (
	"strings"
	"unicode"
)

func (e *Encoding) ToUpper(d string) string {
	return strings.ToUpperSpecial(e.specialCase, d)
}

func (e *Encoding) ToLower(d string) string {
	return strings.ToLowerSpecial(e.specialCase, d)
}

func LookupSpecialCase(label string) unicode.SpecialCase {
	label = strings.ToLower(strings.Trim(label, "\t\n\r\f "))
	return specailCases[label].c
}

var specailCases = map[string]struct {
	c unicode.SpecialCase
}{
	"utf-8":          {nil},
	"ibm866":         {nil},
	"iso-8859-2":     {nil},
	"iso-8859-3":     {nil},
	"iso-8859-4":     {nil},
	"iso-8859-5":     {nil},
	"iso-8859-6":     {nil},
	"iso-8859-7":     {nil},
	"iso-8859-8":     {nil},
	"iso-8859-8-i":   {nil},
	"iso-8859-10":    {nil},
	"iso-8859-13":    {nil},
	"iso-8859-14":    {nil},
	"iso-8859-15":    {nil},
	"iso-8859-16":    {nil},
	"koi8-r":         {nil},
	"macintosh":      {nil},
	"windows-874":    {nil},
	"windows-1250":   {nil},
	"windows-1251":   {nil},
	"windows-1252":   {nil},
	"windows-1253":   {nil},
	"windows-1254":   {nil},
	"windows-1255":   {nil},
	"windows-1256":   {nil},
	"windows-1257":   {nil},
	"windows-1258":   {nil},
	"x-mac-cyrillic": {nil},
	"gbk":            {GBKCase},
	"gb18030":        {nil},
	"hz-gb-2312":     {nil},
	"big5":           {nil},
	"euc-jp":         {nil},
	"iso-2022-jp":    {nil},
	"shift_jis":      {nil},
	"euc-kr":         {nil},
	"replacement":    {nil},
	"utf-16be":       {nil},
	"utf-16le":       {nil},
	"x-user-defined": {nil},
}

// follow https://dev.mysql.com/worklog/task/?id=4583 for GBK
var GBKCase = unicode.SpecialCase{
	unicode.CaseRange{0x00E0, 0x00E1, [unicode.MaxCase]rune{0, 0, 0}},
	unicode.CaseRange{0x00E8, 0x00EA, [unicode.MaxCase]rune{0, 0, 0}},
	unicode.CaseRange{0x00EC, 0x00ED, [unicode.MaxCase]rune{0, 0, 0}},
	unicode.CaseRange{0x00F2, 0x00F3, [unicode.MaxCase]rune{0, 0, 0}},
	unicode.CaseRange{0x00F9, 0x00FA, [unicode.MaxCase]rune{0, 0, 0}},
	unicode.CaseRange{0x00FC, 0x00FC, [unicode.MaxCase]rune{0, 0, 0}},
	unicode.CaseRange{0x0101, 0x0101, [unicode.MaxCase]rune{0, 0, 0}},
	unicode.CaseRange{0x0113, 0x0113, [unicode.MaxCase]rune{0, 0, 0}},
	unicode.CaseRange{0x011B, 0x011B, [unicode.MaxCase]rune{0, 0, 0}},
	unicode.CaseRange{0x012B, 0x012B, [unicode.MaxCase]rune{0, 0, 0}},
	unicode.CaseRange{0x0144, 0x0144, [unicode.MaxCase]rune{0, 0, 0}},
	unicode.CaseRange{0x0148, 0x0148, [unicode.MaxCase]rune{0, 0, 0}},
	unicode.CaseRange{0x014D, 0x014D, [unicode.MaxCase]rune{0, 0, 0}},
	unicode.CaseRange{0x016B, 0x016B, [unicode.MaxCase]rune{0, 0, 0}},
	unicode.CaseRange{0x01CE, 0x01CE, [unicode.MaxCase]rune{0, 0, 0}},
	unicode.CaseRange{0x01D0, 0x01D0, [unicode.MaxCase]rune{0, 0, 0}},
	unicode.CaseRange{0x01D2, 0x01D2, [unicode.MaxCase]rune{0, 0, 0}},
	unicode.CaseRange{0x01D4, 0x01D4, [unicode.MaxCase]rune{0, 0, 0}},
	unicode.CaseRange{0x01D6, 0x01D6, [unicode.MaxCase]rune{0, 0, 0}},
	unicode.CaseRange{0x01D8, 0x01D8, [unicode.MaxCase]rune{0, 0, 0}},
	unicode.CaseRange{0x01DA, 0x01DA, [unicode.MaxCase]rune{0, 0, 0}},
	unicode.CaseRange{0x01DC, 0x01DC, [unicode.MaxCase]rune{0, 0, 0}},
	unicode.CaseRange{0x216A, 0x216B, [unicode.MaxCase]rune{0, 0, 0}},
}
