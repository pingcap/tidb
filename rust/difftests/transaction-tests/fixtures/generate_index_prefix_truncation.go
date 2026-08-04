// Copyright 2026 PingCAP, Inc.
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

//go:build ignore

// This generator calls tablecodec.TruncateIndexValue itself -- the function
// that decides the PREFIX INDEX KEY a row is written under -- and prints, per
// case, the datum KIND and the exact bytes Go left behind. Its stdout is the
// reviewed fixture stored in index_prefix_truncation.tsv.
//
// The cases concentrate on invalid UTF-8, because that is where Go's rune
// counting (utf8.RuneCount / bytes.Runes, ONE replacement per invalid BYTE)
// parts company with a from_utf8_lossy-style count (one replacement per
// maximal invalid SUBSEQUENCE). A prefix key built on the wrong count is a
// key an index lookup never rebuilds.
package main

import (
	"encoding/hex"
	"fmt"

	"github.com/pingcap/tidb/pkg/meta/model"
	pmodel "github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/types"
)

func col(cs, coll string) *model.ColumnInfo {
	c := &model.ColumnInfo{Name: pmodel.NewCIStr("c")}
	c.FieldType = *types.NewFieldType(mysql.TypeVarchar)
	c.SetCharset(cs)
	c.SetCollate(coll)
	return c
}

func kindName(k byte) string {
	switch k {
	case types.KindString:
		return "String"
	case types.KindBytes:
		return "Bytes"
	case types.KindInt64:
		return "Int"
	default:
		return fmt.Sprintf("Kind%d", k)
	}
}

func run(label string, d types.Datum, length int, cs, coll string) {
	idxCol := &model.IndexColumn{Length: length}
	tablecodec.TruncateIndexValue(&d, idxCol, col(cs, coll))
	fmt.Printf("%s\t%s\t%s\n", label, kindName(d.Kind()), hex.EncodeToString(d.GetBytes()))
}

func main() {
	cases := []struct {
		name string
		raw  []byte
	}{
		{"ascii_abcdef", []byte("abcdef")},
		{"utf8_3char", []byte("\xe4\xb8\xad\xe6\x96\x87\xe5\xad\x97")},
		{"trunc_emoji_head", []byte{0xF0, 0x9F}},
		{"trunc_emoji_pad", []byte{0xF0, 0x9F, 0x92, 'a', 'b'}},
		{"ff_run", []byte{0xFF, 0xFF, 0xFF, 0xFF}},
		{"mixed", []byte{'a', 0xC3, 0x28, 'b', 0xE2, 0x82, 'c'}},
		{"overlong", []byte{0xC0, 0xAF, 0xC0, 0xAF}},
		{"surrogate", []byte{0xED, 0xA0, 0x80, 'x'}},
		{"emoji_pair", []byte("\xf0\x9f\x98\x80\xf0\x9f\x98\x81")},
		{"empty", []byte{}},
		{"nul_embedded", []byte{0x00, 0x41, 0x00, 0x42}},
		{"max_rune", []byte{0xF4, 0x8F, 0xBF, 0xBF, 0x41}},
		{"above_max_rune", []byte{0xF5, 0x80, 0x80, 0x80, 0x41}},
	}
	sets := []struct{ tag, cs, coll string }{
		{"utf8mb4", charset.CharsetUTF8MB4, "utf8mb4_bin"},
		{"bin", charset.CharsetBin, charset.CollationBin},
		{"ascii", charset.CharsetASCII, "ascii_bin"},
	}
	for _, c := range cases {
		for _, s := range sets {
			for _, l := range []int{0, 1, 2, 3, 4, 8} {
				run(fmt.Sprintf("string/%s/%s/%d", c.name, s.tag, l),
					types.NewCollationStringDatum(string(c.raw), "utf8mb4_bin"), l, s.cs, s.coll)
				run(fmt.Sprintf("bytes/%s/%s/%d", c.name, s.tag, l),
					types.NewBytesDatum(append([]byte(nil), c.raw...)), l, s.cs, s.coll)
			}
		}
	}
	// A non-string, non-bytes kind is never a truncation candidate.
	run("int/seven/utf8mb4/1", types.NewIntDatum(7), 1, charset.CharsetUTF8MB4, "utf8mb4_bin")
}
