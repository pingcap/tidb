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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//go:build icugen

// Command icugen extracts collation element tables from ICU4C and writes them as pure-Go source
// into package icudata. It is the build-time-ICU step: ICU is needed only here, never in the
// shipped binary. It is guarded by the `icugen` build tag so normal builds never compile it.
//
// Regenerate (needs ICU4C; paths below assume Homebrew icu4c@78 on macOS):
//
//	go run -tags icugen ./pkg/util/collate/icudata/icugen
//
// On other platforms, adjust the #cgo paths to the local ICU install.
package main

/*
#cgo CFLAGS: -I/opt/homebrew/opt/icu4c@78/include
#cgo LDFLAGS: -L/opt/homebrew/opt/icu4c@78/lib -Wl,-rpath,/opt/homebrew/opt/icu4c@78/lib -licui18n -licuuc -licudata
#include <stdlib.h>
#include <unicode/ucol.h>
#include <unicode/ucoleitr.h>
#include <unicode/uset.h>
#include <unicode/ustring.h>

static int ce_of(const char* locale, const char* utf8, int32_t* out, int cap) {
    UErrorCode s = U_ZERO_ERROR;
    UCollator* c = ucol_open(locale, &s);
    if (U_FAILURE(s)) return -1;
    UChar buf[512]; int32_t len = 0; UErrorCode s2 = U_ZERO_ERROR;
    u_strFromUTF8(buf, 512, &len, utf8, -1, &s2);
    if (U_FAILURE(s2)) { ucol_close(c); return -1; }
    UCollationElements* it = ucol_openElements(c, buf, len, &s);
    if (U_FAILURE(s)) { ucol_close(c); return -1; }
    int n = 0;
    for (;;) {
        UErrorCode se = U_ZERO_ERROR;
        int32_t ce = ucol_next(it, &se);
        if (ce == UCOL_NULLORDER || U_FAILURE(se)) break;
        if (n < cap) out[n] = ce;
        n++;
    }
    ucol_closeElements(it);
    ucol_close(c);
    return n;
}

static const char* icu_version() { return U_ICU_VERSION; }

static int contractions_of(const char* locale, char* out, int cap) {
    UErrorCode s = U_ZERO_ERROR;
    UCollator* c = ucol_open(locale, &s);
    if (U_FAILURE(s)) return -1;
    USet* contr = uset_openEmpty();
    USet* exp = uset_openEmpty();
    ucol_getContractionsAndExpansions(c, contr, exp, 0, &s);
    int cnt = uset_getItemCount(contr);
    int pos = 0;
    for (int i = 0; i < cnt; i++) {
        UChar ustr[64]; UChar32 start, end; UErrorCode se = U_ZERO_ERROR;
        int32_t slen = uset_getItem(contr, i, &start, &end, ustr, 64, &se);
        if (slen > 0) {
            char u8[256]; int32_t u8len = 0; UErrorCode s2 = U_ZERO_ERROR;
            u_strToUTF8(u8, 256, &u8len, ustr, slen, &s2);
            if (U_SUCCESS(s2) && pos + u8len + 1 < cap) {
                for (int k = 0; k < u8len; k++) out[pos++] = u8[k];
                out[pos++] = '\n';
            }
        }
    }
    uset_close(contr); uset_close(exp); ucol_close(c);
    out[pos] = 0;
    return pos;
}
*/
import "C"

import (
	"bytes"
	"fmt"
	"go/format"
	"os"
	"sort"
	"strings"
	"unsafe"
)

const bmpEnd = 0x10000

// cesOf returns the packed collation elements of s under locale (empty locale = ICU root).
func cesOf(locale, s string) []uint64 {
	cl := C.CString(locale)
	cs := C.CString(s)
	defer C.free(unsafe.Pointer(cl))
	defer C.free(unsafe.Pointer(cs))
	const cap = 1024
	buf := make([]C.int32_t, cap)
	n := int(C.ce_of(cl, cs, &buf[0], C.int(cap)))
	if n < 0 {
		panic("ICU ce_of failed for locale " + locale)
	}
	out := make([]uint64, 0, n)
	for i := 0; i < n; i++ {
		o := uint32(buf[i])
		p := (o >> 16) & 0xFFFF
		sec := (o >> 8) & 0xFF
		ter := o & 0xFF
		out = append(out, uint64(p)<<32|uint64(sec)<<16|uint64(ter))
	}
	return out
}

func contractionsOf(locale string) []string {
	cl := C.CString(locale)
	defer C.free(unsafe.Pointer(cl))
	const cap = 1 << 16
	buf := make([]C.char, cap)
	C.contractions_of(cl, &buf[0], C.int(cap))
	raw := C.GoString(&buf[0])
	if raw == "" {
		return nil
	}
	return strings.Split(strings.TrimRight(raw, "\n"), "\n")
}

func isSurrogate(cp int) bool { return cp >= 0xD800 && cp <= 0xDFFF }

func equalCEs(a, b []uint64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

type locale struct{ loc, varName string }

func main() {
	dir := "pkg/util/collate/icudata"

	// 1. Root table over the BMP.
	root := make([][]uint64, bmpEnd)
	for cp := 0; cp < bmpEnd; cp++ {
		if isSurrogate(cp) {
			continue
		}
		root[cp] = cesOf("", string(rune(cp)))
	}
	writeRoot(dir, root)

	// 2. Per-locale deltas + contractions.
	for _, l := range []locale{
		{"da", "Danish"}, {"de", "German"}, {"es", "Spanish"}, {"fr", "French"}, {"sv", "Swedish"},
	} {
		writeLocale(dir, l, root)
	}

	// 3. Version stamp (the collversion hazard: index keys depend on this).
	writeVersion(dir, C.GoString(C.icu_version()))
	fmt.Println("icugen: done")
}

func writeVersion(dir, icuVersion string) {
	var b bytes.Buffer
	b.WriteString(header)
	b.WriteString("package icudata\n\n")
	b.WriteString("// ICUVersion is the ICU library version these tables were extracted from. Index keys on\n")
	b.WriteString("// ICU-derived collations are byte sequences derived from this data; if regenerating from a\n")
	b.WriteString("// different ICU version changes any weights, existing indexes must be rebuilt. See the package\n")
	b.WriteString("// doc for the upgrade/REINDEX policy.\n")
	fmt.Fprintf(&b, "const ICUVersion = %q\n", icuVersion)
	format1(dir+"/version_generated.go", b.Bytes())
}

func writeRoot(dir string, root [][]uint64) {
	var ce []uint64
	off := make([]uint32, bmpEnd+1)
	for cp := 0; cp < bmpEnd; cp++ {
		off[cp] = uint32(len(ce))
		ce = append(ce, root[cp]...)
	}
	off[bmpEnd] = uint32(len(ce))

	var b bytes.Buffer
	b.WriteString(header)
	b.WriteString("package icudata\n\n")
	b.WriteString("// Root is the ICU root (locale-neutral) collation element table over the BMP.\n")
	b.WriteString("var Root = RootTable{\n\tCEData: []uint64{\n")
	writeU64s(&b, ce)
	b.WriteString("\t},\n\tOffset: []uint32{\n")
	writeU32s(&b, off)
	b.WriteString("\t},\n}\n")
	format1(dir+"/root_generated.go", b.Bytes())
}

func writeLocale(dir string, l locale, root [][]uint64) {
	override := map[int][]uint64{}
	for cp := 0; cp < bmpEnd; cp++ {
		if isSurrogate(cp) {
			continue
		}
		ces := cesOf(l.loc, string(rune(cp)))
		if !equalCEs(ces, root[cp]) {
			override[cp] = ces
		}
	}
	contr := map[string][]uint64{}
	maxRunes := 0
	for _, s := range contractionsOf(l.loc) {
		contr[s] = cesOf(l.loc, s)
		if n := len([]rune(s)); n > maxRunes {
			maxRunes = n
		}
	}

	var b bytes.Buffer
	b.WriteString(header)
	b.WriteString("package icudata\n\n")
	fmt.Fprintf(&b, "// %s is the ICU tailoring for locale %q (delta over Root).\n", l.varName, l.loc)
	fmt.Fprintf(&b, "var %s = TailoringData{\n", l.varName)
	fmt.Fprintf(&b, "\tMaxContractionRunes: %d,\n", maxRunes)
	b.WriteString("\tOverride: map[rune][]uint64{\n")
	cps := make([]int, 0, len(override))
	for cp := range override {
		cps = append(cps, cp)
	}
	sort.Ints(cps)
	for _, cp := range cps {
		fmt.Fprintf(&b, "\t\t0x%X: {%s},\n", cp, joinU64(override[cp]))
	}
	b.WriteString("\t},\n\tContraction: map[string][]uint64{\n")
	keys := make([]string, 0, len(contr))
	for k := range contr {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		fmt.Fprintf(&b, "\t\t%q: {%s},\n", k, joinU64(contr[k]))
	}
	b.WriteString("\t},\n}\n")
	format1(fmt.Sprintf("%s/%s_generated.go", dir, strings.ToLower(l.varName)), b.Bytes())
}

func writeU64s(b *bytes.Buffer, vs []uint64) {
	for i, v := range vs {
		if i%12 == 0 {
			b.WriteString("\t\t")
		}
		fmt.Fprintf(b, "0x%X, ", v)
		if i%12 == 11 {
			b.WriteString("\n")
		}
	}
	b.WriteString("\n")
}

func writeU32s(b *bytes.Buffer, vs []uint32) {
	for i, v := range vs {
		if i%16 == 0 {
			b.WriteString("\t\t")
		}
		fmt.Fprintf(b, "0x%X, ", v)
		if i%16 == 15 {
			b.WriteString("\n")
		}
	}
	b.WriteString("\n")
}

func joinU64(vs []uint64) string {
	parts := make([]string, len(vs))
	for i, v := range vs {
		parts[i] = fmt.Sprintf("0x%X", v)
	}
	return strings.Join(parts, ", ")
}

func format1(path string, src []byte) {
	out, err := format.Source(src)
	if err != nil {
		// Write unformatted to aid debugging, then fail.
		_ = os.WriteFile(path+".broken", src, 0644)
		panic(fmt.Sprintf("format %s: %v", path, err))
	}
	if err := os.WriteFile(path, out, 0644); err != nil {
		panic(err)
	}
	fmt.Println("wrote", path)
}

const header = `// Copyright 2026 PingCAP, Inc.
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

// Code generated by "util/collate/icudata/icugen"; DO NOT EDIT.

`
