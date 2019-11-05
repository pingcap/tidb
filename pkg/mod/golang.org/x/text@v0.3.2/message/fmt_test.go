// Copyright 2017 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package message

import (
	"bytes"
	"fmt"
	"io"
	"math"
	"reflect"
	"runtime"
	"strings"
	"testing"
	"time"

	"golang.org/x/text/language"
)

type (
	renamedBool       bool
	renamedInt        int
	renamedInt8       int8
	renamedInt16      int16
	renamedInt32      int32
	renamedInt64      int64
	renamedUint       uint
	renamedUint8      uint8
	renamedUint16     uint16
	renamedUint32     uint32
	renamedUint64     uint64
	renamedUintptr    uintptr
	renamedString     string
	renamedBytes      []byte
	renamedFloat32    float32
	renamedFloat64    float64
	renamedComplex64  complex64
	renamedComplex128 complex128
)

func TestFmtInterface(t *testing.T) {
	p := NewPrinter(language.Und)
	var i1 interface{}
	i1 = "abc"
	s := p.Sprintf("%s", i1)
	if s != "abc" {
		t.Errorf(`Sprintf("%%s", empty("abc")) = %q want %q`, s, "abc")
	}
}

var (
	NaN    = math.NaN()
	posInf = math.Inf(1)
	negInf = math.Inf(-1)

	intVar = 0

	array  = [5]int{1, 2, 3, 4, 5}
	iarray = [4]interface{}{1, "hello", 2.5, nil}
	slice  = array[:]
	islice = iarray[:]
)

type A struct {
	i int
	j uint
	s string
	x []int
}

type I int

func (i I) String() string {
	p := NewPrinter(language.Und)
	return p.Sprintf("<%d>", int(i))
}

type B struct {
	I I
	j int
}

type C struct {
	i int
	B
}

type F int

func (f F) Format(s fmt.State, c rune) {
	p := NewPrinter(language.Und)
	p.Fprintf(s, "<%c=F(%d)>", c, int(f))
}

type G int

func (g G) GoString() string {
	p := NewPrinter(language.Und)
	return p.Sprintf("GoString(%d)", int(g))
}

type S struct {
	F F // a struct field that Formats
	G G // a struct field that GoStrings
}

type SI struct {
	I interface{}
}

// P is a type with a String method with pointer receiver for testing %p.
type P int

var pValue P

func (p *P) String() string {
	return "String(p)"
}

var barray = [5]renamedUint8{1, 2, 3, 4, 5}
var bslice = barray[:]

type byteStringer byte

func (byteStringer) String() string {
	return "X"
}

var byteStringerSlice = []byteStringer{'h', 'e', 'l', 'l', 'o'}

type byteFormatter byte

func (byteFormatter) Format(f fmt.State, _ rune) {
	p := NewPrinter(language.Und)
	p.Fprint(f, "X")
}

var byteFormatterSlice = []byteFormatter{'h', 'e', 'l', 'l', 'o'}

var fmtTests = []struct {
	fmt string
	val interface{}
	out string
}{
	// The behavior of the following tests differs from that of the fmt package.

	// Unlike with the fmt package, it is okay to have extra arguments for
	// strings without format parameters. This is because it is impossible to
	// distinguish between reordered or ordered format strings in this case.
	// (For reordered format strings it is okay to not use arguments.)
	{"", nil, ""},
	{"", 2, ""},
	{"no args", "hello", "no args"},

	{"%017091901790959340919092959340919017929593813360", 0, "%!(NOVERB)"},
	{"%184467440737095516170v", 0, "%!(NOVERB)"},
	// Extra argument errors should format without flags set.
	{"%010.2", "12345", "%!(NOVERB)"},

	// Some key other differences, asides from localized values:
	// - NaN values should not use affixes; so no signs (CLDR requirement)
	// - Infinity uses patterns, so signs may be different (CLDR requirement)
	// - The # flag is used to disable localization.

	// All following tests are analogous to those of the fmt package, but with
	// localized numbers when appropriate.
	{"%d", 12345, "12,345"},
	{"%v", 12345, "12,345"},
	{"%t", true, "true"},

	// basic string
	{"%s", "abc", "abc"},
	{"%q", "abc", `"abc"`},
	{"%x", "abc", "616263"},
	{"%x", "\xff\xf0\x0f\xff", "fff00fff"},
	{"%X", "\xff\xf0\x0f\xff", "FFF00FFF"},
	{"%x", "", ""},
	{"% x", "", ""},
	{"%#x", "", ""},
	{"%# x", "", ""},
	{"%x", "xyz", "78797a"},
	{"%X", "xyz", "78797A"},
	{"% x", "xyz", "78 79 7a"},
	{"% X", "xyz", "78 79 7A"},
	{"%#x", "xyz", "0x78797a"},
	{"%#X", "xyz", "0X78797A"},
	{"%# x", "xyz", "0x78 0x79 0x7a"},
	{"%# X", "xyz", "0X78 0X79 0X7A"},

	// basic bytes
	{"%s", []byte("abc"), "abc"},
	{"%s", [3]byte{'a', 'b', 'c'}, "abc"},
	{"%s", &[3]byte{'a', 'b', 'c'}, "&abc"},
	{"%q", []byte("abc"), `"abc"`},
	{"%x", []byte("abc"), "616263"},
	{"%x", []byte("\xff\xf0\x0f\xff"), "fff00fff"},
	{"%X", []byte("\xff\xf0\x0f\xff"), "FFF00FFF"},
	{"%x", []byte(""), ""},
	{"% x", []byte(""), ""},
	{"%#x", []byte(""), ""},
	{"%# x", []byte(""), ""},
	{"%x", []byte("xyz"), "78797a"},
	{"%X", []byte("xyz"), "78797A"},
	{"% x", []byte("xyz"), "78 79 7a"},
	{"% X", []byte("xyz"), "78 79 7A"},
	{"%#x", []byte("xyz"), "0x78797a"},
	{"%#X", []byte("xyz"), "0X78797A"},
	{"%# x", []byte("xyz"), "0x78 0x79 0x7a"},
	{"%# X", []byte("xyz"), "0X78 0X79 0X7A"},

	// escaped strings
	{"%q", "", `""`},
	{"%#q", "", "``"},
	{"%q", "\"", `"\""`},
	{"%#q", "\"", "`\"`"},
	{"%q", "`", `"` + "`" + `"`},
	{"%#q", "`", `"` + "`" + `"`},
	{"%q", "\n", `"\n"`},
	{"%#q", "\n", `"\n"`},
	{"%q", `\n`, `"\\n"`},
	{"%#q", `\n`, "`\\n`"},
	{"%q", "abc", `"abc"`},
	{"%#q", "abc", "`abc`"},
	{"%q", "日本語", `"日本語"`},
	{"%+q", "日本語", `"\u65e5\u672c\u8a9e"`},
	{"%#q", "日本語", "`日本語`"},
	{"%#+q", "日本語", "`日本語`"},
	{"%q", "\a\b\f\n\r\t\v\"\\", `"\a\b\f\n\r\t\v\"\\"`},
	{"%+q", "\a\b\f\n\r\t\v\"\\", `"\a\b\f\n\r\t\v\"\\"`},
	{"%#q", "\a\b\f\n\r\t\v\"\\", `"\a\b\f\n\r\t\v\"\\"`},
	{"%#+q", "\a\b\f\n\r\t\v\"\\", `"\a\b\f\n\r\t\v\"\\"`},
	{"%q", "☺", `"☺"`},
	{"% q", "☺", `"☺"`}, // The space modifier should have no effect.
	{"%+q", "☺", `"\u263a"`},
	{"%#q", "☺", "`☺`"},
	{"%#+q", "☺", "`☺`"},
	{"%10q", "⌘", `       "⌘"`},
	{"%+10q", "⌘", `  "\u2318"`},
	{"%-10q", "⌘", `"⌘"       `},
	{"%+-10q", "⌘", `"\u2318"  `},
	{"%010q", "⌘", `0000000"⌘"`},
	{"%+010q", "⌘", `00"\u2318"`},
	{"%-010q", "⌘", `"⌘"       `}, // 0 has no effect when - is present.
	{"%+-010q", "⌘", `"\u2318"  `},
	{"%#8q", "\n", `    "\n"`},
	{"%#+8q", "\r", `    "\r"`},
	{"%#-8q", "\t", "`	`     "},
	{"%#+-8q", "\b", `"\b"    `},
	{"%q", "abc\xffdef", `"abc\xffdef"`},
	{"%+q", "abc\xffdef", `"abc\xffdef"`},
	{"%#q", "abc\xffdef", `"abc\xffdef"`},
	{"%#+q", "abc\xffdef", `"abc\xffdef"`},
	// Runes that are not printable.
	{"%q", "\U0010ffff", `"\U0010ffff"`},
	{"%+q", "\U0010ffff", `"\U0010ffff"`},
	{"%#q", "\U0010ffff", "`􏿿`"},
	{"%#+q", "\U0010ffff", "`􏿿`"},
	// Runes that are not valid.
	{"%q", string(0x110000), `"�"`},
	{"%+q", string(0x110000), `"\ufffd"`},
	{"%#q", string(0x110000), "`�`"},
	{"%#+q", string(0x110000), "`�`"},

	// characters
	{"%c", uint('x'), "x"},
	{"%c", 0xe4, "ä"},
	{"%c", 0x672c, "本"},
	{"%c", '日', "日"},
	{"%.0c", '⌘', "⌘"}, // Specifying precision should have no effect.
	{"%3c", '⌘', "  ⌘"},
	{"%-3c", '⌘', "⌘  "},
	// Runes that are not printable.
	{"%c", '\U00000e00', "\u0e00"},
	{"%c", '\U0010ffff', "\U0010ffff"},
	// Runes that are not valid.
	{"%c", -1, "�"},
	{"%c", 0xDC80, "�"},
	{"%c", rune(0x110000), "�"},
	{"%c", int64(0xFFFFFFFFF), "�"},
	{"%c", uint64(0xFFFFFFFFF), "�"},

	// escaped characters
	{"%q", uint(0), `'\x00'`},
	{"%+q", uint(0), `'\x00'`},
	{"%q", '"', `'"'`},
	{"%+q", '"', `'"'`},
	{"%q", '\'', `'\''`},
	{"%+q", '\'', `'\''`},
	{"%q", '`', "'`'"},
	{"%+q", '`', "'`'"},
	{"%q", 'x', `'x'`},
	{"%+q", 'x', `'x'`},
	{"%q", 'ÿ', `'ÿ'`},
	{"%+q", 'ÿ', `'\u00ff'`},
	{"%q", '\n', `'\n'`},
	{"%+q", '\n', `'\n'`},
	{"%q", '☺', `'☺'`},
	{"%+q", '☺', `'\u263a'`},
	{"% q", '☺', `'☺'`},  // The space modifier should have no effect.
	{"%.0q", '☺', `'☺'`}, // Specifying precision should have no effect.
	{"%10q", '⌘', `       '⌘'`},
	{"%+10q", '⌘', `  '\u2318'`},
	{"%-10q", '⌘', `'⌘'       `},
	{"%+-10q", '⌘', `'\u2318'  `},
	{"%010q", '⌘', `0000000'⌘'`},
	{"%+010q", '⌘', `00'\u2318'`},
	{"%-010q", '⌘', `'⌘'       `}, // 0 has no effect when - is present.
	{"%+-010q", '⌘', `'\u2318'  `},
	// Runes that are not printable.
	{"%q", '\U00000e00', `'\u0e00'`},
	{"%q", '\U0010ffff', `'\U0010ffff'`},
	// Runes that are not valid.
	{"%q", int32(-1), "%!q(int32=-1)"},
	{"%q", 0xDC80, `'�'`},
	{"%q", rune(0x110000), "%!q(int32=1,114,112)"},
	{"%q", int64(0xFFFFFFFFF), "%!q(int64=68,719,476,735)"},
	{"%q", uint64(0xFFFFFFFFF), "%!q(uint64=68,719,476,735)"},

	// width
	{"%5s", "abc", "  abc"},
	{"%2s", "\u263a", " ☺"},
	{"%-5s", "abc", "abc  "},
	{"%-8q", "abc", `"abc"   `},
	{"%05s", "abc", "00abc"},
	{"%08q", "abc", `000"abc"`},
	{"%5s", "abcdefghijklmnopqrstuvwxyz", "abcdefghijklmnopqrstuvwxyz"},
	{"%.5s", "abcdefghijklmnopqrstuvwxyz", "abcde"},
	{"%.0s", "日本語日本語", ""},
	{"%.5s", "日本語日本語", "日本語日本"},
	{"%.10s", "日本語日本語", "日本語日本語"},
	{"%.5s", []byte("日本語日本語"), "日本語日本"},
	{"%.5q", "abcdefghijklmnopqrstuvwxyz", `"abcde"`},
	{"%.5x", "abcdefghijklmnopqrstuvwxyz", "6162636465"},
	{"%.5q", []byte("abcdefghijklmnopqrstuvwxyz"), `"abcde"`},
	{"%.5x", []byte("abcdefghijklmnopqrstuvwxyz"), "6162636465"},
	{"%.3q", "日本語日本語", `"日本語"`},
	{"%.3q", []byte("日本語日本語"), `"日本語"`},
	{"%.1q", "日本語", `"日"`},
	{"%.1q", []byte("日本語"), `"日"`},
	{"%.1x", "日本語", "e6"},
	{"%.1X", []byte("日本語"), "E6"},
	{"%10.1q", "日本語日本語", `       "日"`},
	{"%10v", nil, "     <nil>"},
	{"%-10v", nil, "<nil>     "},

	// integers
	{"%d", uint(12345), "12,345"},
	{"%d", int(-12345), "-12,345"},
	{"%d", ^uint8(0), "255"},
	{"%d", ^uint16(0), "65,535"},
	{"%d", ^uint32(0), "4,294,967,295"},
	{"%d", ^uint64(0), "18,446,744,073,709,551,615"},
	{"%d", int8(-1 << 7), "-128"},
	{"%d", int16(-1 << 15), "-32,768"},
	{"%d", int32(-1 << 31), "-2,147,483,648"},
	{"%d", int64(-1 << 63), "-9,223,372,036,854,775,808"},
	{"%.d", 0, ""},
	{"%.0d", 0, ""},
	{"%6.0d", 0, "      "},
	{"%06.0d", 0, "      "},
	{"% d", 12345, " 12,345"},
	{"%+d", 12345, "+12,345"},
	{"%+d", -12345, "-12,345"},
	{"%b", 7, "111"},
	{"%b", -6, "-110"},
	{"%b", ^uint32(0), "11111111111111111111111111111111"},
	{"%b", ^uint64(0), "1111111111111111111111111111111111111111111111111111111111111111"},
	{"%b", int64(-1 << 63), zeroFill("-1", 63, "")},
	{"%o", 01234, "1234"},
	{"%#o", 01234, "01234"},
	{"%o", ^uint32(0), "37777777777"},
	{"%o", ^uint64(0), "1777777777777777777777"},
	{"%#X", 0, "0X0"},
	{"%x", 0x12abcdef, "12abcdef"},
	{"%X", 0x12abcdef, "12ABCDEF"},
	{"%x", ^uint32(0), "ffffffff"},
	{"%X", ^uint64(0), "FFFFFFFFFFFFFFFF"},
	{"%.20b", 7, "00000000000000000111"},
	{"%10d", 12345, "    12,345"},
	{"%10d", -12345, "   -12,345"},
	{"%+10d", 12345, "   +12,345"},
	{"%010d", 12345, "0,000,012,345"},
	{"%010d", -12345, "-0,000,012,345"},
	{"%20.8d", 1234, "          00,001,234"},
	{"%20.8d", -1234, "         -00,001,234"},
	{"%020.8d", 1234, "          00,001,234"},
	{"%020.8d", -1234, "         -00,001,234"},
	{"%-20.8d", 1234, "00,001,234          "},
	{"%-20.8d", -1234, "-00,001,234         "},
	{"%-#20.8x", 0x1234abc, "0x01234abc          "},
	{"%-#20.8X", 0x1234abc, "0X01234ABC          "},
	{"%-#20.8o", 01234, "00001234            "},

	// Test correct f.intbuf overflow checks.
	{"%068d", 1, "00," + strings.Repeat("000,", 21) + "001"},
	{"%068d", -1, "-00," + strings.Repeat("000,", 21) + "001"},
	{"%#.68x", 42, zeroFill("0x", 68, "2a")},
	{"%.68d", -42, "-00," + strings.Repeat("000,", 21) + "042"},
	{"%+.68d", 42, "+00," + strings.Repeat("000,", 21) + "042"},
	{"% .68d", 42, " 00," + strings.Repeat("000,", 21) + "042"},
	{"% +.68d", 42, "+00," + strings.Repeat("000,", 21) + "042"},

	// unicode format
	{"%U", 0, "U+0000"},
	{"%U", -1, "U+FFFFFFFFFFFFFFFF"},
	{"%U", '\n', `U+000A`},
	{"%#U", '\n', `U+000A`},
	{"%+U", 'x', `U+0078`},       // Plus flag should have no effect.
	{"%# U", 'x', `U+0078 'x'`},  // Space flag should have no effect.
	{"%#.2U", 'x', `U+0078 'x'`}, // Precisions below 4 should print 4 digits.
	{"%U", '\u263a', `U+263A`},
	{"%#U", '\u263a', `U+263A '☺'`},
	{"%U", '\U0001D6C2', `U+1D6C2`},
	{"%#U", '\U0001D6C2', `U+1D6C2 '𝛂'`},
	{"%#14.6U", '⌘', "  U+002318 '⌘'"},
	{"%#-14.6U", '⌘', "U+002318 '⌘'  "},
	{"%#014.6U", '⌘', "  U+002318 '⌘'"},
	{"%#-014.6U", '⌘', "U+002318 '⌘'  "},
	{"%.68U", uint(42), zeroFill("U+", 68, "2A")},
	{"%#.68U", '日', zeroFill("U+", 68, "65E5") + " '日'"},

	// floats
	{"%+.3e", 0.0, "+0.000\u202f×\u202f10⁰⁰"},
	{"%+.3e", 1.0, "+1.000\u202f×\u202f10⁰⁰"},
	{"%+.3f", -1.0, "-1.000"},
	{"%+.3F", -1.0, "-1.000"},
	{"%+.3F", float32(-1.0), "-1.000"},
	{"%+07.2f", 1.0, "+001.00"},
	{"%+07.2f", -1.0, "-001.00"},
	{"%-07.2f", 1.0, "1.00   "},
	{"%-07.2f", -1.0, "-1.00  "},
	{"%+-07.2f", 1.0, "+1.00  "},
	{"%+-07.2f", -1.0, "-1.00  "},
	{"%-+07.2f", 1.0, "+1.00  "},
	{"%-+07.2f", -1.0, "-1.00  "},
	{"%+10.2f", +1.0, "     +1.00"},
	{"%+10.2f", -1.0, "     -1.00"},
	{"% .3E", -1.0, "-1.000\u202f×\u202f10⁰⁰"},
	{"% .3e", 1.0, " 1.000\u202f×\u202f10⁰⁰"},
	{"%+.3g", 0.0, "+0"},
	{"%+.3g", 1.0, "+1"},
	{"%+.3g", -1.0, "-1"},
	{"% .3g", -1.0, "-1"},
	{"% .3g", 1.0, " 1"},
	{"%b", float32(1.0), "8388608p-23"},
	{"%b", 1.0, "4503599627370496p-52"},
	// Test sharp flag used with floats.
	{"%#g", 1e-323, "1.00000e-323"},
	{"%#g", -1.0, "-1.00000"},
	{"%#g", 1.1, "1.10000"},
	{"%#g", 123456.0, "123456."},
	{"%#g", 1234567.0, "1.234567e+06"},
	{"%#g", 1230000.0, "1.23000e+06"},
	{"%#g", 1000000.0, "1.00000e+06"},
	{"%#.0f", 1.0, "1."},
	{"%#.0e", 1.0, "1.e+00"},
	{"%#.0g", 1.0, "1."},
	{"%#.0g", 1100000.0, "1.e+06"},
	{"%#.4f", 1.0, "1.0000"},
	{"%#.4e", 1.0, "1.0000e+00"},
	{"%#.4g", 1.0, "1.000"},
	{"%#.4g", 100000.0, "1.000e+05"},
	{"%#.0f", 123.0, "123."},
	{"%#.0e", 123.0, "1.e+02"},
	{"%#.0g", 123.0, "1.e+02"},
	{"%#.4f", 123.0, "123.0000"},
	{"%#.4e", 123.0, "1.2300e+02"},
	{"%#.4g", 123.0, "123.0"},
	{"%#.4g", 123000.0, "1.230e+05"},
	{"%#9.4g", 1.0, "    1.000"},
	// The sharp flag has no effect for binary float format.
	{"%#b", 1.0, "4503599627370496p-52"},
	// Precision has no effect for binary float format.
	{"%.4b", float32(1.0), "8388608p-23"},
	{"%.4b", -1.0, "-4503599627370496p-52"},
	// Test correct f.intbuf boundary checks.
	{"%.68f", 1.0, zeroFill("1.", 68, "")},
	{"%.68f", -1.0, zeroFill("-1.", 68, "")},
	// float infinites and NaNs
	{"%f", posInf, "∞"},
	{"%.1f", negInf, "-∞"},
	{"% f", NaN, "NaN"},
	{"%20f", posInf, "                   ∞"},
	{"% 20F", posInf, "                   ∞"},
	{"% 20e", negInf, "                  -∞"},
	{"%+20E", negInf, "                  -∞"},
	{"% +20g", negInf, "                  -∞"},
	{"%+-20G", posInf, "+∞                  "},
	{"%20e", NaN, "                 NaN"},
	{"% +20E", NaN, "                 NaN"},
	{"% -20g", NaN, "NaN                 "},
	{"%+-20G", NaN, "NaN                 "},
	// Zero padding does not apply to infinities and NaN.
	{"%+020e", posInf, "                  +∞"},
	{"%-020f", negInf, "-∞                  "},
	{"%-020E", NaN, "NaN                 "},

	// complex values
	{"%.f", 0i, "(0+0i)"},
	{"% .f", 0i, "( 0+0i)"},
	{"%+.f", 0i, "(+0+0i)"},
	{"% +.f", 0i, "(+0+0i)"},
	{"%+.3e", 0i, "(+0.000\u202f×\u202f10⁰⁰+0.000\u202f×\u202f10⁰⁰i)"},
	{"%+.3f", 0i, "(+0.000+0.000i)"},
	{"%+.3g", 0i, "(+0+0i)"},
	{"%+.3e", 1 + 2i, "(+1.000\u202f×\u202f10⁰⁰+2.000\u202f×\u202f10⁰⁰i)"},
	{"%+.3f", 1 + 2i, "(+1.000+2.000i)"},
	{"%+.3g", 1 + 2i, "(+1+2i)"},
	{"%.3e", 0i, "(0.000\u202f×\u202f10⁰⁰+0.000\u202f×\u202f10⁰⁰i)"},
	{"%.3f", 0i, "(0.000+0.000i)"},
	{"%.3F", 0i, "(0.000+0.000i)"},
	{"%.3F", complex64(0i), "(0.000+0.000i)"},
	{"%.3g", 0i, "(0+0i)"},
	{"%.3e", 1 + 2i, "(1.000\u202f×\u202f10⁰⁰+2.000\u202f×\u202f10⁰⁰i)"},
	{"%.3f", 1 + 2i, "(1.000+2.000i)"},
	{"%.3g", 1 + 2i, "(1+2i)"},
	{"%.3e", -1 - 2i, "(-1.000\u202f×\u202f10⁰⁰-2.000\u202f×\u202f10⁰⁰i)"},
	{"%.3f", -1 - 2i, "(-1.000-2.000i)"},
	{"%.3g", -1 - 2i, "(-1-2i)"},
	{"% .3E", -1 - 2i, "(-1.000\u202f×\u202f10⁰⁰-2.000\u202f×\u202f10⁰⁰i)"},
	{"%+.3g", 1 + 2i, "(+1+2i)"},
	{"%+.3g", complex64(1 + 2i), "(+1+2i)"},
	{"%#g", 1 + 2i, "(1.00000+2.00000i)"},
	{"%#g", 123456 + 789012i, "(123456.+789012.i)"},
	{"%#g", 1e-10i, "(0.00000+1.00000e-10i)"},
	{"%#g", -1e10 - 1.11e100i, "(-1.00000e+10-1.11000e+100i)"},
	{"%#.0f", 1.23 + 1.0i, "(1.+1.i)"},
	{"%#.0e", 1.23 + 1.0i, "(1.e+00+1.e+00i)"},
	{"%#.0g", 1.23 + 1.0i, "(1.+1.i)"},
	{"%#.0g", 0 + 100000i, "(0.+1.e+05i)"},
	{"%#.0g", 1230000 + 0i, "(1.e+06+0.i)"},
	{"%#.4f", 1 + 1.23i, "(1.0000+1.2300i)"},
	{"%#.4e", 123 + 1i, "(1.2300e+02+1.0000e+00i)"},
	{"%#.4g", 123 + 1.23i, "(123.0+1.230i)"},
	{"%#12.5g", 0 + 100000i, "(      0.0000 +1.0000e+05i)"},
	{"%#12.5g", 1230000 - 0i, "(  1.2300e+06     +0.0000i)"},
	{"%b", 1 + 2i, "(4503599627370496p-52+4503599627370496p-51i)"},
	{"%b", complex64(1 + 2i), "(8388608p-23+8388608p-22i)"},
	// The sharp flag has no effect for binary complex format.
	{"%#b", 1 + 2i, "(4503599627370496p-52+4503599627370496p-51i)"},
	// Precision has no effect for binary complex format.
	{"%.4b", 1 + 2i, "(4503599627370496p-52+4503599627370496p-51i)"},
	{"%.4b", complex64(1 + 2i), "(8388608p-23+8388608p-22i)"},
	// complex infinites and NaNs
	{"%f", complex(posInf, posInf), "(∞+∞i)"},
	{"%f", complex(negInf, negInf), "(-∞-∞i)"},
	{"%f", complex(NaN, NaN), "(NaN+NaNi)"},
	{"%.1f", complex(posInf, posInf), "(∞+∞i)"},
	{"% f", complex(posInf, posInf), "( ∞+∞i)"},
	{"% f", complex(negInf, negInf), "(-∞-∞i)"},
	{"% f", complex(NaN, NaN), "(NaN+NaNi)"},
	{"%8e", complex(posInf, posInf), "(       ∞      +∞i)"},
	{"% 8E", complex(posInf, posInf), "(       ∞      +∞i)"},
	{"%+8f", complex(negInf, negInf), "(      -∞      -∞i)"},
	{"% +8g", complex(negInf, negInf), "(      -∞      -∞i)"}, // TODO(g)
	{"% -8G", complex(NaN, NaN), "(NaN     +NaN    i)"},
	{"%+-8b", complex(NaN, NaN), "(+NaN    +NaN    i)"},
	// Zero padding does not apply to infinities and NaN.
	{"%08f", complex(posInf, posInf), "(       ∞      +∞i)"},
	{"%-08g", complex(negInf, negInf), "(-∞      -∞      i)"},
	{"%-08G", complex(NaN, NaN), "(NaN     +NaN    i)"},

	// old test/fmt_test.go
	{"%e", 1.0, "1.000000\u202f×\u202f10⁰⁰"},
	{"%e", 1234.5678e3, "1.234568\u202f×\u202f10⁰⁶"},
	{"%e", 1234.5678e-8, "1.234568\u202f×\u202f10⁻⁰⁵"},
	{"%e", -7.0, "-7.000000\u202f×\u202f10⁰⁰"},
	{"%e", -1e-9, "-1.000000\u202f×\u202f10⁻⁰⁹"},
	{"%f", 1234.5678e3, "1,234,567.800000"},
	{"%f", 1234.5678e-8, "0.000012"},
	{"%f", -7.0, "-7.000000"},
	{"%f", -1e-9, "-0.000000"},
	{"%g", 1234.5678e3, "1.2345678\u202f×\u202f10⁰⁶"},
	{"%g", float32(1234.5678e3), "1.2345678\u202f×\u202f10⁰⁶"},
	{"%g", 1234.5678e-8, "1.2345678\u202f×\u202f10⁻⁰⁵"},
	{"%g", -7.0, "-7"},
	{"%g", -1e-9, "-1\u202f×\u202f10⁻⁰⁹"},
	{"%g", float32(-1e-9), "-1\u202f×\u202f10⁻⁰⁹"},
	{"%E", 1.0, "1.000000\u202f×\u202f10⁰⁰"},
	{"%E", 1234.5678e3, "1.234568\u202f×\u202f10⁰⁶"},
	{"%E", 1234.5678e-8, "1.234568\u202f×\u202f10⁻⁰⁵"},
	{"%E", -7.0, "-7.000000\u202f×\u202f10⁰⁰"},
	{"%E", -1e-9, "-1.000000\u202f×\u202f10⁻⁰⁹"},
	{"%G", 1234.5678e3, "1.2345678\u202f×\u202f10⁰⁶"},
	{"%G", float32(1234.5678e3), "1.2345678\u202f×\u202f10⁰⁶"},
	{"%G", 1234.5678e-8, "1.2345678\u202f×\u202f10⁻⁰⁵"},
	{"%G", -7.0, "-7"},
	{"%G", -1e-9, "-1\u202f×\u202f10⁻⁰⁹"},
	{"%G", float32(-1e-9), "-1\u202f×\u202f10⁻⁰⁹"},
	{"%20.5s", "qwertyuiop", "               qwert"},
	{"%.5s", "qwertyuiop", "qwert"},
	{"%-20.5s", "qwertyuiop", "qwert               "},
	{"%20c", 'x', "                   x"},
	{"%-20c", 'x', "x                   "},
	{"%20.6e", 1.2345e3, "     1.234500\u202f×\u202f10⁰³"},
	{"%20.6e", 1.2345e-3, "    1.234500\u202f×\u202f10⁻⁰³"},
	{"%20e", 1.2345e3, "     1.234500\u202f×\u202f10⁰³"},
	{"%20e", 1.2345e-3, "    1.234500\u202f×\u202f10⁻⁰³"},
	{"%20.8e", 1.2345e3, "   1.23450000\u202f×\u202f10⁰³"},
	{"%20f", 1.23456789e3, "        1,234.567890"},
	{"%20f", 1.23456789e-3, "            0.001235"},
	{"%20f", 12345678901.23456789, "12,345,678,901.234568"},
	{"%-20f", 1.23456789e3, "1,234.567890        "},
	{"%20.8f", 1.23456789e3, "      1,234.56789000"},
	{"%20.8f", 1.23456789e-3, "          0.00123457"},
	{"%g", 1.23456789e3, "1,234.56789"},
	{"%g", 1.23456789e-3, "0.00123456789"},
	{"%g", 1.23456789e20, "1.23456789\u202f×\u202f10²⁰"},

	// arrays
	{"%v", array, "[1 2 3 4 5]"},
	{"%v", iarray, "[1 hello 2.5 <nil>]"},
	{"%v", barray, "[1 2 3 4 5]"},
	{"%v", &array, "&[1 2 3 4 5]"},
	{"%v", &iarray, "&[1 hello 2.5 <nil>]"},
	{"%v", &barray, "&[1 2 3 4 5]"},

	// slices
	{"%v", slice, "[1 2 3 4 5]"},
	{"%v", islice, "[1 hello 2.5 <nil>]"},
	{"%v", bslice, "[1 2 3 4 5]"},
	{"%v", &slice, "&[1 2 3 4 5]"},
	{"%v", &islice, "&[1 hello 2.5 <nil>]"},
	{"%v", &bslice, "&[1 2 3 4 5]"},

	// byte arrays and slices with %b,%c,%d,%o,%U and %v
	{"%b", [3]byte{65, 66, 67}, "[1000001 1000010 1000011]"},
	{"%c", [3]byte{65, 66, 67}, "[A B C]"},
	{"%d", [3]byte{65, 66, 67}, "[65 66 67]"},
	{"%o", [3]byte{65, 66, 67}, "[101 102 103]"},
	{"%U", [3]byte{65, 66, 67}, "[U+0041 U+0042 U+0043]"},
	{"%v", [3]byte{65, 66, 67}, "[65 66 67]"},
	{"%v", [1]byte{123}, "[123]"},
	{"%012v", []byte{}, "[]"},
	{"%#012v", []byte{}, "[]byte{}"},
	{"%6v", []byte{1, 11, 111}, "[     1     11    111]"},
	{"%06v", []byte{1, 11, 111}, "[000001 000011 000111]"},
	{"%-6v", []byte{1, 11, 111}, "[1      11     111   ]"},
	{"%-06v", []byte{1, 11, 111}, "[1      11     111   ]"},
	{"%#v", []byte{1, 11, 111}, "[]byte{0x1, 0xb, 0x6f}"},
	{"%#6v", []byte{1, 11, 111}, "[]byte{   0x1,    0xb,   0x6f}"},
	{"%#06v", []byte{1, 11, 111}, "[]byte{0x000001, 0x00000b, 0x00006f}"},
	{"%#-6v", []byte{1, 11, 111}, "[]byte{0x1   , 0xb   , 0x6f  }"},
	{"%#-06v", []byte{1, 11, 111}, "[]byte{0x1   , 0xb   , 0x6f  }"},
	// f.space should and f.plus should not have an effect with %v.
	{"% v", []byte{1, 11, 111}, "[ 1  11  111]"},
	{"%+v", [3]byte{1, 11, 111}, "[1 11 111]"},
	{"%# -6v", []byte{1, 11, 111}, "[]byte{ 0x1  ,  0xb  ,  0x6f }"},
	{"%#+-6v", [3]byte{1, 11, 111}, "[3]uint8{0x1   , 0xb   , 0x6f  }"},
	// f.space and f.plus should have an effect with %d.
	{"% d", []byte{1, 11, 111}, "[ 1  11  111]"},
	{"%+d", [3]byte{1, 11, 111}, "[+1 +11 +111]"},
	{"%# -6d", []byte{1, 11, 111}, "[ 1      11     111  ]"},
	{"%#+-6d", [3]byte{1, 11, 111}, "[+1     +11    +111  ]"},

	// floates with %v
	{"%v", 1.2345678, "1.2345678"},
	{"%v", float32(1.2345678), "1.2345678"},

	// complexes with %v
	{"%v", 1 + 2i, "(1+2i)"},
	{"%v", complex64(1 + 2i), "(1+2i)"},

	// structs
	{"%v", A{1, 2, "a", []int{1, 2}}, `{1 2 a [1 2]}`},
	{"%+v", A{1, 2, "a", []int{1, 2}}, `{i:1 j:2 s:a x:[1 2]}`},

	// +v on structs with Stringable items
	{"%+v", B{1, 2}, `{I:<1> j:2}`},
	{"%+v", C{1, B{2, 3}}, `{i:1 B:{I:<2> j:3}}`},

	// other formats on Stringable items
	{"%s", I(23), `<23>`},
	{"%q", I(23), `"<23>"`},
	{"%x", I(23), `3c32333e`},
	{"%#x", I(23), `0x3c32333e`},
	{"%# x", I(23), `0x3c 0x32 0x33 0x3e`},
	// Stringer applies only to string formats.
	{"%d", I(23), `23`},
	// Stringer applies to the extracted value.
	{"%s", reflect.ValueOf(I(23)), `<23>`},

	// go syntax
	{"%#v", A{1, 2, "a", []int{1, 2}}, `message.A{i:1, j:0x2, s:"a", x:[]int{1, 2}}`},
	{"%#v", new(byte), "(*uint8)(0xPTR)"},
	{"%#v", TestFmtInterface, "(func(*testing.T))(0xPTR)"},
	{"%#v", make(chan int), "(chan int)(0xPTR)"},
	{"%#v", uint64(1<<64 - 1), "0xffffffffffffffff"},
	{"%#v", 1000000000, "1000000000"},
	{"%#v", map[string]int{"a": 1}, `map[string]int{"a":1}`},
	{"%#v", map[string]B{"a": {1, 2}}, `map[string]message.B{"a":message.B{I:1, j:2}}`},
	{"%#v", []string{"a", "b"}, `[]string{"a", "b"}`},
	{"%#v", SI{}, `message.SI{I:interface {}(nil)}`},
	{"%#v", []int(nil), `[]int(nil)`},
	{"%#v", []int{}, `[]int{}`},
	{"%#v", array, `[5]int{1, 2, 3, 4, 5}`},
	{"%#v", &array, `&[5]int{1, 2, 3, 4, 5}`},
	{"%#v", iarray, `[4]interface {}{1, "hello", 2.5, interface {}(nil)}`},
	{"%#v", &iarray, `&[4]interface {}{1, "hello", 2.5, interface {}(nil)}`},
	{"%#v", map[int]byte(nil), `map[int]uint8(nil)`},
	{"%#v", map[int]byte{}, `map[int]uint8{}`},
	{"%#v", "foo", `"foo"`},
	{"%#v", barray, `[5]message.renamedUint8{0x1, 0x2, 0x3, 0x4, 0x5}`},
	{"%#v", bslice, `[]message.renamedUint8{0x1, 0x2, 0x3, 0x4, 0x5}`},
	{"%#v", []int32(nil), "[]int32(nil)"},
	{"%#v", 1.2345678, "1.2345678"},
	{"%#v", float32(1.2345678), "1.2345678"},
	// Only print []byte and []uint8 as type []byte if they appear at the top level.
	{"%#v", []byte(nil), "[]byte(nil)"},
	{"%#v", []uint8(nil), "[]byte(nil)"},
	{"%#v", []byte{}, "[]byte{}"},
	{"%#v", []uint8{}, "[]byte{}"},
	{"%#v", reflect.ValueOf([]byte{}), "[]uint8{}"},
	{"%#v", reflect.ValueOf([]uint8{}), "[]uint8{}"},
	{"%#v", &[]byte{}, "&[]uint8{}"},
	{"%#v", &[]byte{}, "&[]uint8{}"},
	{"%#v", [3]byte{}, "[3]uint8{0x0, 0x0, 0x0}"},
	{"%#v", [3]uint8{}, "[3]uint8{0x0, 0x0, 0x0}"},

	// slices with other formats
	{"%#x", []int{1, 2, 15}, `[0x1 0x2 0xf]`},
	{"%x", []int{1, 2, 15}, `[1 2 f]`},
	{"%d", []int{1, 2, 15}, `[1 2 15]`},
	{"%d", []byte{1, 2, 15}, `[1 2 15]`},
	{"%q", []string{"a", "b"}, `["a" "b"]`},
	{"% 02x", []byte{1}, "01"},
	{"% 02x", []byte{1, 2, 3}, "01 02 03"},

	// Padding with byte slices.
	{"%2x", []byte{}, "  "},
	{"%#2x", []byte{}, "  "},
	{"% 02x", []byte{}, "00"},
	{"%# 02x", []byte{}, "00"},
	{"%-2x", []byte{}, "  "},
	{"%-02x", []byte{}, "  "},
	{"%8x", []byte{0xab}, "      ab"},
	{"% 8x", []byte{0xab}, "      ab"},
	{"%#8x", []byte{0xab}, "    0xab"},
	{"%# 8x", []byte{0xab}, "    0xab"},
	{"%08x", []byte{0xab}, "000000ab"},
	{"% 08x", []byte{0xab}, "000000ab"},
	{"%#08x", []byte{0xab}, "00000xab"},
	{"%# 08x", []byte{0xab}, "00000xab"},
	{"%10x", []byte{0xab, 0xcd}, "      abcd"},
	{"% 10x", []byte{0xab, 0xcd}, "     ab cd"},
	{"%#10x", []byte{0xab, 0xcd}, "    0xabcd"},
	{"%# 10x", []byte{0xab, 0xcd}, " 0xab 0xcd"},
	{"%010x", []byte{0xab, 0xcd}, "000000abcd"},
	{"% 010x", []byte{0xab, 0xcd}, "00000ab cd"},
	{"%#010x", []byte{0xab, 0xcd}, "00000xabcd"},
	{"%# 010x", []byte{0xab, 0xcd}, "00xab 0xcd"},
	{"%-10X", []byte{0xab}, "AB        "},
	{"% -010X", []byte{0xab}, "AB        "},
	{"%#-10X", []byte{0xab, 0xcd}, "0XABCD    "},
	{"%# -010X", []byte{0xab, 0xcd}, "0XAB 0XCD "},
	// Same for strings
	{"%2x", "", "  "},
	{"%#2x", "", "  "},
	{"% 02x", "", "00"},
	{"%# 02x", "", "00"},
	{"%-2x", "", "  "},
	{"%-02x", "", "  "},
	{"%8x", "\xab", "      ab"},
	{"% 8x", "\xab", "      ab"},
	{"%#8x", "\xab", "    0xab"},
	{"%# 8x", "\xab", "    0xab"},
	{"%08x", "\xab", "000000ab"},
	{"% 08x", "\xab", "000000ab"},
	{"%#08x", "\xab", "00000xab"},
	{"%# 08x", "\xab", "00000xab"},
	{"%10x", "\xab\xcd", "      abcd"},
	{"% 10x", "\xab\xcd", "     ab cd"},
	{"%#10x", "\xab\xcd", "    0xabcd"},
	{"%# 10x", "\xab\xcd", " 0xab 0xcd"},
	{"%010x", "\xab\xcd", "000000abcd"},
	{"% 010x", "\xab\xcd", "00000ab cd"},
	{"%#010x", "\xab\xcd", "00000xabcd"},
	{"%# 010x", "\xab\xcd", "00xab 0xcd"},
	{"%-10X", "\xab", "AB        "},
	{"% -010X", "\xab", "AB        "},
	{"%#-10X", "\xab\xcd", "0XABCD    "},
	{"%# -010X", "\xab\xcd", "0XAB 0XCD "},

	// renamings
	{"%v", renamedBool(true), "true"},
	{"%d", renamedBool(true), "%!d(message.renamedBool=true)"},
	{"%o", renamedInt(8), "10"},
	{"%d", renamedInt8(-9), "-9"},
	{"%v", renamedInt16(10), "10"},
	{"%v", renamedInt32(-11), "-11"},
	{"%X", renamedInt64(255), "FF"},
	{"%v", renamedUint(13), "13"},
	{"%o", renamedUint8(14), "16"},
	{"%X", renamedUint16(15), "F"},
	{"%d", renamedUint32(16), "16"},
	{"%X", renamedUint64(17), "11"},
	{"%o", renamedUintptr(18), "22"},
	{"%x", renamedString("thing"), "7468696e67"},
	{"%d", renamedBytes([]byte{1, 2, 15}), `[1 2 15]`},
	{"%q", renamedBytes([]byte("hello")), `"hello"`},
	{"%x", []renamedUint8{'h', 'e', 'l', 'l', 'o'}, "68656c6c6f"},
	{"%X", []renamedUint8{'h', 'e', 'l', 'l', 'o'}, "68656C6C6F"},
	{"%s", []renamedUint8{'h', 'e', 'l', 'l', 'o'}, "hello"},
	{"%q", []renamedUint8{'h', 'e', 'l', 'l', 'o'}, `"hello"`},
	{"%v", renamedFloat32(22), "22"},
	{"%v", renamedFloat64(33), "33"},
	{"%v", renamedComplex64(3 + 4i), "(3+4i)"},
	{"%v", renamedComplex128(4 - 3i), "(4-3i)"},

	// Formatter
	{"%x", F(1), "<x=F(1)>"},
	{"%x", G(2), "2"},
	{"%+v", S{F(4), G(5)}, "{F:<v=F(4)> G:5}"},

	// GoStringer
	{"%#v", G(6), "GoString(6)"},
	{"%#v", S{F(7), G(8)}, "message.S{F:<v=F(7)>, G:GoString(8)}"},

	// %T
	{"%T", byte(0), "uint8"},
	{"%T", reflect.ValueOf(nil), "reflect.Value"},
	{"%T", (4 - 3i), "complex128"},
	{"%T", renamedComplex128(4 - 3i), "message.renamedComplex128"},
	{"%T", intVar, "int"},
	{"%6T", &intVar, "  *int"},
	{"%10T", nil, "     <nil>"},
	{"%-10T", nil, "<nil>     "},

	// %p with pointers
	{"%p", (*int)(nil), "0x0"},
	{"%#p", (*int)(nil), "0"},
	{"%p", &intVar, "0xPTR"},
	{"%#p", &intVar, "PTR"},
	{"%p", &array, "0xPTR"},
	{"%p", &slice, "0xPTR"},
	{"%8.2p", (*int)(nil), "    0x00"},
	{"%-20.16p", &intVar, "0xPTR  "},
	// %p on non-pointers
	{"%p", make(chan int), "0xPTR"},
	{"%p", make(map[int]int), "0xPTR"},
	{"%p", func() {}, "0xPTR"},
	{"%p", 27, "%!p(int=27)"},  // not a pointer at all
	{"%p", nil, "%!p(<nil>)"},  // nil on its own has no type ...
	{"%#p", nil, "%!p(<nil>)"}, // ... and hence is not a pointer type.
	// pointers with specified base
	{"%b", &intVar, "PTR_b"},
	{"%d", &intVar, "PTR_d"},
	{"%o", &intVar, "PTR_o"},
	{"%x", &intVar, "PTR_x"},
	{"%X", &intVar, "PTR_X"},
	// %v on pointers
	{"%v", nil, "<nil>"},
	{"%#v", nil, "<nil>"},
	{"%v", (*int)(nil), "<nil>"},
	{"%#v", (*int)(nil), "(*int)(nil)"},
	{"%v", &intVar, "0xPTR"},
	{"%#v", &intVar, "(*int)(0xPTR)"},
	{"%8.2v", (*int)(nil), "   <nil>"},
	{"%-20.16v", &intVar, "0xPTR  "},
	// string method on pointer
	{"%s", &pValue, "String(p)"}, // String method...
	{"%p", &pValue, "0xPTR"},     // ... is not called with %p.

	// %d on Stringer should give integer if possible
	{"%s", time.Time{}.Month(), "January"},
	{"%d", time.Time{}.Month(), "1"},

	// erroneous things
	{"%s %", "hello", "hello %!(NOVERB)"},
	{"%s %.2", "hello", "hello %!(NOVERB)"},

	// The "<nil>" show up because maps are printed by
	// first obtaining a list of keys and then looking up
	// each key. Since NaNs can be map keys but cannot
	// be fetched directly, the lookup fails and returns a
	// zero reflect.Value, which formats as <nil>.
	// This test is just to check that it shows the two NaNs at all.
	{"%v", map[float64]int{NaN: 1, NaN: 2}, "map[NaN:<nil> NaN:<nil>]"},

	// Comparison of padding rules with C printf.
	/*
		C program:
		#include <stdio.h>

		char *format[] = {
			"[%.2f]",
			"[% .2f]",
			"[%+.2f]",
			"[%7.2f]",
			"[% 7.2f]",
			"[%+7.2f]",
			"[% +7.2f]",
			"[%07.2f]",
			"[% 07.2f]",
			"[%+07.2f]",
			"[% +07.2f]"
		};

		int main(void) {
			int i;
			for(i = 0; i < 11; i++) {
				printf("%s: ", format[i]);
				printf(format[i], 1.0);
				printf(" ");
				printf(format[i], -1.0);
				printf("\n");
			}
		}

		Output:
			[%.2f]: [1.00] [-1.00]
			[% .2f]: [ 1.00] [-1.00]
			[%+.2f]: [+1.00] [-1.00]
			[%7.2f]: [   1.00] [  -1.00]
			[% 7.2f]: [   1.00] [  -1.00]
			[%+7.2f]: [  +1.00] [  -1.00]
			[% +7.2f]: [  +1.00] [  -1.00]
			[%07.2f]: [0001.00] [-001.00]
			[% 07.2f]: [ 001.00] [-001.00]
			[%+07.2f]: [+001.00] [-001.00]
			[% +07.2f]: [+001.00] [-001.00]

	*/
	{"%.2f", 1.0, "1.00"},
	{"%.2f", -1.0, "-1.00"},
	{"% .2f", 1.0, " 1.00"},
	{"% .2f", -1.0, "-1.00"},
	{"%+.2f", 1.0, "+1.00"},
	{"%+.2f", -1.0, "-1.00"},
	{"%7.2f", 1.0, "   1.00"},
	{"%7.2f", -1.0, "  -1.00"},
	{"% 7.2f", 1.0, "   1.00"},
	{"% 7.2f", -1.0, "  -1.00"},
	{"%+7.2f", 1.0, "  +1.00"},
	{"%+7.2f", -1.0, "  -1.00"},
	{"% +7.2f", 1.0, "  +1.00"},
	{"% +7.2f", -1.0, "  -1.00"},
	// Padding with 0's indicates minimum number of integer digits minus the
	// period, if present, and minus the sign if it is fixed.
	// TODO: consider making this number the number of significant digits.
	{"%07.2f", 1.0, "0,001.00"},
	{"%07.2f", -1.0, "-0,001.00"},
	{"% 07.2f", 1.0, " 001.00"},
	{"% 07.2f", -1.0, "-001.00"},
	{"%+07.2f", 1.0, "+001.00"},
	{"%+07.2f", -1.0, "-001.00"},
	{"% +07.2f", 1.0, "+001.00"},
	{"% +07.2f", -1.0, "-001.00"},

	// Complex numbers: exhaustively tested in TestComplexFormatting.
	{"%7.2f", 1 + 2i, "(   1.00  +2.00i)"},
	{"%+07.2f", -1 - 2i, "(-001.00-002.00i)"},

	// Use spaces instead of zero if padding to the right.
	{"%0-5s", "abc", "abc  "},
	{"%-05.1f", 1.0, "1.0  "},

	// float and complex formatting should not change the padding width
	// for other elements. See issue 14642.
	{"%06v", []interface{}{+10.0, 10}, "[000,010 000,010]"},
	{"%06v", []interface{}{-10.0, 10}, "[-000,010 000,010]"},
	{"%06v", []interface{}{+10.0 + 10i, 10}, "[(000,010+00,010i) 000,010]"},
	{"%06v", []interface{}{-10.0 + 10i, 10}, "[(-000,010+00,010i) 000,010]"},

	// integer formatting should not alter padding for other elements.
	{"%03.6v", []interface{}{1, 2.0, "x"}, "[000,001 002 00x]"},
	{"%03.0v", []interface{}{0, 2.0, "x"}, "[    002 000]"},

	// Complex fmt used to leave the plus flag set for future entries in the array
	// causing +2+0i and +3+0i instead of 2+0i and 3+0i.
	{"%v", []complex64{1, 2, 3}, "[(1+0i) (2+0i) (3+0i)]"},
	{"%v", []complex128{1, 2, 3}, "[(1+0i) (2+0i) (3+0i)]"},

	// Incomplete format specification caused crash.
	{"%.", 3, "%!.(int=3)"},

	// Padding for complex numbers. Has been bad, then fixed, then bad again.
	{"%+10.2f", +104.66 + 440.51i, "(   +104.66   +440.51i)"},
	{"%+10.2f", -104.66 + 440.51i, "(   -104.66   +440.51i)"},
	{"%+10.2f", +104.66 - 440.51i, "(   +104.66   -440.51i)"},
	{"%+10.2f", -104.66 - 440.51i, "(   -104.66   -440.51i)"},
	{"%010.2f", +104.66 + 440.51i, "(0,000,104.66+000,440.51i)"},
	{"%+010.2f", +104.66 + 440.51i, "(+000,104.66+000,440.51i)"},
	{"%+010.2f", -104.66 + 440.51i, "(-000,104.66+000,440.51i)"},
	{"%+010.2f", +104.66 - 440.51i, "(+000,104.66-000,440.51i)"},
	{"%+010.2f", -104.66 - 440.51i, "(-000,104.66-000,440.51i)"},

	// []T where type T is a byte with a Stringer method.
	{"%v", byteStringerSlice, "[X X X X X]"},
	{"%s", byteStringerSlice, "hello"},
	{"%q", byteStringerSlice, "\"hello\""},
	{"%x", byteStringerSlice, "68656c6c6f"},
	{"%X", byteStringerSlice, "68656C6C6F"},
	{"%#v", byteStringerSlice, "[]message.byteStringer{0x68, 0x65, 0x6c, 0x6c, 0x6f}"},

	// And the same for Formatter.
	{"%v", byteFormatterSlice, "[X X X X X]"},
	{"%s", byteFormatterSlice, "hello"},
	{"%q", byteFormatterSlice, "\"hello\""},
	{"%x", byteFormatterSlice, "68656c6c6f"},
	{"%X", byteFormatterSlice, "68656C6C6F"},
	// This next case seems wrong, but the docs say the Formatter wins here.
	{"%#v", byteFormatterSlice, "[]message.byteFormatter{X, X, X, X, X}"},

	// reflect.Value handled specially in Go 1.5, making it possible to
	// see inside non-exported fields (which cannot be accessed with Interface()).
	// Issue 8965.
	{"%v", reflect.ValueOf(A{}).Field(0).String(), "<int Value>"}, // Equivalent to the old way.
	{"%v", reflect.ValueOf(A{}).Field(0), "0"},                    // Sees inside the field.

	// verbs apply to the extracted value too.
	{"%s", reflect.ValueOf("hello"), "hello"},
	{"%q", reflect.ValueOf("hello"), `"hello"`},
	{"%#04x", reflect.ValueOf(256), "0x0100"},

	// invalid reflect.Value doesn't crash.
	{"%v", reflect.Value{}, "<invalid reflect.Value>"},
	{"%v", &reflect.Value{}, "<invalid Value>"},
	{"%v", SI{reflect.Value{}}, "{<invalid Value>}"},

	// Tests to check that not supported verbs generate an error string.
	{"%☠", nil, "%!☠(<nil>)"},
	{"%☠", interface{}(nil), "%!☠(<nil>)"},
	{"%☠", int(0), "%!☠(int=0)"},
	{"%☠", uint(0), "%!☠(uint=0)"},
	{"%☠", []byte{0, 1}, "[%!☠(uint8=0) %!☠(uint8=1)]"},
	{"%☠", []uint8{0, 1}, "[%!☠(uint8=0) %!☠(uint8=1)]"},
	{"%☠", [1]byte{0}, "[%!☠(uint8=0)]"},
	{"%☠", [1]uint8{0}, "[%!☠(uint8=0)]"},
	{"%☠", "hello", "%!☠(string=hello)"},
	{"%☠", 1.2345678, "%!☠(float64=1.2345678)"},
	{"%☠", float32(1.2345678), "%!☠(float32=1.2345678)"},
	{"%☠", 1.2345678 + 1.2345678i, "%!☠(complex128=(1.2345678+1.2345678i))"},
	{"%☠", complex64(1.2345678 + 1.2345678i), "%!☠(complex64=(1.2345678+1.2345678i))"},
	{"%☠", &intVar, "%!☠(*int=0xPTR)"},
	{"%☠", make(chan int), "%!☠(chan int=0xPTR)"},
	{"%☠", func() {}, "%!☠(func()=0xPTR)"},
	{"%☠", reflect.ValueOf(renamedInt(0)), "%!☠(message.renamedInt=0)"},
	{"%☠", SI{renamedInt(0)}, "{%!☠(message.renamedInt=0)}"},
	{"%☠", &[]interface{}{I(1), G(2)}, "&[%!☠(message.I=1) %!☠(message.G=2)]"},
	{"%☠", SI{&[]interface{}{I(1), G(2)}}, "{%!☠(*[]interface {}=&[1 2])}"},
	{"%☠", reflect.Value{}, "<invalid reflect.Value>"},
	{"%☠", map[float64]int{NaN: 1}, "map[%!☠(float64=NaN):%!☠(<nil>)]"},
}

// zeroFill generates zero-filled strings of the specified width. The length
// of the suffix (but not the prefix) is compensated for in the width calculation.
func zeroFill(prefix string, width int, suffix string) string {
	return prefix + strings.Repeat("0", width-len(suffix)) + suffix
}

func TestSprintf(t *testing.T) {
	p := NewPrinter(language.Und)
	for _, tt := range fmtTests {
		t.Run(fmt.Sprint(tt.fmt, "/", tt.val), func(t *testing.T) {
			s := p.Sprintf(tt.fmt, tt.val)
			i := strings.Index(tt.out, "PTR")
			if i >= 0 && i < len(s) {
				var pattern, chars string
				switch {
				case strings.HasPrefix(tt.out[i:], "PTR_b"):
					pattern = "PTR_b"
					chars = "01"
				case strings.HasPrefix(tt.out[i:], "PTR_o"):
					pattern = "PTR_o"
					chars = "01234567"
				case strings.HasPrefix(tt.out[i:], "PTR_d"):
					pattern = "PTR_d"
					chars = "0123456789"
				case strings.HasPrefix(tt.out[i:], "PTR_x"):
					pattern = "PTR_x"
					chars = "0123456789abcdef"
				case strings.HasPrefix(tt.out[i:], "PTR_X"):
					pattern = "PTR_X"
					chars = "0123456789ABCDEF"
				default:
					pattern = "PTR"
					chars = "0123456789abcdefABCDEF"
				}
				p := s[:i] + pattern
				for j := i; j < len(s); j++ {
					if !strings.ContainsRune(chars, rune(s[j])) {
						p += s[j:]
						break
					}
				}
				s = p
			}
			if s != tt.out {
				if _, ok := tt.val.(string); ok {
					// Don't requote the already-quoted strings.
					// It's too confusing to read the errors.
					t.Errorf("Sprintf(%q, %q) = <%s> want <%s>", tt.fmt, tt.val, s, tt.out)
				} else {
					t.Errorf("Sprintf(%q, %v) = %q want %q", tt.fmt, tt.val, s, tt.out)
				}
			}
		})
	}
}

var f float64

// TestComplexFormatting checks that a complex always formats to the same
// thing as if done by hand with two singleton prints.
func TestComplexFormatting(t *testing.T) {
	var yesNo = []bool{true, false}
	var values = []float64{1, 0, -1, posInf, negInf, NaN}
	p := NewPrinter(language.Und)
	for _, plus := range yesNo {
		for _, zero := range yesNo {
			for _, space := range yesNo {
				for _, char := range "fFeEgG" {
					realFmt := "%"
					if zero {
						realFmt += "0"
					}
					if space {
						realFmt += " "
					}
					if plus {
						realFmt += "+"
					}
					realFmt += "10.2"
					realFmt += string(char)
					// Imaginary part always has a sign, so force + and ignore space.
					imagFmt := "%"
					if zero {
						imagFmt += "0"
					}
					imagFmt += "+"
					imagFmt += "10.2"
					imagFmt += string(char)
					for _, realValue := range values {
						for _, imagValue := range values {
							one := p.Sprintf(realFmt, complex(realValue, imagValue))
							two := p.Sprintf("("+realFmt+imagFmt+"i)", realValue, imagValue)
							if math.IsNaN(imagValue) {
								p := len(two) - len("NaNi)") - 1
								if two[p] == ' ' {
									two = two[:p] + "+" + two[p+1:]
								} else {
									two = two[:p+1] + "+" + two[p+1:]
								}
							}
							if one != two {
								t.Error(f, one, two)
							}
						}
					}
				}
			}
		}
	}
}

type SE []interface{} // slice of empty; notational compactness.

var reorderTests = []struct {
	format string
	args   SE
	out    string
}{
	{"%[1]d", SE{1}, "1"},
	{"%[2]d", SE{2, 1}, "1"},
	{"%[2]d %[1]d", SE{1, 2}, "2 1"},
	{"%[2]*[1]d", SE{2, 5}, "    2"},
	{"%6.2f", SE{12.0}, " 12.00"}, // Explicit version of next line.
	{"%[3]*.[2]*[1]f", SE{12.0, 2, 6}, " 12.00"},
	{"%[1]*.[2]*[3]f", SE{6, 2, 12.0}, " 12.00"},
	{"%10f", SE{12.0}, " 12.000000"},
	{"%[1]*[3]f", SE{10, 99, 12.0}, " 12.000000"},
	{"%.6f", SE{12.0}, "12.000000"}, // Explicit version of next line.
	{"%.[1]*[3]f", SE{6, 99, 12.0}, "12.000000"},
	{"%6.f", SE{12.0}, "    12"}, //  // Explicit version of next line; empty precision means zero.
	{"%[1]*.[3]f", SE{6, 3, 12.0}, "    12"},
	// An actual use! Print the same arguments twice.
	{"%d %d %d %#[1]o %#o %#o", SE{11, 12, 13}, "11 12 13 013 014 015"},

	// Erroneous cases.
	{"%[d", SE{2, 1}, "%!d(BADINDEX)"},
	{"%]d", SE{2, 1}, "%!](int=2)d%!(EXTRA int=1)"},
	{"%[]d", SE{2, 1}, "%!d(BADINDEX)"},
	{"%[-3]d", SE{2, 1}, "%!d(BADINDEX)"},
	{"%[99]d", SE{2, 1}, "%!d(BADINDEX)"},
	{"%[3]", SE{2, 1}, "%!(NOVERB)"},
	{"%[1].2d", SE{5, 6}, "%!d(BADINDEX)"},
	{"%[1]2d", SE{2, 1}, "%!d(BADINDEX)"},
	{"%3.[2]d", SE{7}, "%!d(BADINDEX)"},
	{"%.[2]d", SE{7}, "%!d(BADINDEX)"},
	{"%d %d %d %#[1]o %#o %#o %#o", SE{11, 12, 13}, "11 12 13 013 014 015 %!o(MISSING)"},
	{"%[5]d %[2]d %d", SE{1, 2, 3}, "%!d(BADINDEX) 2 3"},
	{"%d %[3]d %d", SE{1, 2}, "1 %!d(BADINDEX) 2"}, // Erroneous index does not affect sequence.
	{"%.[]", SE{}, "%!](BADINDEX)"},                // Issue 10675
	{"%.-3d", SE{42}, "%!-(int=42)3d"},             // TODO: Should this set return better error messages?
	// The following messages are interpreted as if there is no substitution,
	// in which case it is okay to have extra arguments. This is different
	// semantics from the fmt package.
	{"%2147483648d", SE{42}, "%!(NOVERB)"},
	{"%-2147483648d", SE{42}, "%!(NOVERB)"},
	{"%.2147483648d", SE{42}, "%!(NOVERB)"},
}

func TestReorder(t *testing.T) {
	p := NewPrinter(language.Und)
	for _, tc := range reorderTests {
		t.Run(fmt.Sprint(tc.format, "/", tc.args), func(t *testing.T) {
			s := p.Sprintf(tc.format, tc.args...)
			if s != tc.out {
				t.Errorf("Sprintf(%q, %v) = %q want %q", tc.format, tc.args, s, tc.out)
			}
		})
	}
}

func BenchmarkSprintfPadding(b *testing.B) {
	b.RunParallel(func(pb *testing.PB) {
		p := NewPrinter(language.English)
		for pb.Next() {
			p.Sprintf("%16f", 1.0)
		}
	})
}

func BenchmarkSprintfEmpty(b *testing.B) {
	b.RunParallel(func(pb *testing.PB) {
		p := NewPrinter(language.English)
		for pb.Next() {
			p.Sprintf("")
		}
	})
}

func BenchmarkSprintfString(b *testing.B) {
	b.RunParallel(func(pb *testing.PB) {
		p := NewPrinter(language.English)
		for pb.Next() {
			p.Sprintf("%s", "hello")
		}
	})
}

func BenchmarkSprintfTruncateString(b *testing.B) {
	b.RunParallel(func(pb *testing.PB) {
		p := NewPrinter(language.English)
		for pb.Next() {
			p.Sprintf("%.3s", "日本語日本語日本語")
		}
	})
}

func BenchmarkSprintfQuoteString(b *testing.B) {
	b.RunParallel(func(pb *testing.PB) {
		p := NewPrinter(language.English)
		for pb.Next() {
			p.Sprintf("%q", "日本語日本語日本語")
		}
	})
}

func BenchmarkSprintfInt(b *testing.B) {
	b.RunParallel(func(pb *testing.PB) {
		p := NewPrinter(language.English)
		for pb.Next() {
			p.Sprintf("%d", 5)
		}
	})
}

func BenchmarkSprintfIntInt(b *testing.B) {
	b.RunParallel(func(pb *testing.PB) {
		p := NewPrinter(language.English)
		for pb.Next() {
			p.Sprintf("%d %d", 5, 6)
		}
	})
}

func BenchmarkSprintfPrefixedInt(b *testing.B) {
	b.RunParallel(func(pb *testing.PB) {
		p := NewPrinter(language.English)
		for pb.Next() {
			p.Sprintf("This is some meaningless prefix text that needs to be scanned %d", 6)
		}
	})
}

func BenchmarkSprintfFloat(b *testing.B) {
	b.RunParallel(func(pb *testing.PB) {
		p := NewPrinter(language.English)
		for pb.Next() {
			p.Sprintf("%g", 5.23184)
		}
	})
}

func BenchmarkSprintfComplex(b *testing.B) {
	b.RunParallel(func(pb *testing.PB) {
		p := NewPrinter(language.English)
		for pb.Next() {
			p.Sprintf("%f", 5.23184+5.23184i)
		}
	})
}

func BenchmarkSprintfBoolean(b *testing.B) {
	b.RunParallel(func(pb *testing.PB) {
		p := NewPrinter(language.English)
		for pb.Next() {
			p.Sprintf("%t", true)
		}
	})
}

func BenchmarkSprintfHexString(b *testing.B) {
	b.RunParallel(func(pb *testing.PB) {
		p := NewPrinter(language.English)
		for pb.Next() {
			p.Sprintf("% #x", "0123456789abcdef")
		}
	})
}

func BenchmarkSprintfHexBytes(b *testing.B) {
	data := []byte("0123456789abcdef")
	b.RunParallel(func(pb *testing.PB) {
		p := NewPrinter(language.English)
		for pb.Next() {
			p.Sprintf("% #x", data)
		}
	})
}

func BenchmarkSprintfBytes(b *testing.B) {
	data := []byte("0123456789abcdef")
	b.RunParallel(func(pb *testing.PB) {
		p := NewPrinter(language.English)
		for pb.Next() {
			p.Sprintf("%v", data)
		}
	})
}

func BenchmarkSprintfStringer(b *testing.B) {
	stringer := I(12345)
	b.RunParallel(func(pb *testing.PB) {
		p := NewPrinter(language.English)
		for pb.Next() {
			p.Sprintf("%v", stringer)
		}
	})
}

func BenchmarkSprintfStructure(b *testing.B) {
	s := &[]interface{}{SI{12345}, map[int]string{0: "hello"}}
	b.RunParallel(func(pb *testing.PB) {
		p := NewPrinter(language.English)
		for pb.Next() {
			p.Sprintf("%#v", s)
		}
	})
}

func BenchmarkManyArgs(b *testing.B) {
	b.RunParallel(func(pb *testing.PB) {
		var buf bytes.Buffer
		p := NewPrinter(language.English)
		for pb.Next() {
			buf.Reset()
			p.Fprintf(&buf, "%2d/%2d/%2d %d:%d:%d %s %s\n", 3, 4, 5, 11, 12, 13, "hello", "world")
		}
	})
}

func BenchmarkFprintInt(b *testing.B) {
	var buf bytes.Buffer
	p := NewPrinter(language.English)
	for i := 0; i < b.N; i++ {
		buf.Reset()
		p.Fprint(&buf, 123456)
	}
}

func BenchmarkFprintfBytes(b *testing.B) {
	data := []byte(string("0123456789"))
	var buf bytes.Buffer
	p := NewPrinter(language.English)
	for i := 0; i < b.N; i++ {
		buf.Reset()
		p.Fprintf(&buf, "%s", data)
	}
}

func BenchmarkFprintIntNoAlloc(b *testing.B) {
	var x interface{} = 123456
	var buf bytes.Buffer
	p := NewPrinter(language.English)
	for i := 0; i < b.N; i++ {
		buf.Reset()
		p.Fprint(&buf, x)
	}
}

var mallocBuf bytes.Buffer
var mallocPointer *int // A pointer so we know the interface value won't allocate.

var mallocTest = []struct {
	count int
	desc  string
	fn    func(p *Printer)
}{
	{0, `Sprintf("")`, func(p *Printer) { p.Sprintf("") }},
	{1, `Sprintf("xxx")`, func(p *Printer) { p.Sprintf("xxx") }},
	{2, `Sprintf("%x")`, func(p *Printer) { p.Sprintf("%x", 7) }},
	{2, `Sprintf("%s")`, func(p *Printer) { p.Sprintf("%s", "hello") }},
	{3, `Sprintf("%x %x")`, func(p *Printer) { p.Sprintf("%x %x", 7, 112) }},
	{2, `Sprintf("%g")`, func(p *Printer) { p.Sprintf("%g", float32(3.14159)) }}, // TODO: Can this be 1?
	{1, `Fprintf(buf, "%s")`, func(p *Printer) { mallocBuf.Reset(); p.Fprintf(&mallocBuf, "%s", "hello") }},
	// If the interface value doesn't need to allocate, amortized allocation overhead should be zero.
	{0, `Fprintf(buf, "%x %x %x")`, func(p *Printer) {
		mallocBuf.Reset()
		p.Fprintf(&mallocBuf, "%x %x %x", mallocPointer, mallocPointer, mallocPointer)
	}},
}

var _ bytes.Buffer

func TestCountMallocs(t *testing.T) {
	switch {
	case testing.Short():
		t.Skip("skipping malloc count in short mode")
	case runtime.GOMAXPROCS(0) > 1:
		t.Skip("skipping; GOMAXPROCS>1")
		// TODO: detect race detecter enabled.
		// case race.Enabled:
		// 	t.Skip("skipping malloc count under race detector")
	}
	p := NewPrinter(language.English)
	for _, mt := range mallocTest {
		mallocs := testing.AllocsPerRun(100, func() { mt.fn(p) })
		if got, max := mallocs, float64(mt.count); got > max {
			t.Errorf("%s: got %v allocs, want <=%v", mt.desc, got, max)
		}
	}
}

type flagPrinter struct{}

func (flagPrinter) Format(f fmt.State, c rune) {
	s := "%"
	for i := 0; i < 128; i++ {
		if f.Flag(i) {
			s += string(i)
		}
	}
	if w, ok := f.Width(); ok {
		s += fmt.Sprintf("%d", w)
	}
	if p, ok := f.Precision(); ok {
		s += fmt.Sprintf(".%d", p)
	}
	s += string(c)
	io.WriteString(f, "["+s+"]")
}

var flagtests = []struct {
	in  string
	out string
}{
	{"%a", "[%a]"},
	{"%-a", "[%-a]"},
	{"%+a", "[%+a]"},
	{"%#a", "[%#a]"},
	{"% a", "[% a]"},
	{"%0a", "[%0a]"},
	{"%1.2a", "[%1.2a]"},
	{"%-1.2a", "[%-1.2a]"},
	{"%+1.2a", "[%+1.2a]"},
	{"%-+1.2a", "[%+-1.2a]"},
	{"%-+1.2abc", "[%+-1.2a]bc"},
	{"%-1.2abc", "[%-1.2a]bc"},
}

func TestFlagParser(t *testing.T) {
	var flagprinter flagPrinter
	for _, tt := range flagtests {
		s := NewPrinter(language.Und).Sprintf(tt.in, &flagprinter)
		if s != tt.out {
			t.Errorf("Sprintf(%q, &flagprinter) => %q, want %q", tt.in, s, tt.out)
		}
	}
}

func TestStructPrinter(t *testing.T) {
	type T struct {
		a string
		b string
		c int
	}
	var s T
	s.a = "abc"
	s.b = "def"
	s.c = 123
	var tests = []struct {
		fmt string
		out string
	}{
		{"%v", "{abc def 123}"},
		{"%+v", "{a:abc b:def c:123}"},
		{"%#v", `message.T{a:"abc", b:"def", c:123}`},
	}
	p := NewPrinter(language.Und)
	for _, tt := range tests {
		out := p.Sprintf(tt.fmt, s)
		if out != tt.out {
			t.Errorf("Sprintf(%q, s) = %#q, want %#q", tt.fmt, out, tt.out)
		}
		// The same but with a pointer.
		out = p.Sprintf(tt.fmt, &s)
		if out != "&"+tt.out {
			t.Errorf("Sprintf(%q, &s) = %#q, want %#q", tt.fmt, out, "&"+tt.out)
		}
	}
}

func TestSlicePrinter(t *testing.T) {
	p := NewPrinter(language.Und)
	slice := []int{}
	s := p.Sprint(slice)
	if s != "[]" {
		t.Errorf("empty slice printed as %q not %q", s, "[]")
	}
	slice = []int{1, 2, 3}
	s = p.Sprint(slice)
	if s != "[1 2 3]" {
		t.Errorf("slice: got %q expected %q", s, "[1 2 3]")
	}
	s = p.Sprint(&slice)
	if s != "&[1 2 3]" {
		t.Errorf("&slice: got %q expected %q", s, "&[1 2 3]")
	}
}

// presentInMap checks map printing using substrings so we don't depend on the
// print order.
func presentInMap(s string, a []string, t *testing.T) {
	for i := 0; i < len(a); i++ {
		loc := strings.Index(s, a[i])
		if loc < 0 {
			t.Errorf("map print: expected to find %q in %q", a[i], s)
		}
		// make sure the match ends here
		loc += len(a[i])
		if loc >= len(s) || (s[loc] != ' ' && s[loc] != ']') {
			t.Errorf("map print: %q not properly terminated in %q", a[i], s)
		}
	}
}

func TestMapPrinter(t *testing.T) {
	p := NewPrinter(language.Und)
	m0 := make(map[int]string)
	s := p.Sprint(m0)
	if s != "map[]" {
		t.Errorf("empty map printed as %q not %q", s, "map[]")
	}
	m1 := map[int]string{1: "one", 2: "two", 3: "three"}
	a := []string{"1:one", "2:two", "3:three"}
	presentInMap(p.Sprintf("%v", m1), a, t)
	presentInMap(p.Sprint(m1), a, t)
	// Pointer to map prints the same but with initial &.
	if !strings.HasPrefix(p.Sprint(&m1), "&") {
		t.Errorf("no initial & for address of map")
	}
	presentInMap(p.Sprintf("%v", &m1), a, t)
	presentInMap(p.Sprint(&m1), a, t)
}

func TestEmptyMap(t *testing.T) {
	const emptyMapStr = "map[]"
	var m map[string]int
	p := NewPrinter(language.Und)
	s := p.Sprint(m)
	if s != emptyMapStr {
		t.Errorf("nil map printed as %q not %q", s, emptyMapStr)
	}
	m = make(map[string]int)
	s = p.Sprint(m)
	if s != emptyMapStr {
		t.Errorf("empty map printed as %q not %q", s, emptyMapStr)
	}
}

// TestBlank checks that Sprint (and hence Print, Fprint) puts spaces in the
// right places, that is, between arg pairs in which neither is a string.
func TestBlank(t *testing.T) {
	p := NewPrinter(language.Und)
	got := p.Sprint("<", 1, ">:", 1, 2, 3, "!")
	expect := "<1>:1 2 3!"
	if got != expect {
		t.Errorf("got %q expected %q", got, expect)
	}
}

// TestBlankln checks that Sprintln (and hence Println, Fprintln) puts spaces in
// the right places, that is, between all arg pairs.
func TestBlankln(t *testing.T) {
	p := NewPrinter(language.Und)
	got := p.Sprintln("<", 1, ">:", 1, 2, 3, "!")
	expect := "< 1 >: 1 2 3 !\n"
	if got != expect {
		t.Errorf("got %q expected %q", got, expect)
	}
}

// TestFormatterPrintln checks Formatter with Sprint, Sprintln, Sprintf.
func TestFormatterPrintln(t *testing.T) {
	p := NewPrinter(language.Und)
	f := F(1)
	expect := "<v=F(1)>\n"
	s := p.Sprint(f, "\n")
	if s != expect {
		t.Errorf("Sprint wrong with Formatter: expected %q got %q", expect, s)
	}
	s = p.Sprintln(f)
	if s != expect {
		t.Errorf("Sprintln wrong with Formatter: expected %q got %q", expect, s)
	}
	s = p.Sprintf("%v\n", f)
	if s != expect {
		t.Errorf("Sprintf wrong with Formatter: expected %q got %q", expect, s)
	}
}

func args(a ...interface{}) []interface{} { return a }

var startests = []struct {
	fmt string
	in  []interface{}
	out string
}{
	{"%*d", args(4, 42), "  42"},
	{"%-*d", args(4, 42), "42  "},
	{"%*d", args(-4, 42), "42  "},
	{"%-*d", args(-4, 42), "42  "},
	{"%.*d", args(4, 42), "0,042"},
	{"%*.*d", args(8, 4, 42), "   0,042"},
	{"%0*d", args(4, 42), "0,042"},
	// Some non-int types for width. (Issue 10732).
	{"%0*d", args(uint(4), 42), "0,042"},
	{"%0*d", args(uint64(4), 42), "0,042"},
	{"%0*d", args('\x04', 42), "0,042"},
	{"%0*d", args(uintptr(4), 42), "0,042"},

	// erroneous
	{"%*d", args(nil, 42), "%!(BADWIDTH)42"},
	{"%*d", args(int(1e7), 42), "%!(BADWIDTH)42"},
	{"%*d", args(int(-1e7), 42), "%!(BADWIDTH)42"},
	{"%.*d", args(nil, 42), "%!(BADPREC)42"},
	{"%.*d", args(-1, 42), "%!(BADPREC)42"},
	{"%.*d", args(int(1e7), 42), "%!(BADPREC)42"},
	{"%.*d", args(uint(1e7), 42), "%!(BADPREC)42"},
	{"%.*d", args(uint64(1<<63), 42), "%!(BADPREC)42"},   // Huge negative (-inf).
	{"%.*d", args(uint64(1<<64-1), 42), "%!(BADPREC)42"}, // Small negative (-1).
	{"%*d", args(5, "foo"), "%!d(string=  foo)"},
	{"%*% %d", args(20, 5), "% 5"},
	{"%*", args(4), "%!(NOVERB)"},
}

func TestWidthAndPrecision(t *testing.T) {
	p := NewPrinter(language.Und)
	for i, tt := range startests {
		t.Run(fmt.Sprint(tt.fmt, tt.in), func(t *testing.T) {
			s := p.Sprintf(tt.fmt, tt.in...)
			if s != tt.out {
				t.Errorf("#%d: %q: got %q expected %q", i, tt.fmt, s, tt.out)
			}
		})
	}
}

// PanicS is a type that panics in String.
type PanicS struct {
	message interface{}
}

// Value receiver.
func (p PanicS) String() string {
	panic(p.message)
}

// PanicGo is a type that panics in GoString.
type PanicGo struct {
	message interface{}
}

// Value receiver.
func (p PanicGo) GoString() string {
	panic(p.message)
}

// PanicF is a type that panics in Format.
type PanicF struct {
	message interface{}
}

// Value receiver.
func (p PanicF) Format(f fmt.State, c rune) {
	panic(p.message)
}

var panictests = []struct {
	desc string
	fmt  string
	in   interface{}
	out  string
}{
	// String
	{"String", "%s", (*PanicS)(nil), "<nil>"}, // nil pointer special case
	{"String", "%s", PanicS{io.ErrUnexpectedEOF}, "%!s(PANIC=unexpected EOF)"},
	{"String", "%s", PanicS{3}, "%!s(PANIC=3)"},
	// GoString
	{"GoString", "%#v", (*PanicGo)(nil), "<nil>"}, // nil pointer special case
	{"GoString", "%#v", PanicGo{io.ErrUnexpectedEOF}, "%!v(PANIC=unexpected EOF)"},
	{"GoString", "%#v", PanicGo{3}, "%!v(PANIC=3)"},
	// Issue 18282. catchPanic should not clear fmtFlags permanently.
	{"Issue 18282", "%#v", []interface{}{PanicGo{3}, PanicGo{3}}, "[]interface {}{%!v(PANIC=3), %!v(PANIC=3)}"},
	// Format
	{"Format", "%s", (*PanicF)(nil), "<nil>"}, // nil pointer special case
	{"Format", "%s", PanicF{io.ErrUnexpectedEOF}, "%!s(PANIC=unexpected EOF)"},
	{"Format", "%s", PanicF{3}, "%!s(PANIC=3)"},
}

func TestPanics(t *testing.T) {
	p := NewPrinter(language.Und)
	for i, tt := range panictests {
		t.Run(fmt.Sprint(tt.desc, "/", tt.fmt, "/", tt.in), func(t *testing.T) {
			s := p.Sprintf(tt.fmt, tt.in)
			if s != tt.out {
				t.Errorf("%d: %q: got %q expected %q", i, tt.fmt, s, tt.out)
			}
		})
	}
}

// recurCount tests that erroneous String routine doesn't cause fatal recursion.
var recurCount = 0

type Recur struct {
	i      int
	failed *bool
}

func (r *Recur) String() string {
	p := NewPrinter(language.Und)
	if recurCount++; recurCount > 10 {
		*r.failed = true
		return "FAIL"
	}
	// This will call badVerb. Before the fix, that would cause us to recur into
	// this routine to print %!p(value). Now we don't call the user's method
	// during an error.
	return p.Sprintf("recur@%p value: %d", r, r.i)
}

func TestBadVerbRecursion(t *testing.T) {
	p := NewPrinter(language.Und)
	failed := false
	r := &Recur{3, &failed}
	p.Sprintf("recur@%p value: %d\n", &r, r.i)
	if failed {
		t.Error("fail with pointer")
	}
	failed = false
	r = &Recur{4, &failed}
	p.Sprintf("recur@%p, value: %d\n", r, r.i)
	if failed {
		t.Error("fail with value")
	}
}

func TestNilDoesNotBecomeTyped(t *testing.T) {
	p := NewPrinter(language.Und)
	type A struct{}
	type B struct{}
	var a *A = nil
	var b B = B{}

	// indirect the Sprintf call through this noVetWarn variable to avoid
	// "go test" failing vet checks in Go 1.10+.
	noVetWarn := p.Sprintf
	got := noVetWarn("%s %s %s %s %s", nil, a, nil, b, nil)

	const expect = "%!s(<nil>) %!s(*message.A=<nil>) %!s(<nil>) {} %!s(<nil>)"
	if got != expect {
		t.Errorf("expected:\n\t%q\ngot:\n\t%q", expect, got)
	}
}

var formatterFlagTests = []struct {
	in  string
	val interface{}
	out string
}{
	// scalar values with the (unused by fmt) 'a' verb.
	{"%a", flagPrinter{}, "[%a]"},
	{"%-a", flagPrinter{}, "[%-a]"},
	{"%+a", flagPrinter{}, "[%+a]"},
	{"%#a", flagPrinter{}, "[%#a]"},
	{"% a", flagPrinter{}, "[% a]"},
	{"%0a", flagPrinter{}, "[%0a]"},
	{"%1.2a", flagPrinter{}, "[%1.2a]"},
	{"%-1.2a", flagPrinter{}, "[%-1.2a]"},
	{"%+1.2a", flagPrinter{}, "[%+1.2a]"},
	{"%-+1.2a", flagPrinter{}, "[%+-1.2a]"},
	{"%-+1.2abc", flagPrinter{}, "[%+-1.2a]bc"},
	{"%-1.2abc", flagPrinter{}, "[%-1.2a]bc"},

	// composite values with the 'a' verb
	{"%a", [1]flagPrinter{}, "[[%a]]"},
	{"%-a", [1]flagPrinter{}, "[[%-a]]"},
	{"%+a", [1]flagPrinter{}, "[[%+a]]"},
	{"%#a", [1]flagPrinter{}, "[[%#a]]"},
	{"% a", [1]flagPrinter{}, "[[% a]]"},
	{"%0a", [1]flagPrinter{}, "[[%0a]]"},
	{"%1.2a", [1]flagPrinter{}, "[[%1.2a]]"},
	{"%-1.2a", [1]flagPrinter{}, "[[%-1.2a]]"},
	{"%+1.2a", [1]flagPrinter{}, "[[%+1.2a]]"},
	{"%-+1.2a", [1]flagPrinter{}, "[[%+-1.2a]]"},
	{"%-+1.2abc", [1]flagPrinter{}, "[[%+-1.2a]]bc"},
	{"%-1.2abc", [1]flagPrinter{}, "[[%-1.2a]]bc"},

	// simple values with the 'v' verb
	{"%v", flagPrinter{}, "[%v]"},
	{"%-v", flagPrinter{}, "[%-v]"},
	{"%+v", flagPrinter{}, "[%+v]"},
	{"%#v", flagPrinter{}, "[%#v]"},
	{"% v", flagPrinter{}, "[% v]"},
	{"%0v", flagPrinter{}, "[%0v]"},
	{"%1.2v", flagPrinter{}, "[%1.2v]"},
	{"%-1.2v", flagPrinter{}, "[%-1.2v]"},
	{"%+1.2v", flagPrinter{}, "[%+1.2v]"},
	{"%-+1.2v", flagPrinter{}, "[%+-1.2v]"},
	{"%-+1.2vbc", flagPrinter{}, "[%+-1.2v]bc"},
	{"%-1.2vbc", flagPrinter{}, "[%-1.2v]bc"},

	// composite values with the 'v' verb.
	{"%v", [1]flagPrinter{}, "[[%v]]"},
	{"%-v", [1]flagPrinter{}, "[[%-v]]"},
	{"%+v", [1]flagPrinter{}, "[[%+v]]"},
	{"%#v", [1]flagPrinter{}, "[1]message.flagPrinter{[%#v]}"},
	{"% v", [1]flagPrinter{}, "[[% v]]"},
	{"%0v", [1]flagPrinter{}, "[[%0v]]"},
	{"%1.2v", [1]flagPrinter{}, "[[%1.2v]]"},
	{"%-1.2v", [1]flagPrinter{}, "[[%-1.2v]]"},
	{"%+1.2v", [1]flagPrinter{}, "[[%+1.2v]]"},
	{"%-+1.2v", [1]flagPrinter{}, "[[%+-1.2v]]"},
	{"%-+1.2vbc", [1]flagPrinter{}, "[[%+-1.2v]]bc"},
	{"%-1.2vbc", [1]flagPrinter{}, "[[%-1.2v]]bc"},
}

func TestFormatterFlags(t *testing.T) {
	p := NewPrinter(language.Und)
	for _, tt := range formatterFlagTests {
		s := p.Sprintf(tt.in, tt.val)
		if s != tt.out {
			t.Errorf("Sprintf(%q, %T) = %q, want %q", tt.in, tt.val, s, tt.out)
		}
	}
}
