package uuid

/****************
 * Date: 3/02/14
 * Time: 10:59 PM
 ***************/

import (
	"crypto/md5"
	"crypto/sha1"
	"fmt"
	"regexp"
	"strings"
	"testing"
)

const (
	secondsPerHour       = 60 * 60
	secondsPerDay        = 24 * secondsPerHour
	unixToInternal int64 = (1969*365 + 1969/4 - 1969/100 + 1969/400) * secondsPerDay
	internalToUnix int64 = -unixToInternal
)

var (
	uuid_goLang Name = "https://google.com/golang.org?q=golang"
	printer     bool = false
	uuid_bytes       = []byte{
		0xAA, 0xCF, 0xEE, 0x12,
		0xD4, 0x00,
		0x27, 0x23,
		0x00,
		0xD3,
		0x23, 0x12, 0x4A, 0x11, 0x89, 0xFF,
	}
	uuid_variants = []byte{
		ReservedNCS, ReservedRFC4122, ReservedMicrosoft, ReservedFuture,
	}
	namespaceUuids = []UUID{
		NamespaceDNS, NamespaceURL, NamespaceOID, NamespaceX500,
	}
	invalidHexStrings = [...]string{
		"foo",
		"6ba7b814-9dad-11d1-80b4-",
		"6ba7b814--9dad-11d1-80b4--00c04fd430c8",
		"6ba7b814-9dad7-11d1-80b4-00c04fd430c8999",
		"{6ba7b814-9dad-1180b4-00c04fd430c8",
		"{6ba7b814--11d1-80b4-00c04fd430c8}",
		"urn:uuid:6ba7b814-9dad-1666666680b4-00c04fd430c8",
	}
	validHexStrings = [...]string{
		"6ba7b8149dad-11d1-80b4-00c04fd430c8}",
		"{6ba7b8149dad-11d1-80b400c04fd430c8}",
		"{6ba7b814-9dad11d180b400c04fd430c8}",
		"6ba7b8149dad-11d1-80b4-00c04fd430c8",
		"6ba7b814-9dad11d1-80b4-00c04fd430c8",
		"6ba7b814-9dad-11d180b4-00c04fd430c8",
		"6ba7b814-9dad-11d1-80b400c04fd430c8",
		"6ba7b8149dad11d180b400c04fd430c8",
		"6ba7b814-9dad-11d1-80b4-00c04fd430c8",
		"{6ba7b814-9dad-11d1-80b4-00c04fd430c8}",
		"{6ba7b814-9dad-11d1-80b4-00c04fd430c8",
		"6ba7b814-9dad-11d1-80b4-00c04fd430c8}",
		"(6ba7b814-9dad-11d1-80b4-00c04fd430c8)",
		"urn:uuid:6ba7b814-9dad-11d1-80b4-00c04fd430c8",
	}
)

func TestUUID_Simple(t *testing.T) {
	u := NewV1()
	outputLn(u)

	u, _ = Parse("6ba7b810-9dad-11d1-80b4-00c04fd430c8")
	u = NewV3(u, Name("test"))
	outputLn(u)
	if !strings.EqualFold("45a113ac-c7f2-30b0-90a5-a399ab912716", u.String()) {
		t.Errorf("Expected string representation to be %s, but got: %s", "45a113ac-c7f2-30b0-90a5-a399ab912716", u.String())
	}

	u = NewV4()
	outputLn(u)

	u = NewV5(u, Name("test"))
	outputLn(u)
}

func TestUUID_New(t *testing.T) {
	u := New(uuid_bytes)
	if u == nil {
		t.Error("Expected a valid UUID")
	}
	if u.Version() != 2 {
		t.Errorf("Expected correct version %d, but got %d", 2, u.Version())
	}
	if u.Variant() != ReservedNCS {
		t.Errorf("Expected ReservedNCS variant %x, but got %x", ReservedNCS, u.Variant())
	}
	if !parseUUIDRegex.MatchString(u.String()) {
		t.Errorf("Expected string representation to be valid, given: %s", u.String())
	}
}

func TestUUID_NewBulk(t *testing.T) {
	for i := 0; i < 1000000; i++ {
		New(uuid_bytes)
	}
}


const (
	clean                   = `[A-Fa-f0-9]{8}[A-Fa-f0-9]{4}[1-5fF][A-Fa-f0-9]{3}[A-Fa-f0-9]{4}[A-Fa-f0-9]{12}`
	cleanHexPattern         = `^` + clean + `$`
	curlyHexPattern         = `^\{` + clean + `\}$`
	bracketHexPattern       = `^\(` + clean + `\)$`
	hyphen                  = `[A-Fa-f0-9]{8}-[A-Fa-f0-9]{4}-[1-5fF][A-Fa-f0-9]{3}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{12}`
	cleanHyphenHexPattern   = `^` + hyphen + `$`
	curlyHyphenHexPattern   = `^\{` + hyphen + `\}$`
	bracketHyphenHexPattern = `^\(` + hyphen + `\)$`
	goIdHexPattern          = `^\[[A-F0-9]{8}-[A-F0-9]{4}-[1-5fF][a-f0-9]{3}-[A-F0-9]{4}-[a-f0-9]{12}\]$`
)

func TestUUID_Formats_String(t *testing.T) {
	ids := []UUID{NewV4(), NewV1()}

	// Reset default
	SwitchFormat(CleanHyphen)

	for _, u := range ids {

		SwitchFormatUpperCase(CurlyHyphen)
		if !regexp.MustCompile(curlyHyphenHexPattern).MatchString(u.String()) {
			t.Error("Curly hyphen UUID string output got", u, u.Version())
		}
		outputLn(u)

		// Clean
		SwitchFormat(Clean)
		if !regexp.MustCompile(cleanHexPattern).MatchString(u.String()) {
			t.Error("Clean UUID string output got", u, u.Version())
		}
		outputLn(u)

		// Curly
		SwitchFormat(Curly)
		if !regexp.MustCompile(curlyHexPattern).MatchString(u.String()) {
			t.Error("Curly clean UUID string output got", u, u.Version())
		}
		outputLn(u)

		// Bracket
		SwitchFormat(Bracket)
		if !regexp.MustCompile(bracketHexPattern).MatchString(u.String()) {
			t.Error("Bracket clean UUID string output got", u, u.Version())
		}
		outputLn(u)

		// Clean Hyphen
		SwitchFormat(CleanHyphen)
		if !regexp.MustCompile(cleanHyphenHexPattern).MatchString(u.String()) {
			t.Error("Clean hyphen UUID string output got", u, u.Version())
		}
		outputLn(u)

		// Bracket Hyphen
		SwitchFormat(BracketHyphen)
		if !regexp.MustCompile(bracketHyphenHexPattern).MatchString(u.String()) {
			t.Error("Bracket hyphen UUID string output got", u, u.Version())
		}
		outputLn(u)

		// Bracket Hyphen
		SwitchFormat(GoIdFormat)
		if !regexp.MustCompile(goIdHexPattern).MatchString(u.String()) {
			t.Error("GoId UUID string output expected", u, u.Version())
		}
		outputLn(u)

		// Reset default
		SwitchFormat(CleanHyphen)
	}
}

func TestUUID_Formatter(t *testing.T) {
	ids := []UUID{NewV4(), NewV1()}

	for _, u := range ids {
		// CurlyHyphen - default
		if !regexp.MustCompile(curlyHyphenHexPattern).MatchString(Formatter(u, CurlyHyphen)) {
			t.Error("Curly hyphen UUID string output got", Formatter(u, CurlyHyphen))
		}
		outputLn(Formatter(u, CurlyHyphen))

		// Clean
		if !regexp.MustCompile(cleanHexPattern).MatchString(Formatter(u, Clean)) {
			t.Error("Clean UUID string output got", Formatter(u, Clean))
		}
		outputLn(Formatter(u, Clean))

		// Curly
		if !regexp.MustCompile(curlyHexPattern).MatchString(Formatter(u, Curly)) {
			t.Error("Curly clean UUID string output", Formatter(u, Curly))
		}
		outputLn(Formatter(u, Curly))

		// Bracket
		if !regexp.MustCompile(bracketHexPattern).MatchString(Formatter(u, Bracket)) {
			t.Error("Bracket clean UUID string output", Formatter(u, Bracket))
		}
		outputLn(Formatter(u, Bracket))

		// Clean Hyphen
		if !regexp.MustCompile(cleanHyphenHexPattern).MatchString(Formatter(u, CleanHyphen)) {
			t.Error("Clean hyphen UUID string output", Formatter(u, CleanHyphen))
		}
		outputLn(Formatter(u, CleanHyphen))

		// Bracket Hyphen
		if !regexp.MustCompile(bracketHyphenHexPattern).MatchString(Formatter(u, BracketHyphen)) {
			t.Error("Bracket hyphen UUID string output", Formatter(u, BracketHyphen))
		}
		outputLn(Formatter(u, BracketHyphen))

		// GoId Format
		if !regexp.MustCompile(goIdHexPattern).MatchString(Formatter(u, GoIdFormat)) {
			t.Error("GoId UUID string output expected", Formatter(u, GoIdFormat))
		}
		outputLn(Formatter(u, GoIdFormat))
	}
}

func TestUUID_NewHex(t *testing.T) {
	s := "f3593cffee9240df408687825b523f13"
	u := NewHex(s)
	if u == nil {
		t.Error("Expected a valid UUID")
	}
	if u.Version() != 4 {
		t.Errorf("Expected correct version %d, but got %d", 4, u.Version())
	}
	if u.Variant() != ReservedNCS {
		t.Errorf("Expected ReservedNCS variant %x, but got %x", ReservedNCS, u.Variant())
	}
	if !parseUUIDRegex.MatchString(u.String()) {
		t.Errorf("Expected string representation to be valid, given: %s", u.String())
	}
}

func TestUUID_NewHexBulk(t *testing.T) {
	for i := 0; i < 1000000; i++ {
		s := "f3593cffee9240df408687825b523f13"
		NewHex(s)
	}
}

func TestUUID_Parse(t *testing.T) {
	for _, v := range invalidHexStrings {
		_, err := Parse(v)
		if err == nil {
			t.Error("Expected error due to invalid UUID string:", v)
		}
	}
	for _, v := range validHexStrings {
		_, err := Parse(v)
		if err != nil {
			t.Error("Expected valid UUID string but got error:", v)
		}
	}
	for _, id := range namespaceUuids {
		_, err := Parse(id.String())
		if err != nil {
			t.Error("Expected valid UUID string but got error:", err)
		}
	}
}

func TestUUID_Sum(t *testing.T) {
	u := new(Array)
	Digest(u, NamespaceDNS, uuid_goLang, md5.New())
	if u.Bytes() == nil {
		t.Error("Expected new data in bytes")
	}
	output(u.Bytes())
	u = new(Array)
	Digest(u, NamespaceDNS, uuid_goLang, sha1.New())
	if u.Bytes() == nil {
		t.Error("Expected new data in bytes")
	}
	output(u.Bytes())
}

// Tests all possible version numbers and that
// each number returned is the same
func TestUUID_Struct_VersionBits(t *testing.T) {
	uStruct := new(Struct)
	uStruct.size = length
	for v := 0; v < 16; v++ {
		for i := 0; i <= 255; i++ {
			uuid_bytes[versionIndex] = byte(i)
			uStruct.Unmarshal(uuid_bytes)
			uStruct.setVersion(v)
			output(uStruct)
			if uStruct.Version() != v {
				t.Errorf("%x does not resolve to %x", byte(uStruct.Version()), v)
			}
			output("\n")
		}
	}
}

// Tests all possible variants with their respective bits set
// Tests whether the expected output comes out on each byte case
func TestUUID_Struct_VariantBits(t *testing.T) {
	for _, v := range uuid_variants {
		for i := 0; i <= 255; i++ {
			uuid_bytes[variantIndex] = byte(i)

			uStruct := createStruct(uuid_bytes, 4, v)
			b := uStruct.sequenceHiAndVariant >> 4
			tVariantConstraint(v, b, uStruct, t)

			if uStruct.Variant() != v {
				t.Errorf("%d does not resolve to %x: get %x", i, v, uStruct.Variant())
			}
		}
	}
}

// Tests all possible variants with their respective bits set
// Tests whether the expected output comes out on each byte case
func TestUUID_Array_VariantBits(t *testing.T) {
	for _, v := range uuid_variants {
		for i := 0; i <= 255; i++ {
			uuid_bytes[variantIndex] = byte(i)

			uArray := createArray(uuid_bytes, 4, v)
			b := uArray[variantIndex] >> 4
			tVariantConstraint(v, b, uArray, t)

			if uArray.Variant() != v {
				t.Errorf("%d does not resolve to %x", i, v)
			}
		}
	}
}

// Tests all possible version numbers and that
// each number returned is the same
func TestUUID_Array_VersionBits(t *testing.T) {
	uArray := new(Array)
	for v := 0; v < 16; v++ {
		for i := 0; i <= 255; i++ {
			uuid_bytes[versionIndex] = byte(i)
			uArray.Unmarshal(uuid_bytes)
			uArray.setVersion(v)
			output(uArray)
			if uArray.Version() != v {
				t.Errorf("%x does not resolve to %x", byte(uArray.Version()), v)
			}
			output("\n")
		}
	}
}

func BenchmarkUUID_Parse(b *testing.B) {
	s := "f3593cff-ee92-40df-4086-87825b523f13"
	for i := 0; i < b.N; i++ {
		_, err := Parse(s)
		if err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	b.ReportAllocs()
}

// *******************************************************

func createStruct(pData []byte, pVersion int, pVariant byte) *Struct {
	o := new(Struct)
	o.size = length
	o.Unmarshal(pData)
	o.setVersion(pVersion)
	o.setVariant(pVariant)
	return o
}

func createArray(pData []byte, pVersion int, pVariant byte) *Array {
	o := new(Array)
	o.Unmarshal(pData)
	o.setVersion(pVersion)
	o.setVariant(pVariant)
	return o
}

func tVariantConstraint(v byte, b byte, o UUID, t *testing.T) {
	output(o)
	switch v {
	case ReservedNCS:
		switch b {
		case 0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07:
			outputF(": %X ", b)
			break
		default:
			t.Errorf("%X most high bits do not resolve to 0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07", b)
		}
	case ReservedRFC4122:
		switch b {
		case 0x08, 0x09, 0x0A, 0x0B:
			outputF(": %X ", b)
			break
		default:
			t.Errorf("%X most high bits do not resolve to 0x08, 0x09, 0x0A, 0x0B", b)
		}
	case ReservedMicrosoft:
		switch b {
		case 0x0C, 0x0D:
			outputF(": %X ", b)
			break
		default:
			t.Errorf("%X most high bits do not resolve to 0x0C, 0x0D", b)
		}
	case ReservedFuture:
		switch b {
		case 0x0E, 0x0F:
			outputF(": %X ", b)
			break
		default:
			t.Errorf("%X most high bits do not resolve to 0x0E, 0x0F", b)
		}
	}
	output("\n")
}

func output(a ...interface{}) {
	if printer {
		fmt.Print(a...)
	}
}

func outputLn(a ...interface{}) {
	if printer {
		fmt.Println(a...)
	}
}

func outputF(format string, a ...interface{}) {
	if printer {
		fmt.Printf(format, a)
	}
}
