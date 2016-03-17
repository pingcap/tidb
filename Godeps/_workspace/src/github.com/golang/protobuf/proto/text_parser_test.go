// Go support for Protocol Buffers - Google's data interchange format
//
// Copyright 2010 The Go Authors.  All rights reserved.
// https://github.com/golang/protobuf
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are
// met:
//
//     * Redistributions of source code must retain the above copyright
// notice, this list of conditions and the following disclaimer.
//     * Redistributions in binary form must reproduce the above
// copyright notice, this list of conditions and the following disclaimer
// in the documentation and/or other materials provided with the
// distribution.
//     * Neither the name of Google Inc. nor the names of its
// contributors may be used to endorse or promote products derived from
// this software without specific prior written permission.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
// "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
// LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
// A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
// OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
// SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
// LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
// DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
// THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
// (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
// OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

package proto_test

import (
	"math"
	"reflect"
	"testing"

	. "github.com/golang/protobuf/proto"
	proto3pb "github.com/golang/protobuf/proto/proto3_proto"
	. "github.com/golang/protobuf/proto/testdata"
)

type UnmarshalTextTest struct {
	in  string
	err string // if "", no error expected
	out *MyMessage
}

func buildExtStructTest(text string) UnmarshalTextTest {
	msg := &MyMessage{
		Count: Int32(42),
	}
	SetExtension(msg, E_Ext_More, &Ext{
		Data: String("Hello, world!"),
	})
	return UnmarshalTextTest{in: text, out: msg}
}

func buildExtDataTest(text string) UnmarshalTextTest {
	msg := &MyMessage{
		Count: Int32(42),
	}
	SetExtension(msg, E_Ext_Text, String("Hello, world!"))
	SetExtension(msg, E_Ext_Number, Int32(1729))
	return UnmarshalTextTest{in: text, out: msg}
}

func buildExtRepStringTest(text string) UnmarshalTextTest {
	msg := &MyMessage{
		Count: Int32(42),
	}
	if err := SetExtension(msg, E_Greeting, []string{"bula", "hola"}); err != nil {
		panic(err)
	}
	return UnmarshalTextTest{in: text, out: msg}
}

var unMarshalTextTests = []UnmarshalTextTest{
	// Basic
	{
		in: " count:42\n  name:\"Dave\" ",
		out: &MyMessage{
			Count: Int32(42),
			Name:  String("Dave"),
		},
	},

	// Empty quoted string
	{
		in: `count:42 name:""`,
		out: &MyMessage{
			Count: Int32(42),
			Name:  String(""),
		},
	},

	// Quoted string concatenation with double quotes
	{
		in: `count:42 name: "My name is "` + "\n" + `"elsewhere"`,
		out: &MyMessage{
			Count: Int32(42),
			Name:  String("My name is elsewhere"),
		},
	},

	// Quoted string concatenation with single quotes
	{
		in: "count:42 name: 'My name is '\n'elsewhere'",
		out: &MyMessage{
			Count: Int32(42),
			Name:  String("My name is elsewhere"),
		},
	},

	// Quoted string concatenations with mixed quotes
	{
		in: "count:42 name: 'My name is '\n\"elsewhere\"",
		out: &MyMessage{
			Count: Int32(42),
			Name:  String("My name is elsewhere"),
		},
	},
	{
		in: "count:42 name: \"My name is \"\n'elsewhere'",
		out: &MyMessage{
			Count: Int32(42),
			Name:  String("My name is elsewhere"),
		},
	},

	// Quoted string with escaped apostrophe
	{
		in: `count:42 name: "HOLIDAY - New Year\'s Day"`,
		out: &MyMessage{
			Count: Int32(42),
			Name:  String("HOLIDAY - New Year's Day"),
		},
	},

	// Quoted string with single quote
	{
		in: `count:42 name: 'Roger "The Ramster" Ramjet'`,
		out: &MyMessage{
			Count: Int32(42),
			Name:  String(`Roger "The Ramster" Ramjet`),
		},
	},

	// Quoted string with all the accepted special characters from the C++ test
	{
		in: `count:42 name: ` + "\"\\\"A string with \\' characters \\n and \\r newlines and \\t tabs and \\001 slashes \\\\ and  multiple   spaces\"",
		out: &MyMessage{
			Count: Int32(42),
			Name:  String("\"A string with ' characters \n and \r newlines and \t tabs and \001 slashes \\ and  multiple   spaces"),
		},
	},

	// Quoted string with quoted backslash
	{
		in: `count:42 name: "\\'xyz"`,
		out: &MyMessage{
			Count: Int32(42),
			Name:  String(`\'xyz`),
		},
	},

	// Quoted string with UTF-8 bytes.
	{
		in: "count:42 name: '\303\277\302\201\xAB'",
		out: &MyMessage{
			Count: Int32(42),
			Name:  String("\303\277\302\201\xAB"),
		},
	},

	// Bad quoted string
	{
		in:  `inner: < host: "\0" >` + "\n",
		err: `line 1.15: invalid quoted string "\0": \0 requires 2 following digits`,
	},

	// Number too large for int64
	{
		in:  "count: 1 others { key: 123456789012345678901 }",
		err: "line 1.23: invalid int64: 123456789012345678901",
	},

	// Number too large for int32
	{
		in:  "count: 1234567890123",
		err: "line 1.7: invalid int32: 1234567890123",
	},

	// Number in hexadecimal
	{
		in: "count: 0x2beef",
		out: &MyMessage{
			Count: Int32(0x2beef),
		},
	},

	// Number in octal
	{
		in: "count: 024601",
		out: &MyMessage{
			Count: Int32(024601),
		},
	},

	// Floating point number with "f" suffix
	{
		in: "count: 4 others:< weight: 17.0f >",
		out: &MyMessage{
			Count: Int32(4),
			Others: []*OtherMessage{
				{
					Weight: Float32(17),
				},
			},
		},
	},

	// Floating point positive infinity
	{
		in: "count: 4 bigfloat: inf",
		out: &MyMessage{
			Count:    Int32(4),
			Bigfloat: Float64(math.Inf(1)),
		},
	},

	// Floating point negative infinity
	{
		in: "count: 4 bigfloat: -inf",
		out: &MyMessage{
			Count:    Int32(4),
			Bigfloat: Float64(math.Inf(-1)),
		},
	},

	// Number too large for float32
	{
		in:  "others:< weight: 12345678901234567890123456789012345678901234567890 >",
		err: "line 1.17: invalid float32: 12345678901234567890123456789012345678901234567890",
	},

	// Number posing as a quoted string
	{
		in:  `inner: < host: 12 >` + "\n",
		err: `line 1.15: invalid string: 12`,
	},

	// Quoted string posing as int32
	{
		in:  `count: "12"`,
		err: `line 1.7: invalid int32: "12"`,
	},

	// Quoted string posing a float32
	{
		in:  `others:< weight: "17.4" >`,
		err: `line 1.17: invalid float32: "17.4"`,
	},

	// Enum
	{
		in: `count:42 bikeshed: BLUE`,
		out: &MyMessage{
			Count:    Int32(42),
			Bikeshed: MyMessage_BLUE.Enum(),
		},
	},

	// Repeated field
	{
		in: `count:42 pet: "horsey" pet:"bunny"`,
		out: &MyMessage{
			Count: Int32(42),
			Pet:   []string{"horsey", "bunny"},
		},
	},

	// Repeated field with list notation
	{
		in: `count:42 pet: ["horsey", "bunny"]`,
		out: &MyMessage{
			Count: Int32(42),
			Pet:   []string{"horsey", "bunny"},
		},
	},

	// Repeated message with/without colon and <>/{}
	{
		in: `count:42 others:{} others{} others:<> others:{}`,
		out: &MyMessage{
			Count: Int32(42),
			Others: []*OtherMessage{
				{},
				{},
				{},
				{},
			},
		},
	},

	// Missing colon for inner message
	{
		in: `count:42 inner < host: "cauchy.syd" >`,
		out: &MyMessage{
			Count: Int32(42),
			Inner: &InnerMessage{
				Host: String("cauchy.syd"),
			},
		},
	},

	// Missing colon for string field
	{
		in:  `name "Dave"`,
		err: `line 1.5: expected ':', found "\"Dave\""`,
	},

	// Missing colon for int32 field
	{
		in:  `count 42`,
		err: `line 1.6: expected ':', found "42"`,
	},

	// Missing required field
	{
		in:  `name: "Pawel"`,
		err: `proto: required field "testdata.MyMessage.count" not set`,
		out: &MyMessage{
			Name: String("Pawel"),
		},
	},

	// Repeated non-repeated field
	{
		in:  `name: "Rob" name: "Russ"`,
		err: `line 1.12: non-repeated field "name" was repeated`,
	},

	// Group
	{
		in: `count: 17 SomeGroup { group_field: 12 }`,
		out: &MyMessage{
			Count: Int32(17),
			Somegroup: &MyMessage_SomeGroup{
				GroupField: Int32(12),
			},
		},
	},

	// Semicolon between fields
	{
		in: `count:3;name:"Calvin"`,
		out: &MyMessage{
			Count: Int32(3),
			Name:  String("Calvin"),
		},
	},
	// Comma between fields
	{
		in: `count:4,name:"Ezekiel"`,
		out: &MyMessage{
			Count: Int32(4),
			Name:  String("Ezekiel"),
		},
	},

	// Extension
	buildExtStructTest(`count: 42 [testdata.Ext.more]:<data:"Hello, world!" >`),
	buildExtStructTest(`count: 42 [testdata.Ext.more] {data:"Hello, world!"}`),
	buildExtDataTest(`count: 42 [testdata.Ext.text]:"Hello, world!" [testdata.Ext.number]:1729`),
	buildExtRepStringTest(`count: 42 [testdata.greeting]:"bula" [testdata.greeting]:"hola"`),

	// Big all-in-one
	{
		in: "count:42  # Meaning\n" +
			`name:"Dave" ` +
			`quote:"\"I didn't want to go.\"" ` +
			`pet:"bunny" ` +
			`pet:"kitty" ` +
			`pet:"horsey" ` +
			`inner:<` +
			`  host:"footrest.syd" ` +
			`  port:7001 ` +
			`  connected:true ` +
			`> ` +
			`others:<` +
			`  key:3735928559 ` +
			`  value:"\x01A\a\f" ` +
			`> ` +
			`others:<` +
			"  weight:58.9  # Atomic weight of Co\n" +
			`  inner:<` +
			`    host:"lesha.mtv" ` +
			`    port:8002 ` +
			`  >` +
			`>`,
		out: &MyMessage{
			Count: Int32(42),
			Name:  String("Dave"),
			Quote: String(`"I didn't want to go."`),
			Pet:   []string{"bunny", "kitty", "horsey"},
			Inner: &InnerMessage{
				Host:      String("footrest.syd"),
				Port:      Int32(7001),
				Connected: Bool(true),
			},
			Others: []*OtherMessage{
				{
					Key:   Int64(3735928559),
					Value: []byte{0x1, 'A', '\a', '\f'},
				},
				{
					Weight: Float32(58.9),
					Inner: &InnerMessage{
						Host: String("lesha.mtv"),
						Port: Int32(8002),
					},
				},
			},
		},
	},
}

func TestUnmarshalText(t *testing.T) {
	for i, test := range unMarshalTextTests {
		pb := new(MyMessage)
		err := UnmarshalText(test.in, pb)
		if test.err == "" {
			// We don't expect failure.
			if err != nil {
				t.Errorf("Test %d: Unexpected error: %v", i, err)
			} else if !reflect.DeepEqual(pb, test.out) {
				t.Errorf("Test %d: Incorrect populated \nHave: %v\nWant: %v",
					i, pb, test.out)
			}
		} else {
			// We do expect failure.
			if err == nil {
				t.Errorf("Test %d: Didn't get expected error: %v", i, test.err)
			} else if err.Error() != test.err {
				t.Errorf("Test %d: Incorrect error.\nHave: %v\nWant: %v",
					i, err.Error(), test.err)
			} else if _, ok := err.(*RequiredNotSetError); ok && test.out != nil && !reflect.DeepEqual(pb, test.out) {
				t.Errorf("Test %d: Incorrect populated \nHave: %v\nWant: %v",
					i, pb, test.out)
			}
		}
	}
}

func TestUnmarshalTextCustomMessage(t *testing.T) {
	msg := &textMessage{}
	if err := UnmarshalText("custom", msg); err != nil {
		t.Errorf("Unexpected error from custom unmarshal: %v", err)
	}
	if UnmarshalText("not custom", msg) == nil {
		t.Errorf("Didn't get expected error from custom unmarshal")
	}
}

// Regression test; this caused a panic.
func TestRepeatedEnum(t *testing.T) {
	pb := new(RepeatedEnum)
	if err := UnmarshalText("color: RED", pb); err != nil {
		t.Fatal(err)
	}
	exp := &RepeatedEnum{
		Color: []RepeatedEnum_Color{RepeatedEnum_RED},
	}
	if !Equal(pb, exp) {
		t.Errorf("Incorrect populated \nHave: %v\nWant: %v", pb, exp)
	}
}

func TestProto3TextParsing(t *testing.T) {
	m := new(proto3pb.Message)
	const in = `name: "Wallace" true_scotsman: true`
	want := &proto3pb.Message{
		Name:         "Wallace",
		TrueScotsman: true,
	}
	if err := UnmarshalText(in, m); err != nil {
		t.Fatal(err)
	}
	if !Equal(m, want) {
		t.Errorf("\n got %v\nwant %v", m, want)
	}
}

func TestMapParsing(t *testing.T) {
	m := new(MessageWithMap)
	const in = `name_mapping:<key:1234 value:"Feist"> name_mapping:<key:1 value:"Beatles">` +
		`msg_mapping:<key:-4, value:<f: 2.0>,>` + // separating commas are okay
		`msg_mapping<key:-2 value<f: 4.0>>` + // no colon after "value"
		`byte_mapping:<key:true value:"so be it">`
	want := &MessageWithMap{
		NameMapping: map[int32]string{
			1:    "Beatles",
			1234: "Feist",
		},
		MsgMapping: map[int64]*FloatingPoint{
			-4: {F: Float64(2.0)},
			-2: {F: Float64(4.0)},
		},
		ByteMapping: map[bool][]byte{
			true: []byte("so be it"),
		},
	}
	if err := UnmarshalText(in, m); err != nil {
		t.Fatal(err)
	}
	if !Equal(m, want) {
		t.Errorf("\n got %v\nwant %v", m, want)
	}
}

func TestOneofParsing(t *testing.T) {
	const in = `name:"Shrek"`
	m := new(Communique)
	want := &Communique{Union: &Communique_Name{"Shrek"}}
	if err := UnmarshalText(in, m); err != nil {
		t.Fatal(err)
	}
	if !Equal(m, want) {
		t.Errorf("\n got %v\nwant %v", m, want)
	}
}

var benchInput string

func init() {
	benchInput = "count: 4\n"
	for i := 0; i < 1000; i++ {
		benchInput += "pet: \"fido\"\n"
	}

	// Check it is valid input.
	pb := new(MyMessage)
	err := UnmarshalText(benchInput, pb)
	if err != nil {
		panic("Bad benchmark input: " + err.Error())
	}
}

func BenchmarkUnmarshalText(b *testing.B) {
	pb := new(MyMessage)
	for i := 0; i < b.N; i++ {
		UnmarshalText(benchInput, pb)
	}
	b.SetBytes(int64(len(benchInput)))
}
