// Copyright 2017 PingCAP, Inc.
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

package expression

import (
	"encoding/hex"
	"strings"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/util/hack"
	"github.com/pingcap/tidb/util/testleak"
	"github.com/pingcap/tidb/util/types"
)

var aesTests = []struct {
	origin interface{}
	key    interface{}
	crypt  interface{}
}{
	{"pingcap", "1234567890123456", "697BFE9B3F8C2F289DD82C88C7BC95C4"},
	{"pingcap123", "1234567890123456", "CEC348F4EF5F84D3AA6C4FA184C65766"},
	{"pingcap", "123456789012345678901234", "6F1589686860C8E8C7A40A78B25FF2C0"},
	{"pingcap", "123", "996E0CA8688D7AD20819B90B273E01C6"},
	{"pingcap", 123, "996E0CA8688D7AD20819B90B273E01C6"},
	{nil, 123, nil},
}

func (s *testEvaluatorSuite) TestAESEncrypt(c *C) {
	defer testleak.AfterTest(c)()
	fc := funcs[ast.AesEncrypt]
	for _, tt := range aesTests {
		str := types.NewDatum(tt.origin)
		key := types.NewDatum(tt.key)
		f, err := fc.getFunction(datumsToConstants([]types.Datum{str, key}), s.ctx)
		crypt, err := f.eval(nil)
		c.Assert(err, IsNil)
		c.Assert(toHex(crypt), DeepEquals, types.NewDatum(tt.crypt))
	}
	s.testNullInput(c, ast.AesDecrypt)
}

func (s *testEvaluatorSuite) TestAESDecrypt(c *C) {
	defer testleak.AfterTest(c)()
	fc := funcs[ast.AesDecrypt]
	for _, test := range aesTests {
		cryptStr := fromHex(test.crypt)
		key := types.NewDatum(test.key)
		f, err := fc.getFunction(datumsToConstants([]types.Datum{cryptStr, key}), s.ctx)
		str, err := f.eval(nil)
		c.Assert(err, IsNil)
		c.Assert(str, DeepEquals, types.NewDatum(test.origin))
	}
	s.testNullInput(c, ast.AesDecrypt)
}

func (s *testEvaluatorSuite) testNullInput(c *C, fnName string) {
	fc := funcs[fnName]
	arg := types.NewStringDatum("str")
	var argNull types.Datum
	f, err := fc.getFunction(datumsToConstants([]types.Datum{arg, argNull}), s.ctx)
	crypt, err := f.eval(nil)
	c.Assert(err, IsNil)
	c.Assert(crypt.IsNull(), IsTrue)

	f, err = fc.getFunction(datumsToConstants([]types.Datum{argNull, arg}), s.ctx)
	crypt, err = f.eval(nil)
	c.Assert(err, IsNil)
	c.Assert(crypt.IsNull(), IsTrue)
}

func toHex(d types.Datum) (h types.Datum) {
	if d.IsNull() {
		return
	}
	x, _ := d.ToString()
	h.SetString(strings.ToUpper(hex.EncodeToString(hack.Slice(x))))
	return
}

func fromHex(str interface{}) (d types.Datum) {
	if str == nil {
		return
	}
	if s, ok := str.(string); ok {
		h, _ := hex.DecodeString(s)
		d.SetBytes(h)
	}
	return d
}

var shaTests = []struct {
	origin interface{}
	crypt  string
}{
	{"test", "a94a8fe5ccb19ba61c4c0873d391e987982fbbd3"},
	{"c4pt0r", "034923dcabf099fc4c8917c0ab91ffcd4c2578a6"},
	{"pingcap", "73bf9ef43a44f42e2ea2894d62f0917af149a006"},
	{"foobar", "8843d7f92416211de9ebb963ff4ce28125932878"},
	{1024, "128351137a9c47206c4507dcf2e6fbeeca3a9079"},
	{123.45, "22f8b438ad7e89300b51d88684f3f0b9fa1d7a32"},
}

func (s *testEvaluatorSuite) TestShaEncrypt(c *C) {
	defer testleak.AfterTest(c)()
	fc := funcs[ast.SHA]
	for _, tt := range shaTests {
		in := types.NewDatum(tt.origin)
		f, _ := fc.getFunction(datumsToConstants([]types.Datum{in}), s.ctx)
		crypt, err := f.eval(nil)
		c.Assert(err, IsNil)
		res, err := crypt.ToString()
		c.Assert(err, IsNil)
		c.Assert(res, Equals, tt.crypt)
	}
	// test NULL input for sha
	var argNull types.Datum
	f, _ := fc.getFunction(datumsToConstants([]types.Datum{argNull}), s.ctx)
	crypt, err := f.eval(nil)
	c.Assert(err, IsNil)
	c.Assert(crypt.IsNull(), IsTrue)
}

var sha2Tests = []struct {
	origin     interface{}
	hashLength interface{}
	crypt      interface{}
	validCase  bool
}{
	{"pingcap", 0, "2871823be240f8ecd1d72f24c99eaa2e58af18b4b8ba99a4fc2823ba5c43930a", true},
	{"pingcap", 224, "cd036dc9bec69e758401379c522454ea24a6327b48724b449b40c6b7", true},
	{"pingcap", 256, "2871823be240f8ecd1d72f24c99eaa2e58af18b4b8ba99a4fc2823ba5c43930a", true},
	{"pingcap", 384, "c50955b6b0c7b9919740d956849eedcb0f0f90bf8a34e8c1f4e071e3773f53bd6f8f16c04425ff728bed04de1b63db51", true},
	{"pingcap", 512, "ea903c574370774c4844a83b7122105a106e04211673810e1baae7c2ae7aba2cf07465e02f6c413126111ef74a417232683ce7ba210052e63c15fc82204aad80", true},
	{13572468, 0, "1c91ab1c162fd0cae60a5bb9880f3e7d5a133a65b6057a644b26973d9c55dcfe", true},
	{13572468, 224, "8ad67735bbf49576219f364f4640d595357a440358d15bf6815a16e4", true},
	{13572468, 256, "1c91ab1c162fd0cae60a5bb9880f3e7d5a133a65b6057a644b26973d9c55dcfe", true},
	{13572468.123, 384, "3b4ee302435dc1e15251efd9f3982b1ca6fe4ac778d3260b7bbf3bea613849677eda830239420e448e4c6dc7c2649d89", true},
	{13572468.123, 512, "4820aa3f2760836557dc1f2d44a0ba7596333fdb60c8a1909481862f4ab0921c00abb23d57b7e67a970363cc3fcb78b25b6a0d45cdcac0e87aa0c96bc51f7f96", true},
	{nil, 224, nil, false},
	{"pingcap", nil, nil, false},
	{"pingcap", 123, nil, false},
}

func (s *testEvaluatorSuite) TestSha2Encrypt(c *C) {
	defer testleak.AfterTest(c)()
	fc := funcs[ast.SHA2]
	for _, tt := range sha2Tests {
		str := types.NewDatum(tt.origin)
		hashLength := types.NewDatum(tt.hashLength)
		f, err := fc.getFunction(datumsToConstants([]types.Datum{str, hashLength}), s.ctx)
		crypt, err := f.eval(nil)
		c.Assert(err, IsNil)
		if tt.validCase {
			res, err := crypt.ToString()
			c.Assert(err, IsNil)
			c.Assert(res, Equals, tt.crypt)
		} else {
			c.Assert(crypt.IsNull(), IsTrue)
		}
	}
}

type md5Test struct {
	out interface{}
	in  interface{}
}

var md5Tests = []md5Test{
	{"d41d8cd98f00b204e9800998ecf8427e", ""},
	{"0cc175b9c0f1b6a831c399e269772661", "a"},
	{"187ef4436122d1cc2f40dc2b92f0eba0", "ab"},
	{"900150983cd24fb0d6963f7d28e17f72", "abc"},
	{"202cb962ac59075b964b07152d234b70", 123},
	{"202cb962ac59075b964b07152d234b70", "123"},
	{"46ddc40585caa8abc07c460b3485781e", 123.123},
	{nil, nil},
}

func (s *testEvaluatorSuite) TestMD5(c *C) {
	defer testleak.AfterTest(c)()
	fc := funcs[ast.MD5]
	for _, tt := range md5Tests {
		arg := types.NewDatum(tt.in)
		f, err := fc.getFunction(datumsToConstants([]types.Datum{arg}), s.ctx)
		c.Assert(err, IsNil)
		out, err := f.eval(nil)
		c.Assert(err, IsNil)
		c.Assert(out, DeepEquals, types.NewDatum(tt.out))
	}
	s.testNullInput(c, ast.AesDecrypt)
}

type compressTest struct {
	in  interface{}
	out interface{}
}

var compressTests = []compressTest{
	{[]byte("hello world"), []byte{120, 156, 202, 72, 205, 201, 201, 87, 40, 207, 47, 202, 73, 1, 4, 0, 0, 255, 255, 26, 11, 4, 93}},
	{[]byte("i love you)"), []byte{120, 156, 202, 84, 200, 201, 47, 75, 85, 168, 204, 47, 213, 4, 4, 0, 0, 255, 255, 23, 142, 3, 230}},
	{nil, nil},
}

func (s *testEvaluatorSuite) TestCompress(c *C) {
	defer testleak.AfterTest(c)()
	fc := funcs[ast.Compress]
	for _, tt := range compressTests {
		arg := types.NewDatum(tt.in)
		f, err := fc.getFunction(datumsToConstants([]types.Datum{arg}), s.ctx)
		c.Assert(err, IsNil)
		out, err := f.eval(nil)
		c.Assert(err, IsNil)
		c.Assert(out, DeepEquals, types.NewDatum(tt.out))
	}
}

func (s *testEvaluatorSuite) TestRandomBytes(c *C) {
	defer testleak.AfterTest(c)()
	fc := funcs[ast.RandomBytes]
	f, err := fc.getFunction(datumsToConstants([]types.Datum{types.NewDatum(32)}), s.ctx)
	c.Assert(err, IsNil)
	out, err := f.eval(nil)
	c.Assert(err, IsNil)
	c.Assert(len(out.GetBytes()), Equals, 32)

	f, err = fc.getFunction(datumsToConstants([]types.Datum{types.NewDatum(1025)}), s.ctx)
	c.Assert(err, IsNil)
	_, err = f.eval(nil)
	c.Assert(err, NotNil)
	f, err = fc.getFunction(datumsToConstants([]types.Datum{types.NewDatum(-32)}), s.ctx)
	c.Assert(err, IsNil)
	_, err = f.eval(nil)
	c.Assert(err, NotNil)
	f, err = fc.getFunction(datumsToConstants([]types.Datum{types.NewDatum(0)}), s.ctx)
	c.Assert(err, IsNil)
	_, err = f.eval(nil)
	c.Assert(err, NotNil)

	f, err = fc.getFunction(datumsToConstants([]types.Datum{types.NewDatum(nil)}), s.ctx)
	c.Assert(err, IsNil)
	out, err = f.eval(nil)
	c.Assert(err, IsNil)
	c.Assert(len(out.GetBytes()), Equals, 0)
}
