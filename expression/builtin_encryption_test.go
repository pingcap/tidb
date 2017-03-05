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

var aesCases = []struct {
	origin string
	key    string
	crypt  string
}{
	{"pingcap", "1234567890123456", "697BFE9B3F8C2F289DD82C88C7BC95C4"},
	{"pingcap123", "1234567890123456", "CEC348F4EF5F84D3AA6C4FA184C65766"},
	{"pingcap", "123456789012345678901234", "6F1589686860C8E8C7A40A78B25FF2C0"},
	{"pingcap", "123", "996E0CA8688D7AD20819B90B273E01C6"},
}

type md5Test struct {
	out interface{}
	in  interface{}
}

var md5Cases = []md5Test{
	{"d41d8cd98f00b204e9800998ecf8427e", ""},
	{"0cc175b9c0f1b6a831c399e269772661", "a"},
	{"187ef4436122d1cc2f40dc2b92f0eba0", "ab"},
	{"900150983cd24fb0d6963f7d28e17f72", "abc"},
	{"202cb962ac59075b964b07152d234b70", 123},
	{"202cb962ac59075b964b07152d234b70", "123"},
	{"46ddc40585caa8abc07c460b3485781e", 123.123},
	{nil, nil},
}

func (s *testEvaluatorSuite) TestAESEncrypt(c *C) {
	defer testleak.AfterTest(c)()
	fc := funcs[ast.AesEncrypt]
	for _, test := range aesCases {
		str := types.NewStringDatum(test.origin)
		key := types.NewStringDatum(test.key)
		f, err := fc.getFunction(datumsToConstants([]types.Datum{str, key}), s.ctx)
		crypt, err := f.eval(nil)
		c.Assert(err, IsNil)
		c.Assert(toHex(crypt), Equals, test.crypt)
	}
	s.testNullInput(c, ast.AesDecrypt)
}

func (s *testEvaluatorSuite) TestAESDecrypt(c *C) {
	defer testleak.AfterTest(c)()
	fc := funcs[ast.AesDecrypt]
	for _, test := range aesCases {
		cryptStr := fromHex(test.crypt)
		key := types.NewStringDatum(test.key)
		f, err := fc.getFunction(datumsToConstants([]types.Datum{cryptStr, key}), s.ctx)
		str, err := f.eval(nil)
		c.Assert(err, IsNil)
		c.Assert(str.GetString(), Equals, test.origin)
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

func toHex(d types.Datum) string {
	x, _ := d.ToString()
	return strings.ToUpper(hex.EncodeToString(hack.Slice(x)))
}

func fromHex(str string) (d types.Datum) {
	h, _ := hex.DecodeString(str)
	d.SetBytes(h)
	return d
}

func (s *testEvaluatorSuite) TestMD5(c *C) {
	defer testleak.AfterTest(c)()
	fc := funcs[ast.MD5]
	for _, test := range md5Cases {
		arg := types.NewDatum(test.in)
		f, err := fc.getFunction(datumsToConstants([]types.Datum{arg}), s.ctx)
		c.Assert(err, IsNil)
		out, err := f.eval(nil)
		c.Assert(err, IsNil)
		c.Assert(out, DeepEquals, types.NewDatum(test.out))
	}
	s.testNullInput(c, ast.AesDecrypt)
}
