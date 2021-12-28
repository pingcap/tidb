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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package expression

import (
	"encoding/hex"
	"fmt"
	"strings"
	"testing"

	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/charset"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/hack"
	"github.com/stretchr/testify/require"
)

var cryptTests = []struct {
	chs      string
	origin   interface{}
	password interface{}
	crypt    interface{}
}{
	{mysql.DefaultCollationName, "", "", ""},
	{mysql.DefaultCollationName, "pingcap", "1234567890123456", "2C35B5A4ADF391"},
	{mysql.DefaultCollationName, "pingcap", "asdfjasfwefjfjkj", "351CC412605905"},
	{mysql.DefaultCollationName, "pingcap123", "123456789012345678901234", "7698723DC6DFE7724221"},
	{mysql.DefaultCollationName, "pingcap#%$%^", "*^%YTu1234567", "8634B9C55FF55E5B6328F449"},
	{mysql.DefaultCollationName, "pingcap", "", "4A77B524BD2C5C"},
	{mysql.DefaultCollationName, "分布式データベース", "pass1234@#$%%^^&", "80CADC8D328B3026D04FB285F36FED04BBCA0CC685BF78B1E687CE"},
	{mysql.DefaultCollationName, "分布式データベース", "分布式7782734adgwy1242", "0E24CFEF272EE32B6E0BFBDB89F29FB43B4B30DAA95C3F914444BC"},
	{mysql.DefaultCollationName, "pingcap", "密匙", "CE5C02A5010010"},
	{"gbk", "pingcap", "密匙", "E407AC6F691ADE"},
	{mysql.DefaultCollationName, "pingcap数据库", "数据库passwd12345667", "36D5F90D3834E30E396BE3226E3B4ED3"},
	{"gbk", "pingcap数据库", "数据库passwd12345667", "B4BDBD6EC8346379F42836E2E0"},
	{mysql.DefaultCollationName, "数据库5667", 123.435, "B22196D0569386237AE12F8AAB"},
	{"gbk", "数据库5667", 123.435, "79E22979BD860EF58229"},
	{mysql.DefaultCollationName, nil, "数据库passwd12345667", nil},
}

func TestSQLDecode(t *testing.T) {
	ctx := createContext(t)
	for _, tt := range cryptTests {
		err := ctx.GetSessionVars().SetSystemVar(variable.CharacterSetConnection, tt.chs)
		require.NoError(t, err)
		err = ctx.GetSessionVars().SetSystemVar(variable.CollationConnection, tt.chs)
		require.NoError(t, err)
		f, err := newFunctionForTest(ctx, ast.Decode, primitiveValsToConstants(ctx, []interface{}{tt.origin, tt.password})...)
		require.NoError(t, err)
		d, err := f.Eval(chunk.Row{})
		require.NoError(t, err)
		if !d.IsNull() {
			d = toHex(d)
		}
		require.Equal(t, types.NewDatum(tt.crypt), d)
	}
	testNullInput(t, ctx, ast.Decode)
}

func TestSQLEncode(t *testing.T) {
	ctx := createContext(t)
	for _, test := range cryptTests {
		err := ctx.GetSessionVars().SetSystemVar(variable.CharacterSetConnection, test.chs)
		require.NoError(t, err)
		err = ctx.GetSessionVars().SetSystemVar(variable.CollationConnection, test.chs)
		require.NoError(t, err)
		var h []byte
		if test.crypt != nil {
			h, _ = hex.DecodeString(test.crypt.(string))
		} else {
			h = nil
		}
		f, err := newFunctionForTest(ctx, ast.Encode, primitiveValsToConstants(ctx, []interface{}{h, test.password})...)
		require.NoError(t, err)
		d, err := f.Eval(chunk.Row{})
		require.NoError(t, err)
		if test.origin != nil {
			enc := charset.FindEncoding(test.chs)
			result, err := enc.Transform(nil, []byte(test.origin.(string)), charset.OpEncode)
			require.NoError(t, err)
			require.Equal(t, types.NewCollationStringDatum(string(result), test.chs), d)
		} else {
			result := types.NewDatum(test.origin)
			require.Equal(t, result.GetBytes(), d.GetBytes())
		}
	}
	testNullInput(t, ctx, ast.Encode)
}

var aesTests = []struct {
	mode   string
	origin interface{}
	params []interface{}
	crypt  interface{}
}{
	// test for ecb
	{"aes-128-ecb", "pingcap", []interface{}{"1234567890123456"}, "697BFE9B3F8C2F289DD82C88C7BC95C4"},
	{"aes-128-ecb", "pingcap123", []interface{}{"1234567890123456"}, "CEC348F4EF5F84D3AA6C4FA184C65766"},
	{"aes-128-ecb", "pingcap", []interface{}{"123456789012345678901234"}, "6F1589686860C8E8C7A40A78B25FF2C0"},
	{"aes-128-ecb", "pingcap", []interface{}{"123"}, "996E0CA8688D7AD20819B90B273E01C6"},
	{"aes-128-ecb", "pingcap", []interface{}{123}, "996E0CA8688D7AD20819B90B273E01C6"},
	{"aes-128-ecb", nil, []interface{}{123}, nil},
	{"aes-192-ecb", "pingcap", []interface{}{"1234567890123456"}, "9B139FD002E6496EA2D5C73A2265E661"},
	{"aes-256-ecb", "pingcap", []interface{}{"1234567890123456"}, "F80DCDEDDBE5663BDB68F74AEDDB8EE3"},
	// test for cbc
	{"aes-128-cbc", "pingcap", []interface{}{"1234567890123456", "1234567890123456"}, "2ECA0077C5EA5768A0485AA522774792"},
	{"aes-128-cbc", "pingcap", []interface{}{"123456789012345678901234", "1234567890123456"}, "483788634DA8817423BA0934FD2C096E"},
	{"aes-192-cbc", "pingcap", []interface{}{"1234567890123456", "1234567890123456"}, "516391DB38E908ECA93AAB22870EC787"},
	{"aes-256-cbc", "pingcap", []interface{}{"1234567890123456", "1234567890123456"}, "5D0E22C1E77523AEF5C3E10B65653C8F"},
	{"aes-256-cbc", "pingcap", []interface{}{"12345678901234561234567890123456", "1234567890123456"}, "A26BA27CA4BE9D361D545AA84A17002D"},
	{"aes-256-cbc", "pingcap", []interface{}{"1234567890123456", "12345678901234561234567890123456"}, "5D0E22C1E77523AEF5C3E10B65653C8F"},
	// test for ofb
	{"aes-128-ofb", "pingcap", []interface{}{"1234567890123456", "1234567890123456"}, "0515A36BBF3DE0"},
	{"aes-128-ofb", "pingcap", []interface{}{"123456789012345678901234", "1234567890123456"}, "C2A93A93818546"},
	{"aes-192-ofb", "pingcap", []interface{}{"1234567890123456", "1234567890123456"}, "FE09DCCF14D458"},
	{"aes-256-ofb", "pingcap", []interface{}{"1234567890123456", "1234567890123456"}, "2E70FCAC0C0834"},
	{"aes-256-ofb", "pingcap", []interface{}{"12345678901234561234567890123456", "1234567890123456"}, "83E2B30A71F011"},
	{"aes-256-ofb", "pingcap", []interface{}{"1234567890123456", "12345678901234561234567890123456"}, "2E70FCAC0C0834"},
	// test for cfb
	{"aes-128-cfb", "pingcap", []interface{}{"1234567890123456", "1234567890123456"}, "0515A36BBF3DE0"},
	{"aes-128-cfb", "pingcap", []interface{}{"123456789012345678901234", "1234567890123456"}, "C2A93A93818546"},
	{"aes-192-cfb", "pingcap", []interface{}{"1234567890123456", "1234567890123456"}, "FE09DCCF14D458"},
	{"aes-256-cfb", "pingcap", []interface{}{"1234567890123456", "1234567890123456"}, "2E70FCAC0C0834"},
	{"aes-256-cfb", "pingcap", []interface{}{"12345678901234561234567890123456", "1234567890123456"}, "83E2B30A71F011"},
	{"aes-256-cfb", "pingcap", []interface{}{"1234567890123456", "12345678901234561234567890123456"}, "2E70FCAC0C0834"},
}

func TestAESEncrypt(t *testing.T) {
	ctx := createContext(t)

	fc := funcs[ast.AesEncrypt]
	for _, tt := range aesTests {
		err := variable.SetSessionSystemVar(ctx.GetSessionVars(), variable.BlockEncryptionMode, tt.mode)
		require.NoError(t, err)
		args := []types.Datum{types.NewDatum(tt.origin)}
		for _, param := range tt.params {
			args = append(args, types.NewDatum(param))
		}
		f, err := fc.getFunction(ctx, datumsToConstants(args))
		require.NoError(t, err)
		crypt, err := evalBuiltinFunc(f, chunk.Row{})
		require.NoError(t, err)
		require.Equal(t, types.NewDatum(tt.crypt), toHex(crypt))
	}
	err := variable.SetSessionSystemVar(ctx.GetSessionVars(), variable.BlockEncryptionMode, "aes-128-ecb")
	require.NoError(t, err)
	testNullInput(t, ctx, ast.AesEncrypt)
	testAmbiguousInput(t, ctx, ast.AesEncrypt)

	// Test GBK String
	enc := charset.FindEncoding("gbk")
	gbkStr, _ := enc.Transform(nil, []byte("你好"), charset.OpEncode)
	gbkTests := []struct {
		mode   string
		chs    string
		origin interface{}
		params []interface{}
		crypt  string
	}{
		// test for ecb
		{"aes-128-ecb", "utf8mb4", "你好", []interface{}{"123"}, "CEBD80EEC6423BEAFA1BB30FD7625CBC"},
		{"aes-128-ecb", "gbk", gbkStr, []interface{}{"123"}, "6AFA9D7BA2C1AED1603E804F75BB0127"},
		{"aes-128-ecb", "utf8mb4", "123", []interface{}{"你好"}, "E03F6D9C1C86B82F5620EE0AA9BD2F6A"},
		{"aes-128-ecb", "gbk", "123", []interface{}{"你好"}, "31A2D26529F0E6A38D406379ABD26FA5"},
		{"aes-128-ecb", "utf8mb4", "你好", []interface{}{"你好"}, "3E2D8211DAE17143F22C2C5969A35263"},
		{"aes-128-ecb", "gbk", gbkStr, []interface{}{"你好"}, "84982910338160D037615D283AD413DE"},
		// test for cbc
		{"aes-128-cbc", "utf8mb4", "你好", []interface{}{"123", "1234567890123456"}, "B95509A516ACED59C3DF4EC41C538D83"},
		{"aes-128-cbc", "gbk", gbkStr, []interface{}{"123", "1234567890123456"}, "D4322D091B5DDE0DEB35B1749DA2483C"},
		{"aes-128-cbc", "utf8mb4", "123", []interface{}{"你好", "1234567890123456"}, "E19E86A9E78E523267AFF36261AD117D"},
		{"aes-128-cbc", "gbk", "123", []interface{}{"你好", "1234567890123456"}, "5A2F8F2C1841CC4E1D1640F1EA2A1A23"},
		{"aes-128-cbc", "utf8mb4", "你好", []interface{}{"你好", "1234567890123456"}, "B73637C73302C909EA63274C07883E71"},
		{"aes-128-cbc", "gbk", gbkStr, []interface{}{"你好", "1234567890123456"}, "61E13E9B00F2E757F4E925D3268227A0"},
	}

	for _, tt := range gbkTests {
		msg := fmt.Sprintf("%v", tt)
		err := ctx.GetSessionVars().SetSystemVar(variable.CharacterSetConnection, tt.chs)
		require.NoError(t, err, msg)
		err = variable.SetSessionSystemVar(ctx.GetSessionVars(), variable.BlockEncryptionMode, tt.mode)
		require.NoError(t, err, msg)

		args := primitiveValsToConstants(ctx, []interface{}{tt.origin})
		args = append(args, primitiveValsToConstants(ctx, tt.params)...)
		f, err := fc.getFunction(ctx, args)

		require.NoError(t, err, msg)
		crypt, err := evalBuiltinFunc(f, chunk.Row{})
		require.NoError(t, err, msg)
		require.Equal(t, types.NewDatum(tt.crypt), toHex(crypt), msg)
	}
}

func TestAESDecrypt(t *testing.T) {
	ctx := createContext(t)

	fc := funcs[ast.AesDecrypt]
	for _, tt := range aesTests {
		msg := fmt.Sprintf("%v", tt)
		err := variable.SetSessionSystemVar(ctx.GetSessionVars(), variable.BlockEncryptionMode, tt.mode)
		require.NoError(t, err, msg)
		args := []types.Datum{fromHex(tt.crypt)}
		for _, param := range tt.params {
			args = append(args, types.NewDatum(param))
		}
		f, err := fc.getFunction(ctx, datumsToConstants(args))
		require.NoError(t, err, msg)
		str, err := evalBuiltinFunc(f, chunk.Row{})
		require.NoError(t, err, msg)
		if tt.origin == nil {
			require.True(t, str.IsNull())
			continue
		}
		require.Equal(t, types.NewCollationStringDatum(tt.origin.(string), charset.CollationBin), str, msg)
	}
	err := variable.SetSessionSystemVar(ctx.GetSessionVars(), variable.BlockEncryptionMode, "aes-128-ecb")
	require.NoError(t, err)
	testNullInput(t, ctx, ast.AesDecrypt)
	testAmbiguousInput(t, ctx, ast.AesDecrypt)

	// Test GBK String
	enc := charset.FindEncoding("gbk")
	r, _ := enc.Transform(nil, []byte("你好"), charset.OpEncode)
	gbkStr := string(r)
	gbkTests := []struct {
		mode   string
		chs    string
		origin interface{}
		params []interface{}
		crypt  string
	}{
		// test for ecb
		{"aes-128-ecb", "utf8mb4", "你好", []interface{}{"123"}, "CEBD80EEC6423BEAFA1BB30FD7625CBC"},
		{"aes-128-ecb", "gbk", gbkStr, []interface{}{"123"}, "6AFA9D7BA2C1AED1603E804F75BB0127"},
		{"aes-128-ecb", "utf8mb4", "123", []interface{}{"你好"}, "E03F6D9C1C86B82F5620EE0AA9BD2F6A"},
		{"aes-128-ecb", "gbk", "123", []interface{}{"你好"}, "31A2D26529F0E6A38D406379ABD26FA5"},
		{"aes-128-ecb", "utf8mb4", "你好", []interface{}{"你好"}, "3E2D8211DAE17143F22C2C5969A35263"},
		{"aes-128-ecb", "gbk", gbkStr, []interface{}{"你好"}, "84982910338160D037615D283AD413DE"},
		// test for cbc
		{"aes-128-cbc", "utf8mb4", "你好", []interface{}{"123", "1234567890123456"}, "B95509A516ACED59C3DF4EC41C538D83"},
		{"aes-128-cbc", "gbk", gbkStr, []interface{}{"123", "1234567890123456"}, "D4322D091B5DDE0DEB35B1749DA2483C"},
		{"aes-128-cbc", "utf8mb4", "123", []interface{}{"你好", "1234567890123456"}, "E19E86A9E78E523267AFF36261AD117D"},
		{"aes-128-cbc", "gbk", "123", []interface{}{"你好", "1234567890123456"}, "5A2F8F2C1841CC4E1D1640F1EA2A1A23"},
		{"aes-128-cbc", "utf8mb4", "你好", []interface{}{"你好", "1234567890123456"}, "B73637C73302C909EA63274C07883E71"},
		{"aes-128-cbc", "gbk", gbkStr, []interface{}{"你好", "1234567890123456"}, "61E13E9B00F2E757F4E925D3268227A0"},
	}

	for _, tt := range gbkTests {
		msg := fmt.Sprintf("%v", tt)
		err := ctx.GetSessionVars().SetSystemVar(variable.CharacterSetConnection, tt.chs)
		require.NoError(t, err, msg)
		err = variable.SetSessionSystemVar(ctx.GetSessionVars(), variable.BlockEncryptionMode, tt.mode)
		require.NoError(t, err, msg)
		// Set charset and collate except first argument
		args := datumsToConstants([]types.Datum{fromHex(tt.crypt)})
		args = append(args, primitiveValsToConstants(ctx, tt.params)...)
		f, err := fc.getFunction(ctx, args)
		require.NoError(t, err, msg)
		str, err := evalBuiltinFunc(f, chunk.Row{})
		require.NoError(t, err, msg)
		require.Equal(t, types.NewCollationStringDatum(tt.origin.(string), charset.CollationBin), str, msg)
	}
}

func testNullInput(t *testing.T, ctx sessionctx.Context, fnName string) {
	err := variable.SetSessionSystemVar(ctx.GetSessionVars(), variable.BlockEncryptionMode, "aes-128-ecb")
	require.NoError(t, err)
	fc := funcs[fnName]
	arg := types.NewStringDatum("str")
	var argNull types.Datum
	f, err := fc.getFunction(ctx, datumsToConstants([]types.Datum{arg, argNull}))
	require.NoError(t, err)
	crypt, err := evalBuiltinFunc(f, chunk.Row{})
	require.NoError(t, err)
	require.True(t, crypt.IsNull())

	f, err = fc.getFunction(ctx, datumsToConstants([]types.Datum{argNull, arg}))
	require.NoError(t, err)
	crypt, err = evalBuiltinFunc(f, chunk.Row{})
	require.NoError(t, err)
	require.True(t, crypt.IsNull())
}

func testAmbiguousInput(t *testing.T, ctx sessionctx.Context, fnName string) {
	fc := funcs[fnName]
	arg := types.NewStringDatum("str")
	// test for modes that require init_vector
	err := variable.SetSessionSystemVar(ctx.GetSessionVars(), variable.BlockEncryptionMode, "aes-128-cbc")
	require.NoError(t, err)
	_, err = fc.getFunction(ctx, datumsToConstants([]types.Datum{arg, arg}))
	require.Error(t, err)
	f, err := fc.getFunction(ctx, datumsToConstants([]types.Datum{arg, arg, types.NewStringDatum("iv < 16 bytes")}))
	require.NoError(t, err)
	_, err = evalBuiltinFunc(f, chunk.Row{})
	require.Error(t, err)

	// test for modes that do not require init_vector
	err = variable.SetSessionSystemVar(ctx.GetSessionVars(), variable.BlockEncryptionMode, "aes-128-ecb")
	require.NoError(t, err)
	f, err = fc.getFunction(ctx, datumsToConstants([]types.Datum{arg, arg, arg}))
	require.NoError(t, err)
	_, err = evalBuiltinFunc(f, chunk.Row{})
	require.NoError(t, err)
	warnings := ctx.GetSessionVars().StmtCtx.GetWarnings()
	require.GreaterOrEqual(t, len(warnings), 1)
}

func toHex(d types.Datum) (h types.Datum) {
	if d.IsNull() {
		return
	}
	x, _ := d.ToString()
	h.SetString(strings.ToUpper(hex.EncodeToString(hack.Slice(x))), mysql.DefaultCollationName)
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

func TestSha1Hash(t *testing.T) {
	ctx := createContext(t)
	sha1Tests := []struct {
		chs    string
		origin interface{}
		crypt  string
	}{
		{mysql.DefaultCollationName, "test", "a94a8fe5ccb19ba61c4c0873d391e987982fbbd3"},
		{mysql.DefaultCollationName, "c4pt0r", "034923dcabf099fc4c8917c0ab91ffcd4c2578a6"},
		{mysql.DefaultCollationName, "pingcap", "73bf9ef43a44f42e2ea2894d62f0917af149a006"},
		{mysql.DefaultCollationName, "foobar", "8843d7f92416211de9ebb963ff4ce28125932878"},
		{mysql.DefaultCollationName, 1024, "128351137a9c47206c4507dcf2e6fbeeca3a9079"},
		{mysql.DefaultCollationName, 123.45, "22f8b438ad7e89300b51d88684f3f0b9fa1d7a32"},
		{"gbk", 123.45, "22f8b438ad7e89300b51d88684f3f0b9fa1d7a32"},
		{"gbk", "一二三", "30cda4eed59a2ff592f2881f39d42fed6e10cad8"},
		{"gbk", "一二三123", "1e24acbf708cd889c1d5be90abc1f14eaf14d0b4"},
		{"gbk", "", "da39a3ee5e6b4b0d3255bfef95601890afd80709"},
	}

	fc := funcs[ast.SHA]
	for _, tt := range sha1Tests {
		err := ctx.GetSessionVars().SetSystemVar(variable.CharacterSetConnection, tt.chs)
		require.NoError(t, err)
		f, _ := fc.getFunction(ctx, primitiveValsToConstants(ctx, []interface{}{tt.origin}))
		crypt, err := evalBuiltinFunc(f, chunk.Row{})
		require.NoError(t, err)
		res, err := crypt.ToString()
		require.NoError(t, err)
		require.Equal(t, tt.crypt, res)
	}
	// test NULL input for sha
	var argNull types.Datum
	f, _ := fc.getFunction(ctx, datumsToConstants([]types.Datum{argNull}))
	crypt, err := evalBuiltinFunc(f, chunk.Row{})
	require.NoError(t, err)
	require.True(t, crypt.IsNull())
}

func TestSha2Hash(t *testing.T) {
	ctx := createContext(t)
	sha2Tests := []struct {
		chs        string
		origin     interface{}
		hashLength interface{}
		crypt      interface{}
		validCase  bool
	}{
		{mysql.DefaultCollationName, "pingcap", 0, "2871823be240f8ecd1d72f24c99eaa2e58af18b4b8ba99a4fc2823ba5c43930a", true},
		{mysql.DefaultCollationName, "pingcap", 224, "cd036dc9bec69e758401379c522454ea24a6327b48724b449b40c6b7", true},
		{mysql.DefaultCollationName, "pingcap", 256, "2871823be240f8ecd1d72f24c99eaa2e58af18b4b8ba99a4fc2823ba5c43930a", true},
		{mysql.DefaultCollationName, "pingcap", 384, "c50955b6b0c7b9919740d956849eedcb0f0f90bf8a34e8c1f4e071e3773f53bd6f8f16c04425ff728bed04de1b63db51", true},
		{mysql.DefaultCollationName, "pingcap", 512, "ea903c574370774c4844a83b7122105a106e04211673810e1baae7c2ae7aba2cf07465e02f6c413126111ef74a417232683ce7ba210052e63c15fc82204aad80", true},
		{mysql.DefaultCollationName, 13572468, 0, "1c91ab1c162fd0cae60a5bb9880f3e7d5a133a65b6057a644b26973d9c55dcfe", true},
		{mysql.DefaultCollationName, 13572468, 224, "8ad67735bbf49576219f364f4640d595357a440358d15bf6815a16e4", true},
		{mysql.DefaultCollationName, 13572468, 256, "1c91ab1c162fd0cae60a5bb9880f3e7d5a133a65b6057a644b26973d9c55dcfe", true},
		{mysql.DefaultCollationName, 13572468.123, 384, "3b4ee302435dc1e15251efd9f3982b1ca6fe4ac778d3260b7bbf3bea613849677eda830239420e448e4c6dc7c2649d89", true},
		{mysql.DefaultCollationName, 13572468.123, 512, "4820aa3f2760836557dc1f2d44a0ba7596333fdb60c8a1909481862f4ab0921c00abb23d57b7e67a970363cc3fcb78b25b6a0d45cdcac0e87aa0c96bc51f7f96", true},
		{mysql.DefaultCollationName, nil, 224, nil, false},
		{mysql.DefaultCollationName, "pingcap", nil, nil, false},
		{mysql.DefaultCollationName, "pingcap", 123, nil, false},
		{"gbk", "pingcap", 0, "2871823be240f8ecd1d72f24c99eaa2e58af18b4b8ba99a4fc2823ba5c43930a", true},
		{"gbk", "pingcap", 224, "cd036dc9bec69e758401379c522454ea24a6327b48724b449b40c6b7", true},
		{"gbk", "pingcap", 256, "2871823be240f8ecd1d72f24c99eaa2e58af18b4b8ba99a4fc2823ba5c43930a", true},
		{"gbk", "pingcap", 384, "c50955b6b0c7b9919740d956849eedcb0f0f90bf8a34e8c1f4e071e3773f53bd6f8f16c04425ff728bed04de1b63db51", true},
		{"gbk", "pingcap", 512, "ea903c574370774c4844a83b7122105a106e04211673810e1baae7c2ae7aba2cf07465e02f6c413126111ef74a417232683ce7ba210052e63c15fc82204aad80", true},
		{"gbk", 13572468, 0, "1c91ab1c162fd0cae60a5bb9880f3e7d5a133a65b6057a644b26973d9c55dcfe", true},
		{"gbk", 13572468, 224, "8ad67735bbf49576219f364f4640d595357a440358d15bf6815a16e4", true},
		{"gbk", 13572468, 256, "1c91ab1c162fd0cae60a5bb9880f3e7d5a133a65b6057a644b26973d9c55dcfe", true},
		{"gbk", 13572468.123, 384, "3b4ee302435dc1e15251efd9f3982b1ca6fe4ac778d3260b7bbf3bea613849677eda830239420e448e4c6dc7c2649d89", true},
		{"gbk", 13572468.123, 512, "4820aa3f2760836557dc1f2d44a0ba7596333fdb60c8a1909481862f4ab0921c00abb23d57b7e67a970363cc3fcb78b25b6a0d45cdcac0e87aa0c96bc51f7f96", true},
		{"gbk", nil, 224, nil, false},
		{"gbk", "pingcap", nil, nil, false},
		{"gbk", "pingcap", 123, nil, false},
		{"gbk", "一二三", 0, "b6c1ae1f8d8a07426ddb13fca5124fb0b9f1f0ef1cca6730615099cf198ca8af", true},
		{"gbk", "一二三", 224, "2362f577783f6cd6cc10b0308f946f479fef868a39d6339b5d74cc6d", true},
		{"gbk", "一二三", 256, "b6c1ae1f8d8a07426ddb13fca5124fb0b9f1f0ef1cca6730615099cf198ca8af", true},
		{"gbk", "一二三", 384, "54e75070f1faab03e7ce808ca2824ed4614ad1d58ee1409d8c1e4fd72ecab12c92ac3a2f919721c2aa09b23e5f3cc8aa", true},
		{"gbk", "一二三", 512, "54fae3d0bb68bb4645af4a97a01fee1a6e3ecf7850f1ba41a994a46d23b60082262d00d9c635ff7ed02203e4806794dfa57c3654b3a4549bfb77ef1ddeab0224", true},
		{"gbk", "一二三123", 0, "de059637dd572c2e21df1dd6d04512ad3a34f71964f14338e966356a091c0e7e", true},
		{"gbk", "一二三123", 224, "a192909220fea1b74bcea87740f7550a2c03cf4f92d4c78ccedc9e3f", true},
		{"gbk", "一二三123", 256, "de059637dd572c2e21df1dd6d04512ad3a34f71964f14338e966356a091c0e7e", true},
		{"gbk", "一二三123", 384, "a487131f07fd46f66d7300be3c10bdae255e3296334a239240b28d32f038983331b276bd717363673e54733b594e7781", true},
		{"gbk", "一二三123", 512, "9336a0844a5a1dc656d02ded28bf768cef9c39b47bd7292c75fc0d27fcb509ca765d24d502e5906e8afe1803fd5ea325e3d855a0206df6bc08fef5e7e34b0082", true},
		{"gbk", "", 0, "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", true},
		{"gbk", "", 224, "d14a028c2a3a2bc9476102bb288234c415a2b01f828ea62ac5b3e42f", true},
		{"gbk", "", 256, "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", true},
		{"gbk", "", 384, "38b060a751ac96384cd9327eb1b1e36a21fdb71114be07434c0cc7bf63f6e1da274edebfe76f65fbd51ad2f14898b95b", true},
		{"gbk", "", 512, "cf83e1357eefb8bdf1542850d66d8007d620e4050b5715dc83f4a921d36ce9ce47d0d13c5d85f2b0ff8318d2877eec2f63b931bd47417a81a538327af927da3e", true},
	}

	fc := funcs[ast.SHA2]
	for _, tt := range sha2Tests {
		err := ctx.GetSessionVars().SetSystemVar(variable.CharacterSetConnection, tt.chs)
		require.NoError(t, err)
		f, err := fc.getFunction(ctx, primitiveValsToConstants(ctx, []interface{}{tt.origin, tt.hashLength}))
		require.NoError(t, err)
		crypt, err := evalBuiltinFunc(f, chunk.Row{})
		require.NoError(t, err)
		if tt.validCase {
			res, err := crypt.ToString()
			require.NoError(t, err)
			require.Equal(t, tt.crypt, res)
		} else {
			require.True(t, crypt.IsNull())
		}
	}
}

func TestMD5Hash(t *testing.T) {
	ctx := createContext(t)

	cases := []struct {
		args     interface{}
		expected string
		charset  string
		isNil    bool
		getErr   bool
	}{
		{"", "d41d8cd98f00b204e9800998ecf8427e", "", false, false},
		{"a", "0cc175b9c0f1b6a831c399e269772661", "", false, false},
		{"ab", "187ef4436122d1cc2f40dc2b92f0eba0", "", false, false},
		{"abc", "900150983cd24fb0d6963f7d28e17f72", "", false, false},
		{"abc", "900150983cd24fb0d6963f7d28e17f72", "gbk", false, false},
		{123, "202cb962ac59075b964b07152d234b70", "", false, false},
		{"123", "202cb962ac59075b964b07152d234b70", "", false, false},
		{"123", "202cb962ac59075b964b07152d234b70", "gbk", false, false},
		{123.123, "46ddc40585caa8abc07c460b3485781e", "", false, false},
		{"一二三", "8093a32450075324682d01456d6e3919", "", false, false},
		{"一二三", "a45d4af7b243e7f393fa09bed72ac73e", "gbk", false, false},
		{"ㅂ123", "0e85d0f68c104b65a15d727e26705596", "", false, false},
		{"ㅂ123", "", "gbk", false, true},
		{nil, "", "", true, false},
	}
	for _, c := range cases {
		err := ctx.GetSessionVars().SetSystemVar(variable.CharacterSetConnection, c.charset)
		require.NoError(t, err)
		f, err := newFunctionForTest(ctx, ast.MD5, primitiveValsToConstants(ctx, []interface{}{c.args})...)
		require.NoError(t, err)
		d, err := f.Eval(chunk.Row{})
		if c.getErr {
			require.Error(t, err)
		} else {
			require.NoError(t, err)
			if c.isNil {
				require.Equal(t, types.KindNull, d.Kind())
			} else {
				require.Equal(t, c.expected, d.GetString())
			}
		}
	}
	_, err := funcs[ast.MD5].getFunction(ctx, []Expression{NewZero()})
	require.NoError(t, err)

}

func TestRandomBytes(t *testing.T) {
	ctx := createContext(t)

	fc := funcs[ast.RandomBytes]
	f, err := fc.getFunction(ctx, datumsToConstants([]types.Datum{types.NewDatum(32)}))
	require.NoError(t, err)
	out, err := evalBuiltinFunc(f, chunk.Row{})
	require.NoError(t, err)
	require.Equal(t, 32, len(out.GetBytes()))

	f, err = fc.getFunction(ctx, datumsToConstants([]types.Datum{types.NewDatum(1025)}))
	require.NoError(t, err)
	_, err = evalBuiltinFunc(f, chunk.Row{})
	require.Error(t, err)
	f, err = fc.getFunction(ctx, datumsToConstants([]types.Datum{types.NewDatum(-32)}))
	require.NoError(t, err)
	_, err = evalBuiltinFunc(f, chunk.Row{})
	require.Error(t, err)
	f, err = fc.getFunction(ctx, datumsToConstants([]types.Datum{types.NewDatum(0)}))
	require.NoError(t, err)
	_, err = evalBuiltinFunc(f, chunk.Row{})
	require.Error(t, err)

	f, err = fc.getFunction(ctx, datumsToConstants([]types.Datum{types.NewDatum(nil)}))
	require.NoError(t, err)
	out, err = evalBuiltinFunc(f, chunk.Row{})
	require.NoError(t, err)
	require.Equal(t, 0, len(out.GetBytes()))
}

func decodeHex(str string) []byte {
	ret, err := hex.DecodeString(str)
	if err != nil {
		panic(err)
	}
	return ret
}

func TestCompress(t *testing.T) {
	ctx := createContext(t)
	fc := funcs[ast.Compress]
	tests := []struct {
		chs    string
		in     interface{}
		expect interface{}
	}{
		{"", "hello world", string(decodeHex("0B000000789CCA48CDC9C95728CF2FCA4901040000FFFF1A0B045D"))},
		{"", "", ""},
		{"", nil, nil},
		{"utf8mb4", "hello world", string(decodeHex("0B000000789CCA48CDC9C95728CF2FCA4901040000FFFF1A0B045D"))},
		{"gbk", "hello world", string(decodeHex("0B000000789CCA48CDC9C95728CF2FCA4901040000FFFF1A0B045D"))},
		{"utf8mb4", "你好", string(decodeHex("06000000789C7AB277C1D3A57B01010000FFFF10450489"))},
		{"gbk", "你好", string(decodeHex("04000000789C3AF278D76140000000FFFF07F40325"))},
	}
	for _, test := range tests {
		err := ctx.GetSessionVars().SetSystemVar(variable.CharacterSetConnection, test.chs)
		require.NoErrorf(t, err, "%v", test)
		arg := primitiveValsToConstants(ctx, []interface{}{test.in})
		f, err := fc.getFunction(ctx, arg)
		require.NoErrorf(t, err, "%v", test)
		out, err := evalBuiltinFunc(f, chunk.Row{})
		require.NoErrorf(t, err, "%v", test)
		if test.expect == nil {
			require.Truef(t, out.IsNull(), "%v", test)
			continue
		}
		require.Equalf(t, types.NewCollationStringDatum(test.expect.(string), charset.CollationBin), out, "%v", test)
	}
}

func TestUncompress(t *testing.T) {
	ctx := createContext(t)
	tests := []struct {
		in     interface{}
		expect interface{}
	}{
		{decodeHex("0B000000789CCB48CDC9C95728CF2FCA4901001A0B045D"), "hello world"},         // zlib result from MySQL
		{decodeHex("0B000000789CCA48CDC9C95728CF2FCA4901040000FFFF1A0B045D"), "hello world"}, // zlib result from TiDB
		{decodeHex("02000000789CCB48CDC9C95728CF2FCA4901001A0B045D"), nil},                   // wrong length in the first four bytes
		{decodeHex(""), ""},
		{"1", nil},
		{"1234", nil},
		{"12345", nil},
		{decodeHex("0B"), nil},
		{decodeHex("0B000000"), nil},
		{decodeHex("0B0000001234"), nil},
		{12345, nil},
		{nil, nil},
	}

	fc := funcs[ast.Uncompress]
	for _, test := range tests {
		arg := types.NewDatum(test.in)
		f, err := fc.getFunction(ctx, datumsToConstants([]types.Datum{arg}))
		require.NoErrorf(t, err, "%v", test)
		out, err := evalBuiltinFunc(f, chunk.Row{})
		require.NoErrorf(t, err, "%v", test)
		if test.expect == nil {
			require.Truef(t, out.IsNull(), "%v", test)
			continue
		}
		require.Equalf(t, types.NewCollationStringDatum(test.expect.(string), charset.CollationBin), out, "%v", test)
	}
}

func TestUncompressLength(t *testing.T) {
	ctx := createContext(t)
	tests := []struct {
		in     interface{}
		expect interface{}
	}{
		{decodeHex("0B000000789CCB48CDC9C95728CF2FCA4901001A0B045D"), int64(11)},         // zlib result from MySQL
		{decodeHex("0B000000789CCA48CDC9C95728CF2FCA4901040000FFFF1A0B045D"), int64(11)}, // zlib result from TiDB
		{decodeHex(""), int64(0)},
		{"1", int64(0)},
		{"123", int64(0)},
		{decodeHex("0B"), int64(0)},
		{decodeHex("0B00"), int64(0)},
		{decodeHex("0B000000"), int64(0x0)},
		{decodeHex("0B0000001234"), int64(0x0B)},
		{12345, int64(875770417)},
		{nil, nil},
	}

	fc := funcs[ast.UncompressedLength]
	for _, test := range tests {
		arg := types.NewDatum(test.in)
		f, err := fc.getFunction(ctx, datumsToConstants([]types.Datum{arg}))
		require.NoErrorf(t, err, "%v", test)
		out, err := evalBuiltinFunc(f, chunk.Row{})
		require.NoErrorf(t, err, "%v", test)
		require.Equalf(t, types.NewDatum(test.expect), out, "%v", test)
	}
}

func TestPassword(t *testing.T) {
	ctx := createContext(t)
	cases := []struct {
		args     interface{}
		expected string
		charset  string
		isNil    bool
		getErr   bool
		getWarn  bool
	}{
		{nil, "", "", false, false, false},
		{"", "", "", false, false, false},
		{"abc", "*0D3CED9BEC10A777AEC23CCC353A8C08A633045E", "", false, false, true},
		{"abc", "*0D3CED9BEC10A777AEC23CCC353A8C08A633045E", "gbk", false, false, true},
		{123, "*23AE809DDACAF96AF0FD78ED04B6A265E05AA257", "", false, false, true},
		{1.23, "*A589EEBA8D3F9E1A34A7EE518FAC4566BFAD5BB6", "", false, false, true},
		{"一二三四", "*D207780722F22B23C254CAC0580D3B6738C19E18", "", false, false, true},
		{"一二三四", "*48E0460AD45CF66AC6B8C18CB8B4BC8A403D935B", "gbk", false, false, true},
		{"ㅂ123", "", "gbk", false, true, false},
		{types.NewDecFromFloatForTest(123.123), "*B15B84262DB34BFB2C817A45A55C405DC7C52BB1", "", false, false, true},
	}

	warnCount := len(ctx.GetSessionVars().StmtCtx.GetWarnings())
	for _, c := range cases {
		err := ctx.GetSessionVars().SetSystemVar(variable.CharacterSetConnection, c.charset)
		require.NoError(t, err)
		f, err := newFunctionForTest(ctx, ast.PasswordFunc, primitiveValsToConstants(ctx, []interface{}{c.args})...)
		require.NoError(t, err)
		d, err := f.Eval(chunk.Row{})
		if c.getErr {
			require.Error(t, err)
			continue
		}
		require.NoError(t, err)
		if c.isNil {
			require.Equal(t, types.KindNull, d.Kind())
		} else {
			require.Equal(t, c.expected, d.GetString())
		}

		warnings := ctx.GetSessionVars().StmtCtx.GetWarnings()
		if c.getWarn {
			require.Equal(t, warnCount+1, len(warnings))

			lastWarn := warnings[len(warnings)-1]
			require.Truef(t, terror.ErrorEqual(errDeprecatedSyntaxNoReplacement, lastWarn.Err), "err %v", lastWarn.Err)

			warnCount = len(warnings)
		} else {
			require.Equal(t, warnCount, len(warnings))
		}
	}

	_, err := funcs[ast.PasswordFunc].getFunction(ctx, []Expression{NewZero()})
	require.NoError(t, err)
}
