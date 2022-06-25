// Copyright 2022 PingCAP, Inc.
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

package schemacmp_test

import (
	"testing"

	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/types"
	. "github.com/pingcap/tidb/util/schemacmp"
	"github.com/stretchr/testify/require"
)

const binary = "binary"

var (
	// INT
	typeInt = types.NewFieldTypeBuilder().SetType(mysql.TypeLong).SetFlag(0).SetFlen(11).SetDecimal(0).SetCharset(binary).SetCollate(binary).SetElems(nil).BuildP()

	// INT NOT NULL
	typeIntNotNull = types.NewFieldTypeBuilder().SetType(mysql.TypeLong).SetFlag(mysql.NoDefaultValueFlag | mysql.NotNullFlag).SetFlen(10).SetDecimal(0).SetCharset(binary).SetCollate(binary).SetElems(nil).BuildP()

	// INT AUTO_INCREMENT UNIQUE
	typeIntAutoIncrementUnique = types.NewFieldTypeBuilder().SetType(mysql.TypeLong).SetFlag(mysql.AutoIncrementFlag | mysql.UniqueKeyFlag).SetFlen(11).SetDecimal(0).SetCharset(binary).SetCollate(binary).SetElems(nil).BuildP()

	// INT NOT NULL, KEY
	typeIntNotNullKey = types.NewFieldTypeBuilder().SetType(mysql.TypeLong).SetFlag(mysql.NoDefaultValueFlag | mysql.MultipleKeyFlag | mysql.NotNullFlag).SetFlen(11).SetDecimal(0).SetCharset(binary).SetCollate(binary).SetElems(nil).BuildP()

	// INT(1)
	typeInt1 = types.NewFieldTypeBuilder().SetType(mysql.TypeLong).SetFlag(0).SetFlen(1).SetDecimal(0).SetCharset(binary).SetCollate(binary).SetElems(nil).BuildP()

	// INT(22)
	typeInt22 = types.NewFieldTypeBuilder().SetType(mysql.TypeLong).SetFlag(0).SetFlen(22).SetDecimal(0).SetCharset(binary).SetCollate(binary).SetElems(nil).BuildP()

	// BIT(4)
	typeBit4 = types.NewFieldTypeBuilder().SetType(mysql.TypeBit).SetFlag(mysql.UnsignedFlag).SetFlen(4).SetDecimal(0).SetCharset(binary).SetCollate(binary).SetElems(nil).BuildP()

	// BIGINT(22) ZEROFILL
	typeBigInt22ZeroFill = types.NewFieldTypeBuilder().SetType(mysql.TypeLonglong).SetFlag(mysql.ZerofillFlag | mysql.UnsignedFlag).SetFlen(22).SetDecimal(0).SetCharset(binary).SetCollate(binary).SetElems(nil).BuildP()

	// DECIMAL(16, 8) DEFAULT 2.5
	typeDecimal16_8 = types.NewFieldTypeBuilder().SetType(mysql.TypeNewDecimal).SetFlag(0).SetFlen(16).SetDecimal(8).SetCharset(binary).SetCollate(binary).SetElems(nil).BuildP()

	// DECIMAL
	typeDecimal = types.NewFieldTypeBuilder().SetType(mysql.TypeNewDecimal).SetFlag(0).SetFlen(11).SetDecimal(0).SetCharset(binary).SetCollate(binary).SetElems(nil).BuildP()

	// DATE
	typeDate = types.NewFieldTypeBuilder().SetType(mysql.TypeDate).SetFlag(mysql.BinaryFlag).SetFlen(10).SetDecimal(0).SetCharset(binary).SetCollate(binary).SetElems(nil).BuildP()

	// DATETIME(3)
	typeDateTime3 = types.NewFieldTypeBuilder().SetType(mysql.TypeDatetime).SetFlag(mysql.BinaryFlag).SetFlen(23).SetDecimal(3).SetCharset(binary).SetCollate(binary).SetElems(nil).BuildP()

	// TIMESTAMP
	typeTimestamp = types.NewFieldTypeBuilder().SetType(mysql.TypeTimestamp).SetFlag(mysql.BinaryFlag).SetFlen(19).SetDecimal(0).SetCharset(binary).SetCollate(binary).SetElems(nil).BuildP()

	// TIME(6)
	typeTime6 = types.NewFieldTypeBuilder().SetType(mysql.TypeDuration).SetFlag(mysql.BinaryFlag).SetFlen(17).SetDecimal(6).SetCharset(binary).SetCollate(binary).SetElems(nil).BuildP()

	// YEAR(4)
	typeYear4 = types.NewFieldTypeBuilder().SetType(mysql.TypeYear).SetFlag(mysql.ZerofillFlag | mysql.UnsignedFlag).SetFlen(4).SetDecimal(0).SetCharset(binary).SetCollate(binary).SetElems(nil).BuildP()

	// CHAR(123)
	typeChar123 = types.NewFieldTypeBuilder().SetType(mysql.TypeString).SetFlag(0).SetFlen(123).SetDecimal(0).SetCharset(mysql.UTF8MB4Charset).SetCollate(mysql.UTF8MB4DefaultCollation).SetElems(nil).BuildP()

	// VARCHAR(65432) CHARSET ascii
	typeVarchar65432CharsetASCII = types.NewFieldTypeBuilder().SetType(mysql.TypeVarchar).SetFlag(0).SetFlen(65432).SetDecimal(0).SetCharset("ascii").SetCollate("ascii_bin").SetElems(nil).BuildP()

	// BINARY(69)
	typeBinary69 = types.NewFieldTypeBuilder().SetType(mysql.TypeString).SetFlag(mysql.BinaryFlag).SetFlen(69).SetDecimal(0).SetCharset(binary).SetCollate(binary).SetElems(nil).BuildP()

	// VARBINARY(420)
	typeVarBinary420 = types.NewFieldTypeBuilder().SetType(mysql.TypeVarchar).SetFlag(mysql.BinaryFlag).SetFlen(420).SetDecimal(0).SetCharset(binary).SetCollate(binary).SetElems(nil).BuildP()

	// LONGBLOB
	typeLongBlob = types.NewFieldTypeBuilder().SetType(mysql.TypeLongBlob).SetFlag(mysql.BinaryFlag).SetFlen(0xffffffff).SetDecimal(0).SetCharset(binary).SetCollate(binary).SetElems(nil).BuildP()

	// MEDIUMTEXT
	typeMediumText = types.NewFieldTypeBuilder().SetType(mysql.TypeMediumBlob).SetFlag(0).SetFlen(0xffffffff).SetDecimal(0).SetCharset(mysql.UTF8MB4Charset).SetCollate(mysql.UTF8MB4DefaultCollation).SetElems(nil).BuildP()

	// ENUM('tidb', 'tikv', 'tiflash', 'golang', 'rust')
	typeEnum5 = types.NewFieldTypeBuilder().SetType(mysql.TypeEnum).SetFlag(0).SetFlen(types.UnspecifiedLength).SetDecimal(0).SetCharset(mysql.UTF8MB4Charset).SetCollate(mysql.UTF8MB4DefaultCollation).SetElems([]string{"tidb", "tikv", "tiflash", "golang", "rust"}).BuildP()

	// ENUM('tidb', 'tikv')
	typeEnum2 = types.NewFieldTypeBuilder().SetType(mysql.TypeEnum).SetFlag(0).SetFlen(types.UnspecifiedLength).SetDecimal(0).SetCharset(mysql.UTF8MB4Charset).SetCollate(mysql.UTF8MB4DefaultCollation).SetElems([]string{"tidb", "tikv"}).BuildP()

	// SET('tidb', 'tikv', 'tiflash', 'golang', 'rust')
	typeSet5 = types.NewFieldTypeBuilder().SetType(mysql.TypeSet).SetFlag(0).SetFlen(types.UnspecifiedLength).SetDecimal(0).SetCharset(mysql.UTF8MB4Charset).SetCollate(mysql.UTF8MB4DefaultCollation).SetElems([]string{"tidb", "tikv", "tiflash", "golang", "rust"}).BuildP()

	// ENUM('tidb', 'tikv')
	typeSet2 = types.NewFieldTypeBuilder().SetType(mysql.TypeSet).SetFlag(0).SetFlen(types.UnspecifiedLength).SetDecimal(0).SetCharset(mysql.UTF8MB4Charset).SetCollate(mysql.UTF8MB4DefaultCollation).SetElems([]string{"tidb", "tikv"}).BuildP()

	// JSON
	typeJSON = types.NewFieldTypeBuilder().SetType(mysql.TypeJSON).SetFlag(mysql.BinaryFlag).SetFlen(0xffffffff).SetDecimal(0).SetCharset(binary).SetCollate(binary).SetElems(nil).BuildP()
)

func TestTypeUnwrap(t *testing.T) {
	testCases := []*types.FieldType{
		typeInt,
		typeIntNotNull,
		typeIntAutoIncrementUnique,
		typeIntNotNullKey,
		typeInt1,
		typeInt22,
		typeBit4,
		typeBigInt22ZeroFill,
		typeDecimal16_8,
		typeDecimal,
		typeDate,
		typeDateTime3,
		typeTimestamp,
		typeTime6,
		typeYear4,
		typeChar123,
		typeVarchar65432CharsetASCII,
		typeBinary69,
		typeVarBinary420,
		typeLongBlob,
		typeMediumText,
		typeEnum5,
		typeEnum2,
		typeSet5,
		typeSet2,
		typeJSON,
	}

	for _, tc := range testCases {
		tt := Type(tc)
		require.EqualValues(t, tc, tt.Unwrap())
	}
}

func TestTypeCompareJoin(t *testing.T) {
	testCases := []struct {
		a             *types.FieldType
		b             *types.FieldType
		compareResult int
		compareError  string
		join          *types.FieldType
		joinError     string
	}{
		{
			a:             typeInt,
			b:             typeInt22,
			compareResult: -1,
			join:          typeInt22,
		},
		{
			a:             typeInt1,
			b:             typeInt,
			compareResult: -1,
			join:          typeInt,
		},
		{
			a:             typeInt,
			b:             typeIntNotNull,
			compareResult: 1,
			join:          typeInt,
		},
		{
			// Cannot join DEFAULT NULL with AUTO_INCREMENT.
			a:            typeInt,
			b:            typeIntAutoIncrementUnique,
			compareError: `at tuple index \d+: distinct singletons.*`, // TODO: Improve error messages.
			joinError:    `at tuple index \d+: distinct singletons.*`,
		},
		{
			// INT NOT NULL <join> INT AUTO_INC UNIQUE = INT AUTO_INC,
			// but an AUTO_INC column must be defined with a key, so the join is invalid.
			a:            typeIntNotNull,
			b:            typeIntAutoIncrementUnique,
			compareError: `at tuple index \d+: combining contradicting orders.*`,
			joinError:    `auto type but not defined as a key`,
		},
		{
			// INT NOT NULL KEY <join> INT AUTO_INC UNIQUE = INT AUTO_INC KEY,
			a:            typeIntNotNullKey,
			b:            typeIntAutoIncrementUnique,
			compareError: `at tuple index \d+: combining contradicting orders.*`,
			join:         types.NewFieldTypeBuilder().SetType(mysql.TypeLong).SetFlag(mysql.AutoIncrementFlag | mysql.MultipleKeyFlag).SetFlen(11).SetDecimal(0).SetCharset(binary).SetCollate(binary).SetElems(nil).BuildP(),
		},
		{
			// DECIMAL of differet Flen/Decimal cannot be compared
			a:            typeDecimal16_8,
			b:            typeDecimal,
			compareError: `at tuple index \d+: distinct singletons.*`,
			joinError:    `at tuple index \d+: distinct singletons.*`,
		},
		{
			a:            typeVarchar65432CharsetASCII,
			b:            typeVarBinary420,
			compareError: `at tuple index \d+: distinct singletons.*`,
			joinError:    `at tuple index \d+: distinct singletons.*`,
		},
		{
			a:             typeEnum5,
			b:             typeEnum2,
			compareResult: 1,
			join:          typeEnum5,
		},
		{
			a:             typeSet2,
			b:             typeSet5,
			compareResult: -1,
			join:          typeSet5,
		},
		{
			a:            typeSet5,
			b:            typeEnum5,
			compareError: `at tuple index \d+: incompatible mysql type.*`,
			joinError:    `at tuple index \d+: incompatible mysql type.*`,
		},
	}

	for _, tc := range testCases {
		a := Type(tc.a)
		b := Type(tc.b)
		cmp, err := a.Compare(b)
		if len(tc.compareError) != 0 {
			if err == nil {
				t.Log(cmp)
			}
			require.Regexp(t, tc.compareError, err)

		} else {
			require.NoError(t, err)
			require.Equal(t, tc.compareResult, cmp)
		}

		cmp, err = b.Compare(a)
		if len(tc.compareError) != 0 {
			require.Regexp(t, tc.compareError, err)
		} else {
			require.NoError(t, err)
			require.Equal(t, -tc.compareResult, cmp)
		}

		wrappedJoin, err := a.Join(b)
		if len(tc.joinError) != 0 {
			require.Regexp(t, tc.joinError, err)
		} else {
			require.NoError(t, err)
			require.EqualValues(t, tc.join, wrappedJoin.Unwrap())
			cmp, err = wrappedJoin.Compare(a)
			require.NoError(t, err)
			require.GreaterOrEqual(t, cmp, 0)

			cmp, err = wrappedJoin.Compare(b)
			require.NoError(t, err)
			require.GreaterOrEqual(t, cmp, 0)
		}
	}
}
