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
	"github.com/pingcap/tidb/parser/types"
	. "github.com/pingcap/tidb/util/schemacmp"
	"github.com/stretchr/testify/require"
)

const binary = "binary"

var (
	// INT
	typeInt = &types.FieldType{
		Tp:      mysql.TypeLong,
		Flag:    0,
		Flen:    11,
		Decimal: 0,
		Charset: binary,
		Collate: binary,
		Elems:   nil,
	}

	// INT NOT NULL
	typeIntNotNull = &types.FieldType{
		Tp:      mysql.TypeLong,
		Flag:    mysql.NoDefaultValueFlag | mysql.NotNullFlag,
		Flen:    10,
		Decimal: 0,
		Charset: binary,
		Collate: binary,
		Elems:   nil,
	}

	// INT AUTO_INCREMENT UNIQUE
	typeIntAutoIncrementUnique = &types.FieldType{
		Tp:      mysql.TypeLong,
		Flag:    mysql.AutoIncrementFlag | mysql.UniqueKeyFlag,
		Flen:    11,
		Decimal: 0,
		Charset: binary,
		Collate: binary,
		Elems:   nil,
	}

	// INT NOT NULL, KEY
	typeIntNotNullKey = &types.FieldType{
		Tp:      mysql.TypeLong,
		Flag:    mysql.NoDefaultValueFlag | mysql.MultipleKeyFlag | mysql.NotNullFlag,
		Flen:    11,
		Decimal: 0,
		Charset: binary,
		Collate: binary,
		Elems:   nil,
	}

	// INT(1)
	typeInt1 = &types.FieldType{
		Tp:      mysql.TypeLong,
		Flag:    0,
		Flen:    1,
		Decimal: 0,
		Charset: binary,
		Collate: binary,
		Elems:   nil,
	}

	// INT(22)
	typeInt22 = &types.FieldType{
		Tp:      mysql.TypeLong,
		Flag:    0,
		Flen:    22,
		Decimal: 0,
		Charset: binary,
		Collate: binary,
		Elems:   nil,
	}

	// BIT(4)
	typeBit4 = &types.FieldType{
		Tp:      mysql.TypeBit,
		Flag:    mysql.UnsignedFlag,
		Flen:    4,
		Decimal: 0,
		Charset: binary,
		Collate: binary,
		Elems:   nil,
	}

	// BIGINT(22) ZEROFILL
	typeBigInt22ZeroFill = &types.FieldType{
		Tp:      mysql.TypeLonglong,
		Flag:    mysql.ZerofillFlag | mysql.UnsignedFlag,
		Flen:    22,
		Decimal: 0,
		Charset: binary,
		Collate: binary,
		Elems:   nil,
	}

	// DECIMAL(16, 8) DEFAULT 2.5
	typeDecimal16_8 = &types.FieldType{
		Tp:      mysql.TypeNewDecimal,
		Flag:    0,
		Flen:    16,
		Decimal: 8,
		Charset: binary,
		Collate: binary,
		Elems:   nil,
	}

	// DECIMAL
	typeDecimal = &types.FieldType{
		Tp:      mysql.TypeNewDecimal,
		Flag:    0,
		Flen:    11,
		Decimal: 0,
		Charset: binary,
		Collate: binary,
		Elems:   nil,
	}

	// DATE
	typeDate = &types.FieldType{
		Tp:      mysql.TypeDate,
		Flag:    mysql.BinaryFlag,
		Flen:    10,
		Decimal: 0,
		Charset: binary,
		Collate: binary,
		Elems:   nil,
	}

	// DATETIME(3)
	typeDateTime3 = &types.FieldType{
		Tp:      mysql.TypeDatetime,
		Flag:    mysql.BinaryFlag,
		Flen:    23,
		Decimal: 3,
		Charset: binary,
		Collate: binary,
		Elems:   nil,
	}

	// TIMESTAMP
	typeTimestamp = &types.FieldType{
		Tp:      mysql.TypeTimestamp,
		Flag:    mysql.BinaryFlag,
		Flen:    19,
		Decimal: 0,
		Charset: binary,
		Collate: binary,
		Elems:   nil,
	}

	// TIME(6)
	typeTime6 = &types.FieldType{
		Tp:      mysql.TypeDuration,
		Flag:    mysql.BinaryFlag,
		Flen:    17,
		Decimal: 6,
		Charset: binary,
		Collate: binary,
		Elems:   nil,
	}

	// YEAR(4)
	typeYear4 = &types.FieldType{
		Tp:      mysql.TypeYear,
		Flag:    mysql.ZerofillFlag | mysql.UnsignedFlag,
		Flen:    4,
		Decimal: 0,
		Charset: binary,
		Collate: binary,
		Elems:   nil,
	}

	// CHAR(123)
	typeChar123 = &types.FieldType{
		Tp:      mysql.TypeString,
		Flag:    0,
		Flen:    123,
		Decimal: 0,
		Charset: mysql.UTF8MB4Charset,
		Collate: mysql.UTF8MB4DefaultCollation,
		Elems:   nil,
	}

	// VARCHAR(65432) CHARSET ascii
	typeVarchar65432CharsetASCII = &types.FieldType{
		Tp:      mysql.TypeVarchar,
		Flag:    0,
		Flen:    65432,
		Decimal: 0,
		Charset: "ascii",
		Collate: "ascii_bin",
		Elems:   nil,
	}

	// BINARY(69)
	typeBinary69 = &types.FieldType{
		Tp:      mysql.TypeString,
		Flag:    mysql.BinaryFlag,
		Flen:    69,
		Decimal: 0,
		Charset: binary,
		Collate: binary,
		Elems:   nil,
	}

	// VARBINARY(420)
	typeVarBinary420 = &types.FieldType{
		Tp:      mysql.TypeVarchar,
		Flag:    mysql.BinaryFlag,
		Flen:    420,
		Decimal: 0,
		Charset: binary,
		Collate: binary,
		Elems:   nil,
	}

	// LONGBLOB
	typeLongBlob = &types.FieldType{
		Tp:      mysql.TypeLongBlob,
		Flag:    mysql.BinaryFlag,
		Flen:    0xffffffff,
		Decimal: 0,
		Charset: binary,
		Collate: binary,
		Elems:   nil,
	}

	// MEDIUMTEXT
	typeMediumText = &types.FieldType{
		Tp:      mysql.TypeMediumBlob,
		Flag:    0,
		Flen:    0xffffff,
		Decimal: 0,
		Charset: mysql.UTF8MB4Charset,
		Collate: mysql.UTF8MB4DefaultCollation,
		Elems:   nil,
	}

	// ENUM('tidb', 'tikv', 'tiflash', 'golang', 'rust')
	typeEnum5 = &types.FieldType{
		Tp:      mysql.TypeEnum,
		Flag:    0,
		Flen:    types.UnspecifiedLength,
		Decimal: 0,
		Charset: mysql.UTF8MB4Charset,
		Collate: mysql.UTF8MB4DefaultCollation,
		Elems:   []string{"tidb", "tikv", "tiflash", "golang", "rust"},
	}

	// ENUM('tidb', 'tikv')
	typeEnum2 = &types.FieldType{
		Tp:      mysql.TypeEnum,
		Flag:    0,
		Flen:    types.UnspecifiedLength,
		Decimal: 0,
		Charset: mysql.UTF8MB4Charset,
		Collate: mysql.UTF8MB4DefaultCollation,
		Elems:   []string{"tidb", "tikv"},
	}

	// SET('tidb', 'tikv', 'tiflash', 'golang', 'rust')
	typeSet5 = &types.FieldType{
		Tp:      mysql.TypeSet,
		Flag:    0,
		Flen:    types.UnspecifiedLength,
		Decimal: 0,
		Charset: mysql.UTF8MB4Charset,
		Collate: mysql.UTF8MB4DefaultCollation,
		Elems:   []string{"tidb", "tikv", "tiflash", "golang", "rust"},
	}

	// ENUM('tidb', 'tikv')
	typeSet2 = &types.FieldType{
		Tp:      mysql.TypeSet,
		Flag:    0,
		Flen:    types.UnspecifiedLength,
		Decimal: 0,
		Charset: mysql.UTF8MB4Charset,
		Collate: mysql.UTF8MB4DefaultCollation,
		Elems:   []string{"tidb", "tikv"},
	}

	// JSON
	typeJSON = &types.FieldType{
		Tp:      mysql.TypeJSON,
		Flag:    mysql.BinaryFlag,
		Flen:    0xffffffff,
		Decimal: 0,
		Charset: binary,
		Collate: binary,
		Elems:   nil,
	}
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
			join: &types.FieldType{
				Tp:      mysql.TypeLong,
				Flag:    mysql.AutoIncrementFlag | mysql.MultipleKeyFlag,
				Flen:    11,
				Decimal: 0,
				Charset: binary,
				Collate: binary,
				Elems:   nil,
			},
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
