// Copyright 2020 PingCAP, Inc.
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
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/parser/charset"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/collate"
	"github.com/pingcap/tidb/util/mock"
	"github.com/stretchr/testify/require"
)

var _ = SerialSuites(&testCollationSuites{})

type testCollationSuites struct{}

func (s *testCollationSuites) TestCompareString(c *C) {
	collate.SetNewCollationEnabledForTest(true)
	defer collate.SetNewCollationEnabledForTest(false)

	c.Assert(types.CompareString("a", "A", "utf8_general_ci"), Equals, 0)
	c.Assert(types.CompareString("À", "A", "utf8_general_ci"), Equals, 0)
	c.Assert(types.CompareString("😜", "😃", "utf8_general_ci"), Equals, 0)
	c.Assert(types.CompareString("a ", "a  ", "utf8_general_ci"), Equals, 0)
	c.Assert(types.CompareString("ß", "s", "utf8_general_ci"), Equals, 0)
	c.Assert(types.CompareString("ß", "ss", "utf8_general_ci"), Not(Equals), 0)

	c.Assert(types.CompareString("a", "A", "utf8_unicode_ci"), Equals, 0)
	c.Assert(types.CompareString("À", "A", "utf8_unicode_ci"), Equals, 0)
	c.Assert(types.CompareString("😜", "😃", "utf8_unicode_ci"), Equals, 0)
	c.Assert(types.CompareString("a ", "a  ", "utf8_unicode_ci"), Equals, 0)
	c.Assert(types.CompareString("ß", "s", "utf8_unicode_ci"), Not(Equals), 0)
	c.Assert(types.CompareString("ß", "ss", "utf8_unicode_ci"), Equals, 0)

	c.Assert(types.CompareString("a", "A", "binary"), Not(Equals), 0)
	c.Assert(types.CompareString("À", "A", "binary"), Not(Equals), 0)
	c.Assert(types.CompareString("😜", "😃", "binary"), Not(Equals), 0)
	c.Assert(types.CompareString("a ", "a  ", "binary"), Not(Equals), 0)

	ctx := mock.NewContext()
	ft := types.NewFieldType(mysql.TypeVarString)
	col1 := &Column{
		RetType: ft,
		Index:   0,
	}
	col2 := &Column{
		RetType: ft,
		Index:   1,
	}
	chk := chunk.NewChunkWithCapacity([]*types.FieldType{ft, ft}, 4)
	chk.Column(0).AppendString("a")
	chk.Column(1).AppendString("A")
	chk.Column(0).AppendString("À")
	chk.Column(1).AppendString("A")
	chk.Column(0).AppendString("😜")
	chk.Column(1).AppendString("😃")
	chk.Column(0).AppendString("a ")
	chk.Column(1).AppendString("a  ")
	for i := 0; i < 4; i++ {
		v, isNull, err := CompareStringWithCollationInfo(ctx, col1, col2, chk.GetRow(0), chk.GetRow(0), "utf8_general_ci")
		c.Assert(err, IsNil)
		c.Assert(isNull, IsFalse)
		c.Assert(v, Equals, int64(0))
	}
}

func newExpr(coll string, coercibility Coercibility, repertoire Repertoire) Expression {
	collation, _ := charset.GetCollationByName(coll)

	fieldType := newStringFieldType()
	fieldType.Collate = coll
	fieldType.Charset = collation.CharsetName

	c := &Constant{RetType: fieldType}
	c.SetRepertoire(repertoire)
	c.SetCoercibility(coercibility)

	return c
}

func TestDeriveCollationFromExprs(t *testing.T) {
	ctx := mock.NewContext()

	tests := []struct {
		id    int
		exprs []Expression
		coll  string
	}{
		{
			1,
			[]Expression{
				newExpr(charset.CollationBin, CoercibilityExplicit, ASCII),
				newExpr(charset.CollationBin, CoercibilityExplicit, ASCII),
			},
			charset.CollationBin,
		},
		{
			2,
			[]Expression{
				newExpr(charset.CollationUTF8MB4, CoercibilityImplicit, UNICODE),
				newExpr(charset.CollationASCII, CoercibilityImplicit, ASCII),
			},
			charset.CollationUTF8MB4,
		},
		{
			3,
			[]Expression{
				newExpr(charset.CollationGBKBin, CoercibilityImplicit, UNICODE),
				newExpr(charset.CollationASCII, CoercibilityImplicit, ASCII),
			},
			charset.CollationGBKBin,
		},
		{
			4,
			[]Expression{
				newExpr(charset.CollationGBKBin, CoercibilityImplicit, UNICODE),
				newExpr(charset.CollationUTF8MB4, CoercibilityExplicit, ASCII),
			},
			charset.CollationUTF8MB4,
		},
		{
			5,
			[]Expression{
				newExpr("utf8mb4_unicode_ci", CoercibilityImplicit, UNICODE),
				newExpr("utf8mb4_general_ci", CoercibilityImplicit, UNICODE),
			},
			charset.CollationUTF8MB4,
		},
		{
			6,
			[]Expression{
				newExpr("utf8mb4_unicode_ci", CoercibilityImplicit, UNICODE),
				newExpr(charset.CollationGBKBin, CoercibilityImplicit, UNICODE),
				newExpr(charset.CollationBin, CoercibilityImplicit, UNICODE),
			},
			charset.CollationBin,
		},
		{
			7,
			[]Expression{
				newExpr(charset.CollationBin, CoercibilityExplicit, UNICODE),
				newExpr(charset.CollationUTF8MB4, CoercibilityNone, UNICODE),
				newExpr(charset.CollationGBKBin, CoercibilityExplicit, ASCII),
			},
			charset.CollationBin,
		},
		{
			8,
			[]Expression{
				newExpr(charset.CollationASCII, CoercibilityImplicit, ASCII),
				newExpr(charset.CollationLatin1, CoercibilityImplicit, UNICODE),
			},
			charset.CollationLatin1,
		},
		{
			9,
			[]Expression{
				newExpr(charset.CollationUTF8, CoercibilityExplicit, UNICODE),
				newExpr(charset.CollationUTF8MB4, CoercibilityExplicit, UNICODE),
			},
			charset.CollationUTF8MB4,
		},
		{
			10,
			[]Expression{
				newExpr(charset.CollationUTF8, CoercibilityExplicit, UNICODE),
				newExpr(charset.CollationGBKBin, CoercibilityExplicit, UNICODE),
				newExpr(charset.CollationBin, CoercibilityExplicit, UNICODE),
			},
			charset.CollationBin,
		},
		{
			11,
			[]Expression{
				newExpr(charset.CollationUTF8, CoercibilityExplicit, UNICODE),
				newExpr(charset.CollationGBKBin, CoercibilityExplicit, UNICODE),
				newExpr(charset.CollationBin, CoercibilityExplicit, UNICODE),
			},
			charset.CollationBin,
		},
	}

	for _, test := range tests {
		chs, coll := DeriveCollationFromExprs(ctx, test.exprs...)
		require.Equal(t, test.coll, coll, test.id)

		collation, err := charset.GetCollationByName(coll)
		require.NoError(t, err, test.id)
		require.Equal(t, collation.CharsetName, chs, test.id)
	}
}
