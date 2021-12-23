// Copyright 2019 PingCAP, Inc.
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

package parser_test

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"testing"

	"github.com/pingcap/tidb/parser"
	"github.com/stretchr/testify/require"
)

func TestNormalize(t *testing.T) {
	tests := []struct {
		input  string
		expect string
	}{
		{"select _utf8mb4'123'", "select (_charset) ?"},
		{"SELECT 1", "select ?"},
		{"select null", "select ?"},
		{"select \\N", "select ?"},
		{"SELECT `null`", "select `null`"},
		{"select * from b where id = 1", "select * from `b` where `id` = ?"},
		{"select 1 from b where id in (1, 3, '3', 1, 2, 3, 4)", "select ? from `b` where `id` in ( ... )"},
		{"select 1 from b where id in (1, a, 4)", "select ? from `b` where `id` in ( ? , `a` , ? )"},
		{"select 1 from b order by 2", "select ? from `b` order by 2"},
		{"select /*+ a hint */ 1", "select ?"},
		{"select /* a hint */ 1", "select ?"},
		{"select truncate(1, 2)", "select truncate ( ... )"},
		{"select -1 + - 2 + b - c + 0.2 + (-2) from c where d in (1, -2, +3)", "select ? + ? + `b` - `c` + ? + ( ? ) from `c` where `d` in ( ... )"},
		{"select * from t where a <= -1 and b < -2 and c = -3 and c > -4 and c >= -5 and e is 1", "select * from `t` where `a` <= ? and `b` < ? and `c` = ? and `c` > ? and `c` >= ? and `e` is ?"},
		{"select count(a), b from t group by 2", "select count ( `a` ) , `b` from `t` group by 2"},
		{"select count(a), b, c from t group by 2, 3", "select count ( `a` ) , `b` , `c` from `t` group by 2 , 3"},
		{"select count(a), b, c from t group by (2, 3)", "select count ( `a` ) , `b` , `c` from `t` group by ( 2 , 3 )"},
		{"select a, b from t order by 1, 2", "select `a` , `b` from `t` order by 1 , 2"},
		{"select count(*) from t", "select count ( ? ) from `t`"},
		{"select * from t Force Index(kk)", "select * from `t`"},
		{"select * from t USE Index(kk)", "select * from `t`"},
		{"select * from t Ignore Index(kk)", "select * from `t`"},
		{"select * from t1 straight_join t2 on t1.id=t2.id", "select * from `t1` join `t2` on `t1` . `id` = `t2` . `id`"},
		{"select * from `table`", "select * from `table`"},
		{"select * from `30`", "select * from `30`"},
		{"select * from `select`", "select * from `select`"},
		// test syntax error, it will be checked by parser, but it should not make normalize dead loop.
		{"select * from t ignore index(", "select * from `t` ignore index"},
		{"select /*+ ", "select "},
		{"select * from 🥳", "select * from"},
		{"select 1 / 2", "select ? / ?"},
		{"select * from t where a = 40 limit ?, ?", "select * from `t` where `a` = ? limit ..."},
		{"select * from t where a > ?", "select * from `t` where `a` > ?"},
		{"select @a=b from t", "select @a = `b` from `t`"},
		{"select * from `table", "select * from"},
	}
	for _, test := range tests {
		normalized := parser.Normalize(test.input)
		digest := parser.DigestNormalized(normalized)
		require.Equal(t, test.expect, normalized)

		normalized2, digest2 := parser.NormalizeDigest(test.input)
		require.Equal(t, normalized, normalized2)
		require.Equalf(t, digest.String(), digest2.String(), "%+v", test)
	}
}

func TestNormalizeDigest(t *testing.T) {
	tests := []struct {
		sql        string
		normalized string
		digest     string
	}{
		{"select 1 from b where id in (1, 3, '3', 1, 2, 3, 4)", "select ? from `b` where `id` in ( ... )", "e1c8cc2738f596dc24f15ef8eb55e0d902910d7298983496362a7b46dbc0b310"},
	}
	for _, test := range tests {
		normalized, digest := parser.NormalizeDigest(test.sql)
		require.Equal(t, test.normalized, normalized)
		require.Equal(t, test.digest, digest.String())

		normalized = parser.Normalize(test.sql)
		digest = parser.DigestNormalized(normalized)
		require.Equal(t, test.normalized, normalized)
		require.Equal(t, test.digest, digest.String())
	}
}

func TestDigestHashEqForSimpleSQL(t *testing.T) {
	sqlGroups := [][]string{
		{"select * from b where id = 1", "select * from b where id = '1'", "select * from b where id =2"},
		{"select 2 from b, c where c.id > 1", "select 4 from b, c where c.id > 23"},
		{"Select 3", "select 1"},
	}
	for _, sqlGroup := range sqlGroups {
		var d string
		for _, sql := range sqlGroup {
			dig := parser.DigestHash(sql)
			if d == "" {
				d = dig.String()
				continue
			}
			require.Equal(t, dig.String(), d)
		}
	}
}

func TestDigestHashNotEqForSimpleSQL(t *testing.T) {
	sqlGroups := [][]string{
		{"select * from b where id = 1", "select a from b where id = 1", "select * from d where bid =1"},
	}
	for _, sqlGroup := range sqlGroups {
		var d string
		for _, sql := range sqlGroup {
			dig := parser.DigestHash(sql)
			if d == "" {
				d = dig.String()
				continue
			}
			require.NotEqual(t, dig.String(), d)
		}
	}
}

func TestGenDigest(t *testing.T) {
	hash := genRandDigest("abc")
	digest := parser.NewDigest(hash)
	require.Equal(t, fmt.Sprintf("%x", hash), digest.String())
	require.Equal(t, hash, digest.Bytes())
	digest = parser.NewDigest(nil)
	require.Equal(t, "", digest.String())
	require.Nil(t, digest.Bytes())
}

func genRandDigest(str string) []byte {
	hasher := sha256.New()
	hasher.Write([]byte(str))
	return hasher.Sum(nil)
}

func BenchmarkDigestHexEncode(b *testing.B) {
	digest1 := genRandDigest("abc")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		hex.EncodeToString(digest1)
	}
}

func BenchmarkDigestSprintf(b *testing.B) {
	digest1 := genRandDigest("abc")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		fmt.Sprintf("%x", digest1)
	}
}
