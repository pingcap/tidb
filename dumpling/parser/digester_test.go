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
	. "github.com/pingcap/check"
	"github.com/pingcap/parser"
)

var _ = Suite(&testSQLDigestSuite{})

type testSQLDigestSuite struct {
}

func (s *testSQLDigestSuite) TestNormalize(c *C) {
	tests := []struct {
		input  string
		expect string
	}{
		{"SELECT 1", "select ?"},
		{"select * from b where id = 1", "select * from b where id = ?"},
		{"select 1 from b where id in (1, 3, '3', 1, 2, 3, 4)", "select ? from b where id in ( ... )"},
		{"select 1 from b where id in (1, a, 4)", "select ? from b where id in ( ? , a , ? )"},
		{"select 1 from b order by 2", "select ? from b order by 2"},
		{"select /*+ a hint */ 1", "select ?"},
		{"select /* a hint */ 1", "select ?"},
		{"select truncate(1, 2)", "select truncate ( ... )"},
		{"select -1 + - 2 + b - c + 0.2 + (-2) from c where d in (1, -2, +3)", "select ? + ? + b - c + ? + ( ? ) from c where d in ( ... )"},
		{"select * from t where a <= -1 and b < -2 and c = -3 and c > -4 and c >= -5 and e is 1", "select * from t where a <= ? and b < ? and c = ? and c > ? and c >= ? and e is ?"},
		{"select count(a), b from t group by 2", "select count ( a ) , b from t group by 2"},
		{"select count(a), b, c from t group by 2, 3", "select count ( a ) , b , c from t group by 2 , 3"},
		{"select count(a), b, c from t group by (2, 3)", "select count ( a ) , b , c from t group by ( 2 , 3 )"},
		{"select a, b from t order by 1, 2", "select a , b from t order by 1 , 2"},
		{"select count(*) from t", "select count ( ? ) from t"},
		{"select * from t Force Index(kk)", "select * from t"},
		{"select * from t USE Index(kk)", "select * from t"},
		{"select * from t Ignore Index(kk)", "select * from t"},
		{"select * from t1 straight_join t2 on t1.id=t2.id", "select * from t1 join t2 on t1 . id = t2 . id"},
		// test syntax error, it will be checked by parser, but it should not make normalize dead loop.
		{"select * from t ignore index(", "select * from t ignore index"},
		{"select /*+ ", "select "},
		{"select * from 🥳", "select * from"},
		{"select 1 / 2", "select ? / ?"},
	}
	for _, test := range tests {
		normalized := parser.Normalize(test.input)
		digest := parser.DigestNormalized(normalized)
		c.Assert(normalized, Equals, test.expect)

		normalized2, digest2 := parser.NormalizeDigest(test.input)
		c.Assert(normalized2, Equals, normalized)
		c.Assert(digest2, Equals, digest, Commentf("%+v", test))
	}
}

func (s *testSQLDigestSuite) TestNormalizeDigest(c *C) {
	tests := []struct {
		sql        string
		normalized string
		digest     string
	}{
		{"select 1 from b where id in (1, 3, '3', 1, 2, 3, 4)", "select ? from b where id in ( ... )", "f36161eef94dbfbd5e2f6b9a2f498a4c7facc6860621fbeb8084f63898275016"},
	}
	for _, test := range tests {
		normalized, digest := parser.NormalizeDigest(test.sql)
		c.Assert(normalized, Equals, test.normalized)
		c.Assert(digest, Equals, test.digest)

		normalized = parser.Normalize(test.sql)
		digest = parser.DigestNormalized(normalized)
		c.Assert(normalized, Equals, test.normalized)
		c.Assert(digest, Equals, test.digest)
	}
}

func (s *testSQLDigestSuite) TestDigestHashEqForSimpleSQL(c *C) {
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
				d = dig
				continue
			}
			c.Assert(d, Equals, dig)
		}
	}
}

func (s *testSQLDigestSuite) TestDigestHashNotEqForSimpleSQL(c *C) {
	sqlGroups := [][]string{
		{"select * from b where id = 1", "select a from b where id = 1", "select * from d where bid =1"},
	}
	for _, sqlGroup := range sqlGroups {
		var d string
		for _, sql := range sqlGroup {
			dig := parser.DigestHash(sql)
			if d == "" {
				d = dig
				continue
			}
			c.Assert(d, Not(Equals), dig)
		}
	}
}
