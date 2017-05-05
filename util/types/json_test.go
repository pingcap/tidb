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

package types

import . "github.com/pingcap/check"

var _ = Suite(&testJsonSuite{})

type testJsonSuite struct{}

func (s *testJsonSuite) TestJsonPathExprLegRe(c *C) {
	var pathExpr = "$.key1[3][*].*.key3"
	matches := jsonPathExprLegRe.FindAllString(pathExpr, -1)
	c.Assert(len(matches), Equals, 5)
	c.Assert(matches[0], Equals, ".key1")
	c.Assert(matches[1], Equals, "[3]")
	c.Assert(matches[2], Equals, "[*]")
	c.Assert(matches[3], Equals, ".*")
	c.Assert(matches[4], Equals, ".key3")
}

func (s *testJsonSuite) TestValidatePathExpr(c *C) {
	var pathExpr = "$ .   key1[3]\t[*].*.key3"
	_, err := validateJsonPathExpr(pathExpr)
	c.Assert(err, IsNil)
}

func (s *testJsonSuite) TestJson(c *C) {
	var (
		jstrList  []string
		jList     []Json
		datumList []Datum
		bytes     []byte
		err       error
		cmp       int
	)
	jstrList = []string{
		`[3, "4", 6.8, true, null, {"a": ["22", false], "b": "x"}]`,
		`[  3  , "4", 6.8, true, null, {"b": "x", "a": ["22", false]}]`,
		// TODO fix json decoder out of sync.
		// `[  3  , "4", 6.8, true, null, {"b" : "x", "a": ["22", false]}]`,
	}
	jList = make([]Json, len(jstrList))
	datumList = make([]Datum, len(jstrList))

	for i, jstr := range jstrList {
		jList[i] = CreateJson(nil)

		err = jList[i].ParseFromString(jstr)
		c.Assert(err, IsNil)

		bytes, err = jList[i].Serialize()
		c.Assert(err, IsNil)

		j := CreateJson(nil)
		err = j.Deserialize(bytes)
		c.Assert(err, IsNil)

		datumList[i].SetMysqlJson(jList[i])
	}
	cmp, err = CompareJson(jList[0], jList[1])
	c.Assert(err, IsNil)
	c.Assert(cmp, Equals, 0)

	cmp, err = datumList[0].CompareDatum(nil, datumList[1])
	c.Assert(err, IsNil)
	c.Assert(cmp, Equals, 0)
}

func (s *testJsonSuite) TestCreateJson(c *C) {
	jstrList := map[string]interface{}{
		`3`:    3,
		`"3"`:  "3",
		`null`: nil,
		`2.5`:  2.5,
	}
	for jstr, primitive := range jstrList {
		j1 := CreateJson(nil)
		j1.ParseFromString(jstr)

		j2 := CreateJson(primitive)
		cmp, err := CompareJson(j1, j2)

		c.Assert(err, IsNil)
		c.Assert(cmp, Equals, 0)
	}
}

func (s *testJsonSuite) TestCompareJson(c *C) {
	var d Datum
	var cmp int
	var err error

	d.SetInt64(3)
	cmp, err = d.compareMysqlJson(nil, CreateJson(3))
	c.Assert(err, IsNil)
	c.Assert(cmp, Equals, 0)

	d.SetString("3")
	cmp, err = d.compareMysqlJson(nil, CreateJson("3"))
	c.Assert(err, IsNil)
	c.Assert(cmp, Equals, 0)
}
