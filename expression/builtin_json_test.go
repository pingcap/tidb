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
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/util/testleak"
	"github.com/pingcap/tidb/util/types/json"
)

func (s *testEvaluatorSuite) TestJsonExtract(c *C) {
	defer testleak.AfterTest(c)()
	fc := funcs[ast.JsonExtract]

	tbl := []struct {
		JsonObj  interface{}
		Path     interface{}
		Expected json.Json
	}{
		{nil, "$", json.CreateJson(nil)},
		{"3", "$", json.CreateJson(3)},
		{`"3"`, "$", json.CreateJson("3")},
		{`{"a": [1, "2", {"aa": true}]}`, "$.a[2].aa", json.CreateJson(1)},
	}
	dtbl := tblToDtbl(tbl)

	for _, t := range dtbl {
		args := datumsToConstants(t["JsonObj"])
		args = append(args, datumsToConstants(t["Path"])...)
		f, err := fc.getFunction(args, s.ctx)
		c.Assert(err, IsNil)

		_, err = f.eval(nil)
		c.Assert(err, IsNil)
	}
}
