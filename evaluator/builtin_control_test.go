// Copyright 2015 PingCAP, Inc.
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

package evaluator

import (
	"errors"

	. "github.com/pingcap/check"

	"github.com/pingcap/tidb/util/testleak"
	"github.com/pingcap/tidb/util/testutil"
	"github.com/pingcap/tidb/util/types"
)

func (s *testEvaluatorSuite) TestIf(c *C) {
	defer testleak.AfterTest(c)()
	tbl := []struct {
		Arg1 interface{}
		Arg2 interface{}
		Arg3 interface{}
		Ret  interface{}
	}{
		{1, 1, 2, 1},
		{nil, 1, 2, 2},
		{0, 1, 2, 2},
	}

	for _, t := range tbl {
		d, err := builtinIf(types.MakeDatums([]interface{}{t.Arg1, t.Arg2, t.Arg3}...), nil)
		c.Assert(err, IsNil)
		c.Assert(d, testutil.DatumEquals, types.NewDatum(t.Ret))
	}

	_, err := builtinIf(types.MakeDatums([]interface{}{errors.New("must error"), 1, 2}...), nil)
	c.Assert(err, NotNil)
}

func (s *testEvaluatorSuite) TestIfNull(c *C) {
	defer testleak.AfterTest(c)()
	tbl := []struct {
		Arg1 interface{}
		Arg2 interface{}
		Ret  interface{}
	}{
		{1, 2, 1},
		{nil, 2, 2},
		{nil, nil, nil},
	}

	for _, t := range tbl {
		d, err := builtinIfNull(types.MakeDatums([]interface{}{t.Arg1, t.Arg2}...), nil)
		c.Assert(err, IsNil)
		c.Assert(d, testutil.DatumEquals, types.NewDatum(t.Ret))
	}
}

func (s *testEvaluatorSuite) TestNullIf(c *C) {
	defer testleak.AfterTest(c)()
	tbl := []struct {
		Arg1 interface{}
		Arg2 interface{}
		Ret  interface{}
	}{
		{1, 1, nil},
		{nil, 2, nil},
		{1, nil, 1},
		{1, 2, 1},
	}

	for _, t := range tbl {
		d, err := builtinNullIf(types.MakeDatums([]interface{}{t.Arg1, t.Arg2}...), nil)
		c.Assert(err, IsNil)
		c.Assert(d, testutil.DatumEquals, types.NewDatum(t.Ret))
	}
}
