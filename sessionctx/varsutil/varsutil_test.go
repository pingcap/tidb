// Copyright 2016 PingCAP, Inc.
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

package varsutil

import (
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util/testleak"
	"github.com/pingcap/tidb/util/types"
)

func TestT(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testVarsutilSuite{})

type testVarsutilSuite struct {
}

func (s *testVarsutilSuite) TestVarsutil(c *C) {
	defer testleak.AfterTest(c)()
	v := variable.NewSessionVars()

	SetSystemVar(v, "autocommit", types.NewStringDatum("1"))
	val := GetSystemVar(v, "autocommit")
	c.Assert(val.GetString(), Equals, "1")
	c.Assert(SetSystemVar(v, "autocommit", types.Datum{}), NotNil)

	SetSystemVar(v, "sql_mode", types.NewStringDatum("strict_trans_tables"))
	val = GetSystemVar(v, "sql_mode")
	c.Assert(val.GetString(), Equals, "STRICT_TRANS_TABLES")
	c.Assert(v.StrictSQLMode, IsTrue)
	SetSystemVar(v, "sql_mode", types.NewStringDatum(""))
	c.Assert(v.StrictSQLMode, IsFalse)

	SetSystemVar(v, "character_set_connection", types.NewStringDatum("utf8"))
	SetSystemVar(v, "collation_connection", types.NewStringDatum("utf8_general_ci"))
	charset, collation := v.GetCharsetInfo()
	c.Assert(charset, Equals, "utf8")
	c.Assert(collation, Equals, "utf8_general_ci")

	c.Assert(SetSystemVar(v, "character_set_results", types.Datum{}), IsNil)

	// Test case for get TiDBSkipConstraintCheck session variable
	d := GetSystemVar(v, variable.TiDBSkipConstraintCheck)
	c.Assert(d.GetString(), Equals, "0")

	// Test case for tidb_skip_constraint_check
	c.Assert(v.SkipConstraintCheck, IsFalse)
	SetSystemVar(v, variable.TiDBSkipConstraintCheck, types.NewStringDatum("0"))
	c.Assert(v.SkipConstraintCheck, IsFalse)
	SetSystemVar(v, variable.TiDBSkipConstraintCheck, types.NewStringDatum("1"))
	c.Assert(v.SkipConstraintCheck, IsTrue)
	SetSystemVar(v, variable.TiDBSkipConstraintCheck, types.NewStringDatum("0"))
	c.Assert(v.SkipConstraintCheck, IsFalse)

	// Test case for change TiDBSkipConstraintCheck session variable.
	SetSystemVar(v, variable.TiDBSkipConstraintCheck, types.NewStringDatum("1"))
	d = GetSystemVar(v, variable.TiDBSkipConstraintCheck)
	c.Assert(d.GetString(), Equals, "1")
}
