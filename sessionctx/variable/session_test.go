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

package variable_test

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util/mock"
	"github.com/pingcap/tidb/util/types"
)

var _ = Suite(&testSessionSuite{})

type testSessionSuite struct {
}

func (*testSessionSuite) TestSession(c *C) {
	ctx := mock.NewContext()

	variable.BindSessionVars(ctx)

	v := variable.GetSessionVars(ctx)
	c.Assert(v, NotNil)

	// For AffectedRows
	v.AddAffectedRows(1)
	c.Assert(v.AffectedRows, Equals, uint64(1))
	v.AddAffectedRows(1)
	c.Assert(v.AffectedRows, Equals, uint64(2))

	// For FoundRows
	v.AddFoundRows(1)
	c.Assert(v.FoundRows, Equals, uint64(1))
	v.AddFoundRows(1)
	c.Assert(v.FoundRows, Equals, uint64(2))

	// For last insert id
	v.SetLastInsertID(uint64(1))
	c.Assert(v.LastInsertID, Equals, uint64(1))

	v.SetSystemVar("autocommit", types.NewStringDatum("1"))
	val := v.GetSystemVar("autocommit")
	c.Assert(val.GetString(), Equals, "1")
	c.Assert(v.SetSystemVar("autocommit", types.Datum{}), NotNil)

	v.SetSystemVar("sql_mode", types.NewStringDatum("strict_trans_tables"))
	val = v.GetSystemVar("sql_mode")
	c.Assert(val.GetString(), Equals, "STRICT_TRANS_TABLES")
	c.Assert(v.StrictSQLMode, IsTrue)
	v.SetSystemVar("sql_mode", types.NewStringDatum(""))
	c.Assert(v.StrictSQLMode, IsFalse)

	v.SetSystemVar("character_set_connection", types.NewStringDatum("utf8"))
	v.SetSystemVar("collation_connection", types.NewStringDatum("utf8_general_ci"))
	charset, collation := variable.GetCharsetInfo(ctx)
	c.Assert(charset, Equals, "utf8")
	c.Assert(collation, Equals, "utf8_general_ci")

	c.Assert(v.SetSystemVar("character_set_results", types.Datum{}), IsNil)

	// Test case for get TiDBSkipConstraintCheck session variable
	d := v.GetSystemVar(variable.TiDBSkipConstraintCheck)
	c.Assert(d.GetString(), Equals, "0")

	// Test case for tidb_skip_constraint_check
	c.Assert(v.SkipConstraintCheck, IsFalse)
	v.SetSystemVar(variable.TiDBSkipConstraintCheck, types.NewStringDatum("0"))
	c.Assert(v.SkipConstraintCheck, IsFalse)
	v.SetSystemVar(variable.TiDBSkipConstraintCheck, types.NewStringDatum("1"))
	c.Assert(v.SkipConstraintCheck, IsTrue)
	v.SetSystemVar(variable.TiDBSkipConstraintCheck, types.NewStringDatum("0"))
	c.Assert(v.SkipConstraintCheck, IsFalse)

	// Test case for change TiDBSkipConstraintCheck session variable.
	v.SetSystemVar(variable.TiDBSkipConstraintCheck, types.NewStringDatum("1"))
	d = v.GetSystemVar(variable.TiDBSkipConstraintCheck)
	c.Assert(d.GetString(), Equals, "1")
}
