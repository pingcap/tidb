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

package db

import (
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/util/mock"
	"github.com/pingcap/tidb/util/testleak"
)

func TestT(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testDBSuite{})

type testDBSuite struct {
}

func (s *testDBSuite) TestDB(c *C) {
	defer testleak.AfterTest(c)()
	ctx := mock.NewContext()

	// Set and check current schema
	schema := "test"
	BindCurrentSchema(ctx, schema)
	x := GetCurrentSchema(ctx)
	c.Assert(x, Equals, schema)

	// Invalid current schema value
	ctx.SetValue(currentDBKey, 1)
	x = GetCurrentSchema(ctx)
	c.Assert(x, Equals, "")

	// For currentDBKeyType.String()
	c.Assert(currentDBKey.String(), Equals, "current_db")
}
