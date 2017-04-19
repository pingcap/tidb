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

package plan_test

import (
	"github.com/juju/errors"
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/plan"
	"github.com/pingcap/tidb/terror"
	"github.com/pingcap/tidb/util/testleak"
)

var _ = Suite(&testValidatorSuite{})

type testValidatorSuite struct {
}

func (s *testValidatorSuite) TestValidator(c *C) {
	defer testleak.AfterTest(c)()
	tests := []struct {
		sql       string
		inPrepare bool
		err       error
	}{
		{"select ?", false, parser.ErrSyntax},
		{"select ?", true, nil},
		{"create table t(id int not null auto_increment default 2, key (id))", true,
			errors.New("Invalid default value for 'id'")},
		{"create table t(id int not null default 2 auto_increment, key (id))", true,
			errors.New("Invalid default value for 'id'")},
		// Default value can be null when the column is primary key in MySQL 5.6.
		// But it can't be null in MySQL 5.7.
		{"create table t(id int auto_increment default null, primary key (id))", true, nil},
		{"create table t(id int default null auto_increment, primary key (id))", true, nil},
		{"create table t(id int not null auto_increment)", true,
			errors.New("Incorrect table definition; there can be only one auto column and it must be defined as a key")},
		{"create table t(id int not null auto_increment, c int auto_increment, key (id, c))", true,
			errors.New("Incorrect table definition; there can be only one auto column and it must be defined as a key")},
		{"create table t(id int not null auto_increment, c int, key (c, id))", true,
			errors.New("Incorrect table definition; there can be only one auto column and it must be defined as a key")},
		{"create table t(id decimal auto_increment, key (id))", true,
			errors.New("Incorrect column specifier for column 'id'")},
		{"create table t(id float auto_increment, key (id))", true, nil},
		{"create table t(id int auto_increment) ENGINE=MYISAM", true, nil},
		{"create table t(a int primary key, b int, c varchar(10), d char(256));", true,
			errors.New("Column length too big for column 'd' (max = 255); use BLOB or TEXT instead")},
		{"create index ib on t(b,a,b);", true, errors.New("[schema:1060]Duplicate column name 'b'")},
		{"alter table t add index idx(a, b, A)", true, errors.New("[schema:1060]Duplicate column name 'A'")},
		{"create table t (a int, b int, index(a, b, A))", true, errors.New("[schema:1060]Duplicate column name 'A'")},
		{"create table t (a int, b int, key(a, b, A))", true, errors.New("[schema:1060]Duplicate column name 'A'")},
		{"create table t (a int, b int, unique(a, b, A))", true, errors.New("[schema:1060]Duplicate column name 'A'")},
		{"create table t (a int, b int, unique key(a, b, A))", true, errors.New("[schema:1060]Duplicate column name 'A'")},
		{"create table t (a int, b int, unique index(a, b, A))", true, errors.New("[schema:1060]Duplicate column name 'A'")},
		{"create table t(c1 int not null primary key, c2 int not null primary key)", true,
			errors.New("[schema:1068]Multiple primary key defined")},
		{"create table t(c1 int not null primary key, c2 int not null, primary key(c1))", true,
			errors.New("[schema:1068]Multiple primary key defined")},
		{"create table t(c1 int not null, c2 int not null, primary key(c1), primary key(c2))", true,
			errors.New("[schema:1068]Multiple primary key defined")},
		{"alter table t auto_increment=1", true, errors.New("[autoid:3]No support for setting auto_increment using alter_table")},
		{"alter table t add column c int auto_increment key, auto_increment=10", true,
			errors.New("[autoid:3]No support for setting auto_increment using alter_table")},
		{"alter table t add column c int auto_increment key", true, nil},
	}

	store, err := tidb.NewStore(tidb.EngineGoLevelDBMemory)
	c.Assert(err, IsNil)
	defer store.Close()
	se, err := tidb.CreateSession(store)
	c.Assert(err, IsNil)
	for _, tt := range tests {
		stmts, err1 := tidb.Parse(se.(context.Context), tt.sql)
		c.Assert(err1, IsNil)
		c.Assert(stmts, HasLen, 1)
		stmt := stmts[0]
		err = plan.Validate(stmt, tt.inPrepare)
		c.Assert(terror.ErrorEqual(err, tt.err), IsTrue)
	}
}
