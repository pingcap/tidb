// Copyright 2022 PingCAP, Inc.
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

package dbutil

import (
	"context"

	"github.com/DATA-DOG/go-sqlmock"
	. "github.com/pingcap/check"
)

func (*testDBSuite) TestShowGrants(c *C) {
	ctx := context.Background()
	db, mock, err := sqlmock.New()
	c.Assert(err, IsNil)

	mockGrants := []string{
		"GRANT ALL PRIVILEGES ON *.* TO 'root'@'localhost' WITH GRANT OPTION",
		"GRANT PROXY ON ''@'' TO 'root'@'localhost' WITH GRANT OPTION",
	}
	rows := sqlmock.NewRows([]string{"Grants for root@localhost"})
	for _, g := range mockGrants {
		rows.AddRow(g)
	}
	mock.ExpectQuery("SHOW GRANTS").WillReturnRows(rows)

	grants, err := ShowGrants(ctx, db, "", "")
	c.Assert(err, IsNil)
	c.Assert(grants, DeepEquals, mockGrants)
	c.Assert(mock.ExpectationsWereMet(), IsNil)
}

func (*testDBSuite) TestShowGrantsWithRoles(c *C) {
	ctx := context.Background()
	db, mock, err := sqlmock.New()
	c.Assert(err, IsNil)

	mockGrantsWithoutRoles := []string{
		"GRANT USAGE ON *.* TO `u1`@`localhost`",
		"GRANT `r1`@`%`,`r2`@`%` TO `u1`@`localhost`",
	}
	rows1 := sqlmock.NewRows([]string{"Grants for root@localhost"})
	for _, g := range mockGrantsWithoutRoles {
		rows1.AddRow(g)
	}
	mock.ExpectQuery("SHOW GRANTS").WillReturnRows(rows1)

	mockGrantsWithRoles := []string{
		"GRANT USAGE ON *.* TO `u1`@`localhost`",
		"GRANT SELECT, INSERT, UPDATE, DELETE ON `db1`.* TO `u1`@`localhost`",
		"GRANT `r1`@`%`,`r2`@`%` TO `u1`@`localhost`",
	}
	rows2 := sqlmock.NewRows([]string{"Grants for root@localhost"})
	for _, g := range mockGrantsWithRoles {
		rows2.AddRow(g)
	}
	mock.ExpectQuery("SHOW GRANTS").WillReturnRows(rows2)

	grants, err := ShowGrants(ctx, db, "", "")
	c.Assert(err, IsNil)
	c.Assert(grants, DeepEquals, mockGrantsWithRoles)
	c.Assert(mock.ExpectationsWereMet(), IsNil)
}

func (*testDBSuite) TestShowGrantsPasswordMasked(c *C) {
	ctx := context.Background()
	db, mock, err := sqlmock.New()
	c.Assert(err, IsNil)

	cases := []struct {
		original string
		expected string
	}{
		{
			"GRANT ALL PRIVILEGES ON *.* TO 'root'@'localhost' IDENTIFIED BY PASSWORD <secret> WITH GRANT OPTION",
			"GRANT ALL PRIVILEGES ON *.* TO 'root'@'localhost' IDENTIFIED BY PASSWORD 'secret' WITH GRANT OPTION",
		},
		{
			"GRANT ALL PRIVILEGES ON *.* TO 'user'@'%' IDENTIFIED BY PASSWORD",
			"GRANT ALL PRIVILEGES ON *.* TO 'user'@'%' IDENTIFIED BY PASSWORD 'secret'",
		},
		{
			"GRANT ALL PRIVILEGES ON *.* TO 'user'@'%' IDENTIFIED BY PASSWORD WITH GRANT OPTION",
			"GRANT ALL PRIVILEGES ON *.* TO 'user'@'%' IDENTIFIED BY PASSWORD 'secret' WITH GRANT OPTION",
		},
		{
			"GRANT ALL PRIVILEGES ON *.* TO 'user'@'%' IDENTIFIED BY PASSWORD <secret>",
			"GRANT ALL PRIVILEGES ON *.* TO 'user'@'%' IDENTIFIED BY PASSWORD 'secret'",
		},
	}

	for _, ca := range cases {
		rows := sqlmock.NewRows([]string{"Grants for root@localhost"})
		rows.AddRow(ca.original)
		mock.ExpectQuery("SHOW GRANTS").WillReturnRows(rows)

		grants, err := ShowGrants(ctx, db, "", "")
		c.Assert(err, IsNil)
		c.Assert(grants, HasLen, 1)
		c.Assert(grants[0], DeepEquals, ca.expected)
		c.Assert(mock.ExpectationsWereMet(), IsNil)
	}
}
