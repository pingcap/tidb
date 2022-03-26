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
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/require"
)

func TestShowGrants(t *testing.T) {
	ctx := context.Background()
	db, mock, err := sqlmock.New()
	require.NoError(t, err)

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
	require.NoError(t, err)
	require.Equal(t, mockGrants, grants)
	require.Nil(t, mock.ExpectationsWereMet())
}

func TestShowGrantsWithRoles(t *testing.T) {
	ctx := context.Background()
	db, mock, err := sqlmock.New()
	require.NoError(t, err)

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
	require.NoError(t, err)
	require.Equal(t, mockGrantsWithRoles, grants)
	require.Nil(t, mock.ExpectationsWereMet())
}

func TestShowGrantsPasswordMasked(t *testing.T) {
	ctx := context.Background()
	db, mock, err := sqlmock.New()
	require.NoError(t, err)

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
		require.NoError(t, err)
		require.Len(t, grants, 1)
		require.Equal(t, ca.expected, grants[0])
		require.Nil(t, mock.ExpectationsWereMet())
	}
}
