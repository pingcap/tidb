// Go MySQL Driver - A MySQL-Driver for Go's database/sql package
//
// Copyright 2017 The Go-MySQL-Driver Authors. All rights reserved.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

// +build go1.8

package mysql

import (
	"database/sql"
	"database/sql/driver"
	"testing"
)

func TestIsolationLevelMapping(t *testing.T) {

	data := []struct {
		level    driver.IsolationLevel
		expected string
	}{
		{
			level:    driver.IsolationLevel(sql.LevelReadCommitted),
			expected: "READ COMMITTED",
		},
		{
			level:    driver.IsolationLevel(sql.LevelRepeatableRead),
			expected: "REPEATABLE READ",
		},
		{
			level:    driver.IsolationLevel(sql.LevelReadUncommitted),
			expected: "READ UNCOMMITTED",
		},
		{
			level:    driver.IsolationLevel(sql.LevelSerializable),
			expected: "SERIALIZABLE",
		},
	}

	for i, td := range data {
		if actual, err := mapIsolationLevel(td.level); actual != td.expected || err != nil {
			t.Fatal(i, td.expected, actual, err)
		}
	}

	// check unsupported mapping
	if actual, err := mapIsolationLevel(driver.IsolationLevel(sql.LevelLinearizable)); actual != "" || err == nil {
		t.Fatal("Expected error on unsupported isolation level")
	}

}
