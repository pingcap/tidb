// Go MySQL Driver - A MySQL-Driver for Go's database/sql package
//
// Copyright 2012 The Go-MySQL-Driver Authors. All rights reserved.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

package mysql

type mysqlResult struct {
	affectedRows int64
	insertID     int64
}

// LastInsertId should return the last MySQL insert id
func (res *mysqlResult) LastInsertId() (int64, error) {
	return res.insertID, nil
}

// RowsAffected indicate how many rows were affected by the command
func (res *mysqlResult) RowsAffected() (int64, error) {
	return res.affectedRows, nil
}
