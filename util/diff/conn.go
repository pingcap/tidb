// Copyright 2019 PingCAP, Inc.
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

package diff

import (
	"context"
	"database/sql"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb-tools/pkg/dbutil"
)

// CreateDB creates sql.DB used for select data
func CreateDB(ctx context.Context, dbConfig dbutil.DBConfig, vars map[string]string, num int) (db *sql.DB, err error) {
	db, err = dbutil.OpenDB(dbConfig, vars)
	if err != nil {
		return nil, errors.Errorf("create db connections %s error %v", dbConfig.String(), err)
	}

	// SetMaxOpenConns and SetMaxIdleConns for connection to avoid error like
	// `dial tcp 10.26.2.1:3306: connect: cannot assign requested address`
	db.SetMaxOpenConns(num)
	db.SetMaxIdleConns(num)

	return db, nil
}

// CreateDBForCP creates sql.DB used for write data for checkpoint
func CreateDBForCP(ctx context.Context, dbConfig dbutil.DBConfig) (cpDB *sql.DB, err error) {
	// set snapshot to empty, this DB used for write checkpoint data
	dbConfig.Snapshot = ""
	cpDB, err = dbutil.OpenDB(dbConfig, nil)
	if err != nil {
		return nil, errors.Errorf("create db connections %+v error %v", dbConfig, err)
	}
	cpDB.SetMaxOpenConns(1)
	cpDB.SetMaxIdleConns(1)

	return cpDB, nil
}
