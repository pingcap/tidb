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

package checker

import (
	"context"
	"database/sql"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/util/dbutil"
)

// DDLSyncer can sync the table structure from upstream(usually MySQL) to ExecutableChecker
type DDLSyncer struct {
	db *sql.DB
	ec *ExecutableChecker
}

// NewDDLSyncer create a new DDLSyncer
func NewDDLSyncer(cfg *dbutil.DBConfig, executableChecker *ExecutableChecker) (*DDLSyncer, error) {
	db, err := dbutil.OpenDB(*cfg, nil)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &DDLSyncer{db, executableChecker}, nil
}

// SyncTable can sync table structure from upstream by table name
func (ds *DDLSyncer) SyncTable(tidbContext context.Context, schemaName string, tableName string) error {
	createTableSQL, err := dbutil.GetCreateTableSQL(context.Background(), ds.db, schemaName, tableName)
	if err != nil {
		return errors.Trace(err)
	}
	err = ds.ec.DropTable(tidbContext, tableName)
	if err != nil {
		return errors.Trace(err)
	}
	err = ds.ec.Execute(tidbContext, createTableSQL)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

// Close the DDLSyncer, if the ExecutableChecker in DDLSyncer is open, it will be closed, too
func (ds *DDLSyncer) Close() error {
	err1 := ds.ec.Close()
	err2 := dbutil.CloseDB(ds.db)
	if err1 != nil {
		return errors.Trace(err1)
	}
	if err2 != nil {
		return errors.Trace(err2)
	}
	return nil
}
