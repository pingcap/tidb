// Copyright 2026 PingCAP, Inc.
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

package checkpoints

import (
	"context"
	"database/sql"

	gmysql "github.com/go-sql-driver/mysql"
	"github.com/pingcap/errors"

	"github.com/pingcap/tidb/pkg/lightning/common"
	"github.com/pingcap/tidb/pkg/lightning/log"
	"github.com/pingcap/tidb/pkg/lightning/pkdbsummary"
	"go.uber.org/zap"
)

const importStatisticsTable = "pkdb_import_statistics_v1"

// ImportStatisticsStore returns the optional statistics sidecar of a checkpoint
// database. It does not extend DB, so external checkpoint implementations and
// disabled checkpoints retain their existing import behavior.
func ImportStatisticsStore(db DB) pkdbsummary.Store {
	switch db := db.(type) {
	case *FileCheckpointsDB:
		db.lock.Lock()
		defer db.lock.Unlock()
		if db.statistics == nil {
			db.statistics = &fileStatisticsStore{db: db}
		}
		return db.statistics
	case *MySQLCheckpointsDB:
		db.statisticsEnabled = true
		return pkdbsummary.NewSQLStore(db.db, db.schema)
	default:
		return nil
	}
}

func (cpdb *MySQLCheckpointsDB) moveImportStatistics(ctx context.Context, newSchema string) {
	if !cpdb.statisticsEnabled {
		return
	}
	query := common.SprintfWithIdentifiers("RENAME TABLE %s.%s TO %s.%s", cpdb.schema, importStatisticsTable, newSchema, importStatisticsTable)
	if _, err := cpdb.db.ExecContext(ctx, query); err != nil {
		log.L().Warn("could not back up import statistics", zap.Error(err))
	}
}

func (cpdb *MySQLCheckpointsDB) removeImportStatistics(ctx context.Context, tx *sql.Tx, tables []string) error {
	err := pkdbsummary.NewSQLStore(cpdb.db, cpdb.schema).RemoveTables(ctx, tx, tables)
	// Checkpoints created by older versions have no optional statistics table.
	if mysqlErr, ok := errors.Cause(err).(*gmysql.MySQLError); ok && mysqlErr.Number == 1146 {
		return nil
	}
	return err
}
