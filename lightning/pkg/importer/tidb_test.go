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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package importer

import (
	"context"
	"database/sql"
	"math"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/go-sql-driver/mysql"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/errno"
	"github.com/pingcap/tidb/pkg/lightning/checkpoints"
	"github.com/pingcap/tidb/pkg/lightning/metric"
	"github.com/pingcap/tidb/pkg/lightning/mydump"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/model"
	tmysql "github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/pingcap/tidb/pkg/util/promutil"
	"github.com/stretchr/testify/require"
)

type tidbSuite struct {
	db     *sql.DB
	mockDB sqlmock.Sqlmock
	timgr  *TiDBManager
}

func newTiDBSuite(t *testing.T) *tidbSuite {
	var s tidbSuite
	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	s.db = db
	s.mockDB = mock
	defaultSQLMode, err := tmysql.GetSQLMode(tmysql.DefaultSQLMode)
	require.NoError(t, err)

	s.timgr = NewTiDBManagerWithDB(db, defaultSQLMode)
	t.Cleanup(func() {
		s.timgr.Close()
		require.NoError(t, s.mockDB.ExpectationsWereMet())
	})
	return &s
}

func TestDropTable(t *testing.T) {
	s := newTiDBSuite(t)
	ctx := context.Background()

	s.mockDB.
		ExpectExec("DROP TABLE `db`.`table`").
		WillReturnResult(sqlmock.NewResult(1, 1))
	s.mockDB.
		ExpectClose()

	err := s.timgr.DropTable(ctx, "`db`.`table`")
	require.NoError(t, err)
}

func TestLoadSchemaInfo(t *testing.T) {
	s := newTiDBSuite(t)

	metrics := metric.NewMetrics(promutil.NewDefaultFactory())
	ctx := metric.WithMetric(context.Background(), metrics)

	tableCntBefore := metric.ReadCounter(metrics.TableCounter.WithLabelValues(metric.TableStatePending, metric.TableResultSuccess))

	// Prepare the mock reply.
	nodes, _, err := s.timgr.parser.Parse(
		"CREATE TABLE `t1` (`a` INT PRIMARY KEY);"+
			"CREATE TABLE `t2` (`b` VARCHAR(20), `c` BOOL, KEY (`b`, `c`));"+
			// an extra table that not exists in dbMetas
			"CREATE TABLE `t3` (`d` VARCHAR(20), `e` BOOL);"+
			"CREATE TABLE `T4` (`f` BIGINT PRIMARY KEY);",
		"", "")
	require.NoError(t, err)
	tableInfos := make([]*model.TableInfo, 0, len(nodes))
	sctx := mock.NewContext()
	for i, node := range nodes {
		require.IsType(t, node, &ast.CreateTableStmt{})
		info, err := ddl.MockTableInfo(sctx, node.(*ast.CreateTableStmt), int64(i+100))
		require.NoError(t, err)
		info.State = model.StatePublic
		tableInfos = append(tableInfos, info)
	}

	dbMetas := []*mydump.MDDatabaseMeta{
		{
			Name: "db",
			Tables: []*mydump.MDTableMeta{
				{
					DB:   "db",
					Name: "t1",
				},
				{
					DB:   "db",
					Name: "t2",
				},
				{
					DB:   "db",
					Name: "t4",
				},
			},
		},
	}

	loaded, err := LoadSchemaInfo(ctx, dbMetas, func(ctx context.Context, schema string) ([]*model.TableInfo, error) {
		require.Equal(t, "db", schema)
		return tableInfos, nil
	})
	require.NoError(t, err)
	require.Equal(t, map[string]*checkpoints.TidbDBInfo{
		"db": {
			Name: "db",
			Tables: map[string]*checkpoints.TidbTableInfo{
				"t1": {
					ID:      100,
					DB:      "db",
					Name:    "t1",
					Core:    tableInfos[0],
					Desired: tableInfos[0],
				},
				"t2": {
					ID:      101,
					DB:      "db",
					Name:    "t2",
					Core:    tableInfos[1],
					Desired: tableInfos[1],
				},
				"t4": {
					ID:      103,
					DB:      "db",
					Name:    "t4",
					Core:    tableInfos[3],
					Desired: tableInfos[3],
				},
			},
		},
	}, loaded)

	tableCntAfter := metric.ReadCounter(metrics.TableCounter.WithLabelValues(metric.TableStatePending, metric.TableResultSuccess))

	require.Equal(t, 3.0, tableCntAfter-tableCntBefore)
}

func TestLoadSchemaInfoMissing(t *testing.T) {
	ctx := context.Background()

	_, err := LoadSchemaInfo(ctx, []*mydump.MDDatabaseMeta{{Name: "asdjalsjdlas"}}, func(ctx context.Context, schema string) ([]*model.TableInfo, error) {
		return nil, errors.Errorf("[schema:1049]Unknown database '%s'", schema)
	})
	require.Regexp(t, ".*Unknown database.*", err.Error())
}

func TestAlterAutoInc(t *testing.T) {
	s := newTiDBSuite(t)
	ctx := context.Background()

	s.mockDB.
		ExpectExec("\\QALTER TABLE `db`.`table` AUTO_INCREMENT=12345\\E").
		WillReturnResult(sqlmock.NewResult(1, 1))
	s.mockDB.
		ExpectExec("\\QALTER TABLE `db`.`table` FORCE AUTO_INCREMENT=9223372036854775807\\E").
		WillReturnResult(sqlmock.NewResult(1, 1))
	s.mockDB.
		ExpectClose()

	err := AlterAutoIncrement(ctx, s.db, "`db`.`table`", 12345)
	require.NoError(t, err)

	err = AlterAutoIncrement(ctx, s.db, "`db`.`table`", uint64(math.MaxInt64)+1)
	require.NoError(t, err)
}

func TestAlterAutoRandom(t *testing.T) {
	s := newTiDBSuite(t)
	ctx := context.Background()

	s.mockDB.
		ExpectExec("\\QALTER TABLE `db`.`table` AUTO_RANDOM_BASE=12345\\E").
		WillReturnResult(sqlmock.NewResult(1, 1))
	s.mockDB.
		ExpectExec("\\QALTER TABLE `db`.`table` AUTO_RANDOM_BASE=288230376151711743\\E").
		WillReturnResult(sqlmock.NewResult(1, 1))
	s.mockDB.
		ExpectClose()

	err := AlterAutoRandom(ctx, s.db, "`db`.`table`", 12345, 288230376151711743)
	require.NoError(t, err)

	// insert 288230376151711743 and try rebase to 288230376151711744
	err = AlterAutoRandom(ctx, s.db, "`db`.`table`", 288230376151711744, 288230376151711743)
	require.NoError(t, err)

	err = AlterAutoRandom(ctx, s.db, "`db`.`table`", uint64(math.MaxInt64)+1, 288230376151711743)
	require.NoError(t, err)
}

func TestObtainRowFormatVersionSucceed(t *testing.T) {
	s := newTiDBSuite(t)
	ctx := context.Background()

	s.mockDB.
		ExpectQuery(`SHOW VARIABLES WHERE Variable_name IN \(.*'tidb_row_format_version'.*\)`).
		WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).
			AddRow("tidb_row_format_version", "2").
			AddRow("max_allowed_packet", "1073741824").
			AddRow("div_precision_increment", "10").
			AddRow("time_zone", "-08:00").
			AddRow("lc_time_names", "ja_JP").
			AddRow("default_week_format", "1").
			AddRow("block_encryption_mode", "aes-256-cbc").
			AddRow("group_concat_max_len", "1073741824"))
	s.mockDB.
		ExpectClose()

	sysVars := ObtainImportantVariables(ctx, s.db, true)
	require.Equal(t, map[string]string{
		"tidb_backoff_weight":     "6",
		"tidb_row_format_version": "2",
		"max_allowed_packet":      "1073741824",
		"div_precision_increment": "10",
		"time_zone":               "-08:00",
		"lc_time_names":           "ja_JP",
		"default_week_format":     "1",
		"block_encryption_mode":   "aes-256-cbc",
		"group_concat_max_len":    "1073741824",
	}, sysVars)
}

func TestObtainRowFormatVersionFailure(t *testing.T) {
	s := newTiDBSuite(t)
	ctx := context.Background()

	s.mockDB.
		ExpectQuery(`SHOW VARIABLES WHERE Variable_name IN \(.*'tidb_row_format_version'.*\)`).
		WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).AddRow("time_zone", "+00:00"))
	s.mockDB.
		ExpectClose()

	sysVars := ObtainImportantVariables(ctx, s.db, true)
	require.Equal(t, map[string]string{
		"tidb_backoff_weight":     "6",
		"tidb_row_format_version": "1",
		"max_allowed_packet":      "67108864",
		"div_precision_increment": "4",
		"time_zone":               "+00:00",
		"lc_time_names":           "en_US",
		"default_week_format":     "0",
		"block_encryption_mode":   "aes-128-ecb",
		"group_concat_max_len":    "1024",
	}, sysVars)
}

func TestObtainNewCollationEnabled(t *testing.T) {
	s := newTiDBSuite(t)
	ctx := context.Background()

	// cannot retry on this err
	permErr := &mysql.MySQLError{Number: errno.ErrAccessDenied}
	s.mockDB.
		ExpectQuery("\\QSELECT variable_value FROM mysql.tidb WHERE variable_name = 'new_collation_enabled'\\E").
		WillReturnError(permErr)
	_, err := ObtainNewCollationEnabled(ctx, s.db)
	require.Equal(t, permErr, errors.Cause(err))

	// this error can retry
	s.mockDB.
		ExpectQuery("\\QSELECT variable_value FROM mysql.tidb WHERE variable_name = 'new_collation_enabled'\\E").
		WillReturnError(&mysql.MySQLError{Number: errno.ErrTiKVServerBusy})
	s.mockDB.
		ExpectQuery("\\QSELECT variable_value FROM mysql.tidb WHERE variable_name = 'new_collation_enabled'\\E").
		WillReturnRows(sqlmock.NewRows([]string{"variable_value"}).RowError(0, sql.ErrNoRows))
	version, err := ObtainNewCollationEnabled(ctx, s.db)
	require.NoError(t, err)
	require.Equal(t, false, version)

	kvMap := map[string]bool{
		"True":  true,
		"False": false,
	}
	for k, v := range kvMap {
		s.mockDB.
			ExpectQuery("\\QSELECT variable_value FROM mysql.tidb WHERE variable_name = 'new_collation_enabled'\\E").
			WillReturnRows(sqlmock.NewRows([]string{"variable_value"}).AddRow(k))

		version, err = ObtainNewCollationEnabled(ctx, s.db)
		require.NoError(t, err)
		require.Equal(t, v, version)
	}
	s.mockDB.
		ExpectClose()
}
