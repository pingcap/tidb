// Copyright 2021 PingCAP, Inc.
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

package errormanager_test

import (
	"bytes"
	"context"
	"database/sql/driver"
	"fmt"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/lightning/backend/encode"
	tidbkv "github.com/pingcap/tidb/pkg/lightning/backend/kv"
	"github.com/pingcap/tidb/pkg/lightning/config"
	"github.com/pingcap/tidb/pkg/lightning/errormanager"
	"github.com/pingcap/tidb/pkg/lightning/log"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/table/tables"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
	tikverr "github.com/tikv/client-go/v2/error"
	"go.uber.org/atomic"
)

func TestReplaceConflictMultipleKeysNonclusteredPk(t *testing.T) {
	p := parser.New()
	node, _, err := p.ParseSQL("create table a (a int primary key nonclustered, b int not null, c int not null, d text, key key_b(b), key key_c(c));")
	require.NoError(t, err)
	mockSctx := mock.NewContext()
	mockSctx.GetSessionVars().EnableClusteredIndex = variable.ClusteredIndexDefModeOff
	info, err := ddl.MockTableInfo(mockSctx, node[0].(*ast.CreateTableStmt), 108)
	require.NoError(t, err)
	info.State = model.StatePublic
	require.False(t, info.PKIsHandle)
	tbl, err := tables.TableFromMeta(tidbkv.NewPanickingAllocators(info.SepAutoInc(), 0), info)
	require.NoError(t, err)
	require.False(t, tbl.Meta().HasClusteredIndex())

	sessionOpts := encode.SessionOptions{
		SQLMode:   mysql.ModeStrictAllTables,
		Timestamp: 1234567890,
	}

	encoder, err := tidbkv.NewBaseKVEncoder(&encode.EncodingConfig{
		Table:          tbl,
		SessionOptions: sessionOpts,
		Logger:         log.L(),
	})
	require.NoError(t, err)
	encoder.SessionCtx.GetSessionVars().RowEncoder.Enable = true

	data1 := []types.Datum{
		types.NewIntDatum(1),
		types.NewIntDatum(1),
		types.NewIntDatum(1),
		types.NewStringDatum("1.csv"),
		types.NewIntDatum(1),
	}
	data2 := []types.Datum{
		types.NewIntDatum(1),
		types.NewIntDatum(1),
		types.NewIntDatum(2),
		types.NewStringDatum("2.csv"),
		types.NewIntDatum(2),
	}
	data3 := []types.Datum{
		types.NewIntDatum(2),
		types.NewIntDatum(2),
		types.NewIntDatum(3),
		types.NewStringDatum("3.csv"),
		types.NewIntDatum(3),
	}
	data4 := []types.Datum{
		types.NewIntDatum(3),
		types.NewIntDatum(3),
		types.NewIntDatum(3),
		types.NewStringDatum("3.csv"),
		types.NewIntDatum(4),
	}
	data5 := []types.Datum{
		types.NewIntDatum(3),
		types.NewIntDatum(3),
		types.NewIntDatum(3),
		types.NewStringDatum("4.csv"),
		types.NewIntDatum(5),
	}
	data6 := []types.Datum{
		types.NewIntDatum(4),
		types.NewIntDatum(4),
		types.NewIntDatum(4),
		types.NewStringDatum("4.csv"),
		types.NewIntDatum(6),
	}
	data7 := []types.Datum{
		types.NewIntDatum(5),
		types.NewIntDatum(4),
		types.NewIntDatum(5),
		types.NewStringDatum("5.csv"),
		types.NewIntDatum(7),
	}
	tctx := encoder.SessionCtx.GetTableCtx()
	_, err = encoder.Table.AddRecord(tctx, data1)
	require.NoError(t, err)
	_, err = encoder.Table.AddRecord(tctx, data2)
	require.NoError(t, err)
	_, err = encoder.Table.AddRecord(tctx, data3)
	require.NoError(t, err)
	_, err = encoder.Table.AddRecord(tctx, data4)
	require.NoError(t, err)
	_, err = encoder.Table.AddRecord(tctx, data5)
	require.NoError(t, err)
	_, err = encoder.Table.AddRecord(tctx, data6)
	require.NoError(t, err)
	_, err = encoder.Table.AddRecord(tctx, data7)
	require.NoError(t, err)
	kvPairs := encoder.SessionCtx.TakeKvPairs()

	data2IndexKey := kvPairs.Pairs[5].Key
	data2IndexValue := kvPairs.Pairs[5].Val
	data6IndexKey := kvPairs.Pairs[17].Key

	data1RowKey := kvPairs.Pairs[0].Key
	data2RowKey := kvPairs.Pairs[3].Key
	data2RowValue := kvPairs.Pairs[3].Val
	data3RowKey := kvPairs.Pairs[6].Key
	data3RowValue := kvPairs.Pairs[6].Val
	data5RowKey := kvPairs.Pairs[12].Key
	data6RowKey := kvPairs.Pairs[15].Key
	data6RowValue := kvPairs.Pairs[15].Val
	data7RowKey := kvPairs.Pairs[18].Key
	data7RowValue := kvPairs.Pairs[18].Val

	data2NonclusteredKey := kvPairs.Pairs[4].Key
	data2NonclusteredValue := kvPairs.Pairs[4].Val
	data3NonclusteredValue := kvPairs.Pairs[7].Val
	data6NonclusteredKey := kvPairs.Pairs[16].Key
	data6NonclusteredValue := kvPairs.Pairs[16].Val
	data7NonclusteredValue := kvPairs.Pairs[19].Val

	db, mockDB, err := sqlmock.New()
	require.NoError(t, err)
	defer func() {
		_ = db.Close()
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mockDB.ExpectExec("CREATE SCHEMA IF NOT EXISTS `lightning_task_info`").
		WillReturnResult(sqlmock.NewResult(1, 1))
	mockDB.ExpectExec("CREATE TABLE IF NOT EXISTS `lightning_task_info`\\.conflict_error_v3.*").
		WillReturnResult(sqlmock.NewResult(2, 1))
	mockDB.ExpectExec("CREATE OR REPLACE VIEW `lightning_task_info`\\.conflict_view.*").
		WillReturnResult(sqlmock.NewResult(3, 1))
	mockDB.ExpectQuery("\\QSELECT _tidb_rowid, raw_key, index_name, raw_value, raw_handle FROM `lightning_task_info`.conflict_error_v3 WHERE table_name = ? AND kv_type = 0 AND _tidb_rowid >= ? and _tidb_rowid < ? ORDER BY _tidb_rowid LIMIT ?\\E").
		WillReturnRows(sqlmock.NewRows([]string{"_tidb_rowid", "raw_key", "index_name", "raw_value", "raw_handle"}).
			AddRow(1, data2RowKey, "PRIMARY", data2RowValue, data1RowKey).
			AddRow(2, data2RowKey, "PRIMARY", data3NonclusteredValue, data2NonclusteredKey).
			AddRow(3, data6RowKey, "PRIMARY", data6RowValue, data5RowKey).
			AddRow(4, data6RowKey, "PRIMARY", data7NonclusteredValue, data6NonclusteredKey))
	mockDB.ExpectBegin()
	mockDB.ExpectExec("INSERT INTO `lightning_task_info`\\.conflict_error_v3.*").
		WithArgs(0, "a", nil, nil, data2NonclusteredKey, data2NonclusteredValue, 2,
			0, "a", nil, nil, data6NonclusteredKey, data6NonclusteredValue, 2).
		WillReturnResult(driver.ResultNoRows)
	mockDB.ExpectCommit()
	for i := 0; i < 2; i++ {
		mockDB.ExpectQuery("\\QSELECT _tidb_rowid, raw_key, index_name, raw_value, raw_handle FROM `lightning_task_info`.conflict_error_v3 WHERE table_name = ? AND kv_type = 0 AND _tidb_rowid >= ? and _tidb_rowid < ? ORDER BY _tidb_rowid LIMIT ?\\E").
			WillReturnRows(sqlmock.NewRows([]string{"_tidb_rowid", "raw_key", "index_name", "raw_value", "raw_handle"}))
	}
	mockDB.ExpectQuery("\\QSELECT _tidb_rowid, raw_key, raw_value FROM `lightning_task_info`.conflict_error_v3 WHERE table_name = ? AND kv_type <> 0 AND _tidb_rowid >= ? and _tidb_rowid < ? ORDER BY _tidb_rowid LIMIT ?\\E").
		WillReturnRows(sqlmock.NewRows([]string{"_tidb_rowid", "raw_key", "raw_value"}).
			AddRow(1, data2NonclusteredKey, data2NonclusteredValue).
			AddRow(2, data6NonclusteredKey, data6NonclusteredValue))
	for i := 0; i < 2; i++ {
		mockDB.ExpectQuery("\\QSELECT _tidb_rowid, raw_key, raw_value FROM `lightning_task_info`.conflict_error_v3 WHERE table_name = ? AND kv_type <> 0 AND _tidb_rowid >= ? and _tidb_rowid < ? ORDER BY _tidb_rowid LIMIT ?\\E").
			WillReturnRows(sqlmock.NewRows([]string{"_tidb_rowid", "raw_key", "raw_value"}))
	}
	mockDB.ExpectBegin()
	mockDB.ExpectExec("DELETE FROM `lightning_task_info`\\.conflict_error_v3.*").
		WillReturnResult(sqlmock.NewResult(0, 2))
	mockDB.ExpectCommit()
	mockDB.ExpectBegin()
	mockDB.ExpectExec("DELETE FROM `lightning_task_info`\\.conflict_error_v3.*").
		WillReturnResult(sqlmock.NewResult(0, 0))
	mockDB.ExpectCommit()

	cfg := config.NewConfig()
	cfg.Conflict.Strategy = config.ReplaceOnDup
	cfg.TikvImporter.Backend = config.BackendLocal
	cfg.App.TaskInfoSchemaName = "lightning_task_info"
	em := errormanager.New(db, cfg, log.L())
	err = em.Init(ctx)
	require.NoError(t, err)

	fnGetLatestCount := atomic.NewInt32(0)
	fnDeleteKeyCount := atomic.NewInt32(0)
	pool := util.NewWorkerPool(16, "resolve duplicate rows by replace")
	err = em.ReplaceConflictKeys(
		ctx, tbl, "a", pool,
		func(ctx context.Context, key []byte) ([]byte, error) {
			fnGetLatestCount.Add(1)
			switch {
			case bytes.Equal(key, data2RowKey):
				return data2RowValue, nil
			case bytes.Equal(key, data2NonclusteredKey):
				if fnGetLatestCount.String() == "3" {
					return data2NonclusteredValue, nil
				}
				return nil, tikverr.ErrNotExist
			case bytes.Equal(key, data6RowKey):
				return data6RowValue, nil
			case bytes.Equal(key, data6NonclusteredKey):
				if fnGetLatestCount.String() == "6" {
					return data6NonclusteredValue, nil
				}
				return nil, tikverr.ErrNotExist
			case bytes.Equal(key, data2IndexKey):
				return data2IndexValue, nil
			case bytes.Equal(key, data3RowKey):
				return data3RowValue, nil
			case bytes.Equal(key, data6IndexKey):
				return data3RowValue, nil
			case bytes.Equal(key, data7RowKey):
				return data7RowValue, nil
			default:
				return nil, fmt.Errorf("key %v is not expected", key)
			}
		},
		func(ctx context.Context, keys [][]byte) error {
			fnDeleteKeyCount.Add(int32(len(keys)))
			for _, key := range keys {
				if !bytes.Equal(key, data2NonclusteredKey) && !bytes.Equal(key, data6NonclusteredKey) && !bytes.Equal(key, data2IndexKey) && !bytes.Equal(key, data3RowKey) && !bytes.Equal(key, data6IndexKey) && !bytes.Equal(key, data7RowKey) {
					return fmt.Errorf("key %v is not expected", key)
				}
			}
			return nil
		},
	)
	require.NoError(t, err)
	require.Equal(t, int32(16), fnGetLatestCount.Load())
	require.Equal(t, int32(6), fnDeleteKeyCount.Load())
	err = mockDB.ExpectationsWereMet()
	require.NoError(t, err)
}

func TestReplaceConflictOneKeyNonclusteredPk(t *testing.T) {
	p := parser.New()
	node, _, err := p.ParseSQL("create table a (a int primary key nonclustered, b int not null, c text, key key_b(b));")
	require.NoError(t, err)
	mockSctx := mock.NewContext()
	mockSctx.GetSessionVars().EnableClusteredIndex = variable.ClusteredIndexDefModeOff
	info, err := ddl.MockTableInfo(mockSctx, node[0].(*ast.CreateTableStmt), 108)
	require.NoError(t, err)
	info.State = model.StatePublic
	require.False(t, info.PKIsHandle)
	tbl, err := tables.TableFromMeta(tidbkv.NewPanickingAllocators(info.SepAutoInc(), 0), info)
	require.NoError(t, err)
	require.False(t, tbl.Meta().HasClusteredIndex())

	sessionOpts := encode.SessionOptions{
		SQLMode:   mysql.ModeStrictAllTables,
		Timestamp: 1234567890,
	}

	encoder, err := tidbkv.NewBaseKVEncoder(&encode.EncodingConfig{
		Table:          tbl,
		SessionOptions: sessionOpts,
		Logger:         log.L(),
	})
	require.NoError(t, err)
	encoder.SessionCtx.GetSessionVars().RowEncoder.Enable = true

	data1 := []types.Datum{
		types.NewIntDatum(1),
		types.NewIntDatum(6),
		types.NewStringDatum("1.csv"),
		types.NewIntDatum(1),
	}
	data2 := []types.Datum{
		types.NewIntDatum(2),
		types.NewIntDatum(6),
		types.NewStringDatum("2.csv"),
		types.NewIntDatum(2),
	}
	data3 := []types.Datum{
		types.NewIntDatum(3),
		types.NewIntDatum(3),
		types.NewStringDatum("3.csv"),
		types.NewIntDatum(3),
	}
	data4 := []types.Datum{
		types.NewIntDatum(3),
		types.NewIntDatum(4),
		types.NewStringDatum("4.csv"),
		types.NewIntDatum(4),
	}
	data5 := []types.Datum{
		types.NewIntDatum(5),
		types.NewIntDatum(4),
		types.NewStringDatum("5.csv"),
		types.NewIntDatum(5),
	}
	tctx := encoder.SessionCtx.GetTableCtx()
	_, err = encoder.Table.AddRecord(tctx, data1)
	require.NoError(t, err)
	_, err = encoder.Table.AddRecord(tctx, data2)
	require.NoError(t, err)
	_, err = encoder.Table.AddRecord(tctx, data3)
	require.NoError(t, err)
	_, err = encoder.Table.AddRecord(tctx, data4)
	require.NoError(t, err)
	_, err = encoder.Table.AddRecord(tctx, data5)
	require.NoError(t, err)
	kvPairs := encoder.SessionCtx.TakeKvPairs()

	data3IndexKey := kvPairs.Pairs[8].Key
	data3IndexValue := kvPairs.Pairs[8].Val
	data4IndexValue := kvPairs.Pairs[11].Val
	data3RowKey := kvPairs.Pairs[6].Key
	data4RowKey := kvPairs.Pairs[9].Key
	data4RowValue := kvPairs.Pairs[9].Val
	data4NonclusteredKey := kvPairs.Pairs[10].Key
	data4NonclusteredValue := kvPairs.Pairs[10].Val

	db, mockDB, err := sqlmock.New()
	require.NoError(t, err)
	defer func() {
		_ = db.Close()
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mockDB.ExpectExec("CREATE SCHEMA IF NOT EXISTS `lightning_task_info`").
		WillReturnResult(sqlmock.NewResult(1, 1))
	mockDB.ExpectExec("CREATE TABLE IF NOT EXISTS `lightning_task_info`\\.conflict_error_v3.*").
		WillReturnResult(sqlmock.NewResult(2, 1))
	mockDB.ExpectExec("CREATE OR REPLACE VIEW `lightning_task_info`\\.conflict_view.*").
		WillReturnResult(sqlmock.NewResult(3, 1))
	mockDB.ExpectQuery("\\QSELECT _tidb_rowid, raw_key, index_name, raw_value, raw_handle FROM `lightning_task_info`.conflict_error_v3 WHERE table_name = ? AND kv_type = 0 AND _tidb_rowid >= ? and _tidb_rowid < ? ORDER BY _tidb_rowid LIMIT ?\\E").
		WillReturnRows(sqlmock.NewRows([]string{"_tidb_rowid", "raw_key", "index_name", "raw_value", "raw_handle"}).
			AddRow(1, data3IndexKey, "PRIMARY", data3IndexValue, data3RowKey).
			AddRow(2, data3IndexKey, "PRIMARY", data4IndexValue, data4RowKey))
	mockDB.ExpectBegin()
	mockDB.ExpectExec("INSERT INTO `lightning_task_info`\\.conflict_error_v3.*").
		WithArgs(0, "a", nil, nil, data4RowKey, data4RowValue, 2).
		WillReturnResult(driver.ResultNoRows)
	mockDB.ExpectCommit()
	for i := 0; i < 2; i++ {
		mockDB.ExpectQuery("\\QSELECT _tidb_rowid, raw_key, index_name, raw_value, raw_handle FROM `lightning_task_info`.conflict_error_v3 WHERE table_name = ? AND kv_type = 0 AND _tidb_rowid >= ? and _tidb_rowid < ? ORDER BY _tidb_rowid LIMIT ?\\E").
			WillReturnRows(sqlmock.NewRows([]string{"_tidb_rowid", "raw_key", "index_name", "raw_value", "raw_handle"}))
	}
	mockDB.ExpectQuery("\\QSELECT _tidb_rowid, raw_key, raw_value FROM `lightning_task_info`.conflict_error_v3 WHERE table_name = ? AND kv_type <> 0 AND _tidb_rowid >= ? and _tidb_rowid < ? ORDER BY _tidb_rowid LIMIT ?\\E").
		WillReturnRows(sqlmock.NewRows([]string{"_tidb_rowid", "raw_key", "raw_value"}).
			AddRow(1, data4RowKey, data4RowValue))
	for i := 0; i < 2; i++ {
		mockDB.ExpectQuery("\\QSELECT _tidb_rowid, raw_key, raw_value FROM `lightning_task_info`.conflict_error_v3 WHERE table_name = ? AND kv_type <> 0 AND _tidb_rowid >= ? and _tidb_rowid < ? ORDER BY _tidb_rowid LIMIT ?\\E").
			WillReturnRows(sqlmock.NewRows([]string{"_tidb_rowid", "raw_key", "raw_value"}))
	}
	mockDB.ExpectBegin()
	mockDB.ExpectExec("DELETE FROM `lightning_task_info`\\.conflict_error_v3.*").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mockDB.ExpectCommit()
	mockDB.ExpectBegin()
	mockDB.ExpectExec("DELETE FROM `lightning_task_info`\\.conflict_error_v3.*").
		WillReturnResult(sqlmock.NewResult(0, 0))
	mockDB.ExpectCommit()

	cfg := config.NewConfig()
	cfg.Conflict.Strategy = config.ReplaceOnDup
	cfg.TikvImporter.Backend = config.BackendLocal
	cfg.App.TaskInfoSchemaName = "lightning_task_info"
	em := errormanager.New(db, cfg, log.L())
	err = em.Init(ctx)
	require.NoError(t, err)

	fnGetLatestCount := atomic.NewInt32(0)
	fnDeleteKeyCount := atomic.NewInt32(0)
	pool := util.NewWorkerPool(16, "resolve duplicate rows by replace")
	err = em.ReplaceConflictKeys(
		ctx, tbl, "a", pool,
		func(ctx context.Context, key []byte) ([]byte, error) {
			fnGetLatestCount.Add(1)
			switch {
			case bytes.Equal(key, data3IndexKey):
				return data3IndexValue, nil
			case bytes.Equal(key, data4RowKey):
				if fnGetLatestCount.String() == "3" {
					return data4RowValue, nil
				}
				return nil, tikverr.ErrNotExist
			case bytes.Equal(key, data4NonclusteredKey):
				return data4NonclusteredValue, nil
			default:
				return nil, fmt.Errorf("key %v is not expected", key)
			}
		},
		func(ctx context.Context, keys [][]byte) error {
			fnDeleteKeyCount.Add(int32(len(keys)))
			for _, key := range keys {
				if !bytes.Equal(key, data4RowKey) && !bytes.Equal(key, data4NonclusteredKey) {
					return fmt.Errorf("key %v is not expected", key)
				}
			}
			return nil
		},
	)
	require.NoError(t, err)
	require.Equal(t, int32(7), fnGetLatestCount.Load())
	require.Equal(t, int32(2), fnDeleteKeyCount.Load())
	err = mockDB.ExpectationsWereMet()
	require.NoError(t, err)
}

func TestReplaceConflictOneUniqueKeyNonclusteredPk(t *testing.T) {
	p := parser.New()
	node, _, err := p.ParseSQL("create table a (a int primary key nonclustered, b int not null, c text, unique key uni_b(b));")
	require.NoError(t, err)
	mockSctx := mock.NewContext()
	mockSctx.GetSessionVars().EnableClusteredIndex = variable.ClusteredIndexDefModeOff
	info, err := ddl.MockTableInfo(mockSctx, node[0].(*ast.CreateTableStmt), 108)
	require.NoError(t, err)
	info.State = model.StatePublic
	require.False(t, info.PKIsHandle)
	tbl, err := tables.TableFromMeta(tidbkv.NewPanickingAllocators(info.SepAutoInc(), 0), info)
	require.NoError(t, err)
	require.False(t, tbl.Meta().HasClusteredIndex())

	sessionOpts := encode.SessionOptions{
		SQLMode:   mysql.ModeStrictAllTables,
		Timestamp: 1234567890,
	}

	encoder, err := tidbkv.NewBaseKVEncoder(&encode.EncodingConfig{
		Table:          tbl,
		SessionOptions: sessionOpts,
		Logger:         log.L(),
	})
	require.NoError(t, err)
	encoder.SessionCtx.GetSessionVars().RowEncoder.Enable = true

	data1 := []types.Datum{
		types.NewIntDatum(1),
		types.NewIntDatum(6),
		types.NewStringDatum("1.csv"),
		types.NewIntDatum(1),
	}
	data2 := []types.Datum{
		types.NewIntDatum(2),
		types.NewIntDatum(6),
		types.NewStringDatum("2.csv"),
		types.NewIntDatum(2),
	}
	data3 := []types.Datum{
		types.NewIntDatum(3),
		types.NewIntDatum(3),
		types.NewStringDatum("3.csv"),
		types.NewIntDatum(3),
	}
	data4 := []types.Datum{
		types.NewIntDatum(3),
		types.NewIntDatum(4),
		types.NewStringDatum("4.csv"),
		types.NewIntDatum(4),
	}
	data5 := []types.Datum{
		types.NewIntDatum(5),
		types.NewIntDatum(4),
		types.NewStringDatum("5.csv"),
		types.NewIntDatum(5),
	}
	tctx := encoder.SessionCtx.GetTableCtx()
	_, err = encoder.Table.AddRecord(tctx, data1)
	require.NoError(t, err)
	_, err = encoder.Table.AddRecord(tctx, data2)
	require.NoError(t, err)
	_, err = encoder.Table.AddRecord(tctx, data3)
	require.NoError(t, err)
	_, err = encoder.Table.AddRecord(tctx, data4)
	require.NoError(t, err)
	_, err = encoder.Table.AddRecord(tctx, data5)
	require.NoError(t, err)
	kvPairs := encoder.SessionCtx.TakeKvPairs()

	data1RowKey := kvPairs.Pairs[0].Key
	data2RowKey := kvPairs.Pairs[3].Key
	data2RowValue := kvPairs.Pairs[3].Val
	data3RowKey := kvPairs.Pairs[6].Key
	data4RowKey := kvPairs.Pairs[9].Key
	data4RowValue := kvPairs.Pairs[9].Val
	data5RowKey := kvPairs.Pairs[12].Key
	data5RowValue := kvPairs.Pairs[12].Val

	data2IndexKey := kvPairs.Pairs[5].Key
	data2IndexValue := kvPairs.Pairs[5].Val
	data3IndexKey := kvPairs.Pairs[8].Key
	data3IndexValue := kvPairs.Pairs[8].Val
	data5IndexKey := kvPairs.Pairs[14].Key
	data5IndexValue := kvPairs.Pairs[14].Val

	data1NonclusteredKey := kvPairs.Pairs[1].Key
	data1NonclusteredValue := kvPairs.Pairs[1].Val
	data2NonclusteredValue := kvPairs.Pairs[4].Val
	data4NonclusteredKey := kvPairs.Pairs[10].Key
	data4NonclusteredValue := kvPairs.Pairs[10].Val
	data5NonclusteredValue := kvPairs.Pairs[13].Val

	db, mockDB, err := sqlmock.New()
	require.NoError(t, err)
	defer func() {
		_ = db.Close()
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mockDB.ExpectExec("CREATE SCHEMA IF NOT EXISTS `lightning_task_info`").
		WillReturnResult(sqlmock.NewResult(1, 1))
	mockDB.ExpectExec("CREATE TABLE IF NOT EXISTS `lightning_task_info`\\.conflict_error_v3.*").
		WillReturnResult(sqlmock.NewResult(2, 1))
	mockDB.ExpectExec("CREATE OR REPLACE VIEW `lightning_task_info`\\.conflict_view.*").
		WillReturnResult(sqlmock.NewResult(3, 1))
	mockDB.ExpectQuery("\\QSELECT _tidb_rowid, raw_key, index_name, raw_value, raw_handle FROM `lightning_task_info`.conflict_error_v3 WHERE table_name = ? AND kv_type = 0 AND _tidb_rowid >= ? and _tidb_rowid < ? ORDER BY _tidb_rowid LIMIT ?\\E").
		WillReturnRows(sqlmock.NewRows([]string{"_tidb_rowid", "raw_key", "index_name", "raw_value", "raw_handle"}).
			AddRow(1, data4NonclusteredKey, "uni_b", data4NonclusteredValue, data4RowKey).
			AddRow(2, data4NonclusteredKey, "uni_b", data5NonclusteredValue, data5RowKey).
			AddRow(3, data1NonclusteredKey, "uni_b", data1NonclusteredValue, data1RowKey).
			AddRow(4, data1NonclusteredKey, "uni_b", data2NonclusteredValue, data2RowKey).
			AddRow(5, data3IndexKey, "PRIMARY", data3IndexValue, data3RowKey).
			AddRow(6, data3IndexKey, "PRIMARY", data4NonclusteredValue, data4RowKey))
	mockDB.ExpectBegin()
	mockDB.ExpectExec("INSERT INTO `lightning_task_info`\\.conflict_error_v3.*").
		WithArgs(0, "a", nil, nil, data5RowKey, data5RowValue, 2,
			0, "a", nil, nil, data2RowKey, data2RowValue, 2,
			0, "a", nil, nil, data4RowKey, data4RowValue, 2).
		WillReturnResult(driver.ResultNoRows)
	mockDB.ExpectCommit()
	for i := 0; i < 2; i++ {
		mockDB.ExpectQuery("\\QSELECT _tidb_rowid, raw_key, index_name, raw_value, raw_handle FROM `lightning_task_info`.conflict_error_v3 WHERE table_name = ? AND kv_type = 0 AND _tidb_rowid >= ? and _tidb_rowid < ? ORDER BY _tidb_rowid LIMIT ?\\E").
			WillReturnRows(sqlmock.NewRows([]string{"_tidb_rowid", "raw_key", "index_name", "raw_value", "raw_handle"}))
	}
	mockDB.ExpectQuery("\\QSELECT _tidb_rowid, raw_key, raw_value FROM `lightning_task_info`.conflict_error_v3 WHERE table_name = ? AND kv_type <> 0 AND _tidb_rowid >= ? and _tidb_rowid < ? ORDER BY _tidb_rowid LIMIT ?\\E").
		WillReturnRows(sqlmock.NewRows([]string{"_tidb_rowid", "raw_key", "raw_value"}).
			AddRow(1, data5RowKey, data5RowValue).
			AddRow(2, data2RowKey, data2RowValue).
			AddRow(3, data4RowKey, data4RowValue))
	for i := 0; i < 2; i++ {
		mockDB.ExpectQuery("\\QSELECT _tidb_rowid, raw_key, raw_value FROM `lightning_task_info`.conflict_error_v3 WHERE table_name = ? AND kv_type <> 0 AND _tidb_rowid >= ? and _tidb_rowid < ? ORDER BY _tidb_rowid LIMIT ?\\E").
			WillReturnRows(sqlmock.NewRows([]string{"_tidb_rowid", "raw_key", "raw_value"}))
	}
	mockDB.ExpectBegin()
	mockDB.ExpectExec("DELETE FROM `lightning_task_info`\\.conflict_error_v3.*").
		WillReturnResult(sqlmock.NewResult(0, 3))
	mockDB.ExpectCommit()
	mockDB.ExpectBegin()
	mockDB.ExpectExec("DELETE FROM `lightning_task_info`\\.conflict_error_v3.*").
		WillReturnResult(sqlmock.NewResult(0, 0))
	mockDB.ExpectCommit()

	cfg := config.NewConfig()
	cfg.Conflict.Strategy = config.ReplaceOnDup
	cfg.TikvImporter.Backend = config.BackendLocal
	cfg.App.TaskInfoSchemaName = "lightning_task_info"
	em := errormanager.New(db, cfg, log.L())
	err = em.Init(ctx)
	require.NoError(t, err)

	fnGetLatestCount := atomic.NewInt32(0)
	fnDeleteKeyCount := atomic.NewInt32(0)
	pool := util.NewWorkerPool(16, "resolve duplicate rows by replace")
	err = em.ReplaceConflictKeys(
		ctx, tbl, "a", pool,
		func(ctx context.Context, key []byte) ([]byte, error) {
			fnGetLatestCount.Add(1)
			switch {
			case bytes.Equal(key, data4NonclusteredKey):
				if fnGetLatestCount.String() != "20" {
					return data4NonclusteredValue, nil
				}
				return nil, tikverr.ErrNotExist
			case bytes.Equal(key, data5RowKey):
				if fnGetLatestCount.String() == "3" {
					return data5RowValue, nil
				}
				return nil, tikverr.ErrNotExist
			case bytes.Equal(key, data1NonclusteredKey):
				return data1NonclusteredValue, nil
			case bytes.Equal(key, data2RowKey):
				if fnGetLatestCount.String() == "6" {
					return data2RowValue, nil
				}
				return nil, tikverr.ErrNotExist
			case bytes.Equal(key, data3IndexKey):
				return data3IndexValue, nil
			case bytes.Equal(key, data4RowKey):
				return data4RowValue, nil
			case bytes.Equal(key, data2IndexKey):
				return data2IndexValue, nil
			case bytes.Equal(key, data5IndexKey):
				return data5IndexValue, nil
			default:
				return nil, fmt.Errorf("key %x is not expected", key)
			}
		},
		func(ctx context.Context, keys [][]byte) error {
			fnDeleteKeyCount.Add(int32(len(keys)))
			for _, key := range keys {
				if !bytes.Equal(key, data5RowKey) && !bytes.Equal(key, data2RowKey) && !bytes.Equal(key, data4RowKey) && !bytes.Equal(key, data2IndexKey) && !bytes.Equal(key, data4NonclusteredKey) && !bytes.Equal(key, data5IndexKey) {
					return fmt.Errorf("key %v is not expected", key)
				}
			}
			return nil
		},
	)
	require.NoError(t, err)
	require.Equal(t, int32(18), fnGetLatestCount.Load())
	require.Equal(t, int32(5), fnDeleteKeyCount.Load())
	err = mockDB.ExpectationsWereMet()
	require.NoError(t, err)
}

func TestReplaceConflictOneUniqueKeyNonclusteredVarcharPk(t *testing.T) {
	p := parser.New()
	node, _, err := p.ParseSQL("create table a (a varchar(20) primary key nonclustered, b int not null, c text, unique key uni_b(b));")
	require.NoError(t, err)
	mockSctx := mock.NewContext()
	mockSctx.GetSessionVars().EnableClusteredIndex = variable.ClusteredIndexDefModeOff
	info, err := ddl.MockTableInfo(mockSctx, node[0].(*ast.CreateTableStmt), 108)
	require.NoError(t, err)
	info.State = model.StatePublic
	require.False(t, info.PKIsHandle)
	tbl, err := tables.TableFromMeta(tidbkv.NewPanickingAllocators(info.SepAutoInc(), 0), info)
	require.NoError(t, err)
	require.False(t, tbl.Meta().HasClusteredIndex())

	sessionOpts := encode.SessionOptions{
		SQLMode:   mysql.ModeStrictAllTables,
		Timestamp: 1234567890,
	}

	encoder, err := tidbkv.NewBaseKVEncoder(&encode.EncodingConfig{
		Table:          tbl,
		SessionOptions: sessionOpts,
		Logger:         log.L(),
	})
	require.NoError(t, err)
	encoder.SessionCtx.GetSessionVars().RowEncoder.Enable = true

	data1 := []types.Datum{
		types.NewStringDatum("x"),
		types.NewIntDatum(6),
		types.NewStringDatum("1.csv"),
		types.NewIntDatum(1),
	}
	data2 := []types.Datum{
		types.NewStringDatum("y"),
		types.NewIntDatum(6),
		types.NewStringDatum("2.csv"),
		types.NewIntDatum(2),
	}
	data3 := []types.Datum{
		types.NewStringDatum("z"),
		types.NewIntDatum(3),
		types.NewStringDatum("3.csv"),
		types.NewIntDatum(3),
	}
	data4 := []types.Datum{
		types.NewStringDatum("z"),
		types.NewIntDatum(4),
		types.NewStringDatum("4.csv"),
		types.NewIntDatum(4),
	}
	data5 := []types.Datum{
		types.NewStringDatum("t"),
		types.NewIntDatum(4),
		types.NewStringDatum("5.csv"),
		types.NewIntDatum(5),
	}
	tctx := encoder.SessionCtx.GetTableCtx()
	_, err = encoder.Table.AddRecord(tctx, data1)
	require.NoError(t, err)
	_, err = encoder.Table.AddRecord(tctx, data2)
	require.NoError(t, err)
	_, err = encoder.Table.AddRecord(tctx, data3)
	require.NoError(t, err)
	_, err = encoder.Table.AddRecord(tctx, data4)
	require.NoError(t, err)
	_, err = encoder.Table.AddRecord(tctx, data5)
	require.NoError(t, err)
	kvPairs := encoder.SessionCtx.TakeKvPairs()

	data1RowKey := kvPairs.Pairs[0].Key
	data2RowKey := kvPairs.Pairs[3].Key
	data2RowValue := kvPairs.Pairs[3].Val
	data3RowKey := kvPairs.Pairs[6].Key
	data4RowKey := kvPairs.Pairs[9].Key
	data4RowValue := kvPairs.Pairs[9].Val
	data5RowKey := kvPairs.Pairs[12].Key
	data5RowValue := kvPairs.Pairs[12].Val

	data2IndexKey := kvPairs.Pairs[5].Key
	data2IndexValue := kvPairs.Pairs[5].Val
	data3IndexKey := kvPairs.Pairs[8].Key
	data3IndexValue := kvPairs.Pairs[8].Val
	data4IndexValue := kvPairs.Pairs[11].Val
	data5IndexKey := kvPairs.Pairs[14].Key
	data5IndexValue := kvPairs.Pairs[14].Val

	data1NonclusteredKey := kvPairs.Pairs[1].Key
	data1NonclusteredValue := kvPairs.Pairs[1].Val
	data2NonclusteredValue := kvPairs.Pairs[4].Val
	data4NonclusteredKey := kvPairs.Pairs[10].Key
	data4NonclusteredValue := kvPairs.Pairs[10].Val
	data5NonclusteredValue := kvPairs.Pairs[13].Val

	db, mockDB, err := sqlmock.New()
	require.NoError(t, err)
	defer func() {
		_ = db.Close()
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mockDB.ExpectExec("CREATE SCHEMA IF NOT EXISTS `lightning_task_info`").
		WillReturnResult(sqlmock.NewResult(1, 1))
	mockDB.ExpectExec("CREATE TABLE IF NOT EXISTS `lightning_task_info`\\.conflict_error_v3.*").
		WillReturnResult(sqlmock.NewResult(2, 1))
	mockDB.ExpectExec("CREATE OR REPLACE VIEW `lightning_task_info`\\.conflict_view.*").
		WillReturnResult(sqlmock.NewResult(3, 1))
	mockDB.ExpectQuery("\\QSELECT _tidb_rowid, raw_key, index_name, raw_value, raw_handle FROM `lightning_task_info`.conflict_error_v3 WHERE table_name = ? AND kv_type = 0 AND _tidb_rowid >= ? and _tidb_rowid < ? ORDER BY _tidb_rowid LIMIT ?\\E").
		WillReturnRows(sqlmock.NewRows([]string{"_tidb_rowid", "raw_key", "index_name", "raw_value", "raw_handle"}).
			AddRow(1, data4NonclusteredKey, "uni_b", data4NonclusteredValue, data4RowKey).
			AddRow(2, data4NonclusteredKey, "uni_b", data5NonclusteredValue, data5RowKey).
			AddRow(3, data1NonclusteredKey, "uni_b", data1NonclusteredValue, data1RowKey).
			AddRow(4, data1NonclusteredKey, "uni_b", data2NonclusteredValue, data2RowKey).
			AddRow(5, data3IndexKey, "PRIMARY", data3IndexValue, data3RowKey).
			AddRow(6, data3IndexKey, "PRIMARY", data4IndexValue, data4RowKey))
	mockDB.ExpectBegin()
	mockDB.ExpectExec("INSERT INTO `lightning_task_info`\\.conflict_error_v3.*").
		WithArgs(0, "a", nil, nil, data5RowKey, data5RowValue, 2,
			0, "a", nil, nil, data2RowKey, data2RowValue, 2,
			0, "a", nil, nil, data4RowKey, data4RowValue, 2).
		WillReturnResult(driver.ResultNoRows)
	mockDB.ExpectCommit()
	for i := 0; i < 2; i++ {
		mockDB.ExpectQuery("\\QSELECT _tidb_rowid, raw_key, index_name, raw_value, raw_handle FROM `lightning_task_info`.conflict_error_v3 WHERE table_name = ? AND kv_type = 0 AND _tidb_rowid >= ? and _tidb_rowid < ? ORDER BY _tidb_rowid LIMIT ?\\E").
			WillReturnRows(sqlmock.NewRows([]string{"_tidb_rowid", "raw_key", "index_name", "raw_value", "raw_handle"}))
	}
	mockDB.ExpectQuery("\\QSELECT _tidb_rowid, raw_key, raw_value FROM `lightning_task_info`.conflict_error_v3 WHERE table_name = ? AND kv_type <> 0 AND _tidb_rowid >= ? and _tidb_rowid < ? ORDER BY _tidb_rowid LIMIT ?\\E").
		WillReturnRows(sqlmock.NewRows([]string{"_tidb_rowid", "raw_key", "raw_value"}).
			AddRow(1, data5RowKey, data5RowValue).
			AddRow(2, data2RowKey, data2RowValue).
			AddRow(3, data4RowKey, data4RowValue))
	for i := 0; i < 2; i++ {
		mockDB.ExpectQuery("\\QSELECT _tidb_rowid, raw_key, raw_value FROM `lightning_task_info`.conflict_error_v3 WHERE table_name = ? AND kv_type <> 0 AND _tidb_rowid >= ? and _tidb_rowid < ? ORDER BY _tidb_rowid LIMIT ?\\E").
			WillReturnRows(sqlmock.NewRows([]string{"_tidb_rowid", "raw_key", "raw_value"}))
	}
	mockDB.ExpectBegin()
	mockDB.ExpectExec("DELETE FROM `lightning_task_info`\\.conflict_error_v3.*").
		WillReturnResult(sqlmock.NewResult(0, 3))
	mockDB.ExpectCommit()
	mockDB.ExpectBegin()
	mockDB.ExpectExec("DELETE FROM `lightning_task_info`\\.conflict_error_v3.*").
		WillReturnResult(sqlmock.NewResult(0, 0))
	mockDB.ExpectCommit()

	cfg := config.NewConfig()
	cfg.Conflict.Strategy = config.ReplaceOnDup
	cfg.TikvImporter.Backend = config.BackendLocal
	cfg.App.TaskInfoSchemaName = "lightning_task_info"
	em := errormanager.New(db, cfg, log.L())
	err = em.Init(ctx)
	require.NoError(t, err)

	fnGetLatestCount := atomic.NewInt32(0)
	fnDeleteKeyCount := atomic.NewInt32(0)
	pool := util.NewWorkerPool(16, "resolve duplicate rows by replace")
	err = em.ReplaceConflictKeys(
		ctx, tbl, "a", pool,
		func(ctx context.Context, key []byte) ([]byte, error) {
			fnGetLatestCount.Add(1)
			switch {
			case bytes.Equal(key, data4NonclusteredKey):
				if fnGetLatestCount.String() != "20" {
					return data4NonclusteredValue, nil
				}
				return nil, tikverr.ErrNotExist
			case bytes.Equal(key, data5RowKey):
				if fnGetLatestCount.String() == "3" {
					return data5RowValue, nil
				}
				return nil, tikverr.ErrNotExist
			case bytes.Equal(key, data1NonclusteredKey):
				return data1NonclusteredValue, nil
			case bytes.Equal(key, data2RowKey):
				if fnGetLatestCount.String() == "6" {
					return data2RowValue, nil
				}
				return nil, tikverr.ErrNotExist
			case bytes.Equal(key, data3IndexKey):
				return data3IndexValue, nil
			case bytes.Equal(key, data4RowKey):
				if fnGetLatestCount.String() == "9" {
					return data4RowValue, nil
				}
				return nil, tikverr.ErrNotExist
			case bytes.Equal(key, data2IndexKey):
				return data2IndexValue, nil
			case bytes.Equal(key, data5IndexKey):
				return data5IndexValue, nil
			default:
				return nil, fmt.Errorf("key %x is not expected", key)
			}
		},
		func(ctx context.Context, keys [][]byte) error {
			fnDeleteKeyCount.Add(int32(len(keys)))
			for _, key := range keys {
				if !bytes.Equal(key, data5RowKey) && !bytes.Equal(key, data2RowKey) && !bytes.Equal(key, data4RowKey) && !bytes.Equal(key, data2IndexKey) && !bytes.Equal(key, data4NonclusteredKey) && !bytes.Equal(key, data5IndexKey) {
					return fmt.Errorf("key %v is not expected", key)
				}
			}
			return nil
		},
	)
	require.NoError(t, err)
	require.Equal(t, int32(21), fnGetLatestCount.Load())
	require.Equal(t, int32(5), fnDeleteKeyCount.Load())
	err = mockDB.ExpectationsWereMet()
	require.NoError(t, err)
}
