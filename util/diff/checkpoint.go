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
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb-tools/pkg/dbutil"
	"go.uber.org/zap"
)

var (
	// for chunk: means this chunk's data is equal
	// for table: means this all chunk in this table is equal(except ignore chunk)
	successState = "success"

	// for chunk: means this chunk's data is not equal
	// for table: means some chunks' data is not equal or some chunk check failed in this table
	failedState = "failed"

	// for chunk: means meet error when check, don't know the chunk's data is equal or not equal
	// for table: don't have this state
	errorState = "error"

	// for chunk: means this chunk is not in check
	// for table: this table is checking but not finished
	notCheckedState = "not_checked"

	// for chunk: means this chunk is checking
	// for table: don't have this state
	checkingState = "checking"

	// for chunk: this chunk is ignored. if sample is not 100%, will ignore some chunk
	// for table: don't have this state
	ignoreState = "ignore"

	checkpointSchemaName = "sync_diff_inspector"

	summaryTableName = "summary"

	chunkTableName = "chunk"
)

// tableSummaryInfo saves a table's summary information
type tableSummaryInfo struct {
	sync.RWMutex

	totalNum   int64
	successNum int64
	failedNum  int64
	ignoreNum  int64
}

func newTableSummaryInfo(totalNum int64) *tableSummaryInfo {
	return &tableSummaryInfo{
		totalNum: totalNum,
	}
}

func (s *tableSummaryInfo) addSuccessNum() {
	s.Lock()
	s.successNum++
	s.Unlock()
}

func (s *tableSummaryInfo) addFailedNum() {
	s.Lock()
	s.failedNum++
	s.Unlock()
}

func (s *tableSummaryInfo) addIgnoreNum() {
	s.Lock()
	s.ignoreNum++
	s.Unlock()
}

func (s *tableSummaryInfo) get() (totalNum, successNum, failedNum, ignoreNum int64) {
	s.RLock()
	defer s.RUnlock()
	return s.totalNum, s.successNum, s.failedNum, s.ignoreNum

}

// saveChunk saves the chunk's info to `chunk` table
func saveChunk(ctx context.Context, db *sql.DB, chunkID int, instanceID, schema, table, checksum string, chunk *ChunkRange) error {
	chunkBytes, err := json.Marshal(chunk)
	if err != nil {
		return errors.Trace(err)
	}

	sql := fmt.Sprintf("REPLACE INTO `%s`.`%s` VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?);", checkpointSchemaName, chunkTableName)
	err = dbutil.ExecSQLWithRetry(ctx, db, sql, chunkID, instanceID, schema, table, chunk.Where, checksum, string(chunkBytes), chunk.State, time.Now())
	if err != nil {
		log.Error("save chunk info failed", zap.Error(err))
		return errors.Trace(err)
	}
	return nil
}

// initChunks initials the chunks' information into chunk table
func initChunks(ctx context.Context, db *sql.DB, instanceID, schema, table string, chunks []*ChunkRange) error {
	beginTime := time.Now()
	batch := 100
	num := 0
	sqlPrefix := fmt.Sprintf("INSERT INTO `%s`.`%s` VALUES", checkpointSchemaName, chunkTableName)

	valuesPlaceholders := "(?, ?, ?, ?, ?, ?, ?, ?, ?)"
	valuesPlaceholdersArray := make([]string, 0, batch)
	values := make([]interface{}, 0, 9*batch)

	for i, chunk := range chunks {
		num++
		chunkBytes, err := json.Marshal(chunk)
		if err != nil {
			return errors.Trace(err)
		}

		values = append(values, chunk.ID, instanceID, schema, table, chunk.Where, "", string(chunkBytes), chunk.State, time.Now())
		valuesPlaceholdersArray = append(valuesPlaceholdersArray, valuesPlaceholders)

		if num >= batch || i == len(chunks)-1 {
			sql := fmt.Sprintf("%s%s", sqlPrefix, strings.Join(valuesPlaceholdersArray, ", "))
			err = dbutil.ExecSQLWithRetry(ctx, db, sql, values...)
			if err != nil {
				log.Error("save chunk info failed", zap.Error(err))
				return errors.Trace(err)
			}
			num = 0
			valuesPlaceholdersArray = valuesPlaceholdersArray[:0]
			values = values[:0]
		}
	}

	log.Info("initial chunks", zap.Duration("cost", time.Since(beginTime)))

	return nil
}

// getChunk gets chunk info from table `chunk` by chunkID
func getChunk(ctx context.Context, db *sql.DB, instanceID, schema, table string, chunkID int) (*ChunkRange, error) {
	query := fmt.Sprintf("SELECT `chunk_str` FROM `%s`.`%s` WHERE `instance_id` = ? AND `schema` = ? AND `table` = ? AND `chunk_id` = ? limit 1", checkpointSchemaName, chunkTableName)
	rows, err := db.QueryContext(ctx, query, instanceID, schema, table, chunkID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		fields, err1 := dbutil.ScanRow(rows)
		if err1 != nil {
			return nil, errors.Trace(err1)
		}

		chunkStr := fields["chunk_str"].Data
		chunk := new(ChunkRange)
		err := json.Unmarshal(chunkStr, &chunk)
		if err != nil {
			return nil, err
		}
		return chunk, nil
	}

	if rows.Err() != nil {
		return nil, errors.Trace(rows.Err())
	}

	return nil, errors.NotFoundf("instanceID %d, schema %s, table %s, chunk %d", instanceID, schema, table, chunkID)
}

// loadChunks loads chunk info from table `chunk`
func loadChunks(ctx context.Context, db *sql.DB, instanceID, schema, table string) ([]*ChunkRange, error) {
	chunks := make([]*ChunkRange, 0, 100)

	query := fmt.Sprintf("SELECT `chunk_str` FROM `%s`.`%s` WHERE `instance_id` = ? AND `schema` = ? AND `table` = ?", checkpointSchemaName, chunkTableName)
	rows, err := db.QueryContext(ctx, query, instanceID, schema, table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		fields, err1 := dbutil.ScanRow(rows)
		if err1 != nil {
			return nil, errors.Trace(err1)
		}

		chunkStr := fields["chunk_str"].Data
		chunk := new(ChunkRange)
		err := json.Unmarshal(chunkStr, &chunk)
		if err != nil {
			return nil, err
		}
		chunk.updateColumnOffset()
		chunks = append(chunks, chunk)
	}

	return chunks, errors.Trace(rows.Err())
}

// getTableSummary returns a table's total chunk num, check success chunk num, check failed chunk num, check ignore chunk num and the state
func getTableSummary(ctx context.Context, db *sql.DB, schema, table string) (total int64, success int64, failed int64, ignore int64, state string, err error) {
	query := fmt.Sprintf("SELECT `chunk_num`, `check_success_num`, `check_failed_num`, `check_ignore_num`, `state` FROM `%s`.`%s` WHERE `schema` = ? AND `table` = ? LIMIT 1",
		checkpointSchemaName, summaryTableName)
	rows, err := db.QueryContext(ctx, query, schema, table)
	if err != nil {
		return 0, 0, 0, 0, "", errors.Trace(err)
	}
	defer rows.Close()

	var totalNum, successNum, failedNum, ignoreNum sql.NullInt64
	var stateStr sql.NullString
	for rows.Next() {
		err1 := rows.Scan(&totalNum, &successNum, &failedNum, &ignoreNum, &stateStr)
		if err1 != nil {
			return 0, 0, 0, 0, "", errors.Trace(err1)
		}

		if !totalNum.Valid || !successNum.Valid || !failedNum.Valid || !ignoreNum.Valid || !stateStr.Valid {
			return 0, 0, 0, 0, "", errors.Errorf("some values are invalid, query: %s, args: %v", query, []interface{}{schema, table})
		}

		return totalNum.Int64, successNum.Int64, failedNum.Int64, ignoreNum.Int64, stateStr.String, nil

	}
	if rows.Err() != nil {
		return 0, 0, 0, 0, "", errors.Trace(rows.Err())
	}

	return 0, 0, 0, 0, "", errors.NotFoundf("schema %s, table %s summary info", schema, table)
}

// initTableSummary initials a table's summary info in table `summary`
func initTableSummary(ctx context.Context, db *sql.DB, schema, table string, configHash string) error {
	sql := fmt.Sprintf("REPLACE INTO `%s`.`%s`(`schema`, `table`, `state`, `config_hash`) VALUES(?, ?, ?, ?)", checkpointSchemaName, summaryTableName)
	err := dbutil.ExecSQLWithRetry(ctx, db, sql, schema, table, notCheckedState, configHash)
	if err != nil {
		log.Error("save summary info failed", zap.Error(err))
		return errors.Trace(err)
	}

	return nil
}

// updateTableSummary gets summary info from `chunk` table, and then update `summary` table
func updateTableSummary(ctx context.Context, db *sql.DB, instanceID, schema, table string, summaryInfo *tableSummaryInfo) error {
	if summaryInfo == nil {
		return nil
	}

	total, successNum, failedNum, ignoreNum := summaryInfo.get()
	if total == 0 {
		// don't need to update summary info
		return nil
	}

	log.Info("summary info", zap.String("instance_id", instanceID), zap.String("schema", schema), zap.String("table", table), zap.Int64("chunk num", total), zap.Int64("success num", successNum), zap.Int64("failed num", failedNum), zap.Int64("ignore num", ignoreNum))

	checkedNum := successNum + failedNum + ignoreNum
	state := checkingState
	if checkedNum == 0 {
		state = notCheckedState
	} else if checkedNum == total {
		if total == successNum+ignoreNum {
			state = successState
		} else {
			state = failedState
		}
	}

	updateSQL := fmt.Sprintf("UPDATE `%s`.`%s` SET `chunk_num` = ?, `check_success_num` = ?, `check_failed_num` = ?, `check_ignore_num` = ?, `state` = ? WHERE `schema` = ? AND `table` = ?", checkpointSchemaName, summaryTableName)
	err := dbutil.ExecSQLWithRetry(ctx, db, updateSQL, total, successNum, failedNum, ignoreNum, state, schema, table)
	if err != nil {
		return errors.Trace(err)
	}

	return nil
}

// createCheckpointTable creates checkpoint tables, include `summary` and `chunk`
func createCheckpointTable(ctx context.Context, db *sql.DB) error {
	createSchemaSQL := fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s`;", checkpointSchemaName)
	_, err := db.ExecContext(ctx, createSchemaSQL)
	if err != nil {
		log.Info("create schema", zap.Error(err))
		return errors.Trace(err)
	}

	/* example
	mysql> select * from sync_diff_inspector.summary;
	+--------+-------+-----------+-------------------+------------------+------------------+---------+----------------------------------+---------------------+
	| schema | table | chunk_num | check_success_num | check_failed_num | check_ignore_num | state   | config_hash                      | update_time         |
	+--------+-------+-----------+-------------------+------------------+------------------+---------+----------------------------------+---------------------+
	| diff   | test  |       112 |               104 |                0 |                8 | success | 91f302052783672b01af3e2b0e7d66ff | 2019-03-26 12:42:11 |
	+--------+-------+-----------+-------------------+------------------+------------------+---------+----------------------------------+---------------------+

	note: config_hash is the hash value for the config, if config is changed, will clear the history checkpoint.
	*/
	createSummaryTableSQL :=
		"CREATE TABLE IF NOT EXISTS `sync_diff_inspector`.`summary`(" +
			"`schema` varchar(64), `table` varchar(64)," +
			"`chunk_num` int not null default 0," +
			"`check_success_num` int not null default 0," +
			"`check_failed_num` int not null default 0," +
			"`check_ignore_num` int not null default 0," +
			"`state` enum('not_checked', 'checking', 'success', 'failed') DEFAULT 'not_checked'," +
			"`config_hash` varchar(50)," +
			"`update_time` datetime ON UPDATE CURRENT_TIMESTAMP," +
			"PRIMARY KEY(`schema`, `table`));"

	_, err = db.ExecContext(ctx, createSummaryTableSQL)
	if err != nil {
		log.Error("create chunk table", zap.Error(err))
		return errors.Trace(err)
	}

	/* example
	mysql> select * from sync_diff_inspector.chunk where chunk_id = 2;;
	+----------+-------------+--------+-------+---------------------------------+-------------+-----------+---------+---------------------+
	| chunk_id | instance_id | schema | table | range                           |  checksum   | chunk_str | state   | update_time         |
	+----------+-------------+--------+-------+---------------------------------+-------------+-----------+---------+---------------------+
	|        2 | target-1    | diff   | test1 | (`a` >= ? AND `a` < ? AND TRUE) |  91f3020527 |  .....    | success | 2019-03-26 12:41:42 |
	+----------+-------------+--------+-------+---------------------------------+-------------+-----------+---------+---------------------+
	*/
	createChunkTableSQL :=
		"CREATE TABLE IF NOT EXISTS `sync_diff_inspector`.`chunk`(" +
			"`chunk_id` int," +
			"`instance_id` varchar(64)," +
			"`schema` varchar(64)," +
			"`table` varchar(64)," +
			"`range` text," +
			"`checksum` varchar(20)," +
			"`chunk_str` text," +
			"`state` enum('not_checked', 'checking', 'success', 'failed', 'ignore', 'error') DEFAULT 'not_checked'," +
			"`update_time` datetime ON UPDATE CURRENT_TIMESTAMP," +
			"PRIMARY KEY(`schema`, `table`, `instance_id`, `chunk_id`));"
	_, err = db.ExecContext(ctx, createChunkTableSQL)
	if err != nil {
		log.Error("create chunk table", zap.Error(err))
		return errors.Trace(err)
	}

	return nil
}

// cleanCheckpoint deletes the table's checkpoint info in table `summary` and `chunk`
func cleanCheckpoint(ctx context.Context, db *sql.DB, schema, table string) error {
	where := "`schema` = ? AND `table` = ?"
	args := []interface{}{schema, table}

	err := dbutil.DeleteRows(ctx, db, checkpointSchemaName, summaryTableName, where, args)
	if err != nil {
		return errors.Trace(err)
	}

	err = dbutil.DeleteRows(ctx, db, checkpointSchemaName, chunkTableName, where, args)
	if err != nil {
		return errors.Trace(err)
	}

	return nil
}

// dropCheckpoint drops the database `sync_diff_inspector`
func dropCheckpoint(ctx context.Context, db *sql.DB) error {
	dropSchemaSQL := fmt.Sprintf("DROP DATABASE IF EXISTS `%s`;", checkpointSchemaName)
	_, err := db.ExecContext(ctx, dropSchemaSQL)
	if err != nil {
		log.Error("drop schema", zap.Error(err))
		return errors.Trace(err)
	}

	return nil
}

// loadFromCheckPoint returns true if we should use the history checkpoint
func loadFromCheckPoint(ctx context.Context, db *sql.DB, schema, table, configHash string) (bool, error) {
	query := fmt.Sprintf("SELECT `state`, `config_hash` FROM `%s`.`%s` WHERE `schema` = ? AND `table` = ? LIMIT 1;", checkpointSchemaName, summaryTableName)
	rows, err := db.QueryContext(ctx, query, schema, table)
	if err != nil {
		return false, errors.Trace(err)
	}
	defer rows.Close()

	var state, cfgHash sql.NullString

	for rows.Next() {
		err1 := rows.Scan(&state, &cfgHash)
		if err1 != nil {
			return false, errors.Trace(err1)
		}

		if cfgHash.Valid {
			if configHash != cfgHash.String {
				return false, nil
			}
		}

		if state.Valid {
			// is state is success, will begin a new check for this table
			// if state is not checked, the chunk info maybe not exists, so just return false
			if state.String == successState || state.String == notCheckedState {
				return false, nil
			}
		}

		return true, nil
	}

	return false, errors.Trace(rows.Err())
}
