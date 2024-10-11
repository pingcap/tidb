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

package diff

import (
	"context"
	"testing"
	"time"

	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/dbutil"
	"github.com/pingcap/tidb/util/importer"
	"github.com/stretchr/testify/require"
)

func TestSplitRange(t *testing.T) {
	t.Skip("remove it after migrate CI from tidb-tools")
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	conn, err := createConn()
	require.NoError(t, err)
	defer conn.Close()

	_, err = conn.ExecContext(ctx, "CREATE DATABASE IF NOT EXISTS `test`")
	require.NoError(t, err)

	_, err = conn.ExecContext(ctx, "DROP TABLE IF EXISTS `test`.`test_chunk`")
	require.NoError(t, err)

	createTableSQL := `CREATE TABLE test.test_chunk (
		a date NOT NULL,
		b datetime DEFAULT NULL,
		c time DEFAULT NULL,
		d varchar(10) COLLATE latin1_bin DEFAULT NULL,
		e int(10) DEFAULT NULL,
		h year(4) DEFAULT NULL,
		PRIMARY KEY (a))`

	dataCount := 10000
	cfg := &importer.Config{
		TableSQL:    createTableSQL,
		WorkerCount: 5,
		JobCount:    dataCount,
		Batch:       100,
		DBCfg:       dbutil.GetDBConfigFromEnv("test"),
	}

	// generate data for test.test_chunk
	importer.DoProcess(cfg)
	defer conn.ExecContext(ctx, "DROP TABLE IF EXISTS `test`.`test_chunk`")

	// only work on tidb, so don't assert err here
	_, _ = conn.ExecContext(ctx, "ANALYZE TABLE `test`.`test_chunk`")

	tableInfo, err := dbutil.GetTableInfo(ctx, conn, "test", "test_chunk")
	require.NoError(t, err)

	tableInstance := &TableInstance{
		Conn:   conn,
		Schema: "test",
		Table:  "test_chunk",
		info:   tableInfo,
	}

	// split chunks
	fields, err := getSplitFields(tableInstance.info, nil)
	require.NoError(t, err)
	chunks, err := getChunksForTable(tableInstance, fields, 100, "TRUE", "", false)
	require.NoError(t, err)

	// get data count from every chunk, and the sum of them should equal to the table's count.
	chunkDataCount := 0
	for _, chunk := range chunks {
		conditions, args := chunk.toString("")
		count, err := dbutil.GetRowCount(ctx, tableInstance.Conn, tableInstance.Schema, tableInstance.Table, conditions, stringSliceToInterfaceSlice(args))
		require.NoError(t, err)
		chunkDataCount += int(count)
	}
	require.Equal(t, dataCount, chunkDataCount)
}

func TestChunkUpdate(t *testing.T) {
	chunk := &ChunkRange{
		Bounds: []*Bound{
			{
				Column:   "a",
				Lower:    "1",
				Upper:    "2",
				HasLower: true,
				HasUpper: true,
			}, {
				Column:   "b",
				Lower:    "3",
				Upper:    "4",
				HasLower: true,
				HasUpper: true,
			},
		},
	}

	testCases := []struct {
		boundArgs  []string
		expectStr  string
		expectArgs []string
	}{
		{
			[]string{"a", "5", "6"},
			"((`a` > ?) OR (`a` = ? AND `b` > ?)) AND ((`a` < ?) OR (`a` = ? AND `b` <= ?))",
			[]string{"5", "5", "3", "6", "6", "4"},
		}, {
			[]string{"b", "5", "6"},
			"((`a` > ?) OR (`a` = ? AND `b` > ?)) AND ((`a` < ?) OR (`a` = ? AND `b` <= ?))",
			[]string{"1", "1", "5", "2", "2", "6"},
		}, {
			[]string{"c", "7", "8"},
			"((`a` > ?) OR (`a` = ? AND `b` > ?) OR (`a` = ? AND `b` = ? AND `c` > ?)) AND ((`a` < ?) OR (`a` = ? AND `b` < ?) OR (`a` = ? AND `b` = ? AND `c` <= ?))",
			[]string{"1", "1", "3", "1", "3", "7", "2", "2", "4", "2", "4", "8"},
		},
	}

	for _, cs := range testCases {
		newChunk := chunk.copyAndUpdate(cs.boundArgs[0], cs.boundArgs[1], cs.boundArgs[2], true, true)
		conditions, args := newChunk.toString("")
		require.Equal(t, cs.expectStr, conditions)
		require.Equal(t, cs.expectArgs, args)
	}

	// the origin chunk is not changed
	conditions, args := chunk.toString("")
	require.Equal(t, "((`a` > ?) OR (`a` = ? AND `b` > ?)) AND ((`a` < ?) OR (`a` = ? AND `b` <= ?))", conditions)
	expectArgs := []string{"1", "1", "3", "2", "2", "4"}
	require.Equal(t, expectArgs, args)
}

func TestChunkToString(t *testing.T) {
	chunk := &ChunkRange{
		Bounds: []*Bound{
			{
				Column:   "a",
				Lower:    "1",
				Upper:    "2",
				HasLower: true,
				HasUpper: true,
			}, {
				Column:   "b",
				Lower:    "3",
				Upper:    "4",
				HasLower: true,
				HasUpper: true,
			}, {
				Column:   "c",
				Lower:    "5",
				Upper:    "6",
				HasLower: true,
				HasUpper: true,
			},
		},
	}

	conditions, args := chunk.toString("")
	require.Equal(t, "((`a` > ?) OR (`a` = ? AND `b` > ?) OR (`a` = ? AND `b` = ? AND `c` > ?)) AND ((`a` < ?) OR (`a` = ? AND `b` < ?) OR (`a` = ? AND `b` = ? AND `c` <= ?))", conditions)
	expectArgs := []string{"1", "1", "3", "1", "3", "5", "2", "2", "4", "2", "4", "6"}
	for i, arg := range args {
		require.Equal(t, expectArgs[i], arg)
	}

	conditions, args = chunk.toString("latin1")
	require.Equal(t, "((`a` COLLATE 'latin1' > ?) OR (`a` = ? AND `b` COLLATE 'latin1' > ?) OR (`a` = ? AND `b` = ? AND `c` COLLATE 'latin1' > ?)) AND ((`a` COLLATE 'latin1' < ?) OR (`a` = ? AND `b` COLLATE 'latin1' < ?) OR (`a` = ? AND `b` = ? AND `c` COLLATE 'latin1' <= ?))", conditions)
	expectArgs = []string{"1", "1", "3", "1", "3", "5", "2", "2", "4", "2", "4", "6"}
	for i, arg := range args {
		require.Equal(t, expectArgs[i], arg)
	}
}

func TestRangeLimit(t *testing.T) {
	t.Skip("remove it after migrate CI from tidb-tools")
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	conn, err := createConn()
	require.NoError(t, err)
	defer conn.Close()

	_, err = conn.ExecContext(ctx, "CREATE DATABASE IF NOT EXISTS `test`")
	require.NoError(t, err)

	_, err = conn.ExecContext(ctx, "DROP TABLE IF EXISTS `test`.`test_range`")
	require.NoError(t, err)

	createTableSQL := `CREATE TABLE test.test_range (
		a int NOT NULL,
		b datetime DEFAULT NULL,
		c time DEFAULT NULL,
		d varchar(10) COLLATE latin1_bin DEFAULT NULL,
		e int(10) DEFAULT NULL,
		h year(4) DEFAULT NULL,
		PRIMARY KEY (a))`

	// will generate 0-9 at column `a`
	dataCount := 10
	cfg := &importer.Config{
		TableSQL:    createTableSQL,
		WorkerCount: 5,
		JobCount:    dataCount,
		Batch:       100,
		DBCfg:       dbutil.GetDBConfigFromEnv("test"),
	}

	// generate data for test.test_chunk
	importer.DoProcess(cfg)
	defer conn.ExecContext(ctx, "DROP TABLE IF EXISTS `test`.`test_range`")

	// only work on tidb, so don't assert err here
	_, _ = conn.ExecContext(ctx, "ANALYZE TABLE `test`.`test_range`")

	tableInfo, err := dbutil.GetTableInfo(ctx, conn, "test", "test_range")
	require.NoError(t, err)

	tableInstance := &TableInstance{
		Conn:   conn,
		Schema: "test",
		Table:  "test_range",
		info:   tableInfo,
	}

	require.Nil(t, createCheckpointTable(ctx, conn))
	chunks, err := SplitChunks(ctx, tableInstance, "a,d", "a > 7", 1, "", false, conn)
	require.NoError(t, err)
	defer conn.ExecContext(ctx, "DROP DATABASE sync_diff_inspector")
	// a > 7 and chunkSize = 1 should return 2 chunk
	require.Len(t, chunks, 2)
	count := 0

	for _, chunk := range chunks {
		rows, _, err := getChunkRows(ctx, conn, "test", "test_range", tableInfo, chunk.Where, util.StringsToInterfaces(chunk.Args), "")
		require.NoError(t, err)
		for rows.Next() {
			count++
		}
	}
	require.Equal(t, 2, count)
}
