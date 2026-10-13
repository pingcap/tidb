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

package export

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	srv "github.com/pingcap/tidb/pkg/server"
	"github.com/pingcap/tidb/pkg/server/tests/servertestkit"
	"github.com/pingcap/tidb/pkg/testkit"
	tf "github.com/pingcap/tidb/pkg/util/table-filter"
	"github.com/stretchr/testify/require"
)

const parityTablePrefix = "parity.t."

// TestDumpServiceMatchesSQLDump verifies that the KV dump path exports the same
// data as the regular SQL dump path against the same real TiDB server, for every
// export format. The SQL path gets its column types from go-sql-driver, so the
// comparison checks the KV path's driver-name mapping against the real server's
// output instead of a hand-written expectation.
func TestDumpServiceMatchesSQLDump(t *testing.T) {
	// The server binds a random TCP port only when it knows it runs under go test.
	originalRunInGoTest := srv.RunInGoTest
	srv.RunInGoTest = true
	t.Cleanup(func() { srv.RunInGoTest = originalRunInGoTest })

	ts := servertestkit.CreateTidbTestSuite(t)
	db, err := sql.Open("mysql", ts.GetDSN())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	dbt := testkit.NewDBTestKit(t, db)
	dbt.MustExec("create database if not exists parity")
	dbt.MustExec(`create table parity.t (
		c_tinyint tinyint,
		c_smallint smallint,
		c_mediumint mediumint,
		c_int int,
		c_bigint bigint,
		c_bigint_unsigned bigint unsigned,
		c_float float,
		c_double double,
		c_decimal decimal(10,2),
		c_bit bit(8),
		c_char char(10),
		c_varchar varchar(10),
		c_binary binary(10),
		c_varbinary varbinary(10),
		c_text text,
		c_blob blob,
		c_date date,
		c_datetime datetime,
		c_timestamp timestamp,
		c_time time,
		c_year year,
		c_enum enum('a','b'),
		c_set set('a','b'),
		c_json json,
		c_enum_binary enum('a','b') character set binary,
		c_set_binary set('a','b') character set binary
	)`)
	dbt.MustExec(`insert into parity.t values
		(1, 2, 3, 4, 5, 6, 1.5, 2.5, 12.30, b'1010', 'ch', 'vc', x'00ff', x'0102', 'hello', x'0304',
		 '2020-01-02', '2020-01-02 03:04:05', '2020-01-02 03:04:05', '03:04:05', 2020, 'a', 'a,b', '{"k": 1}', 'a', 'a,b'),
		(2, 3, 4, 5, 6, 7, 3.5, 4.5, -1.25, b'0101', 'ch2', 'vc2', x'aa', x'bb', '', x'', 
		 '2021-03-04', '2021-03-04 05:06:07', '2021-03-04 05:06:07', '05:06:07', 2021, 'b', '', 'null', 'b', ''),
		(-128, -32768, -8388608, -2147483648, -9223372036854775808, 18446744073709551615,
		 3.4028234e38, 1.7976931348623157e308, -99999999.99, b'11111111',
		 'a"b\\c', '0123456789', x'00010203040506070809', x'ffeeddccbbaa99887766',
		 '中文😀', x'00ff', '1000-01-01', '9999-12-31 23:59:59', '2038-01-19 03:14:07',
		 '-838:59:59', 2155, 'b', 'a,b', '{"nested": [1, 2, 3]}', 'b', 'a,b')`)

	newConf := func(configure func(*Config)) *Config {
		conf := DefaultConfig()
		conf.Host = "127.0.0.1"
		conf.Port = int(ts.Port)
		conf.User = "root"
		conf.StatusAddr = ""
		conf.OutputDirPath = t.TempDir()
		conf.NoHeader = true
		conf.TableFilter = tf.NewSchemasFilter("parity")
		// Match the KV decoder's fixed UTC session so temporal values compare equal.
		conf.SessionParams = map[string]any{"time_zone": "+00:00"}
		configure(conf)
		return conf
	}

	formats := []struct {
		name      string
		configure func(*Config)
		read      func(*testing.T, string) []string
	}{
		{
			name: "sql",
			configure: func(conf *Config) {
				conf.FileType = FileFormatSQLTextString
			},
			read: readSQLDataRows,
		},
		{
			name: "csv",
			configure: func(conf *Config) {
				conf.FileType = FileFormatCSVString
				conf.CsvOutputDialect = CSVDialectDefault
			},
			read: readCSVDataRows,
		},
		{
			name: "csv_snowflake",
			configure: func(conf *Config) {
				conf.FileType = FileFormatCSVString
				// Snowflake dialect hex-encodes binary columns, so a wrong
				// string/binary classification (for example exporting TEXT as
				// BLOB) shows up in the CSV.
				conf.CsvOutputDialect = CSVDialectSnowflake
			},
			read: readCSVDataRows,
		},
		{
			name: "parquet",
			configure: func(conf *Config) {
				conf.FileType = FileFormatParquetString
			},
			read: readParquetData,
		},
	}

	for _, format := range formats {
		t.Run(format.name, func(t *testing.T) {
			sqlConf := newConf(format.configure)
			sqlDumper, err := NewDumper(context.Background(), sqlConf)
			require.NoError(t, err)
			require.NoError(t, sqlDumper.Dump())
			require.NoError(t, sqlDumper.Close())

			kvConf := newConf(format.configure)
			kvConf.DumpService = (&url.URL{Scheme: "unix", Path: startTestDumpService(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				serveKVTestRequest(ts.Store, w, r)
			}))}).String()
			kvDumper, err := NewDumper(context.Background(), kvConf)
			require.NoError(t, err)
			require.NoError(t, kvDumper.Dump())
			require.NoError(t, kvDumper.Close())

			sqlOutput := format.read(t, sqlConf.OutputDirPath)
			require.NotEmpty(t, sqlOutput)
			require.Equal(t, sqlOutput, format.read(t, kvConf.OutputDirPath))
		})
	}
}

func parityDataFiles(t *testing.T, dir, extension string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	var files []string
	for _, entry := range entries {
		name := entry.Name()
		if strings.HasPrefix(name, parityTablePrefix) && strings.HasSuffix(name, extension) {
			files = append(files, filepath.Join(dir, name))
		}
	}
	return files
}

// readSQLDataRows returns the sorted value tuples of all INSERT statements for
// the parity table. The INSERT column list is skipped because it is not part of
// the row values.
func readSQLDataRows(t *testing.T, dir string) []string {
	t.Helper()
	var rows []string
	for _, path := range parityDataFiles(t, dir, ".sql") {
		content, err := os.ReadFile(path)
		require.NoError(t, err)
		for _, line := range strings.Split(string(content), "\n") {
			line = strings.TrimSpace(line)
			if strings.HasPrefix(line, "(") {
				// Drop the statement separator ("," between rows, ";" at the end)
				// so statement batching does not affect the comparison.
				rows = append(rows, strings.TrimRight(line, ",;"))
			}
		}
	}
	sort.Strings(rows)
	return rows
}

// readCSVDataRows returns the sorted data rows of all CSV files for the parity
// table, independent of chunk splitting and row order.
func readCSVDataRows(t *testing.T, dir string) []string {
	t.Helper()
	var rows []string
	for _, path := range parityDataFiles(t, dir, ".csv") {
		content, err := os.ReadFile(path)
		require.NoError(t, err)
		for _, line := range strings.Split(strings.TrimRight(string(content), "\r\n"), "\r\n") {
			if line != "" {
				rows = append(rows, line)
			}
		}
	}
	sort.Strings(rows)
	return rows
}

// readParquetData returns the distinct Parquet schemas followed by the sorted
// JSON-encoded data rows of all files for the parity table. The schema carries
// the physical and logical types the export format assigns to each column, and
// the rows carry the decoded values; chunk splitting may produce several files
// with the same schema.
func readParquetData(t *testing.T, dir string) []string {
	t.Helper()
	seen := make(map[string]struct{})
	var schemas []string
	paths := parityDataFiles(t, dir, ".parquet")
	rows := make([]string, 0, len(paths))
	for _, path := range paths {
		reader, err := file.OpenParquetFile(path, false)
		require.NoError(t, err)
		schema := reader.MetaData().Schema.String()
		if _, ok := seen[schema]; !ok {
			seen[schema] = struct{}{}
			schemas = append(schemas, schema)
		}

		arrowReader, err := pqarrow.NewFileReader(reader, pqarrow.ArrowReadProperties{}, memory.NewGoAllocator())
		require.NoError(t, err)
		table, err := arrowReader.ReadTable(context.Background())
		require.NoError(t, err)
		rows = append(rows, parquetTableRows(t, table)...)
		table.Release()

		require.NoError(t, reader.Close())
	}
	sort.Strings(schemas)
	sort.Strings(rows)
	return append(schemas, rows...)
}

// parquetTableRows renders every row of table as JSON so values of all Parquet
// physical types compare uniformly.
func parquetTableRows(t *testing.T, table arrow.Table) []string {
	t.Helper()
	numRows, numCols := int(table.NumRows()), int(table.NumCols())
	require.NotZero(t, numRows)
	columns := make([][]any, numCols)
	for col := 0; col < numCols; col++ {
		values := make([]any, 0, numRows)
		for _, chunk := range table.Column(col).Data().Chunks() {
			for row := 0; row < chunk.Len(); row++ {
				values = append(values, chunk.GetOneForMarshal(row))
			}
		}
		columns[col] = values
	}
	rows := make([]string, 0, numRows)
	for row := 0; row < numRows; row++ {
		values := make([]any, numCols)
		for col := 0; col < numCols; col++ {
			values[col] = columns[col][row]
		}
		encoded, err := json.Marshal(values)
		require.NoError(t, err)
		rows = append(rows, string(encoded))
	}
	return rows
}
