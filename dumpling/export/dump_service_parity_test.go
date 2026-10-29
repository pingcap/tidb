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

// TestDumpServiceMatchesSQLDump shares one TiDB server across independent parity cases.
func TestDumpServiceMatchesSQLDump(t *testing.T) {
	// The server binds a random TCP port only when it knows it runs under go test.
	originalRunInGoTest := srv.RunInGoTest
	srv.RunInGoTest = true
	t.Cleanup(func() { srv.RunInGoTest = originalRunInGoTest })

	server := servertestkit.CreateTidbTestSuite(t)
	t.Run("types", func(t *testing.T) { testDumpServiceColumnTypes(t, server) })
	t.Run("generated", func(t *testing.T) { testDumpServiceGeneratedColumns(t, server) })
	t.Run("preamble", func(t *testing.T) { testDumpServiceSQLPreamble(t, server) })
	t.Run("column_filter", func(t *testing.T) { testDumpServiceColumnFilter(t, server) })
}

func testDumpServiceColumnFilter(t *testing.T, server *servertestkit.TidbTestSuite) {
	db, err := sql.Open("mysql", server.GetDSN())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	dbt := testkit.NewDBTestKit(t, db)
	dbt.MustExec("create database parity")
	t.Cleanup(func() { dbt.MustExec("drop database parity") })
	dbt.MustExec(`create table parity.t (
		tenant varchar(8), id bigint, secret varchar(16), payload varbinary(8),
		virtual_keep bigint as (id + 1), amount decimal(10,2),
		virtual_drop int as (length(secret)), stored_keep bigint as (id * 3) stored,
		note varchar(16), primary key (tenant, id) clustered, key secret_idx(secret)
	)`)
	dbt.MustExec("set @@tidb_row_format_version=1")
	dbt.MustExec("insert into parity.t(tenant,id,secret,payload,amount,note) values ('a',1,'hidden',x'00ff',12.30,null)")
	dbt.MustExec("set @@tidb_row_format_version=2")
	dbt.MustExec("insert into parity.t(tenant,id,secret,payload,amount,note) values ('b',2,'private',x'',-1.25,'')")
	dbt.MustExec("alter table parity.t add column added int not null default 7")
	dbt.MustExec("create table parity.untouched (id int primary key, secret varchar(16))")
	dbt.MustExec("insert into parity.untouched values (1,'keep')")

	for _, format := range dumpServiceParityFormats {
		for _, noSchemas := range []bool{false, true} {
			name := "schemas"
			if noSchemas {
				name = "data_only"
			}
			t.Run(format.name+"/"+name, func(t *testing.T) {
				formatWithSchema := format
				formatWithSchema.read = func(t *testing.T, dir string) []string {
					rows := format.read(t, dir)
					require.NotEmpty(t, rows)
					path := filepath.Join(dir, "parity.t-schema.sql")
					if noSchemas {
						_, err := os.Stat(path)
						require.ErrorIs(t, err, os.ErrNotExist)
						return rows
					}
					content, err := os.ReadFile(path)
					require.NoError(t, err)
					start := strings.Index(string(content), "CREATE TABLE")
					require.GreaterOrEqual(t, start, 0)
					schemaSQL := string(content[start:])
					schema := parseCreateTableForTest(t, schemaSQL)
					require.Equal(t, []string{"id", "payload", "virtual_keep", "amount", "stored_keep", "note", "added"}, createTableColumnNames(schema))
					require.NotContains(t, createTableConstraintNames(schema), "secret_idx")
					untouched, err := os.ReadFile(filepath.Join(dir, "parity.untouched-schema.sql"))
					require.NoError(t, err)
					require.Contains(t, string(untouched), "`secret`")
					return append(rows, schemaSQL, string(untouched))
				}
				editConfig := func(conf *Config) {
					conf.NoSchemas = noSchemas
					conf.FileSize = 1
					conf.columnFilter = newColumnFilterConfigForTest(t,
						columnFilterRule{Matcher: []string{"parity.t"}, Columns: []string{"*", "!secret", "!tenant"}},
					)
				}
				sqlOutput, serviceOutput := dumpParityOutputs(t, server, formatWithSchema, editConfig, editConfig)
				require.Equal(t, sqlOutput, serviceOutput)
			})
		}
	}
}

// Compare import-session settings in every SQL data and schema file against a
// normal MySQL-protocol export, regardless of the number of data chunks.
func testDumpServiceSQLPreamble(t *testing.T, server *servertestkit.TidbTestSuite) {
	db, err := sql.Open("mysql", server.GetDSN())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	dbt := testkit.NewDBTestKit(t, db)
	dbt.MustExec("create database parity")
	t.Cleanup(func() { dbt.MustExec("drop database parity") })
	dbt.MustExec("create table parity.t (id int primary key)")
	dbt.MustExec("insert into parity.t values (1), (2)")

	format := dumpServiceParityFormat{
		fileType: FileFormatSQLTextString,
		read: func(t *testing.T, dir string) []string {
			t.Helper()
			paths := parityDataFiles(t, dir, ".sql")
			require.NotEmpty(t, paths, "the export must contain SQL data files")
			paths = append(paths, filepath.Join(dir, "parity-schema-create.sql"), filepath.Join(dir, "parity.t-schema.sql"))
			var preamble string
			for _, path := range paths {
				content, err := os.ReadFile(path)
				require.NoError(t, err)
				lines := strings.SplitN(string(content), "\n", 3)
				require.Len(t, lines, 3, path)
				require.True(t, strings.HasPrefix(lines[0], "/*!"), "missing SQL preamble in %s", path)
				header := strings.Join(lines[:2], "\n")
				if preamble == "" {
					preamble = header
				}
				require.Equal(t, preamble, header, path)
			}
			return []string{preamble}
		},
	}
	sqlOutput, serviceOutput := dumpParityOutputs(t, server, format, nil, nil)
	require.Equal(t, sqlOutput, serviceOutput)
}

// Compare driver-derived types and serialized values, including numeric limits,
// temporal values, and text/binary distinctions, across every output format.
func testDumpServiceColumnTypes(t *testing.T, server *servertestkit.TidbTestSuite) {
	db, err := sql.Open("mysql", server.GetDSN())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	dbt := testkit.NewDBTestKit(t, db)
	dbt.MustExec("create database parity")
	t.Cleanup(func() { dbt.MustExec("drop database parity") })
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
	for _, format := range dumpServiceParityFormats {
		t.Run(format.name, func(t *testing.T) {
			sqlOutput, kvOutput := dumpParityOutputs(t, server, format, func(conf *Config) {
				// Match the KV decoder's fixed UTC session for temporal comparisons.
				conf.SessionParams = map[string]any{"time_zone": "+00:00"}
			}, nil)
			require.NotEmpty(t, sqlOutput)
			require.Equal(t, sqlOutput, kvOutput)
		})
	}
}

// Compare none/stored projection with SQL for physical generated values while
// excluding virtual columns, including NULL values and both row encodings.
func testDumpServiceGeneratedColumns(t *testing.T, server *servertestkit.TidbTestSuite) {
	db, err := sql.Open("mysql", server.GetDSN())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	dbt := testkit.NewDBTestKit(t, db)
	dbt.MustExec("create database parity")
	t.Cleanup(func() { dbt.MustExec("drop database parity") })
	dbt.MustExec(`create table parity.t (
		id bigint primary key clustered,
		virtual_value int as (source_value + 1),
		source_value int,
		stored_value int as (source_value * 3) stored
	)`)
	// Both row encodings must preserve stored values and their NULLs. The
	// virtual column sits between output columns to exercise projection order.
	dbt.MustExec("set @@tidb_row_format_version=1")
	dbt.MustExec("insert into parity.t(id, source_value) values (1,2)")
	dbt.MustExec("set @@tidb_row_format_version=2")
	dbt.MustExec("insert into parity.t(id, source_value) values (2,4),(3,null)")
	for _, format := range dumpServiceParityFormats {
		for _, mode := range []GeneratedColumnsMode{GeneratedColumnsNone, GeneratedColumnsStored} {
			if format.fileType == FileFormatSQLTextString && mode == GeneratedColumnsStored {
				continue
			}
			t.Run(format.name+"/"+string(mode), func(t *testing.T) {
				editConfig := func(conf *Config) {
					conf.IncludeGeneratedColumns = mode
				}
				sqlOutput, kvOutput := dumpParityOutputs(t, server, format, editConfig, editConfig)
				require.NotEmpty(t, sqlOutput)
				require.Equal(t, sqlOutput, kvOutput)
			})
		}
	}
}

type dumpServiceParityFormat struct {
	name       string
	fileType   string
	csvDialect CSVDialect
	read       func(*testing.T, string) []string
}

var dumpServiceParityFormats = []dumpServiceParityFormat{
	{name: "sql", fileType: FileFormatSQLTextString, read: readSQLDataRows},
	{name: "csv", fileType: FileFormatCSVString, read: readCSVDataRows},
	// Snowflake hex-encodes binary columns, exposing text/binary misclassification.
	{name: "csv_snowflake", fileType: FileFormatCSVString, csvDialect: CSVDialectSnowflake, read: readCSVDataRows},
	{name: "parquet", fileType: FileFormatParquetString, read: readParquetData},
}

func dumpParityOutputs(
	t *testing.T,
	server *servertestkit.TidbTestSuite,
	format dumpServiceParityFormat,
	editSQLConfig, editServiceConfig func(*Config),
) ([]string, []string) {
	t.Helper()
	dump := func(service string, editConfig func(*Config)) []string {
		conf := DefaultConfig()
		conf.Host = "127.0.0.1"
		conf.Port = int(server.Port)
		conf.User = "root"
		conf.StatusAddr = ""
		conf.OutputDirPath = t.TempDir()
		conf.NoHeader = true
		conf.TableFilter = tf.NewSchemasFilter("parity")
		conf.FileType = format.fileType
		conf.CsvOutputDialect = format.csvDialect
		conf.DumpService = service
		if editConfig != nil {
			editConfig(conf)
		}
		dumper, err := NewDumper(context.Background(), conf)
		require.NoError(t, err)
		defer func() { require.NoError(t, dumper.Close()) }()
		require.NoError(t, dumper.Dump())
		return format.read(t, conf.OutputDirPath)
	}

	sqlOutput := dump("", editSQLConfig)
	socket := startTestDumpService(t, http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		serveKVTestRequest(server.Store, writer, request)
	}))
	service := (&url.URL{Scheme: "unix", Path: socket}).String()
	return sqlOutput, dump(service, editServiceConfig)
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
