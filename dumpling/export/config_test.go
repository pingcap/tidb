// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package export

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/docker/go-units"
	"github.com/pingcap/tidb/br/pkg/version"
	tcontext "github.com/pingcap/tidb/dumpling/context"
	"github.com/pingcap/tidb/pkg/dumpformat/parquetfile"
	"github.com/pingcap/tidb/pkg/objstore/compressedio"
	"github.com/spf13/pflag"
	"github.com/stretchr/testify/require"
)

func TestCreateExternalStorage(t *testing.T) {
	mockConfig := defaultConfigForTest(t)
	loc, err := mockConfig.createExternalStorage(tcontext.Background())
	require.NoError(t, err)
	require.Regexp(t, "^file:", loc.URI())
}

func TestMatchMysqlBugVersion(t *testing.T) {
	cases := []struct {
		serverInfo version.ServerInfo
		expected   bool
	}{
		{version.ParseServerInfo("5.7.25-TiDB-3.0.6"), false},
		{version.ParseServerInfo("8.0.2"), false},
		{version.ParseServerInfo("8.0.3"), true},
		{version.ParseServerInfo("8.0.22"), true},
		{version.ParseServerInfo("8.0.23"), false},
	}
	for _, x := range cases {
		require.Equalf(t, x.expected, matchMysqlBugversion(x.serverInfo), "server info: %s", x.serverInfo)
	}
}

func TestGetConfTables(t *testing.T) {
	tablesList := []string{"db1t1", "db2.t1"}
	_, err := GetConfTables(tablesList)
	require.EqualError(t, err, fmt.Sprintf("--tables-list only accepts qualified table names, but `%s` lacks a dot", tablesList[0]))

	tablesList = []string{"db1.t1", "db2t1"}
	_, err = GetConfTables(tablesList)
	require.EqualError(t, err, fmt.Sprintf("--tables-list only accepts qualified table names, but `%s` lacks a dot", tablesList[1]))

	tablesList = []string{"db1.t1", "db2.t1"}
	expectedDBTables := NewDatabaseTables().
		AppendTables("db1", []string{"t1"}, []uint64{0}).
		AppendTables("db2", []string{"t1"}, []uint64{0})
	actualDBTables, err := GetConfTables(tablesList)
	require.NoError(t, err)
	require.Equal(t, expectedDBTables, actualDBTables)
}

func TestColumnFilters(t *testing.T) {
	columnFilter := columnFilterConfig{
		Filters: []columnFilterRule{
			{Matcher: []string{"db1.*"}, Columns: []string{"*", "!c*"}},
			{Matcher: []string{"db1.t1"}, Columns: []string{"c2"}},
			{Matcher: []string{"db1.t2"}, Columns: []string{"*", "!c3"}},
		},
	}
	require.NoError(t, columnFilter.compileForOption(false, flagColumnFilterFile))

	selectedFields, selectedIndexes, err := columnFilter.applyToColumns("DB1", "T1", []string{"c1", "C2", "c3", "d"})
	require.NoError(t, err)
	require.Equal(t, []string{"C2", "d"}, selectedFields)
	require.Equal(t, []int{1, 3}, selectedIndexes)

	selectedFields, selectedIndexes, err = columnFilter.applyToColumns("db2", "t1", []string{"c1"})
	require.NoError(t, err)
	require.Equal(t, []string{"c1"}, selectedFields)
	require.Equal(t, []int{0}, selectedIndexes)

	selectedFields, selectedIndexes, err = columnFilter.applyToColumns("db1", "t2", []string{"c1", "c3"})
	require.NoError(t, err)
	require.Equal(t, []string{"c1"}, selectedFields)
	require.Equal(t, []int{0}, selectedIndexes)

	columnFilter = columnFilterConfig{
		Filters: []columnFilterRule{
			{Matcher: []string{"db1.t1"}},
		},
	}
	err = columnFilter.compileForOption(false, flagColumnFilterFile)
	require.ErrorContains(t, err, "--column-filter-file filter 0 requires at least one column rule")
}

func TestParseColumnFilterFile(t *testing.T) {
	path := writeColumnFilterFileForTest(t, `
[[filters]]
matcher = ["db1.t1", "db2.t2"]
columns = ["c1", "C2"]
`)
	columnFilter, err := parseColumnFilterConfig(path, false)
	require.NoError(t, err)
	selectedFields, selectedIndexes, err := columnFilter.applyToColumns("DB1", "T1", []string{"c1", "C2", "c3"})
	require.NoError(t, err)
	require.Equal(t, []string{"c1", "C2"}, selectedFields)
	require.Equal(t, []int{0, 1}, selectedIndexes)

	columnFilter, err = parseColumnFilterConfig(path, true)
	require.NoError(t, err)
	selectedFields, selectedIndexes, err = columnFilter.applyToColumns("DB1", "T1", []string{"c1", "c3"})
	require.NoError(t, err)
	require.Equal(t, []string{"c1", "c3"}, selectedFields)
	require.Equal(t, []int{0, 1}, selectedIndexes)

	path = writeColumnFilterFileForTest(t, `
[[filters]]
matcher = ["db.t"]
columns = ["/unterminated"]
`)
	_, err = parseColumnFilterConfig(path, false)
	require.ErrorContains(t, err, "failed to parse --column-filter-file filter 0 columns")

	path = writeColumnFilterFileForTest(t, `
[[filters]]
matcher = ["db.t"]
colums = ["*"]
`)
	_, err = parseColumnFilterConfig(path, false)
	require.ErrorContains(t, err, "--column-filter-file contains unknown TOML keys: filters.colums")

	for _, content := range []string{
		`
[[filters]]
matcher = ["db.t"]
`,
		`
[[filters]]
matcher = ["db.t"]
columns = []
`,
	} {
		path = writeColumnFilterFileForTest(t, content)
		_, err = parseColumnFilterConfig(path, false)
		require.ErrorContains(t, err, "--column-filter-file filter 0 requires at least one column rule")
	}
}

func TestParseColumnFilterFileFlag(t *testing.T) {
	conf := parseConfigFromArgsForTest(t, "--no-schemas")
	require.Empty(t, conf.columnFilter.Filters)

	path := writeColumnFilterFileForTest(t, `
[[filters]]
matcher = ["db1.t1"]
columns = ["*", "!c1", "!c2"]

[[filters]]
matcher = ["db2.t2"]
columns = ["*", "!c3"]
`)
	conf = parseConfigFromArgsForTest(t,
		"--no-schemas",
		"--column-filter-file", path,
	)
	selectedFields, selectedIndexes, err := conf.columnFilter.applyToColumns("db1", "t1", []string{"c1", "c2", "c3"})
	require.NoError(t, err)
	require.Equal(t, []string{"c3"}, selectedFields)
	require.Equal(t, []int{2}, selectedIndexes)

	_, err = parseConfigFromArgsForTestWithErr(t, "--column-filter-file", path)
	require.ErrorContains(t, err, "--column-filter-file requires --no-schemas/-m")

	_, err = parseConfigFromArgsForTestWithErr(t,
		"--no-schemas",
		"--column-filter-file", path,
		"--sql", "select * from t",
	)
	require.ErrorContains(t, err, "can't specify both --sql and --column-filter-file at the same time")
}

func TestParseColumnFilterFlag(t *testing.T) {
	conf := parseConfigFromArgsForTest(t,
		"--no-schemas",
		"--column-filter", `{ matcher = ["db1.t1"], columns = ["*", "!c1", "!c2"] }`,
		"--column-filter", `{ matcher = ["db2.t2"], columns = ["c3"] }`,
	)
	selectedFields, selectedIndexes, err := conf.columnFilter.applyToColumns("db1", "t1", []string{"c1", "c2", "c3"})
	require.NoError(t, err)
	require.Equal(t, []string{"c3"}, selectedFields)
	require.Equal(t, []int{2}, selectedIndexes)

	selectedFields, selectedIndexes, err = conf.columnFilter.applyToColumns("db2", "t2", []string{"c1", "c2", "c3"})
	require.NoError(t, err)
	require.Equal(t, []string{"c3"}, selectedFields)
	require.Equal(t, []int{2}, selectedIndexes)

	_, err = parseConfigFromArgsForTestWithErr(t,
		"--no-schemas",
		"--column-filter", `{ matcher = ["db.t"], columns = ["/unterminated"] }`,
	)
	require.ErrorContains(t, err, "failed to parse --column-filter filter 0 columns")

	_, err = parseConfigFromArgsForTestWithErr(t,
		"--no-schemas",
		"--column-filter", `{ matcher = ["db.t"], colums = ["*"] }`,
	)
	require.ErrorContains(t, err, "--column-filter contains unknown TOML keys: filter.colums")

	for _, arg := range []string{
		`{ matcher = ["db.t"] }`,
		`{ matcher = ["db.t"], columns = [] }`,
	} {
		_, err = parseConfigFromArgsForTestWithErr(t,
			"--no-schemas",
			"--column-filter", arg,
		)
		require.ErrorContains(t, err, "--column-filter filter 0 requires at least one column rule")
	}

	_, err = parseConfigFromArgsForTestWithErr(t,
		"--column-filter", `{ matcher = ["db.t"], columns = ["*"] }`,
	)
	require.ErrorContains(t, err, "--column-filter requires --no-schemas/-m")

	_, err = parseConfigFromArgsForTestWithErr(t,
		"--no-schemas",
		"--column-filter", `{ matcher = ["db.t"], columns = ["*"] }`,
		"--sql", "select * from t",
	)
	require.ErrorContains(t, err, "can't specify both --sql and --column-filter at the same time")
}

func TestColumnFilterConflict(t *testing.T) {
	path := writeColumnFilterFileForTest(t, `
[[filters]]
matcher = ["db.t"]
columns = ["*"]
`)
	_, err := parseConfigFromArgsForTestWithErr(t,
		"--no-schemas",
		"--column-filter", `{ matcher = ["db.t"], columns = ["*"] }`,
		"--column-filter-file", path,
	)
	require.ErrorContains(t, err, "can't specify both --column-filter and --column-filter-file at the same time")
}

func TestColumnFilterOptions(t *testing.T) {
	conf := DefaultConfig()
	conf.NoSchemas = true
	require.NoError(t, validateColumnFilterOptions(conf, flagColumnFilterFile))

	conf = DefaultConfig()
	conf.NoSchemas = true
	conf.SQL = "select * from t"
	require.ErrorContains(
		t,
		validateColumnFilterOptions(conf, flagColumnFilterFile),
		"can't specify both --sql and --column-filter-file at the same time",
	)

	conf = DefaultConfig()
	require.ErrorContains(t, validateColumnFilterOptions(conf, flagColumnFilterFile), "--column-filter-file requires --no-schemas/-m")
}

func writeColumnFilterFileForTest(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "column-filter.toml")
	require.NoError(t, os.WriteFile(path, []byte(content), 0o600))
	return path
}

func newColumnFilterConfigForTest(t *testing.T, filters ...columnFilterRule) columnFilterConfig {
	t.Helper()
	columnFilter := columnFilterConfig{
		Filters: filters,
	}
	require.NoError(t, columnFilter.compileForOption(false, flagColumnFilterFile))
	return columnFilter
}

func TestParseParquetDefaultFlags(t *testing.T) {
	defaultConf := DefaultConfig()
	require.Equal(t, parquetfile.DefaultCompressionType, defaultConf.ParquetCompressType)
	require.EqualValues(t, units.MiB, defaultConf.ParquetPageSize)
	require.EqualValues(t, 120*units.MiB, defaultConf.ParquetRowGroupSize)

	conf := parseConfigFromArgsForTest(t)
	require.EqualValues(t, units.MiB, conf.ParquetPageSize)
	require.EqualValues(t, 120*units.MiB, conf.ParquetRowGroupSize)
	require.Equal(t, parquetfile.DefaultCompressionType, conf.ParquetCompressType)

	t.Run("parseParquetCompressType uses parquetfile policy", func(t *testing.T) {
		tp, err := parseParquetCompressType("")
		require.NoError(t, err)
		require.Equal(t, parquetfile.DefaultCompressionType, tp)

		tp, err = parseParquetCompressType("zstd")
		require.NoError(t, err)
		require.Equal(t, compressedio.Zstd, tp)
	})
}

func TestParseParquetSizeFlags(t *testing.T) {
	conf := parseConfigFromArgsForTest(t,
		"--filetype", "parquet",
		"--parquet-page-size", "2MiB",
		"--parquet-row-group-size", "128MiB",
	)
	require.EqualValues(t, 2*units.MiB, conf.ParquetPageSize)
	require.EqualValues(t, 128*units.MiB, conf.ParquetRowGroupSize)
}

func TestParseCSVOutputDialectAcceptsUppercaseFileTypeAndDialect(t *testing.T) {
	conf := parseConfigFromArgsForTest(t,
		"--filetype", "CSV",
		"--csv-output-dialect", "SNOWFLAKE",
	)
	require.Equal(t, CSVDialectSnowflake, conf.CsvOutputDialect)
}

func TestOutputFilenameTemplateWithRowsValidation(t *testing.T) {
	t.Run("reject template without index when rows split mode and output template are both specified", func(t *testing.T) {
		_, err := parseConfigFromArgsForTestWithErr(t,
			"--rows", "10",
			"--output-filename-template", "{{.DB}}.{{.Table}}",
		)
		require.ErrorContains(t, err, "--output-filename-template must include a standalone {{.Index}} outside conditional blocks (for example: '{{.DB}}.{{.Table}}.{{.Index}}') when split mode is enabled by --rows/-r or --filesize/-F")
	})

	t.Run("accept template with index when rows and output template are both specified", func(t *testing.T) {
		_, err := parseConfigFromArgsForTestWithErr(t,
			"--rows", "10",
			"--output-filename-template", "{{.DB}}.{{.Table}}.{{.Index}}",
		)
		require.NoError(t, err)
	})

	t.Run("reject template when index only appears in a conditional guard", func(t *testing.T) {
		_, err := parseConfigFromArgsForTestWithErr(t,
			"--rows", "10",
			"--output-filename-template", "{{if .Index}}{{end}}{{.DB}}.{{.Table}}",
		)
		require.ErrorContains(t, err, "--output-filename-template must include a standalone {{.Index}} outside conditional blocks (for example: '{{.DB}}.{{.Table}}.{{.Index}}') when split mode is enabled by --rows/-r or --filesize/-F")
	})

	t.Run("reject template when index is only conditionally rendered", func(t *testing.T) {
		_, err := parseConfigFromArgsForTestWithErr(t,
			"--rows", "10",
			"--output-filename-template", "{{if lt .Index 2}}{{.Index}}{{end}}{{.DB}}.{{.Table}}",
		)
		require.ErrorContains(t, err, "--output-filename-template must include a standalone {{.Index}} outside conditional blocks (for example: '{{.DB}}.{{.Table}}.{{.Index}}') when split mode is enabled by --rows/-r or --filesize/-F")
	})

	t.Run("accept template when standalone index exists outside conditionals", func(t *testing.T) {
		_, err := parseConfigFromArgsForTestWithErr(t,
			"--rows", "10",
			"--output-filename-template", "{{if lt .Index 2}}prefix.{{end}}{{.DB}}.{{.Table}}.{{.Index}}",
		)
		require.NoError(t, err)
	})

	t.Run("accept template without index when rows is explicitly set to zero", func(t *testing.T) {
		_, err := parseConfigFromArgsForTestWithErr(t,
			"--rows", "0",
			"--output-filename-template", "{{.DB}}.{{.Table}}",
		)
		require.NoError(t, err)
	})

	t.Run("reject template without index when filesize split mode and output template are both specified", func(t *testing.T) {
		_, err := parseConfigFromArgsForTestWithErr(t,
			"--filesize", "1MiB",
			"--output-filename-template", "{{.DB}}.{{.Table}}",
		)
		require.ErrorContains(t, err, "--output-filename-template must include a standalone {{.Index}} outside conditional blocks (for example: '{{.DB}}.{{.Table}}.{{.Index}}') when split mode is enabled by --rows/-r or --filesize/-F")
	})

	t.Run("accept template with index when filesize and output template are both specified", func(t *testing.T) {
		_, err := parseConfigFromArgsForTestWithErr(t,
			"--filesize", "1MiB",
			"--output-filename-template", "{{.DB}}.{{.Table}}.{{.Index}}",
		)
		require.NoError(t, err)
	})

	t.Run("accept template without index when rows is not specified", func(t *testing.T) {
		_, err := parseConfigFromArgsForTestWithErr(t,
			"--output-filename-template", "{{.DB}}.{{.Table}}",
		)
		require.NoError(t, err)
	})
}

func parseConfigFromArgsForTest(t *testing.T, args ...string) *Config {
	t.Helper()
	conf, err := parseConfigFromArgsForTestWithErr(t, args...)
	require.NoError(t, err)
	return conf
}

func parseConfigFromArgsForTestWithErr(t *testing.T, args ...string) (*Config, error) {
	t.Helper()
	conf := DefaultConfig()
	flags := pflag.NewFlagSet("dumpling", pflag.ContinueOnError)
	conf.DefineFlags(flags)
	oldCommandLine := pflag.CommandLine
	pflag.CommandLine = flags
	t.Cleanup(func() {
		pflag.CommandLine = oldCommandLine
	})
	if err := flags.Parse(args); err != nil {
		return nil, err
	}
	return conf, conf.ParseFromFlags(flags)
}
