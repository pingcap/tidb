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

func TestColumnSelectors(t *testing.T) {
	selectors := &ColumnSelectors{
		Mode: ColumnSelectorModeExclude,
		Selectors: []ColumnSelector{
			{Matcher: []string{"db1.t1"}, Columns: []string{"C1", "c2"}},
			{Matcher: []string{"db1.t2"}, Columns: []string{"c3"}},
		},
	}
	require.NoError(t, selectors.compile(false))

	selectedFields, err := selectors.applyToColumns("DB1", "T1", []string{"c1", "C2", "c3"})
	require.NoError(t, err)
	require.Equal(t, []string{"c3"}, selectedFields)

	selectedFields, err = selectors.applyToColumns("db2", "t1", []string{"c1"})
	require.NoError(t, err)
	require.Equal(t, []string{"c1"}, selectedFields)

	selectedFields, err = selectors.applyToColumns("db1", "t2", []string{"c1"})
	require.NoError(t, err)
	require.Equal(t, []string{"c1"}, selectedFields)

	selectors = newColumnSelectorsForTest(t, ColumnSelectorModeExclude,
		ColumnSelector{Matcher: []string{"db1.t1"}},
	)
	selectedFields, err = selectors.applyToColumns("db1", "t1", []string{"c1"})
	require.NoError(t, err)
	require.Equal(t, []string{"c1"}, selectedFields)
}

func TestParseColumnSelectorsFile(t *testing.T) {
	path := writeColumnSelectorsFileForTest(t, `{
		"mode": "include",
		"columnSelectors": [
			{"matcher": ["db1.t1", "db2.t2"], "columns": ["c1", "C2"]}
		]
	}`)
	selectors, err := ParseColumnSelectorsFile(path, false)
	require.NoError(t, err)
	require.Equal(t, ColumnSelectorModeInclude, selectors.Mode)
	selectedFields, err := selectors.applyToColumns("DB1", "T1", []string{"c1", "C2", "c3"})
	require.NoError(t, err)
	require.Equal(t, []string{"c1", "C2"}, selectedFields)

	selectors, err = ParseColumnSelectorsFile(path, true)
	require.NoError(t, err)
	selectedFields, err = selectors.applyToColumns("DB1", "T1", []string{"c1"})
	require.NoError(t, err)
	require.Equal(t, []string{"c1"}, selectedFields)

	_, err = ParseColumnSelectorsFile("", false)
	require.NoError(t, err)

	path = writeColumnSelectorsFileForTest(t, `{"mode": "REWRITE", "columnSelectors": [{"matcher": ["db.t"], "columns": ["c"]}]}`)
	_, err = ParseColumnSelectorsFile(path, false)
	require.ErrorContains(t, err, "mode must be INCLUDE or EXCLUDE")

	path = writeColumnSelectorsFileForTest(t, `{"mode": "INCLUDE", "columnSelectors": [{"matcher": ["db.t"], "columns": ["c", "C"]}]}`)
	_, err = ParseColumnSelectorsFile(path, false)
	require.ErrorContains(t, err, "duplicate column")
}

func TestParseColumnSelectorsFileFlag(t *testing.T) {
	path := writeColumnSelectorsFileForTest(t, `{
		"mode": "EXCLUDE",
		"columnSelectors": [
			{"matcher": ["db1.t1"], "columns": ["c1", "c2"]},
			{"matcher": ["db2.t2"], "columns": ["c3"]}
		]
	}`)
	conf := parseConfigFromArgsForTest(t,
		"--no-schemas",
		"--column-selectors-file", path,
	)
	selectedFields, err := conf.ColumnSelectors.applyToColumns("db1", "t1", []string{"c1", "c2", "c3"})
	require.NoError(t, err)
	require.Equal(t, []string{"c3"}, selectedFields)

	_, err = parseConfigFromArgsForTestWithErr(t, "--column-selectors-file", path)
	require.ErrorContains(t, err, "--column-selectors-file requires --no-schemas/-m")

	conf = parseConfigFromArgsForTest(t,
		"--no-schemas",
		"--column-selectors-file", path,
		"--sql", "select * from t",
	)
	require.Equal(t, "select * from t", conf.SQL)
	require.NotNil(t, conf.ColumnSelectors)
}

func writeColumnSelectorsFileForTest(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "column-selectors.json")
	require.NoError(t, os.WriteFile(path, []byte(content), 0o600))
	return path
}

func newColumnSelectorsForTest(t *testing.T, mode ColumnSelectorMode, selectors ...ColumnSelector) *ColumnSelectors {
	t.Helper()
	columnSelectors := &ColumnSelectors{
		Mode:      mode,
		Selectors: selectors,
	}
	require.NoError(t, columnSelectors.compile(false))
	return columnSelectors
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
