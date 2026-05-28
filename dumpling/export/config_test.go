// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package export

import (
	"fmt"
	"testing"

	"github.com/pingcap/tidb/br/pkg/version"
	tcontext "github.com/pingcap/tidb/dumpling/context"
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
