// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package export

import (
	"fmt"
	"testing"

	"github.com/docker/go-units"
	"github.com/pingcap/tidb/br/pkg/version"
	tcontext "github.com/pingcap/tidb/dumpling/context"
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

func TestParseParquetDefaultFlags(t *testing.T) {
	defaultConf := DefaultConfig()
	require.Equal(t, compressedio.Snappy, defaultConf.ParquetCompressType)
	require.EqualValues(t, units.MiB, defaultConf.ParquetPageSize)
	require.EqualValues(t, 120*units.MiB, defaultConf.ParquetRowGroupSize)

	conf := parseConfigFromArgsForTest(t)
	require.EqualValues(t, units.MiB, conf.ParquetPageSize)
	require.EqualValues(t, 120*units.MiB, conf.ParquetRowGroupSize)
	require.Equal(t, compressedio.Snappy, conf.ParquetCompressType)
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

func parseConfigFromArgsForTest(t *testing.T, args ...string) *Config {
	t.Helper()

	conf := DefaultConfig()
	flags := pflag.NewFlagSet("dumpling", pflag.ContinueOnError)
	conf.DefineFlags(flags)
	oldCommandLine := pflag.CommandLine
	pflag.CommandLine = flags
	t.Cleanup(func() {
		pflag.CommandLine = oldCommandLine
	})
	require.NoError(t, flags.Parse(args))
	require.NoError(t, conf.ParseFromFlags(flags))
	return conf
}
