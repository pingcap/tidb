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

package config_test

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/pingcap/tidb/br/pkg/lightning/config"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/stretchr/testify/require"
)

func startMockServer(t *testing.T, statusCode int, content string) (*httptest.Server, string, int) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(statusCode)
		_, _ = fmt.Fprint(w, content)
	}))

	url, err := url.Parse(ts.URL)
	require.NoError(t, err)
	host, portString, err := net.SplitHostPort(url.Host)
	require.NoError(t, err)
	port, err := strconv.Atoi(portString)
	require.NoError(t, err)

	return ts, host, port
}

func assignMinimalLegalValue(cfg *config.Config) {
	cfg.TiDB.Host = "123.45.67.89"
	cfg.TiDB.Port = 4567
	cfg.TiDB.StatusPort = 8901
	cfg.TiDB.PdAddr = "234.56.78.90:12345"
	cfg.Mydumper.SourceDir = "file://."
	cfg.TikvImporter.Backend = config.BackendLocal
	cfg.TikvImporter.SortedKVDir = "."
	cfg.TikvImporter.DiskQuota = 1
}

func TestAdjustPdAddrAndPort(t *testing.T) {
	ts, host, port := startMockServer(t, http.StatusOK,
		`{"port":4444,"advertise-address":"","path":"123.45.67.89:1234,56.78.90.12:3456"}`,
	)
	defer ts.Close()

	cfg := config.NewConfig()
	cfg.TiDB.Host = host
	cfg.TiDB.StatusPort = port
	cfg.Mydumper.SourceDir = "."
	cfg.TikvImporter.Backend = config.BackendLocal
	cfg.TikvImporter.SortedKVDir = "."
	cfg.TiDB.DistSQLScanConcurrency = 1

	err := cfg.Adjust(context.Background())
	require.NoError(t, err)
	require.Equal(t, 4444, cfg.TiDB.Port)
	require.Equal(t, "123.45.67.89:1234", cfg.TiDB.PdAddr)
}

func TestAdjustPdAddrAndPortViaAdvertiseAddr(t *testing.T) {
	ts, host, port := startMockServer(t, http.StatusOK,
		`{"port":6666,"advertise-address":"121.212.121.212:5555","path":"34.34.34.34:3434"}`,
	)
	defer ts.Close()

	cfg := config.NewConfig()
	cfg.TiDB.Host = host
	cfg.TiDB.StatusPort = port
	cfg.Mydumper.SourceDir = "."
	cfg.TikvImporter.Backend = config.BackendLocal
	cfg.TikvImporter.SortedKVDir = "."
	cfg.TiDB.DistSQLScanConcurrency = 1

	err := cfg.Adjust(context.Background())
	require.NoError(t, err)
	require.Equal(t, 6666, cfg.TiDB.Port)
	require.Equal(t, "34.34.34.34:3434", cfg.TiDB.PdAddr)
}

func TestAdjustPageNotFound(t *testing.T) {
	ts, host, port := startMockServer(t, http.StatusNotFound, "{}")
	defer ts.Close()

	cfg := config.NewConfig()
	cfg.TiDB.Host = host
	cfg.TiDB.StatusPort = port
	cfg.TikvImporter.Backend = config.BackendLocal
	cfg.TikvImporter.SortedKVDir = "."
	cfg.TiDB.DistSQLScanConcurrency = 1

	err := cfg.Adjust(context.Background())
	require.Error(t, err)
	require.Regexp(t, "cannot fetch settings from TiDB.*", err.Error())
}

func TestAdjustConnectRefused(t *testing.T) {
	ts, host, port := startMockServer(t, http.StatusOK, "{}")

	cfg := config.NewConfig()
	cfg.TiDB.Host = host
	cfg.TiDB.StatusPort = port
	cfg.TikvImporter.Backend = config.BackendLocal
	cfg.TikvImporter.SortedKVDir = "."
	cfg.TiDB.DistSQLScanConcurrency = 1

	ts.Close() // immediately close to ensure connection refused.

	err := cfg.Adjust(context.Background())
	require.Error(t, err)
	require.Regexp(t, "cannot fetch settings from TiDB.*", err.Error())
}

func TestAdjustBackendNotSet(t *testing.T) {
	cfg := config.NewConfig()
	cfg.TiDB.DistSQLScanConcurrency = 1
	err := cfg.Adjust(context.Background())
	require.EqualError(t, err, "[Lightning:Config:ErrInvalidConfig]tikv-importer.backend must not be empty!")
}

func TestAdjustInvalidBackend(t *testing.T) {
	cfg := config.NewConfig()
	cfg.TikvImporter.Backend = "no_such_backend"
	cfg.TiDB.DistSQLScanConcurrency = 1
	err := cfg.Adjust(context.Background())
	require.EqualError(t, err, "[Lightning:Config:ErrInvalidConfig]unsupported `tikv-importer.backend` (no_such_backend)")
}

func TestCheckAndAdjustFilePath(t *testing.T) {
	tmpDir := t.TempDir()
	// use slashPath in url to be compatible with windows
	slashPath := filepath.ToSlash(tmpDir)
	pwd, err := os.Getwd()
	require.NoError(t, err)
	specialDir, err := os.MkdirTemp(tmpDir, "abc??bcd")
	require.NoError(t, err)
	specialDir1, err := os.MkdirTemp(tmpDir, "abc%3F%3F%3Fbcd")
	require.NoError(t, err)

	cfg := config.NewConfig()

	cases := []struct {
		test   string
		expect string
	}{
		{tmpDir, tmpDir},
		{".", filepath.ToSlash(pwd)},
		{specialDir, specialDir},
		{specialDir1, specialDir1},
		{"file://" + slashPath, slashPath},
		{"local://" + slashPath, slashPath},
		{"s3://bucket_name", ""},
		{"s3://bucket_name/path/to/dir", "/path/to/dir"},
		{"gcs://bucketname/path/to/dir", "/path/to/dir"},
		{"gs://bucketname/path/to/dir", "/path/to/dir"},
		{"noop:///", "/"},
	}
	for _, testCase := range cases {
		cfg.Mydumper.SourceDir = testCase.test
		err = cfg.CheckAndAdjustFilePath()
		require.NoError(t, err)
		u, err := url.Parse(cfg.Mydumper.SourceDir)
		require.NoError(t, err)
		require.Equal(t, testCase.expect, u.Path)
	}
}

func TestAdjustFileRoutePath(t *testing.T) {
	cfg := config.NewConfig()
	assignMinimalLegalValue(cfg)

	ctx := context.Background()
	tmpDir := t.TempDir()
	cfg.Mydumper.SourceDir = tmpDir
	invalidPath := filepath.Join(tmpDir, "../test123/1.sql")
	rule := &config.FileRouteRule{Path: invalidPath, Type: "sql", Schema: "test", Table: "tbl"}
	cfg.Mydumper.FileRouters = []*config.FileRouteRule{rule}
	cfg.TiDB.DistSQLScanConcurrency = 1
	err := cfg.Adjust(ctx)
	require.Error(t, err)
	require.Regexp(t, fmt.Sprintf("\\Qfile route path '%s' is not in source dir '%s'\\E", invalidPath, tmpDir), err.Error())

	relPath := filepath.FromSlash("test_dir/1.sql")
	rule.Path = filepath.Join(tmpDir, relPath)
	err = cfg.Adjust(ctx)
	require.NoError(t, err)
	require.Equal(t, relPath, cfg.Mydumper.FileRouters[0].Path)
}

func TestDecodeError(t *testing.T) {
	ts, host, port := startMockServer(t, http.StatusOK, "invalid-string")
	defer ts.Close()

	cfg := config.NewConfig()
	cfg.TiDB.Host = host
	cfg.TiDB.StatusPort = port
	cfg.TikvImporter.Backend = config.BackendLocal
	cfg.TikvImporter.SortedKVDir = "."
	cfg.TiDB.DistSQLScanConcurrency = 1

	err := cfg.Adjust(context.Background())
	require.Error(t, err)
	require.Regexp(t, "cannot fetch settings from TiDB.*", err.Error())
}

func TestInvalidSetting(t *testing.T) {
	ts, host, port := startMockServer(t, http.StatusOK, `{"port": 0}`)
	defer ts.Close()

	cfg := config.NewConfig()
	cfg.TiDB.Host = host
	cfg.TiDB.StatusPort = port
	cfg.TikvImporter.Backend = config.BackendLocal
	cfg.TikvImporter.SortedKVDir = "."
	cfg.TiDB.DistSQLScanConcurrency = 1

	err := cfg.Adjust(context.Background())
	require.EqualError(t, err, "[Lightning:Config:ErrInvalidConfig]invalid `tidb.port` setting")
}

func TestInvalidPDAddr(t *testing.T) {
	ts, host, port := startMockServer(t, http.StatusOK, `{"port": 1234, "path": ",,"}`)
	defer ts.Close()

	cfg := config.NewConfig()
	cfg.TiDB.Host = host
	cfg.TiDB.StatusPort = port
	cfg.TikvImporter.Backend = config.BackendLocal
	cfg.TikvImporter.SortedKVDir = "."
	cfg.TiDB.DistSQLScanConcurrency = 1

	err := cfg.Adjust(context.Background())
	require.EqualError(t, err, "[Lightning:Config:ErrInvalidConfig]invalid `tidb.pd-addr` setting")
}

func TestAdjustWillNotContactServerIfEverythingIsDefined(t *testing.T) {
	cfg := config.NewConfig()
	assignMinimalLegalValue(cfg)
	cfg.TiDB.DistSQLScanConcurrency = 1

	err := cfg.Adjust(context.Background())
	require.NoError(t, err)
	require.Equal(t, 4567, cfg.TiDB.Port)
	require.Equal(t, "234.56.78.90:12345", cfg.TiDB.PdAddr)
}

func TestAdjustWillBatchImportRatioInvalid(t *testing.T) {
	cfg := config.NewConfig()
	assignMinimalLegalValue(cfg)
	cfg.Mydumper.BatchImportRatio = -1
	cfg.TiDB.DistSQLScanConcurrency = 1
	err := cfg.Adjust(context.Background())
	require.NoError(t, err)
	require.Equal(t, 0.75, cfg.Mydumper.BatchImportRatio)
}

func TestAdjustSecuritySection(t *testing.T) {
	testCases := []struct {
		input       string
		expectedCA  string
		expectedTLS string
	}{
		{
			input:       ``,
			expectedCA:  "",
			expectedTLS: "false",
		},
		{
			input: `
				[security]
			`,
			expectedCA:  "",
			expectedTLS: "false",
		},
		{
			input: `
				[security]
				ca-path = "/path/to/ca.pem"
			`,
			expectedCA:  "/path/to/ca.pem",
			expectedTLS: "cluster",
		},
		{
			input: `
				[security]
				ca-path = "/path/to/ca.pem"
				[tidb.security]
			`,
			expectedCA:  "",
			expectedTLS: "false",
		},
		{
			input: `
				[security]
				ca-path = "/path/to/ca.pem"
				[tidb.security]
				ca-path = "/path/to/ca2.pem"
			`,
			expectedCA:  "/path/to/ca2.pem",
			expectedTLS: "cluster",
		},
		{
			input: `
				[security]
				[tidb.security]
				ca-path = "/path/to/ca2.pem"
			`,
			expectedCA:  "/path/to/ca2.pem",
			expectedTLS: "cluster",
		},
		{
			input: `
				[security]
				[tidb]
				tls = "skip-verify"
				[tidb.security]
			`,
			expectedCA:  "",
			expectedTLS: "skip-verify",
		},
	}

	for _, tc := range testCases {
		comment := fmt.Sprintf("input = %s", tc.input)

		cfg := config.NewConfig()
		assignMinimalLegalValue(cfg)
		cfg.TiDB.DistSQLScanConcurrency = 1
		err := cfg.LoadFromTOML([]byte(tc.input))
		require.NoError(t, err, comment)

		err = cfg.Adjust(context.Background())
		require.NoError(t, err, comment)
		require.Equal(t, tc.expectedCA, cfg.TiDB.Security.CAPath, comment)
		require.Equal(t, tc.expectedTLS, cfg.TiDB.TLS, comment)
	}
	// test different tls config name
	cfg := config.NewConfig()
	assignMinimalLegalValue(cfg)
	cfg.Security.CAPath = "/path/to/ca.pem"
	cfg.Security.TLSConfigName = "tidb-tls"
	require.NoError(t, cfg.Adjust(context.Background()))
	require.Equal(t, cfg.TiDB.TLS, cfg.TiDB.Security.TLSConfigName)
}

func TestInvalidCSV(t *testing.T) {
	testCases := []struct {
		input string
		err   string
	}{
		{
			input: `
				[mydumper.csv]
				separator = ''
			`,
			err: "[Lightning:Config:ErrInvalidConfig]`mydumper.csv.separator` must not be empty",
		},
		{
			input: `
				[mydumper.csv]
				separator = 'hello'
				delimiter = 'hel'
			`,
			err: "[Lightning:Config:ErrInvalidConfig]`mydumper.csv.separator` and `mydumper.csv.delimiter` must not be prefix of each other",
		},
		{
			input: `
				[mydumper.csv]
				separator = 'hel'
				delimiter = 'hello'
			`,
			err: "[Lightning:Config:ErrInvalidConfig]`mydumper.csv.separator` and `mydumper.csv.delimiter` must not be prefix of each other",
		},
		{
			input: `
				[mydumper.csv]
				separator = '\'
				backslash-escape = false
			`,
			err: "",
		},
		{
			input: `
				[mydumper.csv]
				separator = '，'
			`,
			err: "",
		},
		{
			input: `
				[mydumper.csv]
				delimiter = ''
			`,
			err: "",
		},
		{
			input: `
				[mydumper.csv]
				delimiter = 'hello'
			`,
			err: "",
		},
		{
			input: `
				[mydumper.csv]
				delimiter = '\'
				backslash-escape = false
			`,
			err: "",
		},
		{
			input: `
				[mydumper.csv]
				separator = '\s'
				delimiter = '\d'
			`,
			err: "",
		},
		{
			input: `
				[mydumper.csv]
				separator = '|'
				delimiter = '|'
			`,
			err: "[Lightning:Config:ErrInvalidConfig]`mydumper.csv.separator` and `mydumper.csv.delimiter` must not be prefix of each other",
		},
		{
			input: `
				[mydumper.csv]
				separator = '\'
				backslash-escape = true
			`,
			err: "[Lightning:Config:ErrInvalidConfig]cannot use '\\' as CSV separator when `mydumper.csv.backslash-escape` is true",
		},
		{
			input: `
				[mydumper.csv]
				delimiter = '\'
				backslash-escape = true
			`,
			err: "[Lightning:Config:ErrInvalidConfig]cannot use '\\' as CSV delimiter when `mydumper.csv.backslash-escape` is true",
		},
		{
			input: `
				[tidb]
				sql-mode = "invalid-sql-mode"
			`,
			err: "[Lightning:Config:ErrInvalidConfig]`mydumper.tidb.sql_mode` must be a valid SQL_MODE: ERROR 1231 (42000): Variable 'sql_mode' can't be set to the value of 'invalid-sql-mode'",
		},
		{
			input: `
				[[routes]]
				schema-pattern = ""
				table-pattern = "shard_table_*"
			`,
			err: "[Lightning:Config:ErrInvalidConfig]file route rule is invalid: schema pattern of table route rule should not be empty",
		},
		{
			input: `
				[[routes]]
				schema-pattern = "schema_*"
				table-pattern = ""
			`,
			err: "[Lightning:Config:ErrInvalidConfig]file route rule is invalid: target schema of table route rule should not be empty",
		},
	}

	for _, tc := range testCases {
		comment := fmt.Sprintf("input = %s", tc.input)
		cfg := config.NewConfig()
		cfg.Mydumper.SourceDir = "file://."
		cfg.TiDB.Port = 4000
		cfg.TiDB.PdAddr = "test.invalid:2379"
		cfg.TikvImporter.Backend = config.BackendLocal
		cfg.TikvImporter.SortedKVDir = "."
		cfg.TiDB.DistSQLScanConcurrency = 1
		err := cfg.LoadFromTOML([]byte(tc.input))
		require.NoError(t, err)

		err = cfg.Adjust(context.Background())
		if tc.err != "" {
			require.EqualError(t, err, tc.err, comment)
		} else {
			require.NoError(t, err)
		}
	}
}

func TestInvalidTOML(t *testing.T) {
	cfg := &config.Config{}
	err := cfg.LoadFromTOML([]byte(`
		invalid[mydumper.csv]
		delimiter = '\'
		backslash-escape = true
	`))
	require.EqualError(t, err, "Near line 0 (last key parsed ''): bare keys cannot contain '['")
}

func TestTOMLUnusedKeys(t *testing.T) {
	cfg := &config.Config{}
	err := cfg.LoadFromTOML([]byte(`
		[lightning]
		typo = 123
	`))
	require.EqualError(t, err, "config file contained unknown configuration options: lightning.typo")
}

func TestDurationUnmarshal(t *testing.T) {
	duration := config.Duration{}
	err := duration.UnmarshalText([]byte("13m20s"))
	require.NoError(t, err)
	require.Equal(t, 13*60+20.0, duration.Duration.Seconds())
	err = duration.UnmarshalText([]byte("13x20s"))
	require.Error(t, err)
	require.Regexp(t, "time: unknown unit .?x.? in duration .?13x20s.?", err.Error())
}

func TestDurationMarshalJSON(t *testing.T) {
	duration := config.Duration{}
	err := duration.UnmarshalText([]byte("13m20s"))
	require.NoError(t, err)
	require.Equal(t, 13*60+20.0, duration.Duration.Seconds())
	result, err := duration.MarshalJSON()
	require.NoError(t, err)
	require.Equal(t, `"13m20s"`, string(result))
}

func TestDuplicateResolutionAlgorithm(t *testing.T) {
	var dra config.DuplicateResolutionAlgorithm
	require.NoError(t, dra.FromStringValue("record"))
	require.Equal(t, config.DupeResAlgRecord, dra)
	require.NoError(t, dra.FromStringValue("none"))
	require.Equal(t, config.DupeResAlgNone, dra)
	require.NoError(t, dra.FromStringValue("remove"))
	require.Equal(t, config.DupeResAlgRemove, dra)

	require.Equal(t, "record", config.DupeResAlgRecord.String())
	require.Equal(t, "none", config.DupeResAlgNone.String())
	require.Equal(t, "remove", config.DupeResAlgRemove.String())
}

func TestLoadConfig(t *testing.T) {
	cfg, err := config.LoadGlobalConfig([]string{"-tidb-port", "sss"}, nil)
	require.EqualError(t, err, `[Lightning:Common:ErrInvalidArgument]invalid argument: invalid value "sss" for flag -tidb-port: parse error`)
	require.Nil(t, cfg)

	cfg, err = config.LoadGlobalConfig([]string{"-V"}, nil)
	require.Equal(t, flag.ErrHelp, err)
	require.Nil(t, cfg)

	cfg, err = config.LoadGlobalConfig([]string{"-config", "not-exists"}, nil)
	require.Error(t, err)
	require.Regexp(t, ".*(no such file or directory|The system cannot find the file specified).*", err.Error())
	require.Nil(t, cfg)

	cfg, err = config.LoadGlobalConfig([]string{"--server-mode"}, nil)
	require.EqualError(t, err, "[Lightning:Config:ErrInvalidConfig]If server-mode is enabled, the status-addr must be a valid listen address")
	require.Nil(t, cfg)

	path, _ := filepath.Abs(".")
	cfg, err = config.LoadGlobalConfig([]string{
		"-L", "debug",
		"-log-file", "/path/to/file.log",
		"-tidb-host", "172.16.30.11",
		"-tidb-port", "4001",
		"-tidb-user", "guest",
		"-tidb-password", "12345",
		"-pd-urls", "172.16.30.11:2379,172.16.30.12:2379",
		"-d", path,
		"-backend", config.BackendLocal,
		"-sorted-kv-dir", ".",
		"-checksum=false",
	}, nil)
	require.NoError(t, err)
	require.Equal(t, "debug", cfg.App.Config.Level)
	require.Equal(t, "/path/to/file.log", cfg.App.Config.File)
	require.Equal(t, "172.16.30.11", cfg.TiDB.Host)
	require.Equal(t, 4001, cfg.TiDB.Port)
	require.Equal(t, "guest", cfg.TiDB.User)
	require.Equal(t, "12345", cfg.TiDB.Psw)
	require.Equal(t, "172.16.30.11:2379,172.16.30.12:2379", cfg.TiDB.PdAddr)
	require.Equal(t, path, cfg.Mydumper.SourceDir)
	require.Equal(t, config.BackendLocal, cfg.TikvImporter.Backend)
	require.Equal(t, ".", cfg.TikvImporter.SortedKVDir)
	require.Equal(t, config.OpLevelOff, cfg.PostRestore.Checksum)
	require.Equal(t, config.OpLevelOptional, cfg.PostRestore.Analyze)

	taskCfg := config.NewConfig()
	err = taskCfg.LoadFromGlobal(cfg)
	require.NoError(t, err)
	require.Equal(t, config.OpLevelOff, taskCfg.PostRestore.Checksum)
	require.Equal(t, config.OpLevelOptional, taskCfg.PostRestore.Analyze)

	taskCfg.Checkpoint.DSN = ""
	taskCfg.Checkpoint.Driver = config.CheckpointDriverMySQL
	taskCfg.TiDB.DistSQLScanConcurrency = 1
	err = taskCfg.Adjust(context.Background())
	require.NoError(t, err)
	require.Equal(t, "guest:12345@tcp(172.16.30.11:4001)/?charset=utf8mb4&sql_mode='"+mysql.DefaultSQLMode+"'&maxAllowedPacket=67108864&tls=false", taskCfg.Checkpoint.DSN)

	result := taskCfg.String()
	require.Regexp(t, `.*"pd-addr":"172.16.30.11:2379,172.16.30.12:2379".*`, result)

	cfg, err = config.LoadGlobalConfig([]string{}, nil)
	require.NoError(t, err)
	require.Regexp(t, ".*lightning.log.*", cfg.App.Config.File)
	cfg, err = config.LoadGlobalConfig([]string{"--log-file", "-"}, nil)
	require.NoError(t, err)
	require.Equal(t, "-", cfg.App.Config.File)
}

func TestDefaultImporterBackendValue(t *testing.T) {
	cfg := config.NewConfig()
	assignMinimalLegalValue(cfg)
	cfg.TikvImporter.Backend = "local"
	cfg.TiDB.DistSQLScanConcurrency = 1
	err := cfg.Adjust(context.Background())
	require.NoError(t, err)
	require.Equal(t, 2, cfg.App.IndexConcurrency)
	require.Equal(t, 6, cfg.App.TableConcurrency)
}

func TestDefaultTidbBackendValue(t *testing.T) {
	cfg := config.NewConfig()
	assignMinimalLegalValue(cfg)
	cfg.TikvImporter.Backend = "tidb"
	cfg.App.RegionConcurrency = 123
	cfg.TiDB.DistSQLScanConcurrency = 1
	err := cfg.Adjust(context.Background())
	require.NoError(t, err)
	require.Equal(t, 123, cfg.App.TableConcurrency)
}

func TestDefaultCouldBeOverwritten(t *testing.T) {
	cfg := config.NewConfig()
	assignMinimalLegalValue(cfg)
	cfg.TikvImporter.Backend = "local"
	cfg.App.IndexConcurrency = 20
	cfg.App.TableConcurrency = 60
	cfg.TiDB.DistSQLScanConcurrency = 1
	err := cfg.Adjust(context.Background())
	require.NoError(t, err)
	require.Equal(t, 20, cfg.App.IndexConcurrency)
	require.Equal(t, 60, cfg.App.TableConcurrency)
}

func TestLoadFromInvalidConfig(t *testing.T) {
	taskCfg := config.NewConfig()
	err := taskCfg.LoadFromGlobal(&config.GlobalConfig{
		ConfigFileContent: []byte("invalid toml"),
	})
	require.Error(t, err)
	require.Regexp(t, "Near line 1.*", err.Error())
}

func TestTomlPostRestore(t *testing.T) {
	cfg := &config.Config{}
	err := cfg.LoadFromTOML([]byte(`
		[post-restore]
		checksum = "req"
	`))
	require.EqualError(t, err, "invalid op level 'req', please choose valid option between ['off', 'optional', 'required']")

	err = cfg.LoadFromTOML([]byte(`
		[post-restore]
		analyze = 123
	`))
	require.EqualError(t, err, "invalid op level '123', please choose valid option between ['off', 'optional', 'required']")

	kvMap := map[string]config.PostOpLevel{
		`"off"`:      config.OpLevelOff,
		`"required"`: config.OpLevelRequired,
		`"optional"`: config.OpLevelOptional,
		"true":       config.OpLevelRequired,
		"false":      config.OpLevelOff,
	}

	var b bytes.Buffer
	enc := toml.NewEncoder(&b)

	for k, v := range kvMap {
		cfg := &config.Config{}
		confStr := fmt.Sprintf("[post-restore]\r\nchecksum= %s\r\n", k)
		err := cfg.LoadFromTOML([]byte(confStr))
		require.NoError(t, err)
		require.Equal(t, v, cfg.PostRestore.Checksum)

		b.Reset()
		require.NoError(t, enc.Encode(cfg.PostRestore))
		require.Regexp(t, fmt.Sprintf(`(?s).*checksum = "\Q%s\E".*`, v), &b)
	}

	for k, v := range kvMap {
		cfg := &config.Config{}
		confStr := fmt.Sprintf("[post-restore]\r\nanalyze= %s\r\n", k)
		err := cfg.LoadFromTOML([]byte(confStr))
		require.NoError(t, err)
		require.Equal(t, v, cfg.PostRestore.Analyze)

		b.Reset()
		require.NoError(t, enc.Encode(cfg.PostRestore))
		require.Regexp(t, fmt.Sprintf(`(?s).*analyze = "\Q%s\E".*`, v), &b)
	}
}

func TestCronEncodeDecode(t *testing.T) {
	cfg := &config.Config{}
	cfg.Cron.SwitchMode.Duration = 1 * time.Minute
	cfg.Cron.LogProgress.Duration = 2 * time.Minute
	cfg.Cron.CheckDiskQuota.Duration = 3 * time.Second
	var b bytes.Buffer
	require.NoError(t, toml.NewEncoder(&b).Encode(cfg.Cron))
	require.Equal(t, "switch-mode = \"1m0s\"\nlog-progress = \"2m0s\"\ncheck-disk-quota = \"3s\"\n", b.String())

	confStr := "[cron]\r\n" + b.String()
	cfg2 := &config.Config{}
	require.NoError(t, cfg2.LoadFromTOML([]byte(confStr)))
	require.Equal(t, cfg.Cron, cfg2.Cron)
}

func TestAdjustWithLegacyBlackWhiteList(t *testing.T) {
	cfg := config.NewConfig()
	assignMinimalLegalValue(cfg)
	require.Equal(t, config.DefaultFilter, cfg.Mydumper.Filter)
	require.False(t, cfg.HasLegacyBlackWhiteList())

	ctx := context.Background()
	cfg.Mydumper.Filter = []string{"test.*"}
	cfg.TiDB.DistSQLScanConcurrency = 1
	require.NoError(t, cfg.Adjust(ctx))
	require.False(t, cfg.HasLegacyBlackWhiteList())

	cfg.BWList.DoDBs = []string{"test"}
	require.EqualError(t, cfg.Adjust(ctx), "[Lightning:Config:ErrInvalidConfig]`mydumper.filter` and `black-white-list` cannot be simultaneously defined")

	cfg.Mydumper.Filter = config.DefaultFilter
	require.NoError(t, cfg.Adjust(ctx))
	require.True(t, cfg.HasLegacyBlackWhiteList())
}

func TestAdjustDiskQuota(t *testing.T) {
	cfg := config.NewConfig()
	assignMinimalLegalValue(cfg)

	base := t.TempDir()
	ctx := context.Background()
	cfg.TikvImporter.Backend = config.BackendLocal
	cfg.TikvImporter.DiskQuota = 0
	cfg.TikvImporter.SortedKVDir = base
	cfg.TiDB.DistSQLScanConcurrency = 1
	require.NoError(t, cfg.Adjust(ctx))
	require.Equal(t, int64(0), int64(cfg.TikvImporter.DiskQuota))
}

func TestDataCharacterSet(t *testing.T) {
	testCases := []struct {
		input string
		err   string
	}{
		{
			input: `
				[mydumper]
				data-character-set = 'binary'
			`,
			err: "",
		},
		{
			input: `
				[mydumper]
				data-character-set = 'utf8mb4'
			`,
			err: "",
		},
		{
			input: `
				[mydumper]
				data-character-set = 'gb18030'
			`,
			err: "",
		},
		{
			input: `
				[mydumper]
				data-invalid-char-replace = "\u2323"
			`,
			err: "",
		},
		{
			input: `
				[mydumper]
				data-invalid-char-replace = "a"
			`,
			err: "",
		},
		{
			input: `
				[mydumper]
				data-invalid-char-replace = "INV"
			`,
			err: "",
		},
		{
			input: `
				[mydumper]
				data-invalid-char-replace = "😊"
			`,
			err: "",
		},
		{
			input: `
				[mydumper]
				data-invalid-char-replace = "😊😭😅😄"
			`,
			err: "",
		},
	}

	for _, tc := range testCases {
		comment := fmt.Sprintf("input = %s", tc.input)
		cfg := config.NewConfig()
		cfg.Mydumper.SourceDir = "file://."
		cfg.TiDB.Port = 4000
		cfg.TiDB.PdAddr = "test.invalid:2379"
		cfg.TikvImporter.Backend = config.BackendLocal
		cfg.TikvImporter.SortedKVDir = "."
		cfg.TiDB.DistSQLScanConcurrency = 1
		err := cfg.LoadFromTOML([]byte(tc.input))
		require.NoError(t, err)
		err = cfg.Adjust(context.Background())
		if tc.err != "" {
			require.EqualError(t, err, tc.err, comment)
		} else {
			require.NoError(t, err, comment)
		}
	}
}

func TestCheckpointKeepStrategy(t *testing.T) {
	tomlCases := map[interface{}]config.CheckpointKeepStrategy{
		true:     config.CheckpointRename,
		false:    config.CheckpointRemove,
		"remove": config.CheckpointRemove,
		"rename": config.CheckpointRename,
		"origin": config.CheckpointOrigin,
	}
	var cp config.CheckpointKeepStrategy
	for key, strategy := range tomlCases {
		err := cp.UnmarshalTOML(key)
		require.NoError(t, err)
		require.Equal(t, strategy, cp)
	}

	defaultCp := "enable = true\r\n"
	cpCfg := &config.Checkpoint{}
	_, err := toml.Decode(defaultCp, cpCfg)
	require.NoError(t, err)
	require.Equal(t, config.CheckpointRemove, cpCfg.KeepAfterSuccess)

	cpFmt := "keep-after-success = %v\r\n"
	for key, strategy := range tomlCases {
		cpValue := key
		if strVal, ok := key.(string); ok {
			cpValue = `"` + strVal + `"`
		}
		tomlStr := fmt.Sprintf(cpFmt, cpValue)
		cpCfg := &config.Checkpoint{}
		_, err := toml.Decode(tomlStr, cpCfg)
		require.NoError(t, err)
		require.Equal(t, strategy, cpCfg.KeepAfterSuccess)
	}

	marshalTextCases := map[config.CheckpointKeepStrategy]string{
		config.CheckpointRemove: "remove",
		config.CheckpointRename: "rename",
		config.CheckpointOrigin: "origin",
	}
	for strategy, value := range marshalTextCases {
		res, err := strategy.MarshalText()
		require.NoError(t, err)
		require.Equal(t, []byte(value), res)
	}
}

func TestLoadCharsetFromConfig(t *testing.T) {
	cases := map[string]config.Charset{
		"binary":  config.Binary,
		"BINARY":  config.Binary,
		"GBK":     config.GBK,
		"gbk":     config.GBK,
		"Gbk":     config.GBK,
		"gB18030": config.GB18030,
		"GB18030": config.GB18030,
	}
	for k, v := range cases {
		charset, err := config.ParseCharset(k)
		require.NoError(t, err)
		require.Equal(t, v, charset)
	}

	_, err := config.ParseCharset("Unknown")
	require.EqualError(t, err, "found unsupported data-character-set: Unknown")
}

func TestCheckAndAdjustForLocalBackend(t *testing.T) {
	cfg := config.NewConfig()
	assignMinimalLegalValue(cfg)

	cfg.TikvImporter.Backend = config.BackendLocal
	cfg.TikvImporter.SortedKVDir = ""
	require.EqualError(t, cfg.CheckAndAdjustForLocalBackend(), "[Lightning:Config:ErrInvalidConfig]tikv-importer.sorted-kv-dir must not be empty!")

	// non exists dir is legal
	cfg.TikvImporter.SortedKVDir = "./not-exists"
	require.NoError(t, cfg.CheckAndAdjustForLocalBackend())

	base := t.TempDir()
	// create empty file
	file := filepath.Join(base, "file")
	require.NoError(t, os.WriteFile(file, []byte(""), 0644))
	cfg.TikvImporter.SortedKVDir = file
	err := cfg.CheckAndAdjustForLocalBackend()
	require.Error(t, err)
	require.Regexp(t, "tikv-importer.sorted-kv-dir (.*) is not a directory", err.Error())

	// legal dir
	cfg.TikvImporter.SortedKVDir = base
	require.NoError(t, cfg.CheckAndAdjustForLocalBackend())
}
