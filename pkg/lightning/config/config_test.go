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

package config

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"math"
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

func assignMinimalLegalValue(cfg *Config) {
	cfg.TiDB.Host = "123.45.67.89"
	cfg.TiDB.Port = 4567
	cfg.TiDB.StatusPort = 8901
	cfg.TiDB.PdAddr = "234.56.78.90:12345"
	cfg.Mydumper.SourceDir = "file://."
	cfg.TikvImporter.Backend = BackendLocal
	cfg.TikvImporter.SortedKVDir = "."
	cfg.TikvImporter.DiskQuota = 1
}

func TestAdjustPdAddrAndPort(t *testing.T) {
	ts, host, port := startMockServer(t, http.StatusOK,
		`{"port":4444,"advertise-address":"","path":"123.45.67.89:1234,56.78.90.12:3456"}`,
	)
	defer ts.Close()

	cfg := NewConfig()
	cfg.TiDB.Host = host
	cfg.TiDB.StatusPort = port
	cfg.Mydumper.SourceDir = "."
	cfg.TikvImporter.Backend = BackendLocal
	cfg.TikvImporter.SortedKVDir = "."
	cfg.TiDB.DistSQLScanConcurrency = 1

	err := cfg.Adjust(context.Background())
	require.NoError(t, err)
	require.Equal(t, 4444, cfg.TiDB.Port)
	require.Equal(t, "123.45.67.89:1234,56.78.90.12:3456", cfg.TiDB.PdAddr)
}

func TestStrictFormat(t *testing.T) {
	ts, host, port := startMockServer(t, http.StatusOK,
		`{"port":4444,"advertise-address":"","path":"123.45.67.89:1234,56.78.90.12:3456"}`,
	)
	defer ts.Close()

	cfg := NewConfig()
	cfg.TiDB.Host = host
	cfg.TiDB.StatusPort = port
	cfg.Mydumper.SourceDir = "."
	cfg.TikvImporter.Backend = BackendLocal
	cfg.TikvImporter.SortedKVDir = "."
	cfg.TiDB.DistSQLScanConcurrency = 1
	cfg.Mydumper.StrictFormat = true

	err := cfg.Adjust(context.Background())
	require.ErrorContains(t, err, "mydumper.strict-format can not be used with empty mydumper.csv.terminator")
	t.Log(err.Error())

	cfg.Mydumper.CSV.Terminator = "\r\n"
	err = cfg.Adjust(context.Background())
	require.NoError(t, err)
}

func TestPausePDSchedulerScope(t *testing.T) {
	ts, host, port := startMockServer(t, http.StatusOK,
		`{"port":4444,"advertise-address":"","path":"123.45.67.89:1234,56.78.90.12:3456"}`,
	)
	defer ts.Close()
	tmpDir := t.TempDir()

	cfg := NewConfig()
	cfg.TiDB.Host = host
	cfg.TiDB.StatusPort = port
	cfg.TikvImporter.Backend = BackendLocal
	cfg.TikvImporter.SortedKVDir = "test"
	cfg.Mydumper.SourceDir = tmpDir
	require.Equal(t, PausePDSchedulerScopeTable, cfg.TikvImporter.PausePDSchedulerScope)

	cfg.TikvImporter.PausePDSchedulerScope = ""
	err := cfg.Adjust(context.Background())
	require.ErrorContains(t, err, "pause-pd-scheduler-scope is invalid")

	cfg.TikvImporter.PausePDSchedulerScope = "xxx"
	err = cfg.Adjust(context.Background())
	require.ErrorContains(t, err, "pause-pd-scheduler-scope is invalid")

	cfg.TikvImporter.PausePDSchedulerScope = "TABLE"
	require.NoError(t, cfg.Adjust(context.Background()))
	require.Equal(t, PausePDSchedulerScopeTable, cfg.TikvImporter.PausePDSchedulerScope)

	cfg.TikvImporter.PausePDSchedulerScope = "globAL"
	require.NoError(t, cfg.Adjust(context.Background()))
	require.Equal(t, PausePDSchedulerScopeGlobal, cfg.TikvImporter.PausePDSchedulerScope)
}

func TestAdjustPdAddrAndPortViaAdvertiseAddr(t *testing.T) {
	ts, host, port := startMockServer(t, http.StatusOK,
		`{"port":6666,"advertise-address":"121.212.121.212:5555","path":"34.34.34.34:3434"}`,
	)
	defer ts.Close()

	cfg := NewConfig()
	cfg.TiDB.Host = host
	cfg.TiDB.StatusPort = port
	cfg.Mydumper.SourceDir = "."
	cfg.TikvImporter.Backend = BackendLocal
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

	cfg := NewConfig()
	cfg.TiDB.Host = host
	cfg.TiDB.StatusPort = port
	cfg.TikvImporter.Backend = BackendLocal
	cfg.TikvImporter.SortedKVDir = "."
	cfg.TiDB.DistSQLScanConcurrency = 1
	cfg.Mydumper.SourceDir = "."

	err := cfg.Adjust(context.Background())
	require.Error(t, err)
	require.Regexp(t, "cannot fetch settings from TiDB.*", err.Error())
}

func TestAdjustConnectRefused(t *testing.T) {
	ts, host, port := startMockServer(t, http.StatusOK, "{}")

	cfg := NewConfig()
	cfg.TiDB.Host = host
	cfg.TiDB.StatusPort = port
	cfg.TikvImporter.Backend = BackendLocal
	cfg.TikvImporter.SortedKVDir = "."
	cfg.TiDB.DistSQLScanConcurrency = 1
	cfg.Mydumper.SourceDir = "."

	ts.Close() // immediately close to ensure connection refused.

	err := cfg.Adjust(context.Background())
	require.Error(t, err)
	require.Regexp(t, "cannot fetch settings from TiDB.*", err.Error())
}

func TestAdjustBackendNotSet(t *testing.T) {
	cfg := NewConfig()
	cfg.TiDB.DistSQLScanConcurrency = 1
	err := cfg.Adjust(context.Background())
	require.EqualError(t, err, "[Lightning:Config:ErrInvalidConfig]tikv-importer.backend must not be empty!")
}

func TestAdjustInvalidBackend(t *testing.T) {
	cfg := NewConfig()
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

	cfg := NewConfig()

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
		err = cfg.Mydumper.adjustFilePath()
		require.NoError(t, err)
		u, err := url.Parse(cfg.Mydumper.SourceDir)
		require.NoError(t, err)
		require.Equal(t, testCase.expect, u.Path)
	}
}

func TestAdjustFileRoutePath(t *testing.T) {
	cfg := NewConfig()
	assignMinimalLegalValue(cfg)

	ctx := context.Background()
	tmpDir := t.TempDir()
	cfg.Mydumper.SourceDir = tmpDir
	invalidPath := filepath.Join(tmpDir, "../test123/1.sql")
	rule := &FileRouteRule{Path: invalidPath, Type: "sql", Schema: "test", Table: "tbl"}
	cfg.Mydumper.FileRouters = []*FileRouteRule{rule}
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

	cfg := NewConfig()
	cfg.TiDB.Host = host
	cfg.TiDB.StatusPort = port
	cfg.TikvImporter.Backend = BackendLocal
	cfg.TikvImporter.SortedKVDir = "."
	cfg.TiDB.DistSQLScanConcurrency = 1
	cfg.Mydumper.SourceDir = "."

	err := cfg.Adjust(context.Background())
	require.Error(t, err)
	require.Regexp(t, "cannot fetch settings from TiDB.*", err.Error())
}

func TestInvalidSetting(t *testing.T) {
	ts, host, port := startMockServer(t, http.StatusOK, `{"port": 0}`)
	defer ts.Close()

	cfg := NewConfig()
	cfg.TiDB.Host = host
	cfg.TiDB.StatusPort = port
	cfg.TikvImporter.Backend = BackendLocal
	cfg.TikvImporter.SortedKVDir = "."
	cfg.TiDB.DistSQLScanConcurrency = 1
	cfg.Mydumper.SourceDir = "."
	cfg.TiDB.PdAddr = "234.56.78.90:12345"

	err := cfg.Adjust(context.Background())
	require.EqualError(t, err, "[Lightning:Config:ErrInvalidConfig]invalid `tidb.port` setting")
}

func TestInvalidPDAddr(t *testing.T) {
	ts, host, port := startMockServer(t, http.StatusOK, `{"port": 1234, "path": ",,"}`)
	defer ts.Close()

	cfg := NewConfig()
	cfg.TiDB.Host = host
	cfg.TiDB.StatusPort = port
	cfg.TikvImporter.Backend = BackendLocal
	cfg.TikvImporter.SortedKVDir = "."
	cfg.TiDB.DistSQLScanConcurrency = 1
	cfg.Mydumper.SourceDir = "."

	err := cfg.Adjust(context.Background())
	require.EqualError(t, err, "[Lightning:Config:ErrInvalidConfig]invalid `tidb.pd-addr` setting")
}

func TestAdjustWillNotContactServerIfEverythingIsDefined(t *testing.T) {
	cfg := NewConfig()
	assignMinimalLegalValue(cfg)
	cfg.TiDB.DistSQLScanConcurrency = 1

	err := cfg.Adjust(context.Background())
	require.NoError(t, err)
	require.Equal(t, 4567, cfg.TiDB.Port)
	require.Equal(t, "234.56.78.90:12345", cfg.TiDB.PdAddr)
}

func TestAdjustWillBatchImportRatioInvalid(t *testing.T) {
	cfg := NewConfig()
	assignMinimalLegalValue(cfg)
	cfg.Mydumper.BatchImportRatio = -1
	cfg.TiDB.DistSQLScanConcurrency = 1
	err := cfg.Adjust(context.Background())
	require.NoError(t, err)
	require.Equal(t, 0.75, cfg.Mydumper.BatchImportRatio)
}

func TestAdjustSecuritySection(t *testing.T) {
	testCases := []struct {
		input          string
		expectedCA     string
		hasTLS         bool
		fallback2NoTLS bool
	}{
		{
			input:          ``,
			expectedCA:     "",
			hasTLS:         false,
			fallback2NoTLS: false,
		},
		{
			input: `
				[security]
			`,
			expectedCA:     "",
			hasTLS:         false,
			fallback2NoTLS: false,
		},
		{
			input: `
				[security]
				ca-path = "/path/to/ca.pem"
			`,
			expectedCA:     "/path/to/ca.pem",
			hasTLS:         false,
			fallback2NoTLS: false,
		},
		{
			input: `
				[security]
				ca-path = "/path/to/ca.pem"
				[tidb.security]
			`,
			expectedCA:     "",
			hasTLS:         false,
			fallback2NoTLS: false,
		},
		{
			input: `
				[security]
				ca-path = "/path/to/ca.pem"
				[tidb.security]
				ca-path = "/path/to/ca2.pem"
			`,
			expectedCA:     "/path/to/ca2.pem",
			hasTLS:         false,
			fallback2NoTLS: false,
		},
		{
			input: `
				[security]
				[tidb.security]
				ca-path = "/path/to/ca2.pem"
			`,
			expectedCA:     "/path/to/ca2.pem",
			hasTLS:         false,
			fallback2NoTLS: false,
		},
		{
			input: `
				[security]
				[tidb]
				tls = "skip-verify"
				[tidb.security]
			`,
			expectedCA:     "",
			hasTLS:         true,
			fallback2NoTLS: false,
		},
		{
			input: `
				[security]
				[tidb]
				tls = "preferred"
				[tidb.security]
			`,
			expectedCA:     "",
			hasTLS:         true,
			fallback2NoTLS: true,
		},
		{
			input: `
				[security]
				[tidb]
				tls = "false"
				[tidb.security]
			`,
			expectedCA:     "",
			hasTLS:         false,
			fallback2NoTLS: false,
		},
		{
			input: `
				[security]
				[tidb]
				tls = "false"
				[tidb.security]
				ca-path = "/path/to/ca2.pem"
			`,
			expectedCA:     "",
			hasTLS:         false,
			fallback2NoTLS: false,
		},
	}

	for _, tc := range testCases {
		comment := fmt.Sprintf("input = %s", tc.input)

		cfg := NewConfig()
		assignMinimalLegalValue(cfg)
		cfg.TiDB.DistSQLScanConcurrency = 1
		err := cfg.LoadFromTOML([]byte(tc.input))
		require.NoError(t, err, comment)

		err = cfg.TiDB.adjust(context.Background(), &cfg.TikvImporter, &cfg.Security, nil)
		require.NoError(t, err, comment)
		require.Equal(t, tc.expectedCA, cfg.TiDB.Security.CAPath, comment)
		if tc.hasTLS {
			require.NotNil(t, cfg.TiDB.Security.TLSConfig, comment)
		} else {
			require.Nil(t, cfg.TiDB.Security.TLSConfig, comment)
		}
		require.Equal(t, tc.fallback2NoTLS, cfg.TiDB.Security.AllowFallbackToPlaintext, comment)
	}
	// test different tls config name
	cfg := NewConfig()
	assignMinimalLegalValue(cfg)
	cfg.Security.CAPath = "/path/to/ca.pem"
	require.NoError(t, cfg.TiDB.adjust(context.Background(), &cfg.TikvImporter, &cfg.Security, nil))
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
			err: "[Lightning:Config:ErrInvalidConfig]cannot use '\\' both as CSV separator and `mydumper.csv.escaped-by`",
		},
		{
			input: `
				[mydumper.csv]
				delimiter = '\'
				escaped-by = '\'
			`,
			err: "[Lightning:Config:ErrInvalidConfig]cannot use '\\' both as CSV delimiter and `mydumper.csv.escaped-by`",
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
		cfg := NewConfig()
		cfg.Mydumper.SourceDir = "file://."
		cfg.TiDB.Port = 4000
		cfg.TiDB.PdAddr = "test.invalid:2379"
		cfg.TikvImporter.Backend = BackendLocal
		cfg.TikvImporter.SortedKVDir = "."
		cfg.TiDB.DistSQLScanConcurrency = 1
		err := cfg.LoadFromTOML([]byte(tc.input))
		require.NoError(t, err)

		err = cfg.Adjust(context.Background())
		if tc.err != "" {
			require.EqualError(t, err, tc.err, comment)
		} else {
			require.NoError(t, err, tc.input)
		}
	}
}

func TestInvalidTOML(t *testing.T) {
	cfg := &Config{}
	err := cfg.LoadFromTOML([]byte(`
		invalid[mydumper.csv]
		delimiter = '\'
		backslash-escape = true
	`))
	require.EqualError(t, err, "toml: line 2: expected '.' or '=', but got '[' instead")
}

func TestStringOrStringSlice(t *testing.T) {
	cfg := &Config{}
	err := cfg.LoadFromTOML([]byte(`
		[mydumper.csv]
		null = '\N'
	`))
	require.NoError(t, err)
	err = cfg.LoadFromTOML([]byte(`
		[mydumper.csv]
		null = [ '\N', 'NULL' ]
	`))
	require.NoError(t, err)
	err = cfg.LoadFromTOML([]byte(`
		[mydumper.csv]
		null = [ '\N', 123 ]
	`))
	require.ErrorContains(t, err, "invalid string slice")
}

func TestTOMLUnusedKeys(t *testing.T) {
	cfg := &Config{}
	err := cfg.LoadFromTOML([]byte(`
		[lightning]
		typo = 123
	`))
	require.EqualError(t, err, "config file contained unknown configuration options: lightning.typo")
}

func TestDurationUnmarshal(t *testing.T) {
	duration := Duration{}
	err := duration.UnmarshalText([]byte("13m20s"))
	require.NoError(t, err)
	require.Equal(t, 13*60+20.0, duration.Duration.Seconds())
	err = duration.UnmarshalText([]byte("13x20s"))
	require.Error(t, err)
	require.Regexp(t, "time: unknown unit .?x.? in duration .?13x20s.?", err.Error())
}

func TestMaxErrorUnmarshal(t *testing.T) {
	type testCase struct {
		TOMLStr        string
		ExpectedValues map[string]int64
		ExpectErrStr   string
		CaseName       string
	}
	for _, tc := range []*testCase{
		{
			TOMLStr: `max-error = 123`,
			ExpectedValues: map[string]int64{
				"syntax":  0,
				"charset": math.MaxInt64,
				"type":    123,
			},
			CaseName: "Normal_Int",
		},
		{
			TOMLStr: `max-error = -123`,
			ExpectedValues: map[string]int64{
				"syntax":  0,
				"charset": math.MaxInt64,
				"type":    0,
			},
			CaseName: "Abnormal_Negative_Int",
		},
		{
			TOMLStr:      `max-error = "abcde"`,
			ExpectErrStr: "invalid max-error 'abcde', should be an integer or a map of string:int64",
			CaseName:     "Abnormal_String",
		},
		{
			TOMLStr: `[max-error]
syntax = 1
charset = 2
type = 3
`,
			ExpectedValues: map[string]int64{
				"syntax":  0,
				"charset": math.MaxInt64,
				"type":    3,
			},
			CaseName: "Normal_Map_All_Set",
		},
		{
			TOMLStr: `[max-error]
type = 1000
`,
			ExpectedValues: map[string]int64{
				"syntax":  0,
				"charset": math.MaxInt64,
				"type":    1000,
			},
			CaseName: "Normal_Map_Partial_Set",
		},
		{
			TOMLStr: `max-error = { type = 123 }`,
			ExpectedValues: map[string]int64{
				"syntax":  0,
				"charset": math.MaxInt64,
				"type":    123,
			},
			CaseName: "Normal_OneLineMap_Partial_Set",
		},
		{
			TOMLStr: `[max-error]
not_exist = 123
`,
			ExpectedValues: map[string]int64{
				"syntax":  0,
				"charset": math.MaxInt64,
				"type":    0,
			},
			CaseName: "Normal_Map_Partial_Set_Invalid_Key",
		},
		{
			TOMLStr: `[max-error]
type = -123
`,
			ExpectedValues: map[string]int64{
				"syntax":  0,
				"charset": math.MaxInt64,
				"type":    0,
			},
			CaseName: "Normal_Map_Partial_Set_Invalid_Value",
		},
		{
			TOMLStr: `[max-error]
type = abc
`,
			ExpectErrStr: `toml: line 2 (last key "max-error.type"): expected value but found "abc" instead`,
			CaseName:     "Normal_Map_Partial_Set_Invalid_ValueType",
		},
	} {
		targetLightningCfg := new(Lightning)
		err := toml.Unmarshal([]byte(tc.TOMLStr), targetLightningCfg)
		if len(tc.ExpectErrStr) > 0 {
			require.Errorf(t, err, "test case: %s", tc.CaseName)
			require.Equalf(t, tc.ExpectErrStr, err.Error(), "test case: %s", tc.CaseName)
		} else {
			require.NoErrorf(t, err, "test case: %s", tc.CaseName)
			require.Equalf(t, tc.ExpectedValues["syntax"], targetLightningCfg.MaxError.Syntax.Load(), "test case: %s", tc.CaseName)
			require.Equalf(t, tc.ExpectedValues["charset"], targetLightningCfg.MaxError.Charset.Load(), "test case: %s", tc.CaseName)
			require.Equalf(t, tc.ExpectedValues["type"], targetLightningCfg.MaxError.Type.Load(), "test case: %s", tc.CaseName)
		}
	}
}

func TestDurationMarshalJSON(t *testing.T) {
	duration := Duration{}
	err := duration.UnmarshalText([]byte("13m20s"))
	require.NoError(t, err)
	require.Equal(t, 13*60+20.0, duration.Duration.Seconds())
	result, err := duration.MarshalJSON()
	require.NoError(t, err)
	require.Equal(t, `"13m20s"`, string(result))
}

func TestDuplicateResolutionAlgorithm(t *testing.T) {
	var dra DuplicateResolutionAlgorithm
	require.NoError(t, dra.FromStringValue(""))
	require.Equal(t, NoneOnDup, dra)
	require.NoError(t, dra.FromStringValue("none"))
	require.Equal(t, NoneOnDup, dra)
	require.NoError(t, dra.FromStringValue("replace"))
	require.Equal(t, ReplaceOnDup, dra)
	require.NoError(t, dra.FromStringValue("ignore"))
	require.Equal(t, IgnoreOnDup, dra)
	require.NoError(t, dra.FromStringValue("error"))
	require.Equal(t, ErrorOnDup, dra)
	require.NoError(t, dra.FromStringValue("remove"))
	require.Equal(t, ReplaceOnDup, dra)
	require.NoError(t, dra.FromStringValue("record"))
	require.Equal(t, ReplaceOnDup, dra)

	require.Equal(t, "", NoneOnDup.String())
	require.Equal(t, "replace", ReplaceOnDup.String())
	require.Equal(t, "ignore", IgnoreOnDup.String())
	require.Equal(t, "error", ErrorOnDup.String())
}

func TestLoadConfig(t *testing.T) {
	cfg, err := LoadGlobalConfig([]string{"-tidb-port", "sss"}, nil)
	require.EqualError(t, err, `[Lightning:Common:ErrInvalidArgument]invalid argument: invalid value "sss" for flag -tidb-port: parse error`)
	require.Nil(t, cfg)

	cfg, err = LoadGlobalConfig([]string{"-V"}, nil)
	require.Equal(t, flag.ErrHelp, err)
	require.Nil(t, cfg)

	cfg, err = LoadGlobalConfig([]string{"-config", "not-exists"}, nil)
	require.Error(t, err)
	require.Regexp(t, ".*(no such file or directory|The system cannot find the file specified).*", err.Error())
	require.Nil(t, cfg)

	cfg, err = LoadGlobalConfig([]string{"--server-mode"}, nil)
	require.EqualError(t, err, "[Lightning:Config:ErrInvalidConfig]If server-mode is enabled, the status-addr must be a valid listen address")
	require.Nil(t, cfg)

	path, _ := filepath.Abs(".")
	cfg, err = LoadGlobalConfig([]string{
		"-L", "debug",
		"-log-file", "/path/to/file.log",
		"-tidb-host", "172.16.30.11",
		"-tidb-port", "4001",
		"-tidb-user", "guest",
		"-tidb-password", "12345",
		"-pd-urls", "172.16.30.11:2379,172.16.30.12:2379",
		"-d", path,
		"-backend", BackendLocal,
		"-sorted-kv-dir", ".",
		"-checksum=false",
	}, nil)
	require.NoError(t, err)
	require.Equal(t, "debug", cfg.App.Level)
	require.Equal(t, "/path/to/file.log", cfg.App.File)
	require.Equal(t, "172.16.30.11", cfg.TiDB.Host)
	require.Equal(t, 4001, cfg.TiDB.Port)
	require.Equal(t, "guest", cfg.TiDB.User)
	require.Equal(t, "12345", cfg.TiDB.Psw)
	require.Equal(t, "172.16.30.11:2379,172.16.30.12:2379", cfg.TiDB.PdAddr)
	require.Equal(t, path, cfg.Mydumper.SourceDir)
	require.Equal(t, BackendLocal, cfg.TikvImporter.Backend)
	require.Equal(t, ".", cfg.TikvImporter.SortedKVDir)
	require.Equal(t, OpLevelOff, cfg.PostRestore.Checksum)
	require.Equal(t, OpLevelOptional, cfg.PostRestore.Analyze)

	taskCfg := NewConfig()
	err = taskCfg.LoadFromGlobal(cfg)
	require.NoError(t, err)
	require.Equal(t, OpLevelOff, taskCfg.PostRestore.Checksum)
	require.Equal(t, OpLevelOptional, taskCfg.PostRestore.Analyze)

	taskCfg.Checkpoint.DSN = ""
	taskCfg.Checkpoint.Driver = CheckpointDriverMySQL
	taskCfg.TiDB.DistSQLScanConcurrency = 1
	err = taskCfg.Adjust(context.Background())
	require.NoError(t, err)
	equivalentDSN := taskCfg.Checkpoint.MySQLParam.ToDriverConfig().FormatDSN()
	expectedDSN := "guest:12345@tcp(172.16.30.11:4001)/?charset=utf8mb4&sql_mode=%27ONLY_FULL_GROUP_BY%2CSTRICT_TRANS_TABLES%2CNO_ZERO_IN_DATE%2CNO_ZERO_DATE%2CERROR_FOR_DIVISION_BY_ZERO%2CNO_AUTO_CREATE_USER%2CNO_ENGINE_SUBSTITUTION%27"
	require.Equal(t, expectedDSN, equivalentDSN)

	result := taskCfg.String()
	require.Regexp(t, `.*"pd-addr":"172.16.30.11:2379,172.16.30.12:2379".*`, result)

	cfg, err = LoadGlobalConfig([]string{}, nil)
	require.NoError(t, err)
	require.Regexp(t, ".*lightning.log.*", cfg.App.File)
	cfg, err = LoadGlobalConfig([]string{"--log-file", "-"}, nil)
	require.NoError(t, err)
	require.Equal(t, "-", cfg.App.File)
}

func TestDefaultImporterBackendValue(t *testing.T) {
	cfg := NewConfig()
	assignMinimalLegalValue(cfg)
	cfg.TikvImporter.Backend = "local"
	cfg.TiDB.DistSQLScanConcurrency = 1
	err := cfg.Adjust(context.Background())
	require.NoError(t, err)
	require.Equal(t, 2, cfg.App.IndexConcurrency)
	require.Equal(t, 6, cfg.App.TableConcurrency)
	require.Equal(t, 4096, cfg.TikvImporter.RegionSplitBatchSize)
}

func TestDefaultTidbBackendValue(t *testing.T) {
	cfg := NewConfig()
	assignMinimalLegalValue(cfg)
	cfg.TikvImporter.Backend = "tidb"
	cfg.App.RegionConcurrency = 123
	cfg.TiDB.DistSQLScanConcurrency = 1
	err := cfg.Adjust(context.Background())
	require.NoError(t, err)
	require.Equal(t, 123, cfg.App.TableConcurrency)
}

func TestDefaultCouldBeOverwritten(t *testing.T) {
	ctx := context.Background()
	cfg := NewConfig()
	assignMinimalLegalValue(cfg)
	cfg.TikvImporter.Backend = "local"
	cfg.App.IndexConcurrency = 20
	cfg.App.TableConcurrency = 60
	cfg.TiDB.DistSQLScanConcurrency = 1
	err := cfg.Adjust(ctx)
	require.NoError(t, err)
	require.Equal(t, 20, cfg.App.IndexConcurrency)
	require.Equal(t, 60, cfg.App.TableConcurrency)

	require.Equal(t, 32768, cfg.TikvImporter.SendKVPairs)
	require.Equal(t, ByteSize(KVWriteBatchSize), cfg.TikvImporter.SendKVSize)

	cfg.TikvImporter.RegionSplitConcurrency = 1
	// backoff can be 0
	cfg.TikvImporter.RegionCheckBackoffLimit = 0
	err = cfg.Adjust(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, cfg.TikvImporter.RegionSplitConcurrency)
	require.Equal(t, 0, cfg.TikvImporter.RegionCheckBackoffLimit)
	cfg.TikvImporter.RegionSplitBatchSize = 0
	err = cfg.Adjust(ctx)
	require.ErrorContains(t, err, "`tikv-importer.region-split-batch-size` got 0, should be larger than 0")
}

func TestLoadFromInvalidConfig(t *testing.T) {
	taskCfg := NewConfig()
	err := taskCfg.LoadFromGlobal(&GlobalConfig{
		ConfigFileContent: []byte("invalid toml"),
	})
	require.Error(t, err)
	require.Regexp(t, "line 1.*", err.Error())
}

func TestTomlPostRestore(t *testing.T) {
	cfg := &Config{}
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

	kvMap := map[string]PostOpLevel{
		`"off"`:      OpLevelOff,
		`"required"`: OpLevelRequired,
		`"optional"`: OpLevelOptional,
		"true":       OpLevelRequired,
		"false":      OpLevelOff,
	}

	var b bytes.Buffer
	enc := toml.NewEncoder(&b)

	for k, v := range kvMap {
		cfg := &Config{}
		confStr := fmt.Sprintf("[post-restore]\r\nchecksum= %s\r\n", k)
		err := cfg.LoadFromTOML([]byte(confStr))
		require.NoError(t, err)
		require.Equal(t, v, cfg.PostRestore.Checksum)

		b.Reset()
		require.NoError(t, enc.Encode(cfg.PostRestore))
		require.Regexp(t, fmt.Sprintf(`(?s).*checksum = "\Q%s\E".*`, v), &b)
	}

	for k, v := range kvMap {
		cfg := &Config{}
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
	cfg := &Config{}
	cfg.Cron.SwitchMode.Duration = 1 * time.Minute
	cfg.Cron.LogProgress.Duration = 2 * time.Minute
	cfg.Cron.CheckDiskQuota.Duration = 3 * time.Second
	var b bytes.Buffer
	require.NoError(t, toml.NewEncoder(&b).Encode(cfg.Cron))
	require.Equal(t, "switch-mode = \"1m0s\"\nlog-progress = \"2m0s\"\ncheck-disk-quota = \"3s\"\n", b.String())

	confStr := "[cron]\r\n" + b.String()
	cfg2 := &Config{}
	require.NoError(t, cfg2.LoadFromTOML([]byte(confStr)))
	require.Equal(t, cfg.Cron, cfg2.Cron)
}

func TestAdjustDiskQuota(t *testing.T) {
	cfg := NewConfig()
	assignMinimalLegalValue(cfg)

	base := t.TempDir()
	ctx := context.Background()
	cfg.TikvImporter.Backend = BackendLocal
	cfg.TikvImporter.DiskQuota = 0
	cfg.TikvImporter.SortedKVDir = base
	cfg.TiDB.DistSQLScanConcurrency = 1
	require.NoError(t, cfg.Adjust(ctx))
	require.Equal(t, int64(0), int64(cfg.TikvImporter.DiskQuota))
}

func TestAdjustConflictStrategy(t *testing.T) {
	cfg := NewConfig()
	assignMinimalLegalValue(cfg)
	ctx := context.Background()

	cfg.TikvImporter.Backend = BackendTiDB
	cfg.Conflict.Strategy = NoneOnDup
	require.NoError(t, cfg.Adjust(ctx))
	require.Equal(t, ErrorOnDup, cfg.Conflict.Strategy)

	cfg.TikvImporter.Backend = BackendLocal
	cfg.Conflict.Strategy = NoneOnDup
	require.NoError(t, cfg.Adjust(ctx))
	require.Empty(t, cfg.Conflict.Strategy)

	cfg.TikvImporter.Backend = BackendLocal
	cfg.Conflict.Strategy = ReplaceOnDup
	require.NoError(t, cfg.Adjust(ctx))
	require.Equal(t, ReplaceOnDup, cfg.Conflict.Strategy)

	cfg.TikvImporter.Backend = BackendLocal
	cfg.Conflict.Strategy = ReplaceOnDup
	cfg.TikvImporter.ParallelImport = true
	cfg.Conflict.PrecheckConflictBeforeImport = true
	require.NoError(t, cfg.Adjust(ctx))

	cfg.TikvImporter.Backend = BackendLocal
	cfg.Conflict.Strategy = ReplaceOnDup
	cfg.TikvImporter.ParallelImport = true
	cfg.Conflict.PrecheckConflictBeforeImport = false
	require.NoError(t, cfg.Adjust(ctx))

	cfg.TikvImporter.Backend = BackendTiDB
	cfg.Conflict.Strategy = IgnoreOnDup
	require.NoError(t, cfg.Adjust(ctx))

	cfg.TikvImporter.Backend = BackendLocal
	cfg.Conflict.Strategy = ReplaceOnDup
	cfg.TikvImporter.ParallelImport = false
	cfg.TikvImporter.DuplicateResolution = ReplaceOnDup
	require.ErrorContains(t, cfg.Adjust(ctx), `conflict.strategy cannot be used with tikv-importer.duplicate-resolution`)

	cfg.TikvImporter.Backend = BackendLocal
	cfg.Conflict.Strategy = NoneOnDup
	cfg.TikvImporter.OnDuplicate = ReplaceOnDup
	cfg.TikvImporter.ParallelImport = false
	cfg.TikvImporter.DuplicateResolution = ReplaceOnDup
	require.ErrorContains(t, cfg.Adjust(ctx), `tikv-importer.on-duplicate cannot be used with tikv-importer.duplicate-resolution`)

	cfg.TikvImporter.Backend = BackendLocal
	cfg.Conflict.Strategy = IgnoreOnDup
	cfg.TikvImporter.DuplicateResolution = NoneOnDup
	require.ErrorContains(t, cfg.Adjust(ctx), `conflict.strategy cannot be set to "ignore" when use tikv-importer.backend = "local"`)

	cfg.TikvImporter.Backend = BackendTiDB
	cfg.Conflict.Strategy = IgnoreOnDup
	cfg.Conflict.PrecheckConflictBeforeImport = true
	require.ErrorContains(t, cfg.Adjust(ctx), `conflict.precheck-conflict-before-import cannot be set to true when use tikv-importer.backend = "tidb"`)

	cfg.TikvImporter.Backend = BackendLocal
	cfg.Conflict.Strategy = NoneOnDup
	cfg.TikvImporter.ParallelImport = false
	cfg.TikvImporter.DuplicateResolution = ReplaceOnDup
	cfg.TikvImporter.OnDuplicate = NoneOnDup
	require.NoError(t, cfg.Adjust(ctx))
	require.Equal(t, ReplaceOnDup, cfg.Conflict.Strategy)
}

func TestAdjustMaxRecordRows(t *testing.T) {
	ctx := context.Background()

	cfg := NewConfig()
	assignMinimalLegalValue(cfg)

	cfg.Conflict.MaxRecordRows = -1
	cfg.Conflict.Strategy = ReplaceOnDup
	require.NoError(t, cfg.Adjust(ctx))
	require.EqualValues(t, 10000, cfg.Conflict.MaxRecordRows)

	cfg.Conflict.MaxRecordRows = -1
	cfg.Conflict.Threshold = 9999
	require.NoError(t, cfg.Adjust(ctx))
	require.EqualValues(t, 9999, cfg.Conflict.MaxRecordRows)

	cfg.Conflict.MaxRecordRows = 1000
	cfg.Conflict.Threshold = 100
	require.NoError(t, cfg.Adjust(ctx))
	require.EqualValues(t, 100, cfg.Conflict.MaxRecordRows)
}

func TestRemoveAllowAllFiles(t *testing.T) {
	cfg := NewConfig()
	assignMinimalLegalValue(cfg)
	ctx := context.Background()

	cfg.Checkpoint.Driver = CheckpointDriverMySQL
	cfg.Checkpoint.DSN = "guest:12345@tcp(172.16.30.11:4001)/?tls=false&allowAllFiles=true&charset=utf8mb4"
	require.NoError(t, cfg.Adjust(ctx))
	require.Equal(t, "guest:12345@tcp(172.16.30.11:4001)/?tls=false&charset=utf8mb4", cfg.Checkpoint.DSN)
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
		cfg := NewConfig()
		cfg.Mydumper.SourceDir = "file://."
		cfg.TiDB.Port = 4000
		cfg.TiDB.PdAddr = "test.invalid:2379"
		cfg.TikvImporter.Backend = BackendLocal
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
	tomlCases := map[any]CheckpointKeepStrategy{
		true:     CheckpointRename,
		false:    CheckpointRemove,
		"remove": CheckpointRemove,
		"rename": CheckpointRename,
		"origin": CheckpointOrigin,
	}
	var cp CheckpointKeepStrategy
	for key, strategy := range tomlCases {
		err := cp.UnmarshalTOML(key)
		require.NoError(t, err)
		require.Equal(t, strategy, cp)
	}

	defaultCp := "enable = true\r\n"
	cpCfg := &Checkpoint{}
	_, err := toml.Decode(defaultCp, cpCfg)
	require.NoError(t, err)
	require.Equal(t, CheckpointRemove, cpCfg.KeepAfterSuccess)

	cpFmt := "keep-after-success = %v\r\n"
	for key, strategy := range tomlCases {
		cpValue := key
		if strVal, ok := key.(string); ok {
			cpValue = `"` + strVal + `"`
		}
		tomlStr := fmt.Sprintf(cpFmt, cpValue)
		cpCfg := &Checkpoint{}
		_, err := toml.Decode(tomlStr, cpCfg)
		require.NoError(t, err)
		require.Equal(t, strategy, cpCfg.KeepAfterSuccess)
	}

	marshalTextCases := map[CheckpointKeepStrategy]string{
		CheckpointRemove: "remove",
		CheckpointRename: "rename",
		CheckpointOrigin: "origin",
	}
	for strategy, value := range marshalTextCases {
		res, err := strategy.MarshalText()
		require.NoError(t, err)
		require.Equal(t, []byte(value), res)
	}
}

func TestLoadCharsetFromConfig(t *testing.T) {
	cases := map[string]Charset{
		"binary":  Binary,
		"BINARY":  Binary,
		"GBK":     GBK,
		"gbk":     GBK,
		"Gbk":     GBK,
		"gB18030": GB18030,
		"GB18030": GB18030,
	}
	for k, v := range cases {
		charset, err := ParseCharset(k)
		require.NoError(t, err)
		require.Equal(t, v, charset)
	}

	_, err := ParseCharset("Unknown")
	require.EqualError(t, err, "found unsupported data-character-set: Unknown")
}

func TestAdjustTikvImporter(t *testing.T) {
	cfg := NewConfig()
	assignMinimalLegalValue(cfg)

	cfg.TikvImporter.Backend = BackendLocal
	cfg.TikvImporter.SortedKVDir = ""
	require.EqualError(t, cfg.TikvImporter.adjust(), "[Lightning:Config:ErrInvalidConfig]tikv-importer.sorted-kv-dir must not be empty!")

	// non exists dir is legal
	cfg.TikvImporter.SortedKVDir = "./not-exists"
	require.NoError(t, cfg.TikvImporter.adjust())

	base := t.TempDir()
	// create empty file
	file := filepath.Join(base, "file")
	require.NoError(t, os.WriteFile(file, []byte(""), 0644))
	cfg.TikvImporter.SortedKVDir = file
	err := cfg.TikvImporter.adjust()
	require.Error(t, err)
	require.Regexp(t, "tikv-importer.sorted-kv-dir (.*) is not a directory", err.Error())

	// legal dir
	cfg.TikvImporter.SortedKVDir = base
	require.NoError(t, cfg.TikvImporter.adjust())

	cfg.TikvImporter.ParallelImport = true
	cfg.TikvImporter.AddIndexBySQL = true
	require.ErrorContains(t, cfg.TikvImporter.adjust(), "tikv-importer.add-index-using-ddl cannot be used with tikv-importer.parallel-import")
}

func TestCreateSeveralConfigsWithDifferentFilters(t *testing.T) {
	originalDefaultCfg := append([]string{}, GetDefaultFilter()...)
	cfg1 := NewConfig()
	require.NoError(t, cfg1.LoadFromTOML([]byte(`
		[mydumper]
		filter = ["db1.tbl1", "db2.*", "!db2.tbl1"]
	`)))
	require.Equal(t, []string{"db1.tbl1", "db2.*", "!db2.tbl1"}, cfg1.Mydumper.Filter)
	require.Equal(t, GetDefaultFilter(), originalDefaultCfg)

	cfg2 := NewConfig()
	require.Equal(t, originalDefaultCfg, cfg2.Mydumper.Filter)
	require.Equal(t, GetDefaultFilter(), originalDefaultCfg)

	gCfg1, err := LoadGlobalConfig([]string{"-f", "db1.tbl1", "-f", "db2.*", "-f", "!db2.tbl1"}, nil)
	require.NoError(t, err)
	require.Equal(t, []string{"db1.tbl1", "db2.*", "!db2.tbl1"}, gCfg1.Mydumper.Filter)
	require.Equal(t, GetDefaultFilter(), originalDefaultCfg)

	gCfg2, err := LoadGlobalConfig([]string{}, nil)
	require.NoError(t, err)
	require.Equal(t, originalDefaultCfg, gCfg2.Mydumper.Filter)
	require.Equal(t, GetDefaultFilter(), originalDefaultCfg)
}

func TestCompressionType(t *testing.T) {
	var ct CompressionType
	require.NoError(t, ct.FromStringValue(""))
	require.Equal(t, CompressionNone, ct)
	require.NoError(t, ct.FromStringValue("gzip"))
	require.Equal(t, CompressionGzip, ct)
	require.NoError(t, ct.FromStringValue("gz"))
	require.Equal(t, CompressionGzip, ct)
	require.EqualError(t, ct.FromStringValue("zstd"), "invalid compression-type 'zstd', please choose valid option between ['gzip']")

	require.Equal(t, "", CompressionNone.String())
	require.Equal(t, "gzip", CompressionGzip.String())
}

func TestAdjustConflict(t *testing.T) {
	cfg := NewConfig()
	assignMinimalLegalValue(cfg)
	var dra DuplicateResolutionAlgorithm

	require.NoError(t, dra.FromStringValue("REPLACE"))
	cfg.Conflict.Strategy = dra
	require.NoError(t, cfg.Conflict.adjust(&cfg.TikvImporter))
	require.EqualValues(t, 10000, cfg.Conflict.Threshold)

	require.NoError(t, dra.FromStringValue("IGNORE"))
	cfg.Conflict.Strategy = dra
	require.ErrorContains(t, cfg.Conflict.adjust(&cfg.TikvImporter), `conflict.strategy cannot be set to "ignore" when use tikv-importer.backend = "local"`)

	cfg.Conflict.Strategy = ErrorOnDup
	cfg.Conflict.Threshold = 1
	require.ErrorContains(t, cfg.Conflict.adjust(&cfg.TikvImporter), `conflict.threshold cannot be set when use conflict.strategy = "error"`)

	cfg.TikvImporter.Backend = BackendTiDB
	cfg.Conflict.Strategy = ReplaceOnDup
	cfg.Conflict.MaxRecordRows = -1
	require.NoError(t, cfg.Conflict.adjust(&cfg.TikvImporter))
	require.EqualValues(t, 0, cfg.Conflict.MaxRecordRows)

	cfg.TikvImporter.Backend = BackendLocal
	cfg.Conflict.Threshold = 1
	cfg.Conflict.MaxRecordRows = 1
	require.NoError(t, cfg.Conflict.adjust(&cfg.TikvImporter))
	cfg.Conflict.MaxRecordRows = 2
	require.NoError(t, cfg.Conflict.adjust(&cfg.TikvImporter))
	require.EqualValues(t, 1, cfg.Conflict.MaxRecordRows)

	cfg.TikvImporter.Backend = BackendTiDB
	cfg.Conflict.Strategy = ReplaceOnDup
	cfg.Conflict.Threshold = 1
	cfg.Conflict.MaxRecordRows = 1
	require.NoError(t, cfg.Conflict.adjust(&cfg.TikvImporter))
	require.EqualValues(t, 0, cfg.Conflict.MaxRecordRows)
}

func TestAdjustBlockSize(t *testing.T) {
	ts, host, port := startMockServer(t, http.StatusOK,
		`{"port":6666,"advertise-address":"121.212.121.212:5555","path":"34.34.34.34:3434"}`,
	)
	defer ts.Close()

	cfg := NewConfig()
	cfg.TiDB.Host = host
	cfg.TiDB.StatusPort = port
	cfg.TikvImporter.Backend = BackendLocal
	cfg.TikvImporter.SortedKVDir = "."
	cfg.TiDB.DistSQLScanConcurrency = 1
	cfg.Mydumper.SourceDir = "."
	cfg.TikvImporter.BlockSize = 0

	err := cfg.Adjust(context.Background())
	require.NoError(t, err)
	require.EqualValues(t, 16384, cfg.TikvImporter.BlockSize)
}
