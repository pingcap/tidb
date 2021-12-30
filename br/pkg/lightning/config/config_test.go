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
	"path/filepath"
	"regexp"
	"strconv"
	"testing"
	"time"

	"github.com/BurntSushi/toml"
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/br/pkg/lightning/config"
	"github.com/pingcap/tidb/parser/mysql"
)

func Test(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&configTestSuite{})

type configTestSuite struct{}

func startMockServer(c *C, statusCode int, content string) (*httptest.Server, string, int) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(statusCode)
		fmt.Fprint(w, content)
	}))

	url, err := url.Parse(ts.URL)
	c.Assert(err, IsNil)
	host, portString, err := net.SplitHostPort(url.Host)
	c.Assert(err, IsNil)
	port, err := strconv.Atoi(portString)
	c.Assert(err, IsNil)

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

func (s *configTestSuite) TestAdjustPdAddrAndPort(c *C) {
	ts, host, port := startMockServer(c, http.StatusOK,
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
	c.Assert(err, IsNil)
	c.Assert(cfg.TiDB.Port, Equals, 4444)
	c.Assert(cfg.TiDB.PdAddr, Equals, "123.45.67.89:1234")
}

func (s *configTestSuite) TestAdjustPdAddrAndPortViaAdvertiseAddr(c *C) {
	ts, host, port := startMockServer(c, http.StatusOK,
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
	c.Assert(err, IsNil)
	c.Assert(cfg.TiDB.Port, Equals, 6666)
	c.Assert(cfg.TiDB.PdAddr, Equals, "34.34.34.34:3434")
}

func (s *configTestSuite) TestAdjustPageNotFound(c *C) {
	ts, host, port := startMockServer(c, http.StatusNotFound, "{}")
	defer ts.Close()

	cfg := config.NewConfig()
	cfg.TiDB.Host = host
	cfg.TiDB.StatusPort = port
	cfg.TikvImporter.Backend = config.BackendLocal
	cfg.TikvImporter.SortedKVDir = "."
	cfg.TiDB.DistSQLScanConcurrency = 1

	err := cfg.Adjust(context.Background())
	c.Assert(err, ErrorMatches, "cannot fetch settings from TiDB.*")
}

func (s *configTestSuite) TestAdjustConnectRefused(c *C) {
	ts, host, port := startMockServer(c, http.StatusOK, "{}")

	cfg := config.NewConfig()
	cfg.TiDB.Host = host
	cfg.TiDB.StatusPort = port
	cfg.TikvImporter.Backend = config.BackendLocal
	cfg.TikvImporter.SortedKVDir = "."
	cfg.TiDB.DistSQLScanConcurrency = 1

	ts.Close() // immediately close to ensure connection refused.

	err := cfg.Adjust(context.Background())
	c.Assert(err, ErrorMatches, "cannot fetch settings from TiDB.*")
}

func (s *configTestSuite) TestAdjustBackendNotSet(c *C) {
	cfg := config.NewConfig()
	cfg.TiDB.DistSQLScanConcurrency = 1
	err := cfg.Adjust(context.Background())
	c.Assert(err, ErrorMatches, "tikv-importer.backend must not be empty!")
}

func (s *configTestSuite) TestAdjustInvalidBackend(c *C) {
	cfg := config.NewConfig()
	cfg.TikvImporter.Backend = "no_such_backend"
	cfg.TiDB.DistSQLScanConcurrency = 1
	err := cfg.Adjust(context.Background())
	c.Assert(err, ErrorMatches, "invalid config: unsupported `tikv-importer\\.backend` \\(no_such_backend\\)")
}

func (s *configTestSuite) TestCheckAndAdjustFilePath(c *C) {
	tmpDir := c.MkDir()
	// use slashPath in url to be compatible with windows
	slashPath := filepath.ToSlash(tmpDir)

	cfg := config.NewConfig()
	cases := []string{
		tmpDir,
		".",
		"file://" + slashPath,
		"local://" + slashPath,
		"s3://bucket_name",
		"s3://bucket_name/path/to/dir",
		"gcs://bucketname/path/to/dir",
		"gs://bucketname/path/to/dir",
		"noop:///",
	}

	for _, testCase := range cases {
		cfg.Mydumper.SourceDir = testCase

		err := cfg.CheckAndAdjustFilePath()
		c.Assert(err, IsNil)
	}

}

func (s *configTestSuite) TestAdjustFileRoutePath(c *C) {
	cfg := config.NewConfig()
	assignMinimalLegalValue(cfg)

	ctx := context.Background()
	tmpDir := c.MkDir()
	cfg.Mydumper.SourceDir = tmpDir
	invalidPath := filepath.Join(tmpDir, "../test123/1.sql")
	rule := &config.FileRouteRule{Path: invalidPath, Type: "sql", Schema: "test", Table: "tbl"}
	cfg.Mydumper.FileRouters = []*config.FileRouteRule{rule}
	cfg.TiDB.DistSQLScanConcurrency = 1
	err := cfg.Adjust(ctx)
	c.Assert(err, ErrorMatches, fmt.Sprintf("\\Qfile route path '%s' is not in source dir '%s'\\E", invalidPath, tmpDir))

	relPath := filepath.FromSlash("test_dir/1.sql")
	rule.Path = filepath.Join(tmpDir, relPath)
	err = cfg.Adjust(ctx)
	c.Assert(err, IsNil)
	c.Assert(cfg.Mydumper.FileRouters[0].Path, Equals, relPath)
}

func (s *configTestSuite) TestDecodeError(c *C) {
	ts, host, port := startMockServer(c, http.StatusOK, "invalid-string")
	defer ts.Close()

	cfg := config.NewConfig()
	cfg.TiDB.Host = host
	cfg.TiDB.StatusPort = port
	cfg.TikvImporter.Backend = config.BackendLocal
	cfg.TikvImporter.SortedKVDir = "."
	cfg.TiDB.DistSQLScanConcurrency = 1

	err := cfg.Adjust(context.Background())
	c.Assert(err, ErrorMatches, "cannot fetch settings from TiDB.*")
}

func (s *configTestSuite) TestInvalidSetting(c *C) {
	ts, host, port := startMockServer(c, http.StatusOK, `{"port": 0}`)
	defer ts.Close()

	cfg := config.NewConfig()
	cfg.TiDB.Host = host
	cfg.TiDB.StatusPort = port
	cfg.TikvImporter.Backend = config.BackendLocal
	cfg.TikvImporter.SortedKVDir = "."
	cfg.TiDB.DistSQLScanConcurrency = 1

	err := cfg.Adjust(context.Background())
	c.Assert(err, ErrorMatches, "invalid `tidb.port` setting")
}

func (s *configTestSuite) TestInvalidPDAddr(c *C) {
	ts, host, port := startMockServer(c, http.StatusOK, `{"port": 1234, "path": ",,"}`)
	defer ts.Close()

	cfg := config.NewConfig()
	cfg.TiDB.Host = host
	cfg.TiDB.StatusPort = port
	cfg.TikvImporter.Backend = config.BackendLocal
	cfg.TikvImporter.SortedKVDir = "."
	cfg.TiDB.DistSQLScanConcurrency = 1

	err := cfg.Adjust(context.Background())
	c.Assert(err, ErrorMatches, "invalid `tidb.pd-addr` setting")
}

func (s *configTestSuite) TestAdjustWillNotContactServerIfEverythingIsDefined(c *C) {
	cfg := config.NewConfig()
	assignMinimalLegalValue(cfg)
	cfg.TiDB.DistSQLScanConcurrency = 1

	err := cfg.Adjust(context.Background())
	c.Assert(err, IsNil)
	c.Assert(cfg.TiDB.Port, Equals, 4567)
	c.Assert(cfg.TiDB.PdAddr, Equals, "234.56.78.90:12345")
}

func (s *configTestSuite) TestAdjustWillBatchImportRatioInvalid(c *C) {
	cfg := config.NewConfig()
	assignMinimalLegalValue(cfg)
	cfg.Mydumper.BatchImportRatio = -1
	cfg.TiDB.DistSQLScanConcurrency = 1
	err := cfg.Adjust(context.Background())
	c.Assert(err, IsNil)
	c.Assert(cfg.Mydumper.BatchImportRatio, Equals, 0.75)
}

func (s *configTestSuite) TestAdjustSecuritySection(c *C) {
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
		comment := Commentf("input = %s", tc.input)

		cfg := config.NewConfig()
		assignMinimalLegalValue(cfg)
		cfg.TiDB.DistSQLScanConcurrency = 1
		err := cfg.LoadFromTOML([]byte(tc.input))
		c.Assert(err, IsNil, comment)

		err = cfg.Adjust(context.Background())
		c.Assert(err, IsNil, comment)
		c.Assert(cfg.TiDB.Security.CAPath, Equals, tc.expectedCA, comment)
		c.Assert(cfg.TiDB.TLS, Equals, tc.expectedTLS, comment)
	}
}

func (s *configTestSuite) TestInvalidCSV(c *C) {
	testCases := []struct {
		input string
		err   string
	}{
		{
			input: `
				[mydumper.csv]
				separator = ''
			`,
			err: "invalid config: `mydumper.csv.separator` must not be empty",
		},
		{
			input: `
				[mydumper.csv]
				separator = 'hello'
				delimiter = 'hel'
			`,
			err: "invalid config: `mydumper.csv.separator` and `mydumper.csv.delimiter` must not be prefix of each other",
		},
		{
			input: `
				[mydumper.csv]
				separator = 'hel'
				delimiter = 'hello'
			`,
			err: "invalid config: `mydumper.csv.separator` and `mydumper.csv.delimiter` must not be prefix of each other",
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
			err: "invalid config: `mydumper.csv.separator` and `mydumper.csv.delimiter` must not be prefix of each other",
		},
		{
			input: `
				[mydumper.csv]
				separator = '\'
				backslash-escape = true
			`,
			err: "invalid config: cannot use '\\' as CSV separator when `mydumper.csv.backslash-escape` is true",
		},
		{
			input: `
				[mydumper.csv]
				delimiter = '\'
				backslash-escape = true
			`,
			err: "invalid config: cannot use '\\' as CSV delimiter when `mydumper.csv.backslash-escape` is true",
		},
		{
			input: `
				[tidb]
				sql-mode = "invalid-sql-mode"
			`,
			err: "invalid config: `mydumper.tidb.sql_mode` must be a valid SQL_MODE: ERROR 1231 (42000): Variable 'sql_mode' can't be set to the value of 'invalid-sql-mode'",
		},
		{
			input: `
				[[routes]]
				schema-pattern = ""
				table-pattern = "shard_table_*"
			`,
			err: "schema pattern of table route rule should not be empty",
		},
		{
			input: `
				[[routes]]
				schema-pattern = "schema_*"
				table-pattern = ""
			`,
			err: "target schema of table route rule should not be empty",
		},
	}

	for _, tc := range testCases {
		comment := Commentf("input = %s", tc.input)

		cfg := config.NewConfig()
		cfg.Mydumper.SourceDir = "file://."
		cfg.TiDB.Port = 4000
		cfg.TiDB.PdAddr = "test.invalid:2379"
		cfg.TikvImporter.Backend = config.BackendLocal
		cfg.TikvImporter.SortedKVDir = "."
		cfg.TiDB.DistSQLScanConcurrency = 1
		err := cfg.LoadFromTOML([]byte(tc.input))
		c.Assert(err, IsNil)

		err = cfg.Adjust(context.Background())
		if tc.err != "" {
			c.Assert(err, ErrorMatches, regexp.QuoteMeta(tc.err), comment)
		} else {
			c.Assert(err, IsNil, comment)
		}
	}
}

func (s *configTestSuite) TestInvalidTOML(c *C) {
	cfg := &config.Config{}
	err := cfg.LoadFromTOML([]byte(`
		invalid[mydumper.csv]
		delimiter = '\'
		backslash-escape = true
	`))
	c.Assert(err, ErrorMatches, regexp.QuoteMeta("Near line 0 (last key parsed ''): bare keys cannot contain '['"))
}

func (s *configTestSuite) TestTOMLUnusedKeys(c *C) {
	cfg := &config.Config{}
	err := cfg.LoadFromTOML([]byte(`
		[lightning]
		typo = 123
	`))
	c.Assert(err, ErrorMatches, regexp.QuoteMeta("config file contained unknown configuration options: lightning.typo"))
}

func (s *configTestSuite) TestDurationUnmarshal(c *C) {
	duration := config.Duration{}
	err := duration.UnmarshalText([]byte("13m20s"))
	c.Assert(err, IsNil)
	c.Assert(duration.Duration.Seconds(), Equals, 13*60+20.0)
	err = duration.UnmarshalText([]byte("13x20s"))
	c.Assert(err, ErrorMatches, "time: unknown unit .?x.? in duration .?13x20s.?")
}

func (s *configTestSuite) TestDurationMarshalJSON(c *C) {
	duration := config.Duration{}
	err := duration.UnmarshalText([]byte("13m20s"))
	c.Assert(err, IsNil)
	c.Assert(duration.Duration.Seconds(), Equals, 13*60+20.0)
	result, err := duration.MarshalJSON()
	c.Assert(err, IsNil)
	c.Assert(string(result), Equals, `"13m20s"`)
}

func (s *configTestSuite) TestDuplicateResolutionAlgorithm(c *C) {
	var dra config.DuplicateResolutionAlgorithm
	dra.FromStringValue("record")
	c.Assert(dra, Equals, config.DupeResAlgRecord)
	dra.FromStringValue("none")
	c.Assert(dra, Equals, config.DupeResAlgNone)
	dra.FromStringValue("remove")
	c.Assert(dra, Equals, config.DupeResAlgRemove)

	c.Assert(config.DupeResAlgRecord.String(), Equals, "record")
	c.Assert(config.DupeResAlgNone.String(), Equals, "none")
	c.Assert(config.DupeResAlgRemove.String(), Equals, "remove")
}

func (s *configTestSuite) TestLoadConfig(c *C) {
	cfg, err := config.LoadGlobalConfig([]string{"-tidb-port", "sss"}, nil)
	c.Assert(err, ErrorMatches, `invalid value "sss" for flag -tidb-port: parse error`)
	c.Assert(cfg, IsNil)

	cfg, err = config.LoadGlobalConfig([]string{"-V"}, nil)
	c.Assert(err, Equals, flag.ErrHelp)
	c.Assert(cfg, IsNil)

	cfg, err = config.LoadGlobalConfig([]string{"-config", "not-exists"}, nil)
	c.Assert(err, ErrorMatches, ".*(no such file or directory|The system cannot find the file specified).*")
	c.Assert(cfg, IsNil)

	cfg, err = config.LoadGlobalConfig([]string{"--server-mode"}, nil)
	c.Assert(err, ErrorMatches, "If server-mode is enabled, the status-addr must be a valid listen address")
	c.Assert(cfg, IsNil)

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
	c.Assert(err, IsNil)
	c.Assert(cfg.App.Config.Level, Equals, "debug")
	c.Assert(cfg.App.Config.File, Equals, "/path/to/file.log")
	c.Assert(cfg.TiDB.Host, Equals, "172.16.30.11")
	c.Assert(cfg.TiDB.Port, Equals, 4001)
	c.Assert(cfg.TiDB.User, Equals, "guest")
	c.Assert(cfg.TiDB.Psw, Equals, "12345")
	c.Assert(cfg.TiDB.PdAddr, Equals, "172.16.30.11:2379,172.16.30.12:2379")
	c.Assert(cfg.Mydumper.SourceDir, Equals, path)
	c.Assert(cfg.TikvImporter.Backend, Equals, config.BackendLocal)
	c.Assert(cfg.TikvImporter.SortedKVDir, Equals, ".")
	c.Assert(cfg.PostRestore.Checksum, Equals, config.OpLevelOff)
	c.Assert(cfg.PostRestore.Analyze, Equals, config.OpLevelOptional)

	taskCfg := config.NewConfig()
	err = taskCfg.LoadFromGlobal(cfg)
	c.Assert(err, IsNil)
	c.Assert(taskCfg.PostRestore.Checksum, Equals, config.OpLevelOff)
	c.Assert(taskCfg.PostRestore.Analyze, Equals, config.OpLevelOptional)

	taskCfg.Checkpoint.DSN = ""
	taskCfg.Checkpoint.Driver = config.CheckpointDriverMySQL
	taskCfg.TiDB.DistSQLScanConcurrency = 1
	err = taskCfg.Adjust(context.Background())
	c.Assert(err, IsNil)
	c.Assert(taskCfg.Checkpoint.DSN, Equals, "guest:12345@tcp(172.16.30.11:4001)/?charset=utf8mb4&sql_mode='"+mysql.DefaultSQLMode+"'&maxAllowedPacket=67108864&tls=false")

	result := taskCfg.String()
	c.Assert(result, Matches, `.*"pd-addr":"172.16.30.11:2379,172.16.30.12:2379".*`)
}

func (s *configTestSuite) TestDefaultImporterBackendValue(c *C) {
	cfg := config.NewConfig()
	assignMinimalLegalValue(cfg)
	cfg.TikvImporter.Backend = "importer"
	cfg.TiDB.DistSQLScanConcurrency = 1
	err := cfg.Adjust(context.Background())
	c.Assert(err, IsNil)
	c.Assert(cfg.App.IndexConcurrency, Equals, 2)
	c.Assert(cfg.App.TableConcurrency, Equals, 6)
}

func (s *configTestSuite) TestDefaultTidbBackendValue(c *C) {
	cfg := config.NewConfig()
	assignMinimalLegalValue(cfg)
	cfg.TikvImporter.Backend = "tidb"
	cfg.App.RegionConcurrency = 123
	cfg.TiDB.DistSQLScanConcurrency = 1
	err := cfg.Adjust(context.Background())
	c.Assert(err, IsNil)
	c.Assert(cfg.App.TableConcurrency, Equals, 123)
}

func (s *configTestSuite) TestDefaultCouldBeOverwritten(c *C) {
	cfg := config.NewConfig()
	assignMinimalLegalValue(cfg)
	cfg.TikvImporter.Backend = "importer"
	cfg.App.IndexConcurrency = 20
	cfg.App.TableConcurrency = 60
	cfg.TiDB.DistSQLScanConcurrency = 1
	err := cfg.Adjust(context.Background())
	c.Assert(err, IsNil)
	c.Assert(cfg.App.IndexConcurrency, Equals, 20)
	c.Assert(cfg.App.TableConcurrency, Equals, 60)
}

func (s *configTestSuite) TestLoadFromInvalidConfig(c *C) {
	taskCfg := config.NewConfig()
	err := taskCfg.LoadFromGlobal(&config.GlobalConfig{
		ConfigFileContent: []byte("invalid toml"),
	})
	c.Assert(err, ErrorMatches, "Near line 1.*")
}

func (s *configTestSuite) TestTomlPostRestore(c *C) {
	cfg := &config.Config{}
	err := cfg.LoadFromTOML([]byte(`
		[post-restore]
		checksum = "req"
	`))
	c.Assert(err, ErrorMatches, regexp.QuoteMeta("invalid op level 'req', please choose valid option between ['off', 'optional', 'required']"))

	err = cfg.LoadFromTOML([]byte(`
		[post-restore]
		analyze = 123
	`))
	c.Assert(err, ErrorMatches, regexp.QuoteMeta("invalid op level '123', please choose valid option between ['off', 'optional', 'required']"))

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
		c.Assert(err, IsNil)
		c.Assert(cfg.PostRestore.Checksum, Equals, v)

		b.Reset()
		c.Assert(enc.Encode(cfg.PostRestore), IsNil)
		c.Assert(&b, Matches, fmt.Sprintf(`(?s).*checksum = "\Q%s\E".*`, v))
	}

	for k, v := range kvMap {
		cfg := &config.Config{}
		confStr := fmt.Sprintf("[post-restore]\r\nanalyze= %s\r\n", k)
		err := cfg.LoadFromTOML([]byte(confStr))
		c.Assert(err, IsNil)
		c.Assert(cfg.PostRestore.Analyze, Equals, v)

		b.Reset()
		c.Assert(enc.Encode(cfg.PostRestore), IsNil)
		c.Assert(&b, Matches, fmt.Sprintf(`(?s).*analyze = "\Q%s\E".*`, v))
	}
}

func (s *configTestSuite) TestCronEncodeDecode(c *C) {
	cfg := &config.Config{}
	cfg.Cron.SwitchMode.Duration = 1 * time.Minute
	cfg.Cron.LogProgress.Duration = 2 * time.Minute
	cfg.Cron.CheckDiskQuota.Duration = 3 * time.Second
	var b bytes.Buffer
	c.Assert(toml.NewEncoder(&b).Encode(cfg.Cron), IsNil)
	c.Assert(b.String(), Equals, "switch-mode = \"1m0s\"\nlog-progress = \"2m0s\"\ncheck-disk-quota = \"3s\"\n")

	confStr := "[cron]\r\n" + b.String()
	cfg2 := &config.Config{}
	c.Assert(cfg2.LoadFromTOML([]byte(confStr)), IsNil)
	c.Assert(cfg2.Cron, DeepEquals, cfg.Cron)
}

func (s *configTestSuite) TestAdjustWithLegacyBlackWhiteList(c *C) {
	cfg := config.NewConfig()
	assignMinimalLegalValue(cfg)
	c.Assert(cfg.Mydumper.Filter, DeepEquals, config.DefaultFilter)
	c.Assert(cfg.HasLegacyBlackWhiteList(), IsFalse)

	ctx := context.Background()
	cfg.Mydumper.Filter = []string{"test.*"}
	cfg.TiDB.DistSQLScanConcurrency = 1
	c.Assert(cfg.Adjust(ctx), IsNil)
	c.Assert(cfg.HasLegacyBlackWhiteList(), IsFalse)

	cfg.BWList.DoDBs = []string{"test"}
	c.Assert(cfg.Adjust(ctx), ErrorMatches, "invalid config: `mydumper\\.filter` and `black-white-list` cannot be simultaneously defined")

	cfg.Mydumper.Filter = config.DefaultFilter
	c.Assert(cfg.Adjust(ctx), IsNil)
	c.Assert(cfg.HasLegacyBlackWhiteList(), IsTrue)
}

func (s *configTestSuite) TestAdjustDiskQuota(c *C) {
	cfg := config.NewConfig()
	assignMinimalLegalValue(cfg)

	base := c.MkDir()
	ctx := context.Background()
	cfg.TikvImporter.Backend = config.BackendLocal
	cfg.TikvImporter.DiskQuota = 0
	cfg.TikvImporter.SortedKVDir = base
	cfg.TiDB.DistSQLScanConcurrency = 1
	c.Assert(cfg.Adjust(ctx), IsNil)
	c.Assert(int64(cfg.TikvImporter.DiskQuota), Equals, int64(0))
}

func (s *configTestSuite) TestDataCharacterSet(c *C) {
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
		comment := Commentf("input = %s", tc.input)

		cfg := config.NewConfig()
		cfg.Mydumper.SourceDir = "file://."
		cfg.TiDB.Port = 4000
		cfg.TiDB.PdAddr = "test.invalid:2379"
		cfg.TikvImporter.Backend = config.BackendLocal
		cfg.TikvImporter.SortedKVDir = "."
		cfg.TiDB.DistSQLScanConcurrency = 1
		err := cfg.LoadFromTOML([]byte(tc.input))
		c.Assert(err, IsNil)
		err = cfg.Adjust(context.Background())
		if tc.err != "" {
			c.Assert(err, ErrorMatches, regexp.QuoteMeta(tc.err), comment)
		} else {
			c.Assert(err, IsNil, comment)
		}
	}
}

func (s *configTestSuite) TestCheckpointKeepStrategy(c *C) {
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
		c.Assert(err, IsNil)
		c.Assert(cp, Equals, strategy)
	}

	defaultCp := "enable = true\r\n"
	cpCfg := &config.Checkpoint{}
	_, err := toml.Decode(defaultCp, cpCfg)
	c.Assert(err, IsNil)
	c.Assert(cpCfg.KeepAfterSuccess, Equals, config.CheckpointRemove)

	cpFmt := "keep-after-success = %v\r\n"
	for key, strategy := range tomlCases {
		cpValue := key
		if strVal, ok := key.(string); ok {
			cpValue = `"` + strVal + `"`
		}
		tomlStr := fmt.Sprintf(cpFmt, cpValue)
		cpCfg := &config.Checkpoint{}
		_, err := toml.Decode(tomlStr, cpCfg)
		c.Assert(err, IsNil)
		c.Assert(cpCfg.KeepAfterSuccess, Equals, strategy)
	}

	marshalTextCases := map[config.CheckpointKeepStrategy]string{
		config.CheckpointRemove: "remove",
		config.CheckpointRename: "rename",
		config.CheckpointOrigin: "origin",
	}
	for strategy, value := range marshalTextCases {
		res, err := strategy.MarshalText()
		c.Assert(err, IsNil)
		c.Assert(res, DeepEquals, []byte(value))
	}
}

func (s configTestSuite) TestLoadCharsetFromConfig(c *C) {
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
		c.Assert(err, IsNil)
		c.Assert(charset, Equals, v)
	}

	_, err := config.ParseCharset("Unknown")
	c.Assert(err, ErrorMatches, "found unsupported data-character-set: Unknown")
}
