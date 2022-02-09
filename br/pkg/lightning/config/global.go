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
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/carlmjohnson/flagext"
	"github.com/pingcap/errors"

	"github.com/pingcap/tidb/br/pkg/lightning/log"
	"github.com/pingcap/tidb/br/pkg/version/build"
)

const (
	CheckModeNormal = "normal"
	CheckModeSample = "sample"

	DefaultSampleRate = 0.01
	DefaultCheckRows  = 1000
)

type GlobalLightning struct {
	log.Config
	StatusAddr        string `toml:"status-addr" json:"status-addr"`
	ServerMode        bool   `toml:"server-mode" json:"server-mode"`
	CheckRequirements bool   `toml:"check-requirements" json:"check-requirements"`

	// The legacy alias for setting "status-addr". The value should always the
	// same as StatusAddr, and will not be published in the JSON encoding.
	PProfPort int `toml:"pprof-port" json:"-"`
}

type GlobalTiDB struct {
	Host       string `toml:"host" json:"host"`
	Port       int    `toml:"port" json:"port"`
	User       string `toml:"user" json:"user"`
	Psw        string `toml:"password" json:"-"`
	StatusPort int    `toml:"status-port" json:"status-port"`
	PdAddr     string `toml:"pd-addr" json:"pd-addr"`
	LogLevel   string `toml:"log-level" json:"log-level"`
}

type GlobalMydumper struct {
	SourceDir string `toml:"data-source-dir" json:"data-source-dir"`
	// Deprecated
	NoSchema      bool             `toml:"no-schema" json:"no-schema"`
	Filter        []string         `toml:"filter" json:"filter"`
	IgnoreColumns []*IgnoreColumns `toml:"ignore-columns" json:"ignore-columns"`
}

type GlobalImporter struct {
	Addr        string `toml:"addr" json:"addr"`
	Backend     string `toml:"backend" json:"backend"`
	SortedKVDir string `toml:"sorted-kv-dir" json:"sorted-kv-dir"`
}

type GlobalConfig struct {
	App          GlobalLightning   `toml:"lightning" json:"lightning"`
	Checkpoint   GlobalCheckpoint  `toml:"checkpoint" json:"checkpoint"`
	TiDB         GlobalTiDB        `toml:"tidb" json:"tidb"`
	Mydumper     GlobalMydumper    `toml:"mydumper" json:"mydumper"`
	TikvImporter GlobalImporter    `toml:"tikv-importer" json:"tikv-importer"`
	PostRestore  GlobalPostRestore `toml:"post-restore" json:"post-restore"`
	Security     Security          `toml:"security" json:"security"`
	CheckOnlyCfg *CheckOnlyConfig  `toml:"-" json:"-"`

	ConfigFileContent []byte
}

type GlobalCheckpoint struct {
	Enable bool `toml:"enable" json:"enable"`
}

type GlobalPostRestore struct {
	Checksum PostOpLevel `toml:"checksum" json:"checksum"`
	Analyze  PostOpLevel `toml:"analyze" json:"analyze"`
}

type CheckOnlyConfig struct {
	Mode string `toml:"-" json:"-"`

	// take effects when Mode=CheckModeSample
	Rate float64 `toml:"-" json:"-"`
	Rows int     `toml:"-" json:"-"`
}

func NewGlobalConfig() *GlobalConfig {
	return &GlobalConfig{
		App: GlobalLightning{
			ServerMode:        false,
			CheckRequirements: true,
		},
		Checkpoint: GlobalCheckpoint{
			Enable: true,
		},
		TiDB: GlobalTiDB{
			Host:       "127.0.0.1",
			User:       "root",
			StatusPort: 10080,
			LogLevel:   "error",
		},
		Mydumper: GlobalMydumper{
			Filter: DefaultFilter,
		},
		TikvImporter: GlobalImporter{
			Backend: "",
		},
		PostRestore: GlobalPostRestore{
			Checksum: OpLevelRequired,
			Analyze:  OpLevelOptional,
		},
	}
}

// Must should be called after LoadGlobalConfig(). If LoadGlobalConfig() returns
// any error, this function will exit the program with an appropriate exit code.
func Must(cfg *GlobalConfig, err error) *GlobalConfig {
	switch errors.Cause(err) {
	case nil:
	case flag.ErrHelp:
		os.Exit(0)
	default:
		fmt.Println("Failed to parse command flags: ", err)
		os.Exit(2)
	}
	return cfg
}

func timestampLogFileName() string {
	return filepath.Join(os.TempDir(), time.Now().Format("lightning.log.2006-01-02T15.04.05Z0700"))
}

// LoadGlobalConfig reads the arguments and fills in the GlobalConfig.
func LoadGlobalConfig(args []string, extraFlags func(*flag.FlagSet)) (*GlobalConfig, error) {
	cfg := NewGlobalConfig()
	fs := flag.NewFlagSet("", flag.ContinueOnError)

	// if both `-c` and `-config` are specified, the last one in the command line will take effect.
	// the default value is assigned immediately after the StringVar() call,
	// so it is fine to not give any default value for `-c`, to keep the `-h` page clean.
	var configFilePath string
	fs.StringVar(&configFilePath, "c", "", "(deprecated alias of -config)")
	fs.StringVar(&configFilePath, "config", "", "tidb-lightning configuration file")
	printVersion := fs.Bool("V", false, "print version of lightning")

	logLevel := flagext.ChoiceVar(fs, "L", "", `log level: info, debug, warn, error, fatal (default info)`, "", "info", "debug", "warn", "warning", "error", "fatal")
	logFilePath := fs.String("log-file", "", "log file path")
	tidbHost := fs.String("tidb-host", "", "TiDB server host")
	tidbPort := fs.Int("tidb-port", 0, "TiDB server port (default 4000)")
	tidbUser := fs.String("tidb-user", "", "TiDB user name to connect")
	tidbPsw := fs.String("tidb-password", "", "TiDB password to connect")
	tidbStatusPort := fs.Int("tidb-status", 0, "TiDB server status port (default 10080)")
	pdAddr := fs.String("pd-urls", "", "PD endpoint address")
	dataSrcPath := fs.String("d", "", "Directory of the dump to import")
	importerAddr := fs.String("importer", "", "address (host:port) to connect to tikv-importer")
	backend := flagext.ChoiceVar(fs, "backend", "", `delivery backend: local, importer, tidb`, "", "local", "importer", "tidb")
	sortedKVDir := fs.String("sorted-kv-dir", "", "path for KV pairs when local backend enabled")
	enableCheckpoint := fs.Bool("enable-checkpoint", true, "whether to enable checkpoints")
	noSchema := fs.Bool("no-schema", false, "ignore schema files, get schema directly from TiDB instead")
	checksum := flagext.ChoiceVar(fs, "checksum", "", "compare checksum after importing.", "", "required", "optional", "off", "true", "false")
	analyze := flagext.ChoiceVar(fs, "analyze", "", "analyze table after importing", "", "required", "optional", "off", "true", "false")
	checkRequirements := fs.Bool("check-requirements", true, "check cluster version before starting")
	tlsCAPath := fs.String("ca", "", "CA certificate path for TLS connection")
	tlsCertPath := fs.String("cert", "", "certificate path for TLS connection")
	tlsKeyPath := fs.String("key", "", "private key path for TLS connection")
	redactInfoLog := fs.Bool("redact-info-log", false, "whether to redact sensitive info in log")

	statusAddr := fs.String("status-addr", "", "the Lightning server address")
	serverMode := fs.Bool("server-mode", false, "start Lightning in server mode, wait for multiple tasks instead of starting immediately")

	checkOnlyCfg := fs.String("check-only", "", "do check only, supported value: normal, sample=rate,rows")

	var filter []string
	flagext.StringsVar(fs, &filter, "f", "select tables to import")

	if extraFlags != nil {
		extraFlags(fs)
	}

	if err := fs.Parse(args); err != nil {
		return nil, errors.Trace(err)
	}
	if *printVersion {
		fmt.Println(build.Info())
		return nil, flag.ErrHelp
	}

	if len(configFilePath) > 0 {
		data, err := os.ReadFile(configFilePath)
		if err != nil {
			return nil, errors.Annotatef(err, "Cannot read config file `%s`", configFilePath)
		}
		if err = toml.Unmarshal(data, cfg); err != nil {
			return nil, errors.Annotatef(err, "Cannot parse config file `%s`", configFilePath)
		}
		cfg.ConfigFileContent = data
	}

	if *logLevel != "" {
		cfg.App.Config.Level = *logLevel
	}
	if *logFilePath != "" {
		cfg.App.Config.File = *logFilePath
	}
	if cfg.App.Config.File == "" {
		cfg.App.Config.File = timestampLogFileName()
	}
	if *tidbHost != "" {
		cfg.TiDB.Host = *tidbHost
	}
	if *tidbPort != 0 {
		cfg.TiDB.Port = *tidbPort
	}
	if *tidbStatusPort != 0 {
		cfg.TiDB.StatusPort = *tidbStatusPort
	}
	if *tidbUser != "" {
		cfg.TiDB.User = *tidbUser
	}
	if *tidbPsw != "" {
		cfg.TiDB.Psw = *tidbPsw
	}
	if *pdAddr != "" {
		cfg.TiDB.PdAddr = *pdAddr
	}
	if *dataSrcPath != "" {
		cfg.Mydumper.SourceDir = *dataSrcPath
	}
	if *importerAddr != "" {
		cfg.TikvImporter.Addr = *importerAddr
	}
	if *serverMode {
		cfg.App.ServerMode = true
	}
	if *statusAddr != "" {
		cfg.App.StatusAddr = *statusAddr
	}
	if *backend != "" {
		cfg.TikvImporter.Backend = *backend
	}
	if *sortedKVDir != "" {
		cfg.TikvImporter.SortedKVDir = *sortedKVDir
	}
	if !*enableCheckpoint {
		cfg.Checkpoint.Enable = false
	}
	if *noSchema {
		cfg.Mydumper.NoSchema = true
	}
	if *checksum != "" {
		_ = cfg.PostRestore.Checksum.FromStringValue(*checksum)
	}
	if *analyze != "" {
		_ = cfg.PostRestore.Analyze.FromStringValue(*analyze)
	}
	if cfg.App.StatusAddr == "" && cfg.App.PProfPort != 0 {
		cfg.App.StatusAddr = fmt.Sprintf(":%d", cfg.App.PProfPort)
	}
	if !*checkRequirements {
		cfg.App.CheckRequirements = false
	}
	if *tlsCAPath != "" {
		cfg.Security.CAPath = *tlsCAPath
	}
	if *tlsCertPath != "" {
		cfg.Security.CertPath = *tlsCertPath
	}
	if *tlsKeyPath != "" {
		cfg.Security.KeyPath = *tlsKeyPath
	}
	if *redactInfoLog {
		cfg.Security.RedactInfoLog = *redactInfoLog
	}
	if len(filter) > 0 {
		cfg.Mydumper.Filter = filter
	}
	if *checkOnlyCfg != "" {
		if cfg.App.ServerMode {
			return nil, errors.New(`cannot set "check-only" if server-mode is enabled`)
		}
		checkOnlyConfig, err := extractCheckOnlyCfg(*checkOnlyCfg)
		if err != nil {
			return nil, err
		}
		cfg.CheckOnlyCfg = checkOnlyConfig
	}

	if cfg.App.StatusAddr == "" && cfg.App.ServerMode {
		return nil, errors.New("If server-mode is enabled, the status-addr must be a valid listen address")
	}

	cfg.App.Config.Adjust()
	return cfg, nil
}

func extractCheckOnlyCfg(str string) (*CheckOnlyConfig, error) {
	splits := strings.Split(str, "=")
	invalidCfg := errors.New("invalid check-only config: " + str)
	if len(splits) > 2 || (splits[0] != CheckModeNormal && splits[0] != CheckModeSample) {
		return nil, invalidCfg
	}

	switch splits[0] {
	case CheckModeNormal:
		if len(splits) != 1 {
			return nil, invalidCfg
		}
		return &CheckOnlyConfig{Mode: CheckModeNormal}, nil
	case CheckModeSample:
		if len(splits) == 1 {
			return &CheckOnlyConfig{Mode: CheckModeSample, Rate: DefaultSampleRate, Rows: DefaultCheckRows}, nil
		}
		if len(splits) != 2 {
			return nil, invalidCfg
		}
		params := strings.Split(splits[1], ",")
		if len(params) != 2 {
			return nil, invalidCfg
		}
		rate, err := strconv.ParseFloat(params[0], 64)
		if err != nil || rate > 1 || rate <= 0 {
			return nil, errors.Wrap(invalidCfg, "rate should be in range (0, 1]")
		}
		rows, err := strconv.ParseInt(params[1], 10, 32)
		if err != nil || rows <= 0 {
			return nil, errors.Wrap(invalidCfg, "rows should be greater than 0")
		}
		return &CheckOnlyConfig{Mode: CheckModeSample, Rate: rate, Rows: int(rows)}, nil
	default:
		return nil, invalidCfg
	}
}
