// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/gluetidb"
	"github.com/pingcap/tidb/br/pkg/redact"
	"github.com/pingcap/tidb/br/pkg/summary"
	"github.com/pingcap/tidb/br/pkg/task"
	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/pingcap/tidb/br/pkg/version/build"
	"github.com/pingcap/tidb/config"
	tidbutils "github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/spf13/cobra"
)

var (
	initOnce        = sync.Once{}
	defaultContext  context.Context
	hasLogFile      uint64
	tidbGlue        = gluetidb.New()
	envLogToTermKey = "BR_LOG_TO_TERM"

	filterOutSysAndMemTables = []string{
		"*.*",
		fmt.Sprintf("!%s.*", utils.TemporaryDBName("*")),
		"!mysql.*",
		"!sys.*",
		"!INFORMATION_SCHEMA.*",
		"!PERFORMANCE_SCHEMA.*",
		"!METRICS_SCHEMA.*",
		"!INSPECTION_SCHEMA.*",
	}
	acceptAllTables = []string{
		"*.*",
	}
)

const (
	// FlagLogLevel is the name of log-level flag.
	FlagLogLevel = "log-level"
	// FlagLogFile is the name of log-file flag.
	FlagLogFile = "log-file"
	// FlagLogFormat is the name of log-format flag.
	FlagLogFormat = "log-format"
	// FlagStatusAddr is the name of status-addr flag.
	FlagStatusAddr = "status-addr"
	// FlagSlowLogFile is the name of slow-log-file flag.
	FlagSlowLogFile = "slow-log-file"
	// FlagRedactLog is whether to redact sensitive information in log, already deprecated by FlagRedactInfoLog
	FlagRedactLog = "redact-log"
	// FlagRedactInfoLog is whether to redact sensitive information in log.
	FlagRedactInfoLog = "redact-info-log"

	flagVersion      = "version"
	flagVersionShort = "V"
)

func timestampLogFileName() string {
	return filepath.Join(os.TempDir(), time.Now().Format("br.log.2006-01-02T15.04.05Z0700"))
}

// AddFlags adds flags to the given cmd.
func AddFlags(cmd *cobra.Command) {
	cmd.Version = build.Info()
	cmd.Flags().BoolP(flagVersion, flagVersionShort, false, "Display version information about BR")
	cmd.SetVersionTemplate("{{printf \"%s\" .Version}}\n")

	cmd.PersistentFlags().StringP(FlagLogLevel, "L", "info",
		"Set the log level")
	cmd.PersistentFlags().String(FlagLogFile, timestampLogFileName(),
		"Set the log file path. If not set, logs will output to temp file")
	cmd.PersistentFlags().String(FlagLogFormat, "text",
		"Set the log format")
	cmd.PersistentFlags().Bool(FlagRedactLog, false,
		"Set whether to redact sensitive info in log, already deprecated by --redact-info-log")
	cmd.PersistentFlags().Bool(FlagRedactInfoLog, false,
		"Set whether to redact sensitive info in log")
	cmd.PersistentFlags().String(FlagStatusAddr, "",
		"Set the HTTP listening address for the status report service. Set to empty string to disable")
	task.DefineCommonFlags(cmd.PersistentFlags())

	cmd.PersistentFlags().StringP(FlagSlowLogFile, "", "",
		"Set the slow log file path. If not set, discard slow logs")
	_ = cmd.PersistentFlags().MarkHidden(FlagSlowLogFile)
	_ = cmd.PersistentFlags().MarkHidden(FlagRedactLog)
}

// Init initializes BR cli.
func Init(cmd *cobra.Command) (err error) {
	initOnce.Do(func() {
		slowLogFilename, e := cmd.Flags().GetString(FlagSlowLogFile)
		if e != nil {
			err = e
			return
		}
		tidbLogCfg := logutil.LogConfig{}
		if len(slowLogFilename) != 0 {
			tidbLogCfg.SlowQueryFile = slowLogFilename
			// Just for special grpc log file,
			// otherwise the info will be print in stdout...
			tidbLogCfg.File.Filename = timestampLogFileName()
		} else {
			// Don't print slow log in br
			config.GetGlobalConfig().Instance.EnableSlowLog.Store(false)
		}
		e = logutil.InitLogger(&tidbLogCfg)
		if e != nil {
			err = e
			return
		}
		// Initialize the logger.
		conf := new(log.Config)
		conf.Level, err = cmd.Flags().GetString(FlagLogLevel)
		if err != nil {
			return
		}
		conf.File.Filename, err = cmd.Flags().GetString(FlagLogFile)
		if err != nil {
			return
		}
		conf.Format, err = cmd.Flags().GetString(FlagLogFormat)
		if err != nil {
			return
		}
		_, outputLogToTerm := os.LookupEnv(envLogToTermKey)
		if outputLogToTerm {
			// Log to term if env `BR_LOG_TO_TERM` is set.
			conf.File.Filename = ""
		}
		if len(conf.File.Filename) != 0 {
			atomic.StoreUint64(&hasLogFile, 1)
			summary.InitCollector(true)
			// cmd.PrintErr prints to stderr, but PrintErrf prints to stdout.
			cmd.PrintErr(fmt.Sprintf("Detail BR log in %s \n", conf.File.Filename))
		}
		lg, p, e := log.InitLogger(conf)
		if e != nil {
			err = e
			return
		}
		log.ReplaceGlobals(lg, p)

		redactLog, e := cmd.Flags().GetBool(FlagRedactLog)
		if e != nil {
			err = e
			return
		}
		redactInfoLog, e := cmd.Flags().GetBool(FlagRedactInfoLog)
		if e != nil {
			err = e
			return
		}
		redact.InitRedact(redactLog || redactInfoLog)
		err = startPProf(cmd)
	})
	return errors.Trace(err)
}

func startPProf(cmd *cobra.Command) error {
	// Initialize the pprof server.
	statusAddr, err := cmd.Flags().GetString(FlagStatusAddr)
	if err != nil {
		return errors.Trace(err)
	}
	ca, cert, key, err := task.ParseTLSTripleFromFlags(cmd.Flags())
	if err != nil {
		return errors.Trace(err)
	}
	// Host isn't used here.
	tls, err := tidbutils.NewTLS(ca, cert, key, "localhost", nil)
	if err != nil {
		return errors.Trace(err)
	}

	if statusAddr != "" {
		return utils.StartPProfListener(statusAddr, tls)
	}
	utils.StartDynamicPProfListener(tls)
	return nil
}

// HasLogFile returns whether we set a log file.
func HasLogFile() bool {
	return atomic.LoadUint64(&hasLogFile) != uint64(0)
}

// SetDefaultContext sets the default context for command line usage.
func SetDefaultContext(ctx context.Context) {
	defaultContext = ctx
}

// GetDefaultContext returns the default context for command line usage.
func GetDefaultContext() context.Context {
	return defaultContext
}
