// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package logutil

import (
	"bytes"
	"fmt"
	"os"
	"path"
	"runtime"
	"strings"
	"sync"

	"github.com/coreos/pkg/capnslog"
	"github.com/juju/errors"
	log "github.com/sirupsen/logrus"
	lumberjack "gopkg.in/natefinch/lumberjack.v2"
)

const (
	defaultLogTimeFormat = "2006/01/02 15:04:05.000"
	defaultLogMaxSize    = 300 // MB
	defaultLogFormat     = "text"
	defaultLogLevel      = log.InfoLevel
)

// FileLogConfig serializes file log related config in toml/json.
type FileLogConfig struct {
	// Log filename, leave empty to disable file log.
	Filename string `toml:"filename" json:"filename"`
	// Is log rotate enabled. TODO.
	LogRotate bool `toml:"log-rotate" json:"log-rotate"`
	// Max size for a single file, in MB.
	MaxSize int `toml:"max-size" json:"max-size"`
	// Max log keep days, default is never deleting.
	MaxDays int `toml:"max-days" json:"max-days"`
	// Maximum number of old log files to retain.
	MaxBackups int `toml:"max-backups" json:"max-backups"`
}

// LogConfig serializes log related config in toml/json.
type LogConfig struct {
	// Log level.
	Level string `toml:"level" json:"level"`
	// Log format. one of json, text, or console.
	Format string `toml:"format" json:"format"`
	// Disable automatic timestamps in output.
	DisableTimestamp bool `toml:"disable-timestamp" json:"disable-timestamp"`
	// File log config.
	File FileLogConfig `toml:"file" json:"file"`
}

// redirectFormatter will redirect etcd logs to logrus logs.
type redirectFormatter struct{}

// Format implements capnslog.Formatter hook.
func (rf *redirectFormatter) Format(pkg string, level capnslog.LogLevel, depth int, entries ...interface{}) {
	if pkg != "" {
		pkg = fmt.Sprint(pkg, ": ")
	}

	logStr := fmt.Sprint(pkg, entries)

	switch level {
	case capnslog.CRITICAL:
		log.Fatalf(logStr)
	case capnslog.ERROR:
		log.Errorf(logStr)
	case capnslog.WARNING:
		log.Warningf(logStr)
	case capnslog.NOTICE:
		log.Infof(logStr)
	case capnslog.INFO:
		log.Infof(logStr)
	case capnslog.DEBUG, capnslog.TRACE:
		log.Debugf(logStr)
	}
}

// Flush only for implementing Formatter.
func (rf *redirectFormatter) Flush() {}

// isSKippedPackageName tests wether path name is on log library calling stack.
func isSkippedPackageName(name string) bool {
	return strings.Contains(name, "github.com/sirupsen/logrus") ||
		strings.Contains(name, "github.com/coreos/pkg/capnslog")
}

// modifyHook injects file name and line pos into log entry.
type contextHook struct{}

// Fire implements logrus.Hook interface
// https://github.com/sirupsen/logrus/issues/63
func (hook *contextHook) Fire(entry *log.Entry) error {
	pc := make([]uintptr, 3)
	cnt := runtime.Callers(6, pc)

	for i := 0; i < cnt; i++ {
		fu := runtime.FuncForPC(pc[i] - 1)
		name := fu.Name()
		if !isSkippedPackageName(name) {
			file, line := fu.FileLine(pc[i] - 1)
			entry.Data["file"] = path.Base(file)
			entry.Data["line"] = line
			break
		}
	}
	return nil
}

// Levels implements logrus.Hook interface.
func (hook *contextHook) Levels() []log.Level {
	return log.AllLevels
}

func stringToLogLevel(level string) log.Level {
	switch strings.ToLower(level) {
	case "fatal":
		return log.FatalLevel
	case "error":
		return log.ErrorLevel
	case "warn", "warning":
		return log.WarnLevel
	case "debug":
		return log.DebugLevel
	case "info":
		return log.InfoLevel
	}
	return defaultLogLevel
}

// textFormatter is for compatability with ngaut/log
type textFormatter struct {
	DisableTimestamp bool
}

// Format implements logrus.Formatter
func (f *textFormatter) Format(entry *log.Entry) ([]byte, error) {
	var b *bytes.Buffer
	if entry.Buffer != nil {
		b = entry.Buffer
	} else {
		b = &bytes.Buffer{}
	}
	if !f.DisableTimestamp {
		fmt.Fprintf(b, "%s ", entry.Time.Format(defaultLogTimeFormat))
	}
	if file, ok := entry.Data["file"]; ok {
		fmt.Fprintf(b, "%s:%v:", file, entry.Data["line"])
	}
	fmt.Fprintf(b, " [%s] %s", entry.Level.String(), entry.Message)
	for k, v := range entry.Data {
		if k != "file" && k != "line" {
			fmt.Fprintf(b, " %v=%v", k, v)
		}
	}
	b.WriteByte('\n')
	return b.Bytes(), nil
}

func stringToLogFormatter(format string, disableTimestamp bool) log.Formatter {
	switch strings.ToLower(format) {
	case "text":
		return &textFormatter{
			DisableTimestamp: disableTimestamp,
		}
	case "json":
		return &log.JSONFormatter{
			TimestampFormat:  defaultLogTimeFormat,
			DisableTimestamp: disableTimestamp,
		}
	case "console":
		return &log.TextFormatter{
			FullTimestamp:    true,
			TimestampFormat:  defaultLogTimeFormat,
			DisableTimestamp: disableTimestamp,
		}
	default:
		return &textFormatter{}
	}
}

// InitFileLog initializes file based logging options.
func InitFileLog(cfg *FileLogConfig) error {
	if st, err := os.Stat(cfg.Filename); err == nil {
		if st.IsDir() {
			return errors.New("can't use directory as log file name")
		}
	}
	if cfg.MaxSize == 0 {
		cfg.MaxSize = defaultLogMaxSize
	}

	// use lumberjack to logrotate
	output := &lumberjack.Logger{
		Filename:   cfg.Filename,
		MaxSize:    cfg.MaxSize,
		MaxBackups: cfg.MaxBackups,
		MaxAge:     cfg.MaxDays,
		LocalTime:  true,
	}

	log.SetOutput(output)
	return nil
}

var once sync.Once

// InitLogger initalizes PD's logger.
func InitLogger(cfg *LogConfig) error {
	var err error
	once.Do(func() {
		log.SetLevel(stringToLogLevel(cfg.Level))
		log.AddHook(&contextHook{})

		if cfg.Format == "" {
			cfg.Format = defaultLogFormat
		}
		log.SetFormatter(stringToLogFormatter(cfg.Format, cfg.DisableTimestamp))

		// etcd log
		capnslog.SetFormatter(&redirectFormatter{})

		if len(cfg.File.Filename) == 0 {
			return
		}

		err = InitFileLog(&cfg.File)
	})
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}
