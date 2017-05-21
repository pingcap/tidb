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

package log

import (
	"os"
	"strings"

	"github.com/Sirupsen/logrus"
	"github.com/juju/errors"
	lumberjack "gopkg.in/natefinch/lumberjack.v2"
)

const (
	defaultLogTimeFormat = "2006/01/02 15:04:05"
	defaultLogMaxSize    = 1024 // MB
	defaultLogFormat     = "text"
	defaultLogLevel      = logrus.InfoLevel
	defaultLogMaxBackups = 10
	defaultLogMaxDays    = 8
)

// FileConfig serializes file log related config in toml/json.
type FileConfig struct {
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

// Config serializes log related config in toml/json.
type Config struct {
	// Log level.
	Level string `toml:"level" json:"level"`
	// Log format. one of json, text, or console.
	Format string `toml:"format" json:"format"`
	// Disable automatic timestamps in output.
	DisableTimestamp bool `toml:"disable-timestamp" json:"disable-timestamp"`
	// File log config.
	File FileConfig `toml:"file" json:"file"`
}

// InitFileLog initializes file based logging options.
func InitFileLog(cfg *FileConfig) error {
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

	logrus.SetOutput(output)
	return nil
}

// SetLevel sets current log level.
func SetLevel(level logrus.Level) {
	logrus.SetLevel(level)
}

// GetLogLevel returns current log level.
func GetLogLevel() logrus.Level {
	return logrus.GetLevel()
}

func stringToLogLevel(level string) logrus.Level {
	switch strings.ToLower(level) {
	case "fatal":
		return logrus.FatalLevel
	case "error":
		return logrus.ErrorLevel
	case "warn", "warning":
		return logrus.WarnLevel
	case "debug":
		return logrus.DebugLevel
	case "info":
		return logrus.InfoLevel
	}
	return defaultLogLevel
}

// init initalizes default logger.
func init() {
	cfg := new(Config)

	cfg.DisableTimestamp = false
	cfg.Format = defaultLogFormat
	cfg.Level = "info"
	cfg.File.Filename = ""

	SetLevelByString(cfg.Level)

	logrus.AddHook(&contextHook{})
	logrus.SetFormatter(stringToLogFormatter(cfg.Format, cfg.DisableTimestamp))
}

// InitLogger initalizes TiDB's logger.
func InitLogger(logFile string, logFormat string) error {
	cfg := new(Config)

	cfg.DisableTimestamp = false
	cfg.Format = logFormat
	cfg.Level = "info"

	cfg.File.Filename = logFile
	cfg.File.LogRotate = true
	cfg.File.MaxBackups = defaultLogMaxBackups
	cfg.File.MaxDays = defaultLogMaxDays
	cfg.File.MaxSize = defaultLogMaxSize

	if cfg.Format == "" {
		cfg.Format = defaultLogFormat
	}
	logrus.SetFormatter(stringToLogFormatter(cfg.Format, cfg.DisableTimestamp))

	if len(cfg.File.Filename) == 0 {
		return nil
	}

	err := InitFileLog(&cfg.File)
	if err != nil {
		return errors.Trace(err)
	}

	return nil
}
