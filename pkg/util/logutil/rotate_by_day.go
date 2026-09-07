// Copyright 2026 PingCAP, Inc.
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

package logutil

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/pingcap/log"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/natefinch/lumberjack.v2"
)

type logFileRotator interface {
	Write([]byte) (int, error)
	Rotate() error
}

type rotateByDayWriteSyncer struct {
	logger      logFileRotator
	nowFunc     func() time.Time
	mu          sync.Mutex
	lastYear    int
	lastYearDay int
	initialized bool
}

func newRotateByDayWriteSyncer(logger logFileRotator) *rotateByDayWriteSyncer {
	return &rotateByDayWriteSyncer{
		logger:  logger,
		nowFunc: time.Now,
	}
}

func (w *rotateByDayWriteSyncer) Write(p []byte) (int, error) {
	if err := w.rotateIfNeeded(); err != nil {
		return 0, err
	}
	return w.logger.Write(p)
}

func (*rotateByDayWriteSyncer) Sync() error {
	return nil
}

func (w *rotateByDayWriteSyncer) rotateIfNeeded() error {
	now := w.nowFunc().Local()
	year, yearDay := now.Year(), now.YearDay()

	w.mu.Lock()
	defer w.mu.Unlock()

	if !w.initialized {
		w.initialized = true
		w.lastYear = year
		w.lastYearDay = yearDay
		return nil
	}
	if w.lastYear == year && w.lastYearDay == yearDay {
		return nil
	}
	if err := w.logger.Rotate(); err != nil {
		return err
	}
	w.lastYear = year
	w.lastYearDay = yearDay
	return nil
}

func initPingCAPLogger(cfg *log.Config, rotateByDay bool, opts ...zap.Option) (*zap.Logger, *log.ZapProperties, error) {
	if !rotateByDay || len(cfg.File.Filename) == 0 {
		return log.InitLogger(cfg, opts...)
	}

	output, errOutput, err := initLogWriteSyncers(cfg, rotateByDay)
	if err != nil {
		return nil, nil, err
	}
	return log.InitLoggerWithWriteSyncer(cfg, output, errOutput, opts...)
}

func initLogWriteSyncers(cfg *log.Config, rotateByDay bool) (zapcore.WriteSyncer, zapcore.WriteSyncer, error) {
	output, err := buildFileLogWriteSyncer(cfg.File, rotateByDay)
	if err != nil {
		return nil, nil, err
	}
	if cfg.File.IsBuffered {
		output = &zapcore.BufferedWriteSyncer{
			WS:            output,
			Size:          cfg.File.BufferSize,
			FlushInterval: cfg.File.BufferFlushInterval,
		}
	}

	if len(cfg.ErrorOutputPath) == 0 {
		return output, output, nil
	}
	errOutput, _, err := zap.Open([]string{cfg.ErrorOutputPath}...)
	if err != nil {
		return nil, nil, err
	}
	return output, errOutput, nil
}

func buildFileLogWriteSyncer(cfg log.FileLogConfig, rotateByDay bool) (zapcore.WriteSyncer, error) {
	lumberjackLogger, err := newLumberjackLogger(cfg)
	if err != nil {
		return nil, err
	}
	if rotateByDay {
		return newRotateByDayWriteSyncer(lumberjackLogger), nil
	}
	return zapcore.AddSync(lumberjackLogger), nil
}

func newLumberjackLogger(cfg log.FileLogConfig) (*lumberjack.Logger, error) {
	dir := filepath.Dir(cfg.Filename)
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return nil, fmt.Errorf("cannot create log directory: %w", err)
	}

	if st, err := os.Stat(cfg.Filename); err == nil {
		if st.IsDir() {
			return nil, fmt.Errorf("can't use directory as log file name")
		}
		file, err := os.OpenFile(cfg.Filename, os.O_WRONLY|os.O_APPEND, 0o600)
		if err != nil {
			return nil, fmt.Errorf("can't write to log file: %w", err)
		}
		if err := file.Close(); err != nil {
			return nil, fmt.Errorf("can't close log file: %w", err)
		}
	} else if os.IsNotExist(err) {
		file, err := os.OpenFile(cfg.Filename, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
		if err != nil {
			return nil, fmt.Errorf("can't create log file: %w", err)
		}
		if err := file.Close(); err != nil {
			return nil, fmt.Errorf("can't close log file: %w", err)
		}
		if err := os.Remove(cfg.Filename); err != nil {
			return nil, fmt.Errorf("can't prepare log file: %w", err)
		}
	} else {
		return nil, fmt.Errorf("error checking log file: %w", err)
	}

	if cfg.MaxSize == 0 {
		cfg.MaxSize = DefaultLogMaxSize
	}

	compress := false
	switch cfg.Compression {
	case "":
	case "gzip":
		compress = true
	default:
		return nil, fmt.Errorf("can't set compression to `%s`", cfg.Compression)
	}

	return &lumberjack.Logger{
		Filename:   cfg.Filename,
		MaxSize:    cfg.MaxSize,
		MaxBackups: cfg.MaxBackups,
		MaxAge:     cfg.MaxDays,
		LocalTime:  true,
		Compress:   compress,
	}, nil
}
