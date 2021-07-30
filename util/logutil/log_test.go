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
	"bufio"
	"context"
	"io"
	"os"
	"runtime"
	"testing"

	"github.com/pingcap/log"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestZapLoggerWithKeys(t *testing.T) {
	if runtime.GOOS == "windows" {
		// Skip this test on windows for two reason:
		// 1. The pattern match fails somehow. It seems windows treat \n as slash and character n.
		// 2. Remove file doesn't work as long as the log instance hold the file.
		t.Skip("skip on windows")
	}

	fileCfg := FileLogConfig{log.FileLogConfig{Filename: "zap_log", MaxSize: 4096}}
	conf := NewLogConfig("info", DefaultLogFormat, "", fileCfg, false)
	err := InitLogger(conf)
	require.NoError(t, err)
	connID := uint64(123)
	ctx := WithConnID(context.Background(), connID)
	testZapLogger(ctx, t, fileCfg.Filename, zapLogWithConnIDPattern)
	err = os.Remove(fileCfg.Filename)
	require.NoError(t, err)

	err = InitLogger(conf)
	require.NoError(t, err)
	key := "ctxKey"
	val := "ctxValue"
	ctx1 := WithKeyValue(context.Background(), key, val)
	testZapLogger(ctx1, t, fileCfg.Filename, zapLogWithKeyValPattern)
	err = os.Remove(fileCfg.Filename)
	require.NoError(t, err)
}

func testZapLogger(ctx context.Context, t *testing.T, fileName, pattern string) {
	Logger(ctx).Debug("debug msg", zap.String("test with key", "true"))
	Logger(ctx).Info("info msg", zap.String("test with key", "true"))
	Logger(ctx).Warn("warn msg", zap.String("test with key", "true"))
	Logger(ctx).Error("error msg", zap.String("test with key", "true"))

	f, err := os.Open(fileName)
	require.NoError(t, err)
	defer func() {
		err = f.Close()
		require.NoError(t, err)
	}()

	r := bufio.NewReader(f)
	for {
		var str string
		str, err = r.ReadString('\n')
		if err != nil {
			break
		}
		require.Regexp(t, pattern, str)
		require.NotContains(t, str, "stack")
		require.NotContains(t, str, "errorVerbose")
	}
	require.Equal(t, io.EOF, err)
}

func TestSetLevel(t *testing.T) {
	conf := NewLogConfig("info", DefaultLogFormat, "", EmptyFileLogConfig, false)
	err := InitLogger(conf)
	require.NoError(t, err)
	require.Equal(t, zap.InfoLevel, log.GetLevel())

	err = SetLevel("warn")
	require.NoError(t, err)
	require.Equal(t, zap.WarnLevel, log.GetLevel())
	err = SetLevel("Error")
	require.NoError(t, err)
	require.Equal(t, zap.ErrorLevel, log.GetLevel())
	err = SetLevel("DEBUG")
	require.NoError(t, err)
	require.Equal(t, zap.DebugLevel, log.GetLevel())
}
