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
	"strings"
	"testing"

	"github.com/pingcap/log"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

const (
	// zapLogPatern is used to match the zap log format, such as the following log:
	// [2019/02/13 15:56:05.385 +08:00] [INFO] [log_test.go:167] ["info message"] [conn=conn1] ["str key"=val] ["int key"=123]
	zapLogWithConnIDPattern = `\[\d\d\d\d/\d\d/\d\d \d\d:\d\d:\d\d.\d\d\d\ (\+|-)\d\d:\d\d\] \[(FATAL|ERROR|WARN|INFO|DEBUG)\] \[([\w_%!$@.,+~-]+|\\.)+:\d+\] \[.*\] \[conn=.*\] (\[.*=.*\]).*\n`
	// [2019/02/13 15:56:05.385 +08:00] [INFO] [log_test.go:167] ["info message"] [ctxKey=ctxKey1] ["str key"=val] ["int key"=123]
	zapLogWithKeyValPattern = `\[\d\d\d\d/\d\d/\d\d \d\d:\d\d:\d\d.\d\d\d\ (\+|-)\d\d:\d\d\] \[(FATAL|ERROR|WARN|INFO|DEBUG)\] \[([\w_%!$@.,+~-]+|\\.)+:\d+\] \[.*\] \[ctxKey=.*\] (\[.*=.*\]).*\n`
)

var PrettyPrint = prettyPrint

func TestZapLoggerWithKeys(t *testing.T) {
	t.Parallel()

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
	os.Remove(fileCfg.Filename)

	err = InitLogger(conf)
	require.NoError(t, err)
	key := "ctxKey"
	val := "ctxValue"
	ctx1 := WithKeyValue(context.Background(), key, val)
	testZapLogger(ctx1, t, fileCfg.Filename, zapLogWithKeyValPattern)
	os.Remove(fileCfg.Filename)
}

func testZapLogger(ctx context.Context, t *testing.T, fileName, pattern string) {
	Logger(ctx).Debug("debug msg", zap.String("test with key", "true"))
	Logger(ctx).Info("info msg", zap.String("test with key", "true"))
	Logger(ctx).Warn("warn msg", zap.String("test with key", "true"))
	Logger(ctx).Error("error msg", zap.String("test with key", "true"))

	f, err := os.Open(fileName)
	require.NoError(t, err)
	defer f.Close()

	r := bufio.NewReader(f)
	for {
		var str string
		str, err = r.ReadString('\n')
		if err != nil {
			break
		}
		require.Regexp(t, pattern, str)
		require.False(t, strings.Contains(str, "stack"))
		require.False(t, strings.Contains(str, "errorVerbose"))
	}
	require.Equal(t, err, io.EOF)
}

func TestSetLevel(t *testing.T) {
	t.Parallel()

	conf := NewLogConfig("info", DefaultLogFormat, "", EmptyFileLogConfig, false)
	err := InitLogger(conf)
	require.NoError(t, err)

	require.Equal(t, log.GetLevel(), zap.InfoLevel)
	err = SetLevel("warn")
	require.NoError(t, err)
	require.Equal(t, log.GetLevel(), zap.WarnLevel)
	err = SetLevel("Error")
	require.NoError(t, err)
	require.Equal(t, log.GetLevel(), zap.ErrorLevel)
	err = SetLevel("DEBUG")
	require.NoError(t, err)
	require.Equal(t, log.GetLevel(), zap.DebugLevel)
}
