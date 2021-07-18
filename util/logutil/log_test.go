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
	"bytes"
	"context"
	"io"
	"os"
	"runtime"
	"strings"
	"testing"

	"github.com/pingcap/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
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

type testLogSuite struct {
	suite.Suite
	buf *bytes.Buffer
}

func (s *testLogSuite) SetupTest() {
	s.buf = &bytes.Buffer{}
}

func (s *testLogSuite) TestZapLoggerWithKeys() {
	assert := assert.New(s.T())
	if runtime.GOOS == "windows" {
		// Skip this test on windows for two reason:
		// 1. The pattern match fails somehow. It seems windows treat \n as slash and character n.
		// 2. Remove file doesn't work as long as the log instance hold the file.
		s.T().Skip("skip on windows")
	}

	fileCfg := FileLogConfig{log.FileLogConfig{Filename: "zap_log", MaxSize: 4096}}
	conf := NewLogConfig("info", DefaultLogFormat, "", fileCfg, false)
	err := InitLogger(conf)
	assert.Nil(err)
	connID := uint64(123)
	ctx := WithConnID(context.Background(), connID)
	s.testZapLogger(ctx, assert, fileCfg.Filename, zapLogWithConnIDPattern)
	os.Remove(fileCfg.Filename)

	err = InitLogger(conf)
	assert.Nil(err)
	key := "ctxKey"
	val := "ctxValue"
	ctx1 := WithKeyValue(context.Background(), key, val)
	s.testZapLogger(ctx1, assert, fileCfg.Filename, zapLogWithKeyValPattern)
	os.Remove(fileCfg.Filename)
}

func (s *testLogSuite) testZapLogger(ctx context.Context, assert *assert.Assertions, fileName, pattern string) {
	Logger(ctx).Debug("debug msg", zap.String("test with key", "true"))
	Logger(ctx).Info("info msg", zap.String("test with key", "true"))
	Logger(ctx).Warn("warn msg", zap.String("test with key", "true"))
	Logger(ctx).Error("error msg", zap.String("test with key", "true"))

	f, err := os.Open(fileName)
	assert.Nil(err)
	defer f.Close()

	r := bufio.NewReader(f)
	for {
		var str string
		str, err = r.ReadString('\n')
		if err != nil {
			break
		}
		assert.Regexp(pattern, str)
		assert.False(strings.Contains(str, "stack"))
		assert.False(strings.Contains(str, "errorVerbose"))
	}
	assert.Equal(err, io.EOF)
}

func (s *testLogSuite) TestSetLevel() {
	assert := assert.New(s.T())
	conf := NewLogConfig("info", DefaultLogFormat, "", EmptyFileLogConfig, false)
	err := InitLogger(conf)
	assert.Nil(err)

	assert.Equal(log.GetLevel(), zap.InfoLevel)
	err = SetLevel("warn")
	assert.Nil(err)
	assert.Equal(log.GetLevel(), zap.WarnLevel)
	err = SetLevel("Error")
	assert.Nil(err)
	assert.Equal(log.GetLevel(), zap.ErrorLevel)
	err = SetLevel("DEBUG")
	assert.Nil(err)
	assert.Equal(log.GetLevel(), zap.DebugLevel)
}

func TestLogTestSuite(t *testing.T) {
	suite.Run(t, new(testLogSuite))
}
