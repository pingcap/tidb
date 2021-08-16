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

	. "github.com/pingcap/check"
	"github.com/pingcap/log"
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

func Test(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testLogSuite{})

type testLogSuite struct {
	buf *bytes.Buffer
}

func (s *testLogSuite) SetUpSuite(_ *C) {
	s.buf = &bytes.Buffer{}
}

func (s *testLogSuite) SetUpTest(_ *C) {
	s.buf = &bytes.Buffer{}
}

func (s *testLogSuite) TestZapLoggerWithKeys(c *C) {
	if runtime.GOOS == "windows" {
		// Skip this test on windows for two reason:
		// 1. The pattern match fails somehow. It seems windows treat \n as slash and character n.
		// 2. Remove file doesn't work as long as the log instance hold the file.
		c.Skip("skip on windows")
	}

	fileCfg := FileLogConfig{log.FileLogConfig{Filename: "zap_log", MaxSize: 4096}}
	conf := NewLogConfig("info", DefaultLogFormat, "", fileCfg, false)
	err := InitZapLogger(conf)
	c.Assert(err, IsNil)
	connID := uint64(123)
	ctx := WithConnID(context.Background(), connID)
	s.testZapLogger(ctx, c, fileCfg.Filename, zapLogWithConnIDPattern)
	os.Remove(fileCfg.Filename)

	err = InitZapLogger(conf)
	c.Assert(err, IsNil)
	key := "ctxKey"
	val := "ctxValue"
	ctx1 := WithKeyValue(context.Background(), key, val)
	s.testZapLogger(ctx1, c, fileCfg.Filename, zapLogWithKeyValPattern)
	os.Remove(fileCfg.Filename)
}

func (s *testLogSuite) testZapLogger(ctx context.Context, c *C, fileName, pattern string) {
	Logger(ctx).Debug("debug msg", zap.String("test with key", "true"))
	Logger(ctx).Info("info msg", zap.String("test with key", "true"))
	Logger(ctx).Warn("warn msg", zap.String("test with key", "true"))
	Logger(ctx).Error("error msg", zap.String("test with key", "true"))

	f, err := os.Open(fileName)
	c.Assert(err, IsNil)
	defer f.Close()

	r := bufio.NewReader(f)
	for {
		var str string
		str, err = r.ReadString('\n')
		if err != nil {
			break
		}
		c.Assert(str, Matches, pattern)
		c.Assert(strings.Contains(str, "stack"), IsFalse)
		c.Assert(strings.Contains(str, "errorVerbose"), IsFalse)
	}
	c.Assert(err, Equals, io.EOF)
}

func (s *testLogSuite) TestSetLevel(c *C) {
	conf := NewLogConfig("info", DefaultLogFormat, "", EmptyFileLogConfig, false)
	err := InitZapLogger(conf)
	c.Assert(err, IsNil)

	c.Assert(log.GetLevel(), Equals, zap.InfoLevel)
	err = SetLevel("warn")
	c.Assert(err, IsNil)
	c.Assert(log.GetLevel(), Equals, zap.WarnLevel)
	err = SetLevel("Error")
	c.Assert(err, IsNil)
	c.Assert(log.GetLevel(), Equals, zap.ErrorLevel)
	err = SetLevel("DEBUG")
	c.Assert(err, IsNil)
	c.Assert(log.GetLevel(), Equals, zap.DebugLevel)
}

func TestGrpcLoggerCreation(t *testing.T) {
	level := "info"
	conf := NewLogConfig(level, DefaultLogFormat, "", EmptyFileLogConfig, false)
	_, p, err := initGRPCLogger(conf)
	// assert after init grpc logger, the original conf is not changed
	require.Equal(t, conf.Level, level)
	require.Nil(t, err)
	require.Equal(t, p.Level.Level(), zap.ErrorLevel)
	os.Setenv("GRPC_DEBUG", "1")
	defer os.Unsetenv("GRPC_DEBUG")
	_, newP, err := initGRPCLogger(conf)
	require.Nil(t, err)
	require.Equal(t, newP.Level.Level(), zap.DebugLevel)
}

func TestSlowQueryLoggerCreation(t *testing.T) {
	level := "warn"
	conf := NewLogConfig(level, DefaultLogFormat, "", EmptyFileLogConfig, false)
	_, prop, err := newSlowQueryLogger(conf)
	// assert after init slow query logger, the original conf is not changed
	require.Equal(t, conf.Level, level)
	require.Nil(t, err)
	require.Equal(t, prop.Level.String(), conf.Level)
}
