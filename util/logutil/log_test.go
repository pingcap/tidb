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
	"fmt"
	"io"
	"os"
	"runtime"
	"strings"
	"testing"

	. "github.com/pingcap/check"
	zaplog "github.com/pingcap/log"
	log "github.com/sirupsen/logrus"
	"go.uber.org/zap"
)

const (
	logPattern = `\d\d\d\d/\d\d/\d\d \d\d:\d\d:\d\d\.\d\d\d ([\w_%!$@.,+~-]+|\\.)+:\d+: \[(fatal|error|warning|info|debug)\] .*?\n`
	// zapLogPatern is used to match the zap log format, such as the following log:
	// [2019/02/13 15:56:05.385 +08:00] [INFO] [log_test.go:167] ["info message"] ["str key"=val] ["int key"=123]
	zapLogPattern = `\[\d\d\d\d/\d\d/\d\d \d\d:\d\d:\d\d.\d\d\d\ (\+|-)\d\d:\d\d\] \[(FATAL|ERROR|WARN|INFO|DEBUG)\] \[([\w_%!$@.,+~-]+|\\.)+:\d+\] \[.*\] (\[.*=.*\]).*\n`
	// [2019/02/13 15:56:05.385 +08:00] [INFO] [log_test.go:167] ["info message"] ["str key"=val] ["int key"=123]
	zapLogWithConnIDPattern = `\[\d\d\d\d/\d\d/\d\d \d\d:\d\d:\d\d.\d\d\d\ (\+|-)\d\d:\d\d\] \[(FATAL|ERROR|WARN|INFO|DEBUG)\] \[([\w_%!$@.,+~-]+|\\.)+:\d+\] \[.*\] \[conn=.*\] (\[.*=.*\]).*\n`
	// [2019/02/13 15:56:05.385 +08:00] [INFO] [log_test.go:167] ["info message"] ["str key"=val] ["int key"=123]
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

func (s *testLogSuite) SetUpSuite(c *C) {
	s.buf = &bytes.Buffer{}
}

func (s *testLogSuite) SetUpTest(c *C) {
	s.buf = &bytes.Buffer{}
}

func (s *testLogSuite) TestStringToLogLevel(c *C) {
	c.Assert(stringToLogLevel("fatal"), Equals, log.FatalLevel)
	c.Assert(stringToLogLevel("ERROR"), Equals, log.ErrorLevel)
	c.Assert(stringToLogLevel("warn"), Equals, log.WarnLevel)
	c.Assert(stringToLogLevel("warning"), Equals, log.WarnLevel)
	c.Assert(stringToLogLevel("debug"), Equals, log.DebugLevel)
	c.Assert(stringToLogLevel("info"), Equals, log.InfoLevel)
	c.Assert(stringToLogLevel("whatever"), Equals, log.InfoLevel)
}

// TestLogging assure log format and log redirection works.
func (s *testLogSuite) TestLogging(c *C) {
	conf := NewLogConfig("warn", DefaultLogFormat, "", NewFileLogConfig(0), false)
	conf.File.Filename = "log_file"
	c.Assert(InitLogger(conf), IsNil)

	log.SetOutput(s.buf)

	log.Infof("[this message should not be sent to buf]")
	c.Assert(s.buf.Len(), Equals, 0)

	log.Warningf("[this message should be sent to buf]")
	entry, err := s.buf.ReadString('\n')
	c.Assert(err, IsNil)
	c.Assert(entry, Matches, logPattern)

	log.Warnf("this message comes from logrus")
	entry, err = s.buf.ReadString('\n')
	c.Assert(err, IsNil)
	c.Assert(entry, Matches, logPattern)
	c.Assert(strings.Contains(entry, "log_test.go"), IsTrue)
}

func (s *testLogSuite) TestSlowQueryLogger(c *C) {
	fileName := "slow_query"
	os.Remove(fileName)
	conf := NewLogConfig("info", DefaultLogFormat, fileName, NewFileLogConfig(DefaultLogMaxSize), false)
	c.Assert(conf.File.MaxSize, Equals, DefaultLogMaxSize)
	err := InitLogger(conf)
	c.Assert(err, IsNil)
	defer os.Remove(fileName)

	SlowQueryLogger.Debug("debug message")
	SlowQueryLogger.Info("info message")
	SlowQueryLogger.Warn("warn message")
	SlowQueryLogger.Error("error message")
	c.Assert(s.buf.Len(), Equals, 0)

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
		if strings.HasPrefix(str, "# ") {
			c.Assert(str, Matches, `# Time: .*?\n`)
		} else {
			c.Assert(str, Matches, `.*? message\n`)
		}
	}
	c.Assert(err, Equals, io.EOF)
}

func (s *testLogSuite) TestLoggerKeepOrder(c *C) {
	conf := NewLogConfig("warn", DefaultLogFormat, "", EmptyFileLogConfig, true)
	c.Assert(InitLogger(conf), IsNil)
	logger := log.StandardLogger()
	ft, ok := logger.Formatter.(*textFormatter)
	c.Assert(ok, IsTrue)
	ft.EnableEntryOrder = true
	logger.Out = s.buf
	logEntry := log.NewEntry(logger)
	logEntry.Data = log.Fields{
		"connectionId": 1,
		"costTime":     "1",
		"database":     "test",
		"sql":          "select 1",
		"txnStartTS":   1,
	}

	_, _, line, _ := runtime.Caller(0)
	logEntry.WithField("type", "slow-query").WithField("succ", true).Warnf("slow-query")
	expectMsg := fmt.Sprintf("log_test.go:%v: [warning] slow-query connectionId=1 costTime=1 database=test sql=select 1 succ=true txnStartTS=1 type=slow-query\n", line+1)
	c.Assert(s.buf.String(), Equals, expectMsg)

	s.buf.Reset()
	logEntry.Data = log.Fields{
		"a": "a",
		"d": "d",
		"e": "e",
		"b": "b",
		"f": "f",
		"c": "c",
	}

	_, _, line, _ = runtime.Caller(0)
	logEntry.Warnf("slow-query")
	expectMsg = fmt.Sprintf("log_test.go:%v: [warning] slow-query a=a b=b c=c d=d e=e f=f\n", line+1)
	c.Assert(s.buf.String(), Equals, expectMsg)
}

func (s *testLogSuite) TestSlowQueryZapLogger(c *C) {
	if runtime.GOOS == "windows" {
		// Skip this test on windows for two reasons:
		// 1. The pattern match fails somehow. It seems windows treat \n as slash and character n.
		// 2. Remove file doesn't work as long as the log instance hold the file.
		c.Skip("skip on windows")
	}

	fileName := "slow_query"
	conf := NewLogConfig("info", DefaultLogFormat, fileName, EmptyFileLogConfig, false)
	err := InitZapLogger(conf)
	c.Assert(err, IsNil)
	defer os.Remove(fileName)

	SlowQueryZapLogger.Debug("debug message", zap.String("str key", "val"))
	SlowQueryZapLogger.Info("info message", zap.String("str key", "val"))
	SlowQueryZapLogger.Warn("warn", zap.Int("int key", 123))
	SlowQueryZapLogger.Error("error message", zap.Bool("bool key", true))

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
		c.Assert(str, Matches, zapLogPattern)
	}
	c.Assert(err, Equals, io.EOF)

}

func (s *testLogSuite) TestZapLoggerWithKeys(c *C) {
	if runtime.GOOS == "windows" {
		// Skip this test on windows for two reason:
		// 1. The pattern match fails somehow. It seems windows treat \n as slash and character n.
		// 2. Remove file doesn't work as long as the log instance hold the file.
		c.Skip("skip on windows")
	}

	fileCfg := FileLogConfig{zaplog.FileLogConfig{Filename: "zap_log", MaxSize: 4096}}
	conf := NewLogConfig("info", DefaultLogFormat, "", fileCfg, false)
	err := InitZapLogger(conf)
	c.Assert(err, IsNil)
	connID := uint32(123)
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

	c.Assert(zaplog.GetLevel(), Equals, zap.InfoLevel)
	err = SetLevel("warn")
	c.Assert(err, IsNil)
	c.Assert(zaplog.GetLevel(), Equals, zap.WarnLevel)
	err = SetLevel("Error")
	c.Assert(err, IsNil)
	c.Assert(zaplog.GetLevel(), Equals, zap.ErrorLevel)
	err = SetLevel("DEBUG")
	c.Assert(err, IsNil)
	c.Assert(zaplog.GetLevel(), Equals, zap.DebugLevel)
}
