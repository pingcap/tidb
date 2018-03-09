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
	"fmt"
	"io"
	"os"
	"runtime"
	"strings"
	"testing"

	. "github.com/pingcap/check"
	log "github.com/sirupsen/logrus"
)

const (
	logPattern = `\d\d\d\d/\d\d/\d\d \d\d:\d\d:\d\d\.\d\d\d ([\w_%!$@.,+~-]+|\\.)+:\d+: \[(fatal|error|warning|info|debug)\] .*?\n`
)

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
	conf := &LogConfig{Level: "warn", File: FileLogConfig{}}
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
	fmt.Println(entry, logPattern)
	c.Assert(strings.Contains(entry, "log_test.go"), IsTrue)
}

func (s *testLogSuite) TestSlowQueryLogger(c *C) {
	fileName := "slow_query"
	conf := &LogConfig{Level: "info", File: FileLogConfig{}, SlowQueryFile: fileName}
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
		c.Assert(str, Matches, logPattern)
	}
	c.Assert(err, Equals, io.EOF)
}

func (s *testLogSuite) TestSlowQueryLoggerKeepOrder(c *C) {
	fileName := "slow_query"
	conf := &LogConfig{Level: "warn", File: FileLogConfig{}, Format: "text", DisableTimestamp: true, SlowQueryFile: fileName}
	c.Assert(InitLogger(conf), IsNil)
	defer os.Remove(fileName)
	ft, ok := SlowQueryLogger.Formatter.(*textFormatter)
	c.Assert(ok, IsTrue)
	c.Assert(ft.EnableEntryOrder, IsTrue)
	SlowQueryLogger.Out = s.buf
	logEntry := log.NewEntry(SlowQueryLogger)
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
