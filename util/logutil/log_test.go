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
