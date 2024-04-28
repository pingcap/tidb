// Copyright 2021 PingCAP, Inc.
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
	"testing"

	"github.com/pingcap/tidb/pkg/testkit/testsetup"
	"go.uber.org/goleak"
)

const (
	// zapLogWithoutCheckKeyPattern is used to match the zap log format but do not check some specified key, such as the following log:
	// [2019/02/13 15:56:05.385 +08:00] [INFO] [log_test.go:167] ["info message"]["str key"=val] ["int key"=123]
	zapLogWithoutCheckKeyPattern = `\[\d\d\d\d/\d\d/\d\d \d\d:\d\d:\d\d.\d\d\d\ (\+|-)\d\d:\d\d\] \[(FATAL|ERROR|WARN|INFO|DEBUG)\] \[([\w_%!$@.,+~-]+|\\.)+:\d+\] \[.*\] (\[.*=.*\]).*\n`
	// zapLogPatern is used to match the zap log format, such as the following log:
	// [2019/02/13 15:56:05.385 +08:00] [INFO] [log_test.go:167] ["info message"] [conn=conn1] ["str key"=val] ["int key"=123]
	zapLogWithConnIDPattern = `\[\d\d\d\d/\d\d/\d\d \d\d:\d\d:\d\d.\d\d\d\ (\+|-)\d\d:\d\d\] \[(FATAL|ERROR|WARN|INFO|DEBUG)\] \[([\w_%!$@.,+~-]+|\\.)+:\d+\] \[.*\] \[conn=.*\] (\[.*=.*\]).*\n`
	// [2019/02/13 15:56:05.385 +08:00] [INFO] [log_test.go:167] ["info message"] [conn=conn1] [session_alias=alias] ["str key"=val] ["int key"=123]
	zapLogWithTraceInfoPattern = `\[\d\d\d\d/\d\d/\d\d \d\d:\d\d:\d\d.\d\d\d\ (\+|-)\d\d:\d\d\] \[(FATAL|ERROR|WARN|INFO|DEBUG)\] \[([\w_%!$@.,+~-]+|\\.)+:\d+\] \[.*\] \[conn=.*\] \[session_alias=.*\] (\[.*=.*\]).*\n`
	// [2019/02/13 15:56:05.385 +08:00] [INFO] [log_test.go:167] ["info message"] [ctxKey=ctxKey1] ["str key"=val] ["int key"=123]
	zapLogWithKeyValPatternByCtx = `\[\d\d\d\d/\d\d/\d\d \d\d:\d\d:\d\d.\d\d\d\ (\+|-)\d\d:\d\d\] \[(FATAL|ERROR|WARN|INFO|DEBUG)\] \[([\w_%!$@.,+~-]+|\\.)+:\d+\] \[.*\] \[ctxKey=.*\] (\[.*=.*\]).*\n`
	// [2019/02/13 15:56:05.385 +08:00] [INFO] [log_test.go:167] ["info message"] [coreKey=coreKey1] ["str key"=val] ["int key"=123]
	zapLogWithKeyValPatternByCore = `\[\d\d\d\d/\d\d/\d\d \d\d:\d\d:\d\d.\d\d\d\ (\+|-)\d\d:\d\d\] \[(FATAL|ERROR|WARN|INFO|DEBUG)\] \[([\w_%!$@.,+~-]+|\\.)+:\d+\] \[.*\] \[coreKey=.*\] (\[.*=.*\]).*\n`
)

var (
	PrettyPrint = prettyPrint
)

func TestMain(m *testing.M) {
	testsetup.SetupForCommonTest()
	opts := []goleak.Option{
		goleak.IgnoreTopFunction("github.com/golang/glog.(*fileSink).flushDaemon"),
		goleak.IgnoreTopFunction("github.com/bazelbuild/rules_go/go/tools/bzltestutil.RegisterTimeoutHandler.func1"),
		goleak.IgnoreTopFunction("github.com/lestrrat-go/httprc.runFetchWorker"),
		goleak.IgnoreTopFunction("gopkg.in/natefinch/lumberjack%2ev2.(*Logger).millRun"),
	}
	goleak.VerifyTestMain(m, opts...)
}
