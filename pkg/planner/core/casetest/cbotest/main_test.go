// Copyright 2023 PingCAP, Inc.
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

package cbotest

import (
	"archive/zip"
	"flag"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/pingcap/tidb/pkg/testkit/testdata"
	"github.com/pingcap/tidb/pkg/testkit/testmain"
	"github.com/pingcap/tidb/pkg/testkit/testsetup"
	"go.uber.org/goleak"
)

var testDataMap = make(testdata.BookKeeper)

func TestMain(m *testing.M) {
	testsetup.SetupForCommonTest()
	flag.Parse()
	_, file, _, _ := runtime.Caller(0)
	pkgDir := filepath.Dir(file)
	testDataMap.LoadTestSuiteData("testdata", "analyze_suite", true)
	// unzip stats.zip in local testdata
	testdataDir := filepath.Join(pkgDir, "testdata")
	zipPath := filepath.Join(testdataDir, "stats.zip")
	var extracted []string
	if _, err := os.Stat(zipPath); err == nil {
		extracted = unzip(zipPath, testdataDir)
	}
	opts := []goleak.Option{
		goleak.IgnoreTopFunction("github.com/golang/glog.(*fileSink).flushDaemon"),
		goleak.IgnoreTopFunction("github.com/bazelbuild/rules_go/go/tools/bzltestutil.RegisterTimeoutHandler.func1"),
		goleak.IgnoreTopFunction("github.com/lestrrat-go/httprc.runFetchWorker"),
		goleak.IgnoreTopFunction("go.etcd.io/etcd/client/pkg/v3/logutil.(*MergeLogger).outputLoop"),
		goleak.IgnoreTopFunction("gopkg.in/natefinch/lumberjack%2ev2.(*Logger).millRun"),
		goleak.IgnoreTopFunction("github.com/tikv/client-go/v2/txnkv/transaction.keepAlive"),
		goleak.IgnoreTopFunction("go.opencensus.io/stats/view.(*worker).start"),
	}

	callback := func(i int) int {
		testDataMap.GenerateOutputIfNeeded()
		for _, name := range extracted {
			_ = os.Remove(filepath.Join(testdataDir, name))
		}
		return i
	}
	goleak.VerifyTestMain(testmain.WrapTestingM(m, callback), opts...)
}

func GetAnalyzeSuiteData() testdata.TestData {
	return testDataMap["analyze_suite"]
}

func unzip(zipPath, dstDir string) []string {
	zipReader, err := zip.OpenReader(zipPath)
	if err != nil {
		panic(err)
	}
	defer zipReader.Close()

	names := make([]string, 0, len(zipReader.File))
	for _, file := range zipReader.File {
		name, err := extractFile(file, dstDir)
		if err != nil {
			panic(err)
		}
		names = append(names, name)
	}
	return names
}

func extractFile(file *zip.File, destDir string) (string, error) {
	filePath := filepath.Join(destDir, file.Name)

	if err := os.MkdirAll(filepath.Dir(filePath), 0755); err != nil {
		return "", err
	}

	destFile, err := os.OpenFile(filePath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, file.Mode())
	if err != nil {
		return "", err
	}
	defer destFile.Close()

	srcFile, err := file.Open()
	if err != nil {
		return "", err
	}
	defer srcFile.Close()
	_, err = io.Copy(destFile, srcFile)
	return file.Name, err
}
