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
// See the License for the specific language governing permissions and
// limitations under the License.

package ranger_test

import (
	"fmt"
	"os"
	"testing"

	"github.com/pingcap/tidb/testkit/testdata"
	"github.com/pingcap/tidb/util/testbridge"
	"go.uber.org/goleak"
)

var td testdata.TestData

func TestMain(m *testing.M) {
	testbridge.WorkaroundGoCheckFlags()

	var err error
	td, err = testdata.LoadTestSuiteData("testdata", "ranger_suite")
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "testdata: Errors on loads test data from file: %v\n", err)
		os.Exit(1)
	}
	defer func() {
		_ = td.GenerateOutputIfNeeded()
	}()

	opts := []goleak.Option{
		goleak.IgnoreTopFunction("go.etcd.io/etcd/pkg/logutil.(*MergeLogger).outputLoop"),
		goleak.IgnoreTopFunction("go.opencensus.io/stats/view.(*worker).start"),
	}
	goleak.VerifyTestMain(m, opts...)
}
