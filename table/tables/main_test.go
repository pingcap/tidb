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

package tables_test

import (
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/store/mockstore"
	"log"
	"os"
	"testing"

	"github.com/pingcap/tidb/util/testbridge"
	"go.uber.org/goleak"
)

var s = testIndexSuite{}

func TestMain(m *testing.M) {
	testbridge.WorkaroundGoCheckFlags()

	setUpIndexSuite()
	exitCode := m.Run()
	tearDownIndexSuite()
	if exitCode != 0 {
		os.Exit(exitCode)
	}

	opts := []goleak.Option{
		goleak.IgnoreTopFunction("go.etcd.io/etcd/pkg/logutil.(*MergeLogger).outputLoop"),
		goleak.IgnoreTopFunction("go.opencensus.io/stats/view.(*worker).start"),
	}
	goleak.VerifyTestMain(m, opts...)
}

func setUpIndexSuite() {
	println("setUpIndexSuite")
	store, err := mockstore.NewMockStore()
	if err != nil {
		log.Fatal(err)
	}
	s.s = store
	s.dom, err = session.BootstrapSession(store)
	if err != nil {
		log.Fatal(err)
	}
}

func tearDownIndexSuite() {
	println("tearDownIndexSuite")
	s.dom.Close()
	err := s.s.Close()
	if err != nil {
		log.Fatal(err)
	}
}
