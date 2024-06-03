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

package chunk

import (
	"os"
	"testing"

	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/testkit/testsetup"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	testsetup.SetupForCommonTest()

	path, _ := os.MkdirTemp("", "tidb_enable_tmp_storage_on_oom")
	config.UpdateGlobal(func(conf *config.Config) {
		conf.TempStoragePath = path
	})

	opts := []goleak.Option{
		goleak.IgnoreTopFunction("github.com/golang/glog.(*fileSink).flushDaemon"),
		goleak.IgnoreTopFunction("github.com/bazelbuild/rules_go/go/tools/bzltestutil.RegisterTimeoutHandler.func1"),
		goleak.IgnoreTopFunction("github.com/lestrrat-go/httprc.runFetchWorker"),
		goleak.IgnoreTopFunction("go.opencensus.io/stats/view.(*worker).start"),
	}

	goleak.VerifyTestMain(wrapper{
		M:       m,
		cleanUp: func() { os.RemoveAll(path) },
	}, opts...)
}

// wrap *testing.M to do the clean up after m.Run() and before os.Exit()
type wrapper struct {
	*testing.M
	cleanUp func()
}

func (m wrapper) Run() int {
	defer m.cleanUp()
	return m.M.Run()
}
