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

	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/testkit/testsetup"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	testsetup.SetupForCommonTest()

	path, _ := os.MkdirTemp("", "oom-use-tmp-storage")
	config.UpdateGlobal(func(conf *config.Config) {
		conf.TempStoragePath = path
	})

	goleak.VerifyTestMain(wrapper{
		M:       m,
		cleanUp: func() { os.RemoveAll(path) },
	})
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
