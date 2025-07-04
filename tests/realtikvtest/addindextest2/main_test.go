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

package addindextest

import (
	"flag"
	"testing"

	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/tests/realtikvtest"
	"github.com/pingcap/tidb/tests/realtikvtest/testutils"
)

// FullMode is a flag identify it should be run in full mode.
// In full mode, the test will run all the cases.
var FullMode = flag.Bool("full-mode", false, "whether tests run in full mode")

func TestMain(m *testing.M) {
	config.UpdateGlobal(func(conf *config.Config) {
		conf.Store = config.StoreTypeTiKV
	})
	testutils.UpdateTiDBConfig()
	realtikvtest.RunTestMain(m)
}
