// Copyright 2019 PingCAP, Inc.
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

package profile_test

import (
	"testing"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/session"
	"github.com/pingcap/tidb/pkg/store/mockstore"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/util/cpuprofile"
	"github.com/pingcap/tidb/pkg/util/profile"
	"github.com/stretchr/testify/require"
	"go.opencensus.io/stats/view"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

func TestProfiles(t *testing.T) {
	defer view.Stop()
	var err error
	var store kv.Storage
	var dom *domain.Domain

	store, err = mockstore.NewMockStore()
	require.NoError(t, err)
	defer func() {
		err := store.Close()
		require.NoError(t, err)
	}()

	session.DisableStats4Test()
	dom, err = session.BootstrapSession(store)
	require.NoError(t, err)
	defer dom.Close()

	oldValue := profile.CPUProfileInterval
	profile.CPUProfileInterval = 2 * time.Second
	defer func() {
		profile.CPUProfileInterval = oldValue
	}()
	require.NoError(t, cpuprofile.StartCPUProfiler())
	defer cpuprofile.StopCPUProfiler()

	tk := testkit.NewTestKit(t, store)
	core, recorded := observer.New(zap.InfoLevel)
	restore := log.ReplaceGlobals(zap.New(core), &log.ZapProperties{
		Core:  core,
		Level: zap.NewAtomicLevelAt(zap.InfoLevel),
	})
	defer restore()

	profileTables := []string{
		"tidb_profile_cpu",
		"tidb_profile_memory",
		"tidb_profile_allocs",
		"tidb_profile_mutex",
		"tidb_profile_block",
		"tidb_profile_goroutines",
	}
	for _, tableName := range profileTables {
		tk.MustQuery("select * from performance_schema." + tableName)
		require.Len(t, recorded.FilterMessage("profiling request received").
			FilterField(zap.String("table", "performance_schema."+tableName)).All(), 1)
	}
}
