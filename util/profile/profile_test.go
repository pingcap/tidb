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

	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/util/profile"
	"github.com/stretchr/testify/require"
)

func TestProfiles(t *testing.T) {
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

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("select * from performance_schema.tidb_profile_cpu")
	tk.MustExec("select * from performance_schema.tidb_profile_memory")
	tk.MustExec("select * from performance_schema.tidb_profile_allocs")
	tk.MustExec("select * from performance_schema.tidb_profile_mutex")
	tk.MustExec("select * from performance_schema.tidb_profile_block")
	tk.MustExec("select * from performance_schema.tidb_profile_goroutines")
}
