// Copyright 2018 PingCAP, Inc.
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

package perfschema_test

import (
	"path/filepath"
	"runtime"
	"testing"

	"github.com/pingcap/tidb/infoschema/perfschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/testkit"
	"github.com/stretchr/testify/require"
)

func TestPredefinedTables(t *testing.T) {
	require.True(t, perfschema.IsPredefinedTable("EVENTS_statements_summary_by_digest"))
	require.False(t, perfschema.IsPredefinedTable("statements"))
}

func TestPerfSchemaTables(t *testing.T) {
	store, clean := newMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use performance_schema")
	tk.MustQuery("select * from global_status where variable_name = 'Ssl_verify_mode'").Check(testkit.Rows())
	tk.MustQuery("select * from session_status where variable_name = 'Ssl_verify_mode'").Check(testkit.Rows())
	tk.MustQuery("select * from setup_actors").Check(testkit.Rows())
	tk.MustQuery("select * from events_stages_history_long").Check(testkit.Rows())
}

func newMockStore(t *testing.T) (store kv.Storage, clean func()) {
	var err error
	store, err = mockstore.NewMockStore()
	require.NoError(t, err)
	session.DisableStats4Test()

	dom, err := session.BootstrapSession(store)
	require.NoError(t, err)

	clean = func() {
		dom.Close()
		err := store.Close()
		require.NoError(t, err)
	}

	return
}

func currentSourceDir() string {
	_, file, _, _ := runtime.Caller(0)
	return filepath.Dir(file)
}
