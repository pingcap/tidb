// Copyright 2022 PingCAP, Inc.
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

package brietest

import (
	"os"
	"path"
	"testing"

	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/tests/realtikvtest"
	"github.com/stretchr/testify/require"
)

func TestBackupAndRestore(t *testing.T) {
	if !*realtikvtest.WithRealTiKV {
		t.Skip("only run BR SQL integration test with tikv store")
	}

	store, clean := realtikvtest.CreateMockStoreAndSetup(t)
	defer clean()

	cfg := config.GetGlobalConfig()
	cfg.Store = "tikv"
	cfg.Path = "127.0.0.1:2379"
	config.StoreGlobalConfig(cfg)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("create database if not exists br")
	tk.MustExec("use br")
	tk.MustExec("create table t1(v int)")
	tk.MustExec("insert into t1 values (1)")
	tk.MustExec("insert into t1 values (2)")
	tk.MustExec("insert into t1 values (3)")
	tk.MustQuery("select count(*) from t1").Check(testkit.Rows("3"))

	tk.MustExec("create database if not exists br02")
	tk.MustExec("use br02")
	tk.MustExec("create table t1(v int)")

	tmpDir := path.Join(os.TempDir(), "bk1")
	require.NoError(t, os.RemoveAll(tmpDir))
	// backup database to tmp dir
	tk.MustQuery("backup database br to 'local://" + tmpDir + "'")

	// remove database for recovery
	tk.MustExec("drop database br")
	tk.MustExec("drop database br02")

	// restore database with backup data
	tk.MustQuery("restore database * from 'local://" + tmpDir + "'")
	tk.MustExec("use br")
	tk.MustQuery("select count(*) from t1").Check(testkit.Rows("3"))
	tk.MustExec("drop database br")
}
