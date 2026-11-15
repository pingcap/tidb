// Copyright 2026 PingCAP, Inc.
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

package ddl_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/config"
	ddlutil "github.com/pingcap/tidb/pkg/ddl/util"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func TestRecoverAndFlashbackTableRejectSPFreshVectorIndex(t *testing.T) {
	spfreshBootstrapURL := mockSPFreshBootstrapEndpoint(t)
	restore := config.RestoreFunc()
	defer restore()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.TiKVAPIServiceAddrs = []string{spfreshBootstrapURL}
	})

	store, _ := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	defer func(originGC bool) {
		if originGC {
			ddlutil.EmulatorGCEnable()
		} else {
			ddlutil.EmulatorGCDisable()
		}
	}(ddlutil.IsEmulatorGCEnable())
	ddlutil.EmulatorGCDisable()

	safePoint := time.Now().Add(-48 * time.Hour).Format("20060102-15:04:05 -0700 MST")
	tk.MustExec(fmt.Sprintf(`
		insert high_priority into mysql.tidb values ('tikv_gc_safe_point', '%[1]s', '')
		on duplicate key update variable_value = '%[1]s'
	`, safePoint))

	tk.MustExec(`
		create table t_spfresh_recover(
			a int,
			b vector(3),
			vector index idx((VEC_COSINE_DISTANCE(b))) USING SPFresh
		);
	`)
	tk.MustExec("drop table t_spfresh_recover")
	dropJobID := tk.MustQuery("admin show ddl jobs 1").Rows()[0][0].(string)
	requireLatestDDLJob := func(jobID, jobType string) {
		row := tk.MustQuery("admin show ddl jobs 1").Rows()[0]
		require.Equal(t, jobID, row[0])
		require.Equal(t, jobType, row[3])
	}

	const unsupported = "recover/flashback table or schema with SPFresh vector index is not supported"
	tk.MustContainErrMsg("recover table t_spfresh_recover", unsupported)
	requireLatestDDLJob(dropJobID, "drop table")
	tk.MustContainErrMsg(fmt.Sprintf("recover table by job %s", dropJobID), unsupported)
	requireLatestDDLJob(dropJobID, "drop table")
	tk.MustContainErrMsg("flashback table t_spfresh_recover", unsupported)
	requireLatestDDLJob(dropJobID, "drop table")
	tk.MustContainErrMsg("flashback table t_spfresh_recover to t_spfresh_recovered", unsupported)
	requireLatestDDLJob(dropJobID, "drop table")
}
