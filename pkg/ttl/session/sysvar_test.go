// Copyright 2022 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package session_test

import (
	"fmt"
	"strconv"
	"testing"

	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func TestSysVarTTLJobEnable(t *testing.T) {
	origEnableDDL := vardef.EnableTTLJob.Load()
	defer func() {
		vardef.EnableTTLJob.Store(origEnableDDL)
	}()

	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set @@global.tidb_ttl_job_enable=0")
	require.False(t, vardef.EnableTTLJob.Load())
	tk.MustQuery("select @@global.tidb_ttl_job_enable").Check(testkit.Rows("0"))
	tk.MustQuery("select @@tidb_ttl_job_enable").Check(testkit.Rows("0"))

	tk.MustExec("set @@global.tidb_ttl_job_enable=1")
	require.True(t, vardef.EnableTTLJob.Load())
	tk.MustQuery("select @@global.tidb_ttl_job_enable").Check(testkit.Rows("1"))
	tk.MustQuery("select @@tidb_ttl_job_enable").Check(testkit.Rows("1"))

	tk.MustExec("set @@global.tidb_ttl_job_enable=0")
	require.False(t, vardef.EnableTTLJob.Load())
	tk.MustQuery("select @@global.tidb_ttl_job_enable").Check(testkit.Rows("0"))
	tk.MustQuery("select @@tidb_ttl_job_enable").Check(testkit.Rows("0"))
}

func TestSysVarTTLScanBatchSize(t *testing.T) {
	origScanBatchSize := vardef.TTLScanBatchSize.Load()
	defer func() {
		vardef.TTLScanBatchSize.Store(origScanBatchSize)
	}()

	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set @@global.tidb_ttl_scan_batch_size=789")
	require.Equal(t, int64(789), vardef.TTLScanBatchSize.Load())
	tk.MustQuery("select @@global.tidb_ttl_scan_batch_size").Check(testkit.Rows("789"))
	tk.MustQuery("select @@tidb_ttl_scan_batch_size").Check(testkit.Rows("789"))

	tk.MustExec("set @@global.tidb_ttl_scan_batch_size=0")
	require.Equal(t, int64(1), vardef.TTLScanBatchSize.Load())
	tk.MustQuery("select @@global.tidb_ttl_scan_batch_size").Check(testkit.Rows("1"))
	tk.MustQuery("select @@tidb_ttl_scan_batch_size").Check(testkit.Rows("1"))

	maxVal := int64(vardef.DefTiDBTTLScanBatchMaxSize)
	tk.MustExec(fmt.Sprintf("set @@global.tidb_ttl_scan_batch_size=%d", maxVal+1))
	require.Equal(t, maxVal, vardef.TTLScanBatchSize.Load())
	tk.MustQuery("select @@global.tidb_ttl_scan_batch_size").Check(testkit.Rows(strconv.FormatInt(maxVal, 10)))
	tk.MustQuery("select @@tidb_ttl_scan_batch_size").Check(testkit.Rows(strconv.FormatInt(maxVal, 10)))
}

func TestSysVarTTLScanDeleteBatchSize(t *testing.T) {
	origScanBatchSize := vardef.TTLScanBatchSize.Load()
	defer func() {
		vardef.TTLScanBatchSize.Store(origScanBatchSize)
	}()

	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set @@global.tidb_ttl_delete_batch_size=789")
	require.Equal(t, int64(789), vardef.TTLDeleteBatchSize.Load())
	tk.MustQuery("select @@global.tidb_ttl_delete_batch_size").Check(testkit.Rows("789"))
	tk.MustQuery("select @@tidb_ttl_delete_batch_size").Check(testkit.Rows("789"))

	tk.MustExec("set @@global.tidb_ttl_delete_batch_size=0")
	require.Equal(t, int64(1), vardef.TTLDeleteBatchSize.Load())
	tk.MustQuery("select @@global.tidb_ttl_delete_batch_size").Check(testkit.Rows("1"))
	tk.MustQuery("select @@tidb_ttl_delete_batch_size").Check(testkit.Rows("1"))

	maxVal := int64(vardef.DefTiDBTTLDeleteBatchMaxSize)
	tk.MustExec(fmt.Sprintf("set @@global.tidb_ttl_delete_batch_size=%d", maxVal+1))
	require.Equal(t, maxVal, vardef.TTLDeleteBatchSize.Load())
	tk.MustQuery("select @@global.tidb_ttl_delete_batch_size").Check(testkit.Rows(strconv.FormatInt(maxVal, 10)))
	tk.MustQuery("select @@tidb_ttl_delete_batch_size").Check(testkit.Rows(strconv.FormatInt(maxVal, 10)))
}

func TestSysVarTTLScanDeleteLimit(t *testing.T) {
	origDeleteLimit := vardef.TTLDeleteRateLimit.Load()
	defer func() {
		vardef.TTLDeleteRateLimit.Store(origDeleteLimit)
	}()

	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustQuery("select @@global.tidb_ttl_delete_rate_limit").Check(testkit.Rows("0"))

	tk.MustExec("set @@global.tidb_ttl_delete_rate_limit=100000")
	require.Equal(t, int64(100000), vardef.TTLDeleteRateLimit.Load())
	tk.MustQuery("select @@global.tidb_ttl_delete_rate_limit").Check(testkit.Rows("100000"))
	tk.MustQuery("select @@tidb_ttl_delete_rate_limit").Check(testkit.Rows("100000"))

	tk.MustExec("set @@global.tidb_ttl_delete_rate_limit=0")
	require.Equal(t, int64(0), vardef.TTLDeleteRateLimit.Load())
	tk.MustQuery("select @@global.tidb_ttl_delete_rate_limit").Check(testkit.Rows("0"))
	tk.MustQuery("select @@tidb_ttl_delete_rate_limit").Check(testkit.Rows("0"))

	tk.MustExec("set @@global.tidb_ttl_delete_rate_limit=-1")
	require.Equal(t, int64(0), vardef.TTLDeleteRateLimit.Load())
	tk.MustQuery("select @@global.tidb_ttl_delete_rate_limit").Check(testkit.Rows("0"))
	tk.MustQuery("select @@tidb_ttl_delete_rate_limit").Check(testkit.Rows("0"))
}
