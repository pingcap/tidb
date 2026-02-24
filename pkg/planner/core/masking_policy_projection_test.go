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

package core_test

import (
	"testing"

	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func TestMaskingPolicyProjection(t *testing.T) {
	store, _ := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(id int primary key, c varchar(10))")
	tk.MustExec("insert into t values (1, 'a'), (2, 'b')")
	tk.MustExec("create masking policy p on t(c) as concat(c, 'x') enable")

	tk.MustQuery("select c from t order by id").Check(testkit.Rows("ax", "bx"))
	tk.MustQuery("select concat(c, '-') from t where c = 'a'").Check(testkit.Rows("ax-"))

	// Predicate should still use original value.
	rows := tk.MustQuery("select c from t where c = 'ax'").Rows()
	require.Len(t, rows, 0)
}

func TestMaskingPolicyBlobAndClob(t *testing.T) {
	store, _ := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t_blob_clob")
	tk.MustExec("create table t_blob_clob(id int primary key, c longtext, b longblob, b2 longblob)")
	tk.MustExec("insert into t_blob_clob values (1, 'secret', x'31323334', x'616263')")
	tk.MustExec("create masking policy p_clob on t_blob_clob(c) as mask_full(c, '#') enable")
	tk.MustExec("create masking policy p_blob_full on t_blob_clob(b) as mask_full(b, '*') enable")
	tk.MustExec("create masking policy p_blob_null on t_blob_clob(b2) as mask_null(b2) enable")

	tk.MustQuery("select c, hex(b), b2 is null from t_blob_clob").
		Check(testkit.Rows("###### 2A2A2A2A 1"))
}
