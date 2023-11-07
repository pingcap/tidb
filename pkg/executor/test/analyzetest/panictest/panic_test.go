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

package panictest

import (
	"testing"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func TestPanicInHandleResultErrorWithSingleGoroutine(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("create table tbl_2 ( col_20 decimal default 84232 , col_21 tinyint not null , col_22 int default 80814394 , col_23 mediumint default -8036687 not null , col_24 smallint default 9185 not null , col_25 tinyint unsigned default 65 , col_26 char(115) default 'ZyfroRODMbNDRZnPNRW' not null , col_27 bigint not null , col_28 tinyint not null , col_29 char(130) default 'UMApsVgzHblmY' , primary key idx_14 ( col_28,col_22 ) , unique key idx_15 ( col_24,col_22 ) , key idx_16 ( col_21,col_20,col_24,col_25,col_27,col_28,col_26,col_29 ) , key idx_17 ( col_24,col_25 ) , unique key idx_18 ( col_25,col_23,col_29,col_27,col_26,col_22 ) , key idx_19 ( col_25,col_22,col_26,col_23 ) , unique key idx_20 ( col_22,col_24,col_28,col_29,col_26,col_20 ) , key idx_21 ( col_25,col_24,col_26,col_29,col_27,col_22,col_28 ) );")
	tk.MustExec("insert ignore into tbl_2 values ( 942,33,-1915007317,3408149,-3699,193,'Trywdis',1876334369465184864,115,null );")
	fp := "github.com/pingcap/tidb/pkg/executor/handleResultsErrorSingleThreadPanic"
	require.NoError(t, failpoint.Enable(fp, `panic("TestPanicInHandleResultErrorWithSingleGoroutine")`))
	defer func() {
		require.NoError(t, failpoint.Disable(fp))
	}()
	tk.MustExecToErr("analyze table tbl_2;")
}

func TestPanicInHandleAnalyzeWorkerPanic(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("create table tbl_2 ( col_20 decimal default 84232 , col_21 tinyint not null , col_22 int default 80814394 , col_23 mediumint default -8036687 not null , col_24 smallint default 9185 not null , col_25 tinyint unsigned default 65 , col_26 char(115) default 'ZyfroRODMbNDRZnPNRW' not null , col_27 bigint not null , col_28 tinyint not null , col_29 char(130) default 'UMApsVgzHblmY' , primary key idx_14 ( col_28,col_22 ) , unique key idx_15 ( col_24,col_22 ) , key idx_16 ( col_21,col_20,col_24,col_25,col_27,col_28,col_26,col_29 ) , key idx_17 ( col_24,col_25 ) , unique key idx_18 ( col_25,col_23,col_29,col_27,col_26,col_22 ) , key idx_19 ( col_25,col_22,col_26,col_23 ) , unique key idx_20 ( col_22,col_24,col_28,col_29,col_26,col_20 ) , key idx_21 ( col_25,col_24,col_26,col_29,col_27,col_22,col_28 ) );")
	tk.MustExec("insert ignore into tbl_2 values ( 942,33,-1915007317,3408149,-3699,193,'Trywdis',1876334369465184864,115,null );")
	fp := "github.com/pingcap/tidb/pkg/executor/handleAnalyzeWorkerPanic"
	require.NoError(t, failpoint.Enable(fp, `panic("TestPanicInHandleAnalyzeWorkerPanic")`))
	defer func() {
		require.NoError(t, failpoint.Disable(fp))
	}()
	tk.MustExecToErr("analyze table tbl_2;")
}
