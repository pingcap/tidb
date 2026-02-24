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

	"github.com/pingcap/tidb/pkg/errno"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func TestMaskingPolicyRestrictOnSubquerySources(t *testing.T) {
	store, _ := testkit.CreateMockStoreAndDomain(t)
	tkRoot := testkit.NewTestKit(t, store)
	require.NoError(t, tkRoot.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil, nil))
	tkRoot.MustExec("use test")

	tkRoot.MustExec("drop table if exists src_restrict, src_none, dst")
	tkRoot.MustExec("create table src_restrict(c varchar(20))")
	tkRoot.MustExec("create table src_none(c varchar(20))")
	tkRoot.MustExec("create table dst(c varchar(20))")
	tkRoot.MustExec("insert into src_restrict values ('secret')")
	tkRoot.MustExec("insert into src_none values ('secret')")

	tkRoot.MustExec(`create masking policy p_restrict on src_restrict(c) as
		case when current_user() = 'root@%' then c else mask_full(c, '*') end
		restrict on (insert_into_select, update_select, delete_select) enable`)
	tkRoot.MustExec(`create masking policy p_none on src_none(c) as
		case when current_user() = 'root@%' then c else mask_full(c, '*') end enable`)

	tkRoot.MustExec("create user if not exists 'u1'@'%'")
	tkRoot.MustExec("grant select, insert, update, delete on test.* to 'u1'@'%'")

	tkUser := testkit.NewTestKit(t, store)
	require.NoError(t, tkUser.Session().Auth(&auth.UserIdentity{Username: "u1", Hostname: "%"}, nil, nil, nil))
	tkUser.MustExec("use test")

	tkUser.MustGetErrCode("insert into dst select c from src_restrict", errno.ErrAccessDeniedToMaskedColumn)
	tkUser.MustGetErrCode("update dst set c = (select c from src_restrict limit 1)", errno.ErrAccessDeniedToMaskedColumn)
	tkUser.MustExec("insert into dst values ('secret')")
	tkUser.MustGetErrCode("delete from dst where c = (select c from src_restrict limit 1)", errno.ErrAccessDeniedToMaskedColumn)

	tkUser.MustExec("insert into dst select c from src_none")
	tkUser.MustQuery("select c from dst order by c").Check(testkit.Rows("******", "secret"))

	tkRoot.MustExec("insert into dst select c from src_restrict")
	tkRoot.MustExec("update dst set c = (select c from src_restrict limit 1)")
	tkRoot.MustExec("delete from dst where c = (select c from src_restrict limit 1)")
}
