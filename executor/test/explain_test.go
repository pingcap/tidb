// Copyright 2023 PingCAP, Inc.
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
package explain

import (
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/pingcap/tidb/testkit"
	"github.com/stretchr/testify/require"
)

func TestLockUnchangedUniqueKeys(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk1 := testkit.NewTestKit(t, store)
	tk2 := testkit.NewTestKit(t, store)
	tk1.MustExec("use test")
	tk2.MustExec("use test")

	for _, shouldLock := range []bool{true, false} {
		for _, tt := range []struct {
			name          string
			create        string
			insert        string
			update        string
			isClusteredPK bool
		}{
			{
				// ref https://github.com/pingcap/tidb/issues/36438
				"Issue36438",
				"create table t (i varchar(10), unique key(i))",
				"insert into t values ('a')",
				"update t set i = 'a'",
				false,
			},
			{
				"ClusteredAndRowUnchanged",
				"create table t (k int, v int, primary key(k) clustered, key sk(k))",
				"insert into t values (1, 10)",
				"update t force index(sk) set v = 10 where k = 1",
				true,
			},
			{
				"ClusteredAndRowUnchangedAndParted",
				"create table t (k int, v int, primary key(k) clustered, key sk(k)) partition by hash(k) partitions 4",
				"insert into t values (1, 10)",
				"update t force index(sk) set v = 10 where k = 1",
				true,
			},
			{
				"ClusteredAndRowChanged",
				"create table t (k int, v int, primary key(k) clustered, key sk(k))",
				"insert into t values (1, 10)",
				"update t force index(sk) set v = 11 where k = 1",
				true,
			},
			{
				"NonClusteredAndRowUnchanged",
				"create table t (k int, v int, primary key(k) nonclustered, key sk(k))",
				"insert into t values (1, 10)",
				"update t force index(sk) set v = 10 where k = 1",
				false,
			},
			{
				"NonClusteredAndRowUnchangedAndParted",
				"create table t (k int, v int, primary key(k) nonclustered, key sk(k)) partition by hash(k) partitions 4",
				"insert into t values (1, 10)",
				"update t force index(sk) set v = 10 where k = 1",
				false,
			},
			{
				"NonClusteredAndRowChanged",
				"create table t (k int, v int, primary key(k) nonclustered, key sk(k))",
				"insert into t values (1, 10)",
				"update t force index(sk) set v = 11 where k = 1",
				false,
			},
			{
				"UniqueAndRowUnchanged",
				"create table t (k int, v int, unique key uk(k), key sk(k))",
				"insert into t values (1, 10)",
				"update t force index(sk) set v = 10 where k = 1",
				false,
			},
			{
				"UniqueAndRowUnchangedAndParted",
				"create table t (k int, v int, unique key uk(k), key sk(k)) partition by hash(k) partitions 4",
				"insert into t values (1, 10)",
				"update t force index(sk) set v = 10 where k = 1",
				false,
			},
			{
				"UniqueAndRowChanged",
				"create table t (k int, v int, unique key uk(k), key sk(k))",
				"insert into t values (1, 10)",
				"update t force index(sk) set v = 11 where k = 1",
				false,
			},
		} {
			t.Run(
				tt.name+"-"+strconv.FormatBool(shouldLock), func(t *testing.T) {
					tk1.MustExec(fmt.Sprintf("set @@tidb_lock_unchanged_keys = %v", shouldLock))
					tk1.MustExec("drop table if exists t")
					tk1.MustExec(tt.create)
					tk1.MustExec(tt.insert)
					tk1.MustExec("begin pessimistic")

					tk1.MustExec(tt.update)

					errCh := make(chan error, 1)
					go func() {
						_, err := tk2.Exec(tt.insert)
						errCh <- err
					}()

					select {
					case <-time.After(100 * time.Millisecond):
						if !shouldLock && !tt.isClusteredPK {
							require.Fail(t, "insert is blocked by update")
						}
						tk1.MustExec("rollback")
						require.Error(t, <-errCh)
					case err := <-errCh:
						require.Error(t, err)
						if shouldLock {
							require.Fail(t, "insert is not blocked by update")
						}
					}
				},
			)
		}
	}
}
