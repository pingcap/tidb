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

// Copyright 2013 The ql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSES/QL-LICENSE file.

package session

import (
	"flag"
	"time"

	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

// WithMockUpgrade is a flag identify whether tests run with mock upgrading.
var WithMockUpgrade = flag.Bool("with-mock-upgrade", false, "whether tests run with mock upgrade")

var allDDLs = []string{
	"create unique index c3_index on mock_sys_partition (c1)",
	"alter table mock_sys_partition add primary key c3_index (c1)",
	"alter table mock_sys_t add primary key idx_pc2 (c2)",
	"alter table mock_sys_t drop primary key",
	"alter table mock_sys_t add unique index idx_uc2 (c2)",
	"alter table mock_sys_t add index idx_c2(c2)",
	"alter table mock_sys_t add column c4 bigint",
	"create table test_create_table(a int)",
	"drop table test_create_table",
	"create database test_create_db",
	"drop database test_create_db",
	"alter table mock_sys_t drop column c3",
	"alter table mock_sys_t_rebase auto_increment = 6000",
	"alter table mock_sys_t_auto shard_row_id_bits = 5",
	"alter table mock_sys_t modify column c11 mediumint",
	"alter table mock_sys_t modify column c11 int",
	"alter table mock_sys_t add column mayNullCol bigint default 1",
	"alter table mock_sys_t modify column mayNullCol bigint default 1 not null",
	"alter table mock_sys_t modify column c11 char(10)",
	"alter table mock_sys_t add constraint fk foreign key a(c1) references mock_sys_t_ref(c1)",
	"alter table mock_sys_t drop foreign key fk",
	"rename table mock_sys_t_rename1 to mock_sys_t_rename11",
	"rename table mock_sys_t_rename11 to mock_sys_t_rename111, mock_sys_t_rename2 to mock_sys_t_rename22",
	"alter table mock_sys_t_cs convert to charset utf8mb4",
	"alter database mock_sys_db_coll charset utf8mb4 collate utf8mb4_bin",
	"alter table mock_sys_partition truncate partition p3",
	"alter table mock_sys_t add column c41 bigint, add column c42 bigint",
	"alter table mock_sys_t drop column c41, drop column c42",
	"alter table mock_sys_t add index idx_v(c1)",
	"alter table mock_sys_t alter index idx_v invisible",
	//"alter table mock_sys_partition exchange partition p0 with table mock_sys_t_partition2",
	"alter table mock_sys_partition add partition (partition p6 values less than (8192))",
	"alter table mock_sys_partition drop partition p6",
	"alter table mock_sys_t add index mul_idx1(c1), add index mul_idx2(c1)",
	"alter table mock_sys_t drop index mul_idx1, drop index mul_idx2",
	"alter database mock_sys_db_placement placement policy = 'alter_x'",
	"alter table mock_sys_t add index rename_idx1(c1)",
	"alter table mock_sys_t rename index rename_idx1 to rename_idx2",
}

var mockLatestVer = currentBootstrapVersion + 1

func mockUpgradeToVerLatest(s Session, ver int64) {
	logutil.BgLogger().Info("mock upgrade to ver latest", zap.Int64("old ver", ver), zap.Int64("mock latest ver", mockLatestVer))
	if ver >= mockLatestVer {
		return
	}
	mustExecute(s, "use mysql")
	mustExecute(s, `create table if not exists mock_sys_partition(
		c1 int, c2 int, c3 int
	)
	partition by range( c1 ) (
		partition p0 values less than (1024),
		partition p1 values less than (2048),
		partition p2 values less than (3072),
		partition p3 values less than (4096),
		partition p4 values less than (7096)
	);`)
	mustExecute(s, `create table if not exists mock_sys_t(
		c1 int, c2 int, c3 int, c11 tinyint, index fk_c1(c1)
	);`)
	mustExecute(s, "create table mock_sys_t_rebase(c1 bigint auto_increment primary key, c2 bigint);")
	mustExecute(s, "create table mock_sys_t_auto(c1 int not null auto_increment unique) shard_row_id_bits = 0")
	mustExecute(s, "create table mock_sys_t_ref (c1 int key, c2 int, c3 int, c11 tinyint);")
	mustExecute(s, "create table mock_sys_t_rename1(c1 bigint, c2 bigint);")
	mustExecute(s, "create table mock_sys_t_rename2(c1 bigint, c2 bigint);")
	mustExecute(s, "create table mock_sys_t_cs(a varchar(10)) charset utf8")
	mustExecute(s, "create database mock_sys_db_coll default charset utf8 collate utf8_bin")
	mustExecute(s, "create table mock_sys_t_partition2(c1 int, c2 int, c3 int)")
	mustExecute(s, "set @@tidb_enable_exchange_partition=1")
	mustExecute(s, "create placement policy alter_x PRIMARY_REGION=\"cn-east-1\", REGIONS=\"cn-east-1\";")
	mustExecute(s, "create database mock_sys_db_placement")
	for _, sql := range allDDLs {
		mustExecute(s, sql)
		logutil.BgLogger().Info("mock upgrade exec", zap.String("sql", sql))
		time.Sleep(20 * time.Millisecond)
	}
}

func addMockBootstrapVersionForTest() {
	if !*WithMockUpgrade {
		return
	}
	bootstrapVersion = append(bootstrapVersion, mockUpgradeToVerLatest)
	currentBootstrapVersion++
}
