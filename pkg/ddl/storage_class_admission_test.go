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
	"testing"

	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func TestStorageClassAdmission(t *testing.T) {
	defer config.RestoreFunc()()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.EnableIA = false
	})
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("USE test")
	// Admission is controlled only by the server configuration.
	tk.MustGetErrCode("SET GLOBAL tidb_enable_ia = ON", 1193)

	options := []string{
		`STORAGE_CLASS = 'IA'`,
		`ENGINE_ATTRIBUTE = '{"storage_class":"ia"}'`,
		`ENGINE_ATTRIBUTE = '{"storage_class":{"tier":"STANDARD","transitions":[{"tier":"IA","after_days":30}]}}'`,
		`ENGINE_ATTRIBUTE = '{"storage_class":[{"tier":"STANDARD"},{"tier":"IA","names_in":["p1"]}]}'`,
		// A policy targeting a future partition must also require admission.
		`ENGINE_ATTRIBUTE = '{"storage_class":{"tier":"IA","names_in":["future"]}}'`,
	}
	partition := " PARTITION BY RANGE (id) (PARTITION p0 VALUES LESS THAN (10), PARTITION p1 VALUES LESS THAN (20))"
	tk.MustExec("CREATE TABLE existing (id INT)" + partition)
	for _, option := range options {
		require.ErrorContains(t, tk.ExecToErr("CREATE TABLE denied (id INT) "+option+partition), "enable-ia")
		require.ErrorContains(t, tk.ExecToErr("ALTER TABLE existing "+option), "enable-ia")
	}

	// Reject before other options in the same ALTER statement can take effect.
	require.ErrorContains(t, tk.ExecToErr("ALTER TABLE existing COMMENT='changed' STORAGE_CLASS='IA'"), "enable-ia")
	tk.MustQuery("SELECT table_comment FROM information_schema.tables WHERE table_schema='test' AND table_name='existing'").Check(testkit.Rows(""))
	require.ErrorContains(t, tk.ExecToErr("ALTER TABLE existing ADD COLUMN v INT, STORAGE_CLASS='IA'"), "enable-ia")
	tk.MustQuery("SELECT column_name FROM information_schema.columns WHERE table_schema='test' AND table_name='existing'").Check(testkit.Rows("id"))

	config.UpdateGlobal(func(conf *config.Config) {
		conf.EnableIA = true
	})
	for _, option := range options {
		tk.MustExec("CREATE TABLE admitted (id INT) " + option + partition)
		tk.MustExec("ALTER TABLE existing " + option)
		tk.MustExec("DROP TABLE admitted")
	}
	tk.MustExec("CREATE TABLE ia (id INT) STORAGE_CLASS='IA'" + partition)
	tk.MustExec("CREATE TABLE scheduled (id INT) " + options[2])
	tk.MustExec("CREATE TABLE future (id INT) " + options[4] + partition)
	tk.MustExec("CREATE TABLE copied LIKE ia")
	tk.MustExec("DROP TABLE copied")
	config.UpdateGlobal(func(conf *config.Config) {
		conf.EnableIA = false
	})
	for _, table := range []string{"ia", "scheduled", "future"} {
		require.ErrorContains(t, tk.ExecToErr("CREATE TABLE copied LIKE "+table), "enable-ia")
		require.NotEmpty(t, tk.MustQuery("SHOW CREATE TABLE "+table).Rows())
	}
	tk.MustExec("INSERT INTO ia VALUES (1)")
	tk.MustQuery("SELECT * FROM ia").Check(testkit.Rows("1"))
	tk.MustExec("ALTER TABLE ia ADD COLUMN v INT DEFAULT 7")
	tk.MustExec("ALTER TABLE ia ADD PARTITION (PARTITION p2 VALUES LESS THAN (30))")
	tk.MustQuery("SELECT tidb_storage_class FROM information_schema.partitions WHERE table_schema='test' AND table_name='ia' AND partition_name='p2'").Check(testkit.Rows("IA"))
	tk.MustExec("ALTER TABLE future ADD PARTITION (PARTITION future VALUES LESS THAN (30))")
	tk.MustQuery("SELECT tidb_storage_class FROM information_schema.partitions WHERE table_schema='test' AND table_name='future' AND partition_name='future'").Check(testkit.Rows("IA"))
	tk.MustExec("ALTER TABLE ia STORAGE_CLASS='STANDARD'")
	tk.MustExec(`ALTER TABLE scheduled ENGINE_ATTRIBUTE = '{"storage_class":"STANDARD"}'`)
	tk.MustExec("CREATE TABLE standard (id INT) STORAGE_CLASS='STANDARD'")
	tk.MustExec("CREATE TABLE standard_copy LIKE standard")
	tk.MustQuery("SELECT * FROM ia").Check(testkit.Rows("1 7"))
}

func TestStorageClassVisibility(t *testing.T) {
	defer config.RestoreFunc()()
	for _, enabled := range []bool{false, true} {
		name := "disabled"
		if enabled {
			name = "enabled"
		}
		t.Run(name, func(t *testing.T) {
			config.UpdateGlobal(func(conf *config.Config) {
				conf.EnableIA = enabled
			})
			store := testkit.CreateMockStore(t)
			tk := testkit.NewTestKit(t, store)
			tables := tk.MustQuery("SHOW TABLES FROM information_schema LIKE 'TIKV_STORAGE_CLASS_TRANSITIONS'")
			metadata := tk.MustQuery("SELECT table_name FROM information_schema.tables WHERE table_schema='INFORMATION_SCHEMA' AND table_name='TIKV_STORAGE_CLASS_TRANSITIONS'")
			columns := tk.MustQuery("SELECT column_name FROM information_schema.columns WHERE table_schema='INFORMATION_SCHEMA' AND table_name='TIKV_STORAGE_CLASS_TRANSITIONS'")
			if enabled {
				tables.Check(testkit.Rows("TIKV_STORAGE_CLASS_TRANSITIONS"))
				metadata.Check(testkit.Rows("TIKV_STORAGE_CLASS_TRANSITIONS"))
				require.Len(t, columns.Rows(), 12)
				tk.MustQuery("SELECT * FROM information_schema.tikv_storage_class_transitions LIMIT 0").Check(testkit.Rows())
				tk.MustQuery("SHOW STORAGE_CLASS TRANSITIONS WHERE 0").Check(testkit.Rows())
			} else {
				tables.Check(testkit.Rows())
				metadata.Check(testkit.Rows())
				columns.Check(testkit.Rows())
				tk.MustGetErrCode("SELECT * FROM information_schema.tikv_storage_class_transitions", 1146)
				tk.MustGetErrCode("SHOW STORAGE_CLASS TRANSITIONS", 8200)
				tk.MustGetErrCode("SHOW STORAGE_CLASS TRANSITIONS LIKE 't%'", 8200)
				tk.MustGetErrCode("SHOW STORAGE_CLASS TRANSITIONS WHERE DIRECTION='TO_IA'", 8200)
			}
			// Shared metadata columns and durable history remain available.
			tk.MustQuery("SELECT table_name FROM information_schema.columns WHERE table_schema='INFORMATION_SCHEMA' AND table_name IN ('TABLES','PARTITIONS') AND column_name='TIDB_STORAGE_CLASS' ORDER BY table_name").Check(testkit.Rows("PARTITIONS", "TABLES"))
			if kerneltype.IsNextGen() {
				tk.MustQuery("SELECT * FROM mysql.tidb_storage_class_transition_history LIMIT 0").Check(testkit.Rows())
			}
		})
	}
}
