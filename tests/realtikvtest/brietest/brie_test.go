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

package brietest

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/executor"
	"github.com/pingcap/tidb/pkg/session"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zapcore"
)

func makeTempDirForBackup(t *testing.T) string {
	d, err := os.MkdirTemp(os.TempDir(), "briesql-*")
	require.NoError(t, err)
	t.Cleanup(func() {
		os.RemoveAll(d)
	})
	return d
}

func TestShowBackupQuery(t *testing.T) {
	tk := initTestKit(t)
	executor.ResetGlobalBRIEQueueForTest()
	tmp := makeTempDirForBackup(t)
	sqlTmp := strings.ReplaceAll(tmp, "'", "''")

	log.SetLevel(zapcore.ErrorLevel)
	tk.MustExec("use test;")
	tk.MustExec("create table foo(pk int primary key auto_increment, v varchar(255));")
	tk.MustExec("insert into foo(v) values " + strings.TrimSuffix(strings.Repeat("('hello, world'),", 100), ",") + ";")
	backupQuery := fmt.Sprintf("BACKUP DATABASE * TO 'local://%s'", sqlTmp)
	_ = tk.MustQuery(backupQuery)
	// NOTE: we assume a auto-increamental ID here.
	// once we implement other ID allocation, we may have to change this case.
	res := tk.MustQuery("show br job query 1;")
	fmt.Println(res.Rows())
	res.CheckContain(backupQuery)

	tk.MustExec("drop table foo;")
	restoreQuery := fmt.Sprintf("RESTORE TABLE `test`.`foo` FROM 'local://%s'", sqlTmp)
	tk.MustQuery(restoreQuery)
	res = tk.MustQuery("show br job query 2;")
	tk.MustExec("drop table foo;")
	res.CheckContain(restoreQuery)
}

func TestShowBackupQueryRedact(t *testing.T) {
	tk := initTestKit(t)

	executor.ResetGlobalBRIEQueueForTest()
	failpoint.Enable("github.com/pingcap/tidb/pkg/executor/block-on-brie", "return")
	defer failpoint.Disable("github.com/pingcap/tidb/pkg/executor/block-on-brie")
	ch := make(chan any)
	go func() {
		tk := testkit.NewTestKit(t, tk.Session().GetStore())
		err := tk.QueryToErr("backup database * to 's3://nonexist/real?endpoint=http://127.0.0.1&access-key=notleaked&secret-access-key=notleaked'")
		require.Error(t, err)
		close(ch)
	}()

	check := func() bool {
		res := tk.MustQuery("show br job query 1;")
		rs := res.Rows()
		if len(rs) == 0 {
			return false
		}
		theItem := rs[0][0].(string)
		if strings.Contains(theItem, "secret-access-key") {
			t.Fatalf("The secret key not redacted: %q", theItem)
		}
		fmt.Println(theItem)
		res.CheckContain("BACKUP DATABASE * TO 's3://nonexist/real'")
		return true
	}
	require.Eventually(t, check, 5*time.Second, 1*time.Second)
	tk.MustExec("cancel br job 1;")
	// Make sure the background job returns.
	// So `goleak` would be happy.
	<-ch
}

func TestCancel(t *testing.T) {
	tk := initTestKit(t)
	executor.ResetGlobalBRIEQueueForTest()
	tk.MustExec("use test;")
	failpoint.Enable("github.com/pingcap/tidb/pkg/executor/block-on-brie", "return")
	defer failpoint.Disable("github.com/pingcap/tidb/pkg/executor/block-on-brie")

	req := require.New(t)
	ch := make(chan struct{})
	go func() {
		tk := testkit.NewTestKit(t, tk.Session().GetStore())
		err := tk.QueryToErr("backup database * to 'noop://'")
		req.Error(err)
		close(ch)
	}()

	check := func() bool {
		wb := tk.Session().GetSessionVars().StmtCtx.WarningCount()
		tk.MustExec("cancel br job 1;")
		wa := tk.Session().GetSessionVars().StmtCtx.WarningCount()
		return wb == wa
	}
	req.Eventually(check, 5*time.Second, 1*time.Second)

	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		req.FailNow("the backup job doesn't be canceled")
	}
}

func TestExistedTables(t *testing.T) {
	tk := initTestKit(t)
	tmp := makeTempDirForBackup(t)
	sqlTmp := strings.ReplaceAll(tmp, "'", "''")
	executor.ResetGlobalBRIEQueueForTest()
	tk.MustExec("use test;")
	for i := range 5 {
		tableName := fmt.Sprintf("foo%d", i)
		tk.MustExec(fmt.Sprintf("create table %s(pk int primary key auto_increment, v varchar(255));", tableName))
		tk.MustExec(fmt.Sprintf("insert into %s(v) values %s;", tableName, strings.TrimSuffix(strings.Repeat("('hello, world'),", 100), ",")))
	}

	// full backup
	done := make(chan struct{})
	go func() {
		defer close(done)
		backupQuery := fmt.Sprintf("BACKUP DATABASE * TO 'local://%s/full'", sqlTmp)
		_ = tk.MustQuery(backupQuery)
	}()
	select {
	case <-time.After(20 * time.Second):
		t.Fatal("Backup operation exceeded")
	case <-done:
	}

	done = make(chan struct{})
	go func() {
		defer close(done)
		restoreQuery := fmt.Sprintf("RESTORE DATABASE * FROM 'local://%s/full'", sqlTmp)
		res, err := tk.Exec(restoreQuery)
		require.NoError(t, err)

		_, err = session.ResultSetToStringSlice(context.Background(), tk.Session(), res)
		require.ErrorContains(t, err, "table already exists")
	}()
	select {
	case <-time.After(20 * time.Second):
		t.Fatal("Restore operation exceeded")
	case <-done:
	}

	// db level backup
	done = make(chan struct{})
	go func() {
		defer close(done)
		backupQuery := fmt.Sprintf("BACKUP DATABASE test TO 'local://%s/db'", sqlTmp)
		_ = tk.MustQuery(backupQuery)
	}()
	select {
	case <-time.After(20 * time.Second):
		t.Fatal("Backup operation exceeded")
	case <-done:
	}

	done = make(chan struct{})
	go func() {
		defer close(done)
		restoreQuery := fmt.Sprintf("Restore DATABASE test FROM 'local://%s/db'", sqlTmp)
		res, err := tk.Exec(restoreQuery)
		require.NoError(t, err)

		_, err = session.ResultSetToStringSlice(context.Background(), tk.Session(), res)
		require.ErrorContains(t, err, "table already exists")
	}()
	select {
	case <-time.After(20 * time.Second):
		t.Fatal("Restore operation exceeded")
	case <-done:
	}

	for i := range 5 {
		tableName := fmt.Sprintf("foo%d", i)
		tk.MustExec(fmt.Sprintf("drop table %s;", tableName))
	}
}

// full backup * -> incremental backup * -> restore full backup * -> restore incremental backup *
func TestExistedTablesOfIncremental(t *testing.T) {
	tk := initTestKit(t)
	tmp := makeTempDirForBackup(t)
	sqlTmp := strings.ReplaceAll(tmp, "'", "''")
	executor.ResetGlobalBRIEQueueForTest()
	tk.MustExec("use test;")
	for i := range 5 {
		tableName := fmt.Sprintf("foo%d", i)
		tk.MustExec(fmt.Sprintf("create table %s(pk int primary key auto_increment, v varchar(255));", tableName))
		tk.MustExec(fmt.Sprintf("insert into %s(v) values %s;", tableName, strings.TrimSuffix(strings.Repeat("('hello, world'),", 100), ",")))
	}

	// full backup
	backupQuery := fmt.Sprintf("BACKUP DATABASE * TO 'local://%s/full'", sqlTmp)
	res := tk.MustQuery(backupQuery)
	backupTs := res.Rows()[0][2].(string)

	// write incremental data
	for i := range 5 {
		tableName := fmt.Sprintf("foo%d", i)
		tk.MustExec(fmt.Sprintf("insert into %s(v) values %s;", tableName, strings.TrimSuffix(strings.Repeat("('hello, world'),", 100), ",")))
	}

	// incremental backup
	IncrementalBackupQuery := fmt.Sprintf("BACKUP DATABASE * TO 'local://%s/incremental' last_backup=%s", sqlTmp, backupTs)
	_ = tk.MustQuery(IncrementalBackupQuery)

	// clean up tables
	for i := range 5 {
		tableName := fmt.Sprintf("foo%d", i)
		tk.MustExec(fmt.Sprintf("drop table %s;", tableName))
	}

	// restore full backup
	restoreQuery := fmt.Sprintf("RESTORE DATABASE * FROM 'local://%s/full'", sqlTmp)
	_ = tk.MustQuery(restoreQuery)

	// restore incremental backup
	restoreIncrementalQuery := fmt.Sprintf("RESTORE DATABASE * FROM 'local://%s/incremental'", sqlTmp)
	_ = tk.MustQuery(restoreIncrementalQuery)

	for i := range 5 {
		tableName := fmt.Sprintf("foo%d", i)
		tk.MustExec(fmt.Sprintf("drop table %s;", tableName))
	}
}

// full backup * -> incremental backup * -> restore full backup `test` -> restore incremental backup `test`
func TestExistedTablesOfIncremental_1(t *testing.T) {
	tk := initTestKit(t)
	tmp := makeTempDirForBackup(t)
	sqlTmp := strings.ReplaceAll(tmp, "'", "''")
	executor.ResetGlobalBRIEQueueForTest()
	tk.MustExec("use test;")
	for i := range 5 {
		tableName := fmt.Sprintf("foo%d", i)
		tk.MustExec(fmt.Sprintf("create table %s(pk int primary key auto_increment, v varchar(255));", tableName))
		tk.MustExec(fmt.Sprintf("insert into %s(v) values %s;", tableName, strings.TrimSuffix(strings.Repeat("('hello, world'),", 100), ",")))
	}

	// full backup
	backupQuery := fmt.Sprintf("BACKUP DATABASE * TO 'local://%s/full'", sqlTmp)
	res := tk.MustQuery(backupQuery)
	backupTs := res.Rows()[0][2].(string)

	// write incremental data
	for i := range 5 {
		tableName := fmt.Sprintf("foo%d", i)
		tk.MustExec(fmt.Sprintf("insert into %s(v) values %s;", tableName, strings.TrimSuffix(strings.Repeat("('hello, world'),", 100), ",")))
	}

	// incremental backup
	IncrementalBackupQuery := fmt.Sprintf("BACKUP DATABASE * TO 'local://%s/incremental' last_backup=%s", sqlTmp, backupTs)
	_ = tk.MustQuery(IncrementalBackupQuery)

	// clean up tables
	for i := range 5 {
		tableName := fmt.Sprintf("foo%d", i)
		tk.MustExec(fmt.Sprintf("drop table %s;", tableName))
	}

	// restore full backup
	restoreQuery := fmt.Sprintf("RESTORE DATABASE test FROM 'local://%s/full'", sqlTmp)
	_ = tk.MustQuery(restoreQuery)

	// restore incremental backup
	restoreIncrementalQuery := fmt.Sprintf("RESTORE DATABASE test FROM 'local://%s/incremental'", sqlTmp)
	_ = tk.MustQuery(restoreIncrementalQuery)

	for i := range 5 {
		tableName := fmt.Sprintf("foo%d", i)
		tk.MustExec(fmt.Sprintf("drop table %s;", tableName))
	}
}

// full backup `test` -> incremental backup `test` -> restore full backup * -> restore incremental backup *
func TestExistedTablesOfIncremental_2(t *testing.T) {
	tk := initTestKit(t)
	tmp := makeTempDirForBackup(t)
	sqlTmp := strings.ReplaceAll(tmp, "'", "''")
	executor.ResetGlobalBRIEQueueForTest()
	tk.MustExec("use test;")
	for i := range 5 {
		tableName := fmt.Sprintf("foo%d", i)
		tk.MustExec(fmt.Sprintf("create table %s(pk int primary key auto_increment, v varchar(255));", tableName))
		tk.MustExec(fmt.Sprintf("insert into %s(v) values %s;", tableName, strings.TrimSuffix(strings.Repeat("('hello, world'),", 100), ",")))
	}

	// full backup
	backupQuery := fmt.Sprintf("BACKUP DATABASE test TO 'local://%s/full'", sqlTmp)
	res := tk.MustQuery(backupQuery)
	backupTs := res.Rows()[0][2].(string)

	// write incremental data
	for i := range 5 {
		tableName := fmt.Sprintf("foo%d", i)
		tk.MustExec(fmt.Sprintf("insert into %s(v) values %s;", tableName, strings.TrimSuffix(strings.Repeat("('hello, world'),", 100), ",")))
	}

	// incremental backup
	IncrementalBackupQuery := fmt.Sprintf("BACKUP DATABASE test TO 'local://%s/incremental' last_backup=%s", sqlTmp, backupTs)
	_ = tk.MustQuery(IncrementalBackupQuery)

	// clean up tables
	for i := range 5 {
		tableName := fmt.Sprintf("foo%d", i)
		tk.MustExec(fmt.Sprintf("drop table %s;", tableName))
	}

	// restore full backup
	restoreQuery := fmt.Sprintf("RESTORE DATABASE * FROM 'local://%s/full'", sqlTmp)
	_ = tk.MustQuery(restoreQuery)

	// restore incremental backup
	restoreIncrementalQuery := fmt.Sprintf("RESTORE DATABASE * FROM 'local://%s/incremental'", sqlTmp)
	_ = tk.MustQuery(restoreIncrementalQuery)

	for i := range 5 {
		tableName := fmt.Sprintf("foo%d", i)
		tk.MustExec(fmt.Sprintf("drop table %s;", tableName))
	}
}
