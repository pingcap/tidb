// Copyright 2023 PingCAP, Inc. Licensed under Apache-2.0.

package brietest

import (
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
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
	tmp := makeTempDirForBackup(t)
	sqlTmp := strings.ReplaceAll(tmp, "'", "''")

	log.SetLevel(zapcore.ErrorLevel)
	tk.MustExec("use test;")
	tk.MustExec("create table foo(pk int primary key auto_increment, v varchar(255));")
	tk.MustExec("insert into foo(v) values " + strings.TrimSuffix(strings.Repeat("('hello, world'),", 100), ",") + ";")
	backupQuery := fmt.Sprintf("backup database * to 'local://%s';", sqlTmp)
	_ = tk.MustQuery(backupQuery)
	// NOTE: we assume a auto-increamental ID here.
	// once we implement other ID allocation, we may have to change this case.
	res := tk.MustQuery("show br job query 1;")
	res.CheckContain(backupQuery)

	tk.MustExec("drop table foo;")
	restoreQuery := fmt.Sprintf("restore table test.foo from 'local://%s';", sqlTmp)
	tk.MustQuery(restoreQuery)
	res = tk.MustQuery("show br job query 2;")
	res.CheckContain(restoreQuery)
}

func TestCancel(t *testing.T) {
	tk := initTestKit(t)
	tk.MustExec("use test;")
	failpoint.Enable("github.com/pingcap/tidb/executor/block-on-brie", "return")

	req := require.New(t)
	ch := make(chan struct{})
	go func() {
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
	req.Eventually(check, 5 * time.Second, 1 * time.Second)

    select {
    case <-ch:
    case <-time.After(5 * time.Second):
        req.FailNow("the backup job doesn't be canceled")
    }
}
