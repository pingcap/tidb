// Copyright 2023-2023 PingCAP Xingchen (Beijing) Technology Co., Ltd.

package executor_test

import (
	"testing"

	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func TestTableUserLoginHistory(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.Session().GetSessionVars().InRestrictedSQL = false

	// query from mysql.login_history and get 0 rows.
	rows := tk.MustQuery("SELECT * FROM mysql.login_history").Rows()
	require.Len(t, rows, 0)
	rows = tk.MustQuery("SELECT * FROM information_schema.user_login_history").Rows()
	require.Len(t, rows, 0)

	// insert into 2 records.
	tk.MustExec(`insert into mysql.LOGIN_HISTORY VALUES('2023-04-06 11:39:17.000000', "10.244.4.0", "root", "%", "mydb", 7602485189326930323, 'success', "127.0.0.100", "")`)
	tk.MustExec(`insert into mysql.login_history VALUES('2023-04-06 11:39:17.000000', "10.244.4.0", "zak", "%", "test", 7602485189326930000, "success", "127.0.0.1", "")`)
	tk.MustExec(`insert into mysql.login_history VALUES('2023-04-06 11:39:18.000000', "10.244.4.0", "root", "%", "test", 7602485189326930325, "success", "127.0.0.1", "")`)
	tk.MustExec(`insert into mysql.login_history VALUES('2023-04-06 11:40:17.000000', "10.244.4.0", "zak", "%", "test", 7602485189326930324, "success", "127.0.0.2", "")`)

	// query from mysql.login_history and get 2 rows.
	tk.MustQuery("SELECT * FROM mysql.login_history").Check(testkit.Rows(
		"2023-04-06 11:39:17.000000 10.244.4.0 root % mydb 7602485189326930323 success 127.0.0.100 ",
		"2023-04-06 11:39:17.000000 10.244.4.0 zak % test 7602485189326930000 success 127.0.0.1 ",
		"2023-04-06 11:39:18.000000 10.244.4.0 root % test 7602485189326930325 success 127.0.0.1 ",
		"2023-04-06 11:40:17.000000 10.244.4.0 zak % test 7602485189326930324 success 127.0.0.2 ",
	))

	// query from information_schema.user_login_history and get 1 rows.
	tk.Session().GetSessionVars().User = &auth.UserIdentity{
		Username:     "root",
		Hostname:     "127.0.0.1",
		AuthUsername: "root",
		AuthHostname: "%",
	}
	tk.MustQuery("SELECT * FROM information_schema.user_login_history").Check(testkit.Rows(
		"2023-04-06 11:39:17.000000 10.244.4.0 root % mydb 7602485189326930323 success 127.0.0.100 ",
		"2023-04-06 11:39:18.000000 10.244.4.0 root % test 7602485189326930325 success 127.0.0.1 ",
	))

	// set session and query from information_schema.user_login_history.
	tk.Session().GetSessionVars().User = &auth.UserIdentity{
		Username:     "zak",
		Hostname:     "127.0.0.1",
		AuthUsername: "zak",
		AuthHostname: "%",
	}
	tk.MustQuery("SELECT * FROM information_schema.user_login_history").Check(testkit.Rows(
		"2023-04-06 11:39:17.000000 10.244.4.0 zak % test 7602485189326930000 success 127.0.0.1 ",
		"2023-04-06 11:40:17.000000 10.244.4.0 zak % test 7602485189326930324 success 127.0.0.2 ",
	))

	tk.Session().GetSessionVars().User = &auth.UserIdentity{
		Hostname:     "127.0.0.2",
		Username:     "zak",
		AuthUsername: "zak",
		AuthHostname: "%",
	}
	tk.MustQuery("SELECT * FROM information_schema.user_login_history").Check(testkit.Rows(
		"2023-04-06 11:39:17.000000 10.244.4.0 zak % test 7602485189326930000 success 127.0.0.1 ",
		"2023-04-06 11:40:17.000000 10.244.4.0 zak % test 7602485189326930324 success 127.0.0.2 ",
	))
}

func TestInsertRecordsWithDuplicateSessionID(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.Session().GetSessionVars().InRestrictedSQL = false

	// insert into 2 records with duplicate sessionID.
	tk.MustExec(`insert into mysql.LOGIN_HISTORY VALUES('2023-04-06 11:39:17.000000', "10.244.4.0", "root", "127.0.0.%", "test", 7602485189326930323, 'success', "127.0.0.1", "")`)
	tk.MustExec(`insert into mysql.login_history VALUES('2023-04-06 11:39:17.000000', "10.244.4.0", "root", "127.0.0.%", "test", 7602485189326930323, "success", "127.0.0.1", "")`)

	// query from mysql.login_history and get 0 rows.
	rows := tk.MustQuery("SELECT * FROM mysql.login_history").Rows()
	require.Len(t, rows, 2)
}
