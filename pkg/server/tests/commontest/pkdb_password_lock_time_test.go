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

package commontest

import (
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/pingcap/tidb/pkg/extension"
	"github.com/pingcap/tidb/pkg/extension/enterprise/whitelist"
	"github.com/pingcap/tidb/pkg/server/tests/servertestkit"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func TestPasswordLockTimeExpiresWithWhitelist(t *testing.T) {
	whitelist.Register4Test()
	t.Cleanup(extension.Reset)
	ts := servertestkit.CreateTidbTestSuite(t)

	for _, tc := range []struct {
		name     string
		value    string
		duration time.Duration
	}{
		{"minute", "1m", time.Minute},
		{"hour", "2h", 2 * time.Hour},
		{"day", "1d", 24 * time.Hour},
		{"legacy_day", "3", 72 * time.Hour},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tk := testkit.NewTestKit(t, ts.Store)
			user := "lock_" + tc.name
			tk.MustExec(fmt.Sprintf("CREATE USER '%s'@'%%' IDENTIFIED BY 'password' FAILED_LOGIN_ATTEMPTS 3 PASSWORD_LOCK_TIME %s", user, tc.value))
			login := func(password string, errorCode uint16) {
				t.Helper()
				db, err := sql.Open("mysql", ts.GetDSN(func(cfg *mysql.Config) {
					cfg.User, cfg.Passwd, cfg.DBName = user, password, ""
				}))
				require.NoError(t, err)
				defer func() { require.NoError(t, db.Close()) }()
				var currentUser string
				err = db.QueryRow("SELECT CURRENT_USER()").Scan(&currentUser)
				if errorCode == 0 {
					require.NoError(t, err)
					require.Equal(t, user+"@%", currentUser)
				} else {
					var mysqlErr *mysql.MySQLError
					require.ErrorAs(t, err, &mysqlErr)
					require.Equal(t, errorCode, mysqlErr.Number)
				}
			}
			login("password", 0)
			login("wrong", 1045)
			login("wrong", 1045)
			login("wrong", 3955)
			stateSQL := fmt.Sprintf("SELECT user_attributes->>'$.Password_locking.password_lock_secs', user_attributes->>'$.Password_locking.password_lock_time_days', user_attributes->>'$.Password_locking.auto_account_locked', user_attributes->>'$.Password_locking.failed_login_count' FROM mysql.user WHERE User='%s' AND Host='%%'", user)
			tk.MustQuery(stateSQL).Check(testkit.Rows(fmt.Sprintf("%d %d Y 3", int64(tc.duration/time.Second), int64(tc.duration/(24*time.Hour)))))
			login("password", 3955)

			// Advance the persisted lock age to exercise both sides of expiry without waiting days.
			for _, elapsed := range []time.Duration{tc.duration - 30*time.Second, tc.duration + 2*time.Second} {
				tk.MustExec("UPDATE mysql.user SET user_attributes=JSON_SET(user_attributes, '$.Password_locking.auto_locked_last_changed', ?) WHERE User=? AND Host='%'", time.Now().Add(-elapsed).Format(time.UnixDate), user)
				require.NoError(t, ts.Domain.NotifyUpdatePrivilege([]string{user}))
				if elapsed < tc.duration {
					login("password", 3955)
				} else {
					login("password", 0)
				}
			}
			tk.MustQuery(stateSQL).Check(testkit.Rows(fmt.Sprintf("%d %d N 0", int64(tc.duration/time.Second), int64(tc.duration/(24*time.Hour)))))
		})
	}
}
