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

package isolation_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/oracle"
)

func setupTxnContextTest(t *testing.T) (kv.Storage, *domain.Domain) {
	store, do := testkit.CreateMockStoreAndDomain(t)

	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("set global pkdb_eal = on")
	t.Cleanup(func() {
		tk.MustExec("set global pkdb_eal = off")
	})
	tk.MustExec("drop table if exists accounts, products")

	tk.MustExec("CREATE TABLE accounts (id INT PRIMARY KEY, name VARCHAR(255), balance INT)")
	tk.MustExec("INSERT INTO accounts (id, name, balance) VALUES (1, 'Alice', 1000), (2, 'Bob', 1500)")

	tk.MustExec("CREATE TABLE products (id INT AUTO_INCREMENT PRIMARY KEY, category VARCHAR(50), name VARCHAR(100))")
	tk.MustExec("INSERT INTO products (category, name) VALUES ('electronics', 'Laptop')")

	return store, do
}

func getAsOfTimeStr(t *testing.T, store kv.Storage) string {
	ts, err := store.GetOracle().GetTimestamp(context.Background(), &oracle.Option{TxnScope: oracle.GlobalTxnScope})
	require.NoError(t, err)
	return oracle.GetTimeFromTS(ts).Format(types.TimeFSPFormat)
}

func TestIsolationChangeRR2RC(t *testing.T) {
	store, _ := setupTxnContextTest(t)

	tk1 := testkit.NewTestKit(t, store)
	tk1.MustExec("use test")

	tk2 := testkit.NewTestKit(t, store)
	tk2.MustExec("use test")

	tk1.MustExec("begin pessimistic")
	tk1.MustQuery("SELECT balance FROM accounts WHERE id = 1").Check(testkit.Rows("1000"))

	tk2.MustExec("begin pessimistic")
	tk2.MustExec("UPDATE accounts SET balance = 1200 WHERE id = 1")
	tk2.MustExec("commit")

	tk1.MustQuery("SELECT balance FROM accounts WHERE id = 1").Check(testkit.Rows("1000"))
	tk1.MustExec("SET TRANSACTION ISOLATION LEVEL READ COMMITTED")                                 // RR -> RC
	tk1.MustQuery("SELECT @@SESSION.transaction_isolation").Check(testkit.Rows("REPEATABLE-READ")) // default isolation level, remains unchanged
	tk1.MustQuery("SELECT @@SESSION.tx_isolation_one_shot").Check(testkit.Rows("READ-COMMITTED"))
	tk1.MustQuery("SELECT balance FROM accounts WHERE id = 1").Check(testkit.Rows("1200")) // take effect
	tk1.MustExec("commit")
}

/*
TestRC2RR:
----------------------------------------------------------------------------------------------------------------------------------------------------------------------------
| 					tk1 (RC) 									| 					tk2 (RC) 						|					tk3 (RC)
----------------------------------------------------------------------------------------------------------------------------------------------------------------------------
| begin pessimistic											    |		                                            |
| SELECT balance FROM accounts WHERE id = 1  -> 1000            |                                                   |
|  																|													| begin pessimistic
|  																|													| SELECT balance FROM accounts WHERE id = 1  -> 1000(RC)
| 																| begin pessimistic                                 |
| 																| UPDATE accounts SET balance = 800 WHERE id = 1    |
| 																| commit									        |
| 																|													| SELECT balance FROM accounts WHERE id = 1  -> 800(RC)
| SET TRANSACTION ISOLATION LEVEL REPEATABLE READ		        |													|
| SELECT @@SESSION.transaction_isolation  -> READ-COMMITTED     |													|
| SELECT balance FROM accounts WHERE id = 1  -> 1000(RR)        |													|
| 																| begin pessimistic                                 |
| 																| UPDATE accounts SET balance = 700 WHERE id = 1    |
| 																| commit                                            |
| SELECT balance FROM accounts WHERE id = 1  -> 1000(RR)        |											    	|
| commit                                                        |											    	|
| 																|													| SET TRANSACTION ISOLATION LEVEL REPEATABLE READ
| 																|													| SELECT balance FROM accounts WHERE id = 1  -> 1000(RR)
| 																|													| commit
----------------------------------------------------------------------------------------------------------------------------------------------------------------------------
*/
func TestIsolationChangeRC2RR(t *testing.T) {
	store, _ := setupTxnContextTest(t)

	tk1 := testkit.NewTestKit(t, store)
	tk1.MustExec("use test")
	tk1.MustExec("SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED")

	tk2 := testkit.NewTestKit(t, store)
	tk2.MustExec("use test")
	tk2.MustExec("SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED")

	tk3 := testkit.NewTestKit(t, store)
	tk3.MustExec("use test")
	tk3.MustExec("SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED")

	tk1.MustExec("begin pessimistic")
	tk1.MustQuery("SELECT balance FROM accounts WHERE id = 1").Check(testkit.Rows("1000")) // RC

	tk3.MustExec("begin pessimistic")
	tk3.MustQuery("SELECT balance FROM accounts WHERE id = 1").Check(testkit.Rows("1000")) // RC

	tk2.MustExec("begin pessimistic")
	tk2.MustExec("UPDATE accounts SET balance = 800 WHERE id = 1")
	tk2.MustExec("commit")

	tk3.MustQuery("SELECT balance FROM accounts WHERE id = 1").Check(testkit.Rows("800")) // RC

	tk1.MustExec("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ") // RC -> RR
	tk1.MustQuery("SELECT @@SESSION.transaction_isolation").Check(testkit.Rows("READ-COMMITTED"))
	tk1.MustQuery("SELECT @@SESSION.tx_isolation_one_shot").Check(testkit.Rows("REPEATABLE-READ"))
	tk1.MustQuery("SELECT balance FROM accounts WHERE id = 1").Check(testkit.Rows("1000")) // RR

	tk2.MustExec("begin pessimistic")
	tk2.MustExec("UPDATE accounts SET balance = 700 WHERE id = 1")
	tk2.MustExec("commit")

	tk1.MustQuery("SELECT balance FROM accounts WHERE id = 1").Check(testkit.Rows("1000")) // RR
	tk1.MustExec("commit")

	tk3.MustExec("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ") // RC -> RR
	tk3.MustQuery("SELECT @@SESSION.tx_isolation_one_shot").Check(testkit.Rows("REPEATABLE-READ"))
	tk3.MustQuery("SELECT balance FROM accounts WHERE id = 1").Check(testkit.Rows("1000")) // RR
	tk3.MustExec("commit")
}

func TestIsolationChangeRC2RRPhantom(t *testing.T) {
	store, _ := setupTxnContextTest(t)

	tk1 := testkit.NewTestKit(t, store)
	tk1.MustExec("use test")
	tk1.MustExec("SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED")

	tk2 := testkit.NewTestKit(t, store)
	tk2.MustExec("use test")
	tk2.MustExec("SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED")

	tk1.MustExec("begin pessimistic")

	tk2.MustExec("begin pessimistic")
	tk2.MustExec("INSERT INTO products (category, name) VALUES ('electronics', 'Mouse')")
	tk2.MustExec("commit")

	tk1.MustQuery("SELECT COUNT(*) FROM products WHERE category = 'electronics'").Check(testkit.Rows("2")) // RC

	tk2.MustExec("begin pessimistic")
	tk2.MustExec("INSERT INTO products (category, name) VALUES ('electronics', 'Keyboard')")
	tk2.MustExec("commit")

	tk1.MustExec("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")                                        // RC -> RR
	tk1.MustQuery("SELECT COUNT(*) FROM products WHERE category = 'electronics'").Check(testkit.Rows("1")) // RR
	tk1.MustQuery("SELECT @@SESSION.transaction_isolation").Check(testkit.Rows("READ-COMMITTED"))
	tk1.MustQuery("SELECT @@SESSION.tx_isolation_one_shot").Check(testkit.Rows("REPEATABLE-READ"))
	tk1.MustExec("SET TRANSACTION ISOLATION LEVEL READ COMMITTED")                                         // RR -> RC
	tk1.MustQuery("SELECT COUNT(*) FROM products WHERE category = 'electronics'").Check(testkit.Rows("3")) // RR
	tk1.MustExec("commit")
}

func TestIsolationChangeIgnoredInOptimisticTxn(t *testing.T) {
	store, _ := setupTxnContextTest(t)

	tk1 := testkit.NewTestKit(t, store)
	tk1.MustExec("use test")
	tk1.MustExec("set @@tidb_txn_mode='optimistic'")

	tk2 := testkit.NewTestKit(t, store)
	tk2.MustExec("use test")
	tk1.MustExec("begin")
	require.False(t, tk1.Session().GetSessionVars().TxnCtx.IsPessimistic)
	tk1.MustQuery("SELECT balance FROM accounts WHERE id = 1").Check(testkit.Rows("1000"))

	tk2.MustExec("UPDATE accounts SET balance = 1200 WHERE id = 1")

	tk1.MustQuery("SELECT balance FROM accounts WHERE id = 1").Check(testkit.Rows("1000"))
	tk1.MustExec("SET TRANSACTION ISOLATION LEVEL READ COMMITTED")
	tk1.MustQuery("SELECT balance FROM accounts WHERE id = 1").Check(testkit.Rows("1000"))
	tk1.MustExec("commit")
}

func TestIsolationChangeIgnoredInStaleReadTxn(t *testing.T) {
	store, _ := setupTxnContextTest(t)

	asOf := getAsOfTimeStr(t, store)

	tkWriter := testkit.NewTestKit(t, store)
	tkWriter.MustExec("use test")
	tkWriter.MustExec("UPDATE accounts SET balance = 1200 WHERE id = 1")

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set @@tidb_txn_mode='pessimistic'")
	tk.MustExec(fmt.Sprintf("START TRANSACTION READ ONLY AS OF TIMESTAMP '%s'", asOf))
	require.True(t, tk.Session().GetSessionVars().TxnCtx.IsStaleness)
	tk.MustQuery("SELECT balance FROM accounts WHERE id = 1").Check(testkit.Rows("1000"))

	tk.MustExec("SET TRANSACTION ISOLATION LEVEL READ COMMITTED")
	require.True(t, tk.Session().GetSessionVars().TxnCtx.IsStaleness)
	tk.MustQuery("SELECT balance FROM accounts WHERE id = 1").Check(testkit.Rows("1000"))
	tk.MustExec("commit")
}

func TestIsolationChangeThenSelectForUpdateLock(t *testing.T) {
	store, _ := setupTxnContextTest(t)

	tk1 := testkit.NewTestKit(t, store)
	tk1.MustExec("use test")
	tk2 := testkit.NewTestKit(t, store)
	tk2.MustExec("use test")

	tk1.MustExec("begin pessimistic")
	tk1.MustExec("SET TRANSACTION ISOLATION LEVEL READ COMMITTED")
	tk1.MustQuery("SELECT balance FROM accounts WHERE id = 1 FOR UPDATE").Check(testkit.Rows("1000"))

	tk2.MustExec("begin pessimistic")
	tk2.MustGetErrCode("SELECT balance FROM accounts WHERE id = 1 FOR UPDATE NOWAIT", mysql.ErrLockAcquireFailAndNoWaitSet)
	tk2.MustExec("rollback")
	tk1.MustExec("commit")
}

func TestIsolationChangeDoesNotLoseSavepoint(t *testing.T) {
	store, _ := setupTxnContextTest(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("begin pessimistic")
	tk.MustExec("UPDATE accounts SET balance = 1100 WHERE id = 1")
	tk.MustExec("SAVEPOINT s1")
	tk.MustExec("UPDATE accounts SET balance = 1200 WHERE id = 1")
	tk.MustExec("SET TRANSACTION ISOLATION LEVEL READ COMMITTED")
	tk.MustExec("ROLLBACK TO SAVEPOINT s1")
	tk.MustQuery("SELECT balance FROM accounts WHERE id = 1").Check(testkit.Rows("1100"))
	tk.MustExec("commit")
}

func TestIsolationChangeRC2RRPointGetAndTableReader(t *testing.T) {
	store, _ := setupTxnContextTest(t)

	tk1 := testkit.NewTestKit(t, store)
	tk1.MustExec("use test")
	tk1.MustExec("SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED")

	tk2 := testkit.NewTestKit(t, store)
	tk2.MustExec("use test")
	tk1.MustExec("begin pessimistic")
	tk1.MustQuery("SELECT balance FROM accounts WHERE id = 1").Check(testkit.Rows("1000"))

	tk2.MustExec("begin pessimistic")
	tk2.MustExec("UPDATE accounts SET balance = 800 WHERE id = 1")
	tk2.MustExec("commit")

	tk1.MustQuery("SELECT balance FROM accounts WHERE id = 1").Check(testkit.Rows("800"))
	tk1.MustExec("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ") // RC -> RR
	tk1.MustQuery("SELECT balance FROM accounts WHERE id = 1").Check(testkit.Rows("1000"))

	tk1.MustQuery("SELECT SUM(balance) FROM accounts").Check(testkit.Rows("2500"))
	tk1.MustExec("commit")
}
