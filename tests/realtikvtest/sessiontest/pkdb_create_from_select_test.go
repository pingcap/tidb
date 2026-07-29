package sessiontest

import (
	"fmt"
	"strings"
	"testing"

	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/tests/realtikvtest"
)

func TestCreateTableAsSelectUsingImportWithAutoRandom(t *testing.T) {
	if !*realtikvtest.WithRealTiKV {
		t.Skip("TiKV store only")
	}
	defer config.RestoreFunc()
	store := realtikvtest.CreateMockStoreAndSetup(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set @@global.tidb_schema_cache_size = 0")
	tk.MustExec("set @@session.tidb_create_from_select_using_import = true")
	tk.MustExec("set @@session.tidb_init_chunk_size = 2")
	tk.MustExec("use test")
	tk.MustExec("create table src_auto_random(v int)")
	values := make([]string, 0, 100)
	for i := range 100 {
		values = append(values, fmt.Sprintf("(%d)", i))
	}
	tk.MustExec("insert into src_auto_random values " + strings.Join(values, ","))

	tk.MustExec("create table dst_auto_random(id bigint auto_random primary key, v int) as select v from src_auto_random")
	tk.MustQuery("select count(*), count(distinct v), min(v), max(v) from dst_auto_random").Check(testkit.Rows("100 100 0 99"))
	tk.MustQuery("select count(*), count(distinct id) from dst_auto_random").Check(testkit.Rows("100 100"))
}
