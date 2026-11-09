package ddl_test

import (
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/ddl"
	ingesttestutil "github.com/pingcap/tidb/pkg/ddl/ingest/testutil"
	"github.com/pingcap/tidb/pkg/errno"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	parsertypes "github.com/pingcap/tidb/pkg/parser/types"
	"github.com/pingcap/tidb/pkg/store/mockstore"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/external"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestVarcharToBlobPreservesRows(t *testing.T) {
	// Disable the stats owner before bootstrap: MockTiKV cannot scan the notifier table.
	t.Cleanup(config.RestoreFunc())
	config.UpdateGlobal(func(conf *config.Config) {
		conf.Instance.TiDBEnableStatsOwner.Store(false)
	})
	// Unistore upgrades legacy row values on commit; MockTiKV preserves both formats.
	store := testkit.CreateMockStore(t, mockstore.WithStoreType(mockstore.MockTiKV))
	for _, format := range []int{1, 2} {
		for _, tc := range []struct {
			charset, target string
			flen            int
			tp              byte
		}{
			{"utf8mb4", "blob", 16383, mysql.TypeBlob},
			{"utf8", "blob", 16383, mysql.TypeBlob},
			{"latin1", "blob", 16383, mysql.TypeBlob},
			{"utf8mb4", "tinyblob", 63, mysql.TypeTinyBlob},
			{"utf8", "tinyblob", 63, mysql.TypeTinyBlob},
			{"latin1", "tinyblob", 63, mysql.TypeTinyBlob},
			{"latin1", "mediumblob", 65535, mysql.TypeMediumBlob},
			{"latin1", "longblob", 65535, mysql.TypeLongBlob},
		} {
			for _, change := range []bool{false, true} {
				t.Run(fmt.Sprintf("format%d/%s/%s/change=%t", format, tc.charset, tc.target, change), func(t *testing.T) {
					tk := testkit.NewTestKit(t, store)
					tk.MustExec("use test")
					tk.MustExec("drop table if exists t")
					tk.MustExec(fmt.Sprintf("set tidb_row_format_version=%d", format))
					tk.MustExec("set tidb_skip_utf8_check=1")
					tk.MustExec(fmt.Sprintf("create table t(id int primary key clustered, c varchar(%d) charset %s, key idx(id))", tc.flen, tc.charset))
					tk.MustExec("insert into t values (1, 'hello  '), (2, '你好é😀'), (3, ''), (4, NULL), (5, ?)", strings.Repeat("😀", tc.flen))
					old := external.GetTableByName(t, tk, "test", "t").Meta()
					readRows := func() [][]byte {
						ver, err := store.CurrentVersion(kv.GlobalTxnScope)
						require.NoError(t, err)
						snapshot := store.GetSnapshot(ver)
						rows := make([][]byte, 5)
						for i := range rows {
							rows[i], err = kv.GetValue(t.Context(), snapshot, tablecodec.EncodeRowKeyWithHandle(old.ID, kv.IntHandle(i+1)))
							require.NoError(t, err)
							require.Equal(t, format == 2, rows[i][0] == 0x80)
						}
						return rows
					}
					before := readRows()
					var jobID atomic.Int64
					var modifyType atomic.Uint32
					testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/getModifyColumnType", func(tp byte) { modifyType.Store(uint32(tp)) })
					testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/beforeRunOneJobStep", func(job *model.Job) {
						if job.Type == model.ActionModifyColumn && job.TableID == old.ID {
							jobID.Store(job.ID)
						}
					})
					name := "c"
					if change {
						tk.MustExec("alter table t change column c c2 " + tc.target)
						name = "c2"
					} else {
						tk.MustExec("alter table t modify column c " + tc.target)
					}
					current := external.GetTableByName(t, tk, "test", "t").Meta()
					require.Len(t, current.Columns, 2)
					col := current.Columns[1]
					require.Equal(t, old.Columns[1].ID, col.ID)
					require.Equal(t, name, col.Name.L)
					require.Equal(t, tc.tp, col.GetType())
					require.Equal(t, "binary", col.GetCharset())
					require.Equal(t, "binary", col.GetCollate())
					require.Nil(t, col.ChangingFieldType)
					require.Equal(t, before, readRows())
					history, err := ddl.GetHistoryJobByID(tk.Session(), jobID.Load())
					require.NoError(t, err)
					require.NotNil(t, history)
					require.Zero(t, history.RowCount)
					require.Equal(t, uint32(model.ModifyTypeNoReorg), modifyType.Load())
					args, err := model.GetFinishedModifyColumnArgs(history)
					require.NoError(t, err)
					require.Empty(t, args.IndexIDs)
					require.Empty(t, args.NewIndexIDs)
					tk.MustQuery(fmt.Sprintf("select id, hex(%s), char_length(%s) from t where id < 5 order by id", name, name)).Check(testkit.Rows(
						"1 68656C6C6F2020 7", "2 E4BDA0E5A5BDC3A9F09F9880 12", "3  0", "4 <nil> <nil>",
					))
					tk.MustQuery("select length(" + name + ") from t where id=5").Check(testkit.Rows(fmt.Sprint(tc.flen * 4)))
					tk.MustExec("admin check table t")
				})
			}
		}
	}
}

func TestVarcharToBlobRejectsUnsupportedConversions(t *testing.T) {
	store := testkit.CreateMockStore(t)
	for _, tc := range []struct {
		name, definition, target string
	}{
		{"index", "c varchar(10), key(c)", "c blob"},
		{"unique", "c varchar(10), unique key(c)", "c blob"},
		{"composite_first", "c varchar(10), key(c,id)", "c blob"},
		{"composite_last", "c varchar(10), key(id,c)", "c blob"},
		{"invisible", "c varchar(10), key(c) invisible", "c blob"},
		{"clustered_primary", "c varchar(10), primary key(c) clustered", "c blob not null"},
		{"nonclustered_primary", "c varchar(10), primary key(c) nonclustered", "c blob not null"},
		{"expression_index", "c varchar(10), key((length(c)))", "c blob"},
		{"partial_condition", "c varchar(10), key(id) where c='1'", "c blob"},
		{"virtual", "c varchar(10) as (id)", "c blob as (id)"},
		{"stored", "c varchar(10) as (id) stored", "c blob as (id) stored"},
		{"stored_gbk", "g varchar(10) charset gbk, c varchar(10) charset utf8mb4 as (g) stored", "c blob as (g) stored"},
		{"new_generated", "c varchar(10)", "c blob as (id)"},
		{"generated_dependency", "c varchar(10), g int as (length(c))", "c blob"},
		{"nonbinary_collation", "c varchar(10)", "c blob collate utf8mb4_bin"},
		{"char", "c char(10)", "c blob"},
		{"text", "c text", "c blob"},
		{"xml", "c varchar(10)", "c xml"},
		{"tinyblob_generated_dependency", "c varchar(10), g int as (length(c))", "c tinyblob"},
		{"mediumblob_partial_condition", "c varchar(10), key(id) where c='1'", "c mediumblob"},
		{"longblob_full_index", "c varchar(10), key(c)", "c longblob"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if tc.name == "xml" {
				previous := parsertypes.EnableExtraDataType.Swap(true)
				t.Cleanup(func() { parsertypes.EnableExtraDataType.Store(previous) })
			}
			tk := testkit.NewTestKit(t, store)
			tk.MustExec("use test")
			tk.MustExec("drop table if exists t")
			tk.MustExec("create table t(id int, " + tc.definition + ") charset utf8mb4")
			before := tk.MustQuery("show create table t").Rows()
			columns := external.GetTableByName(t, tk, "test", "t").Meta().Columns
			switch tc.name {
			case "virtual", "stored":
				tk.MustExec("insert into t(id) values (1)")
			case "stored_gbk":
				tk.MustExec("insert into t(id,g) values (1,'你好')")
			case "xml":
				tk.MustExec("insert into t(id,c) values (1,'<r/>')")
			default:
				tk.MustExec("insert into t(id,c) values (1,'1')")
			}
			rows := tk.MustQuery("select * from t").Rows()
			tk.MustExecToErr("alter table t modify column " + tc.target)
			require.Equal(t, before, tk.MustQuery("show create table t").Rows())
			require.Equal(t, columns, external.GetTableByName(t, tk, "test", "t").Meta().Columns)
			require.Equal(t, rows, tk.MustQuery("select * from t").Rows())
		})
	}

}

func TestVarcharToBlobReorgConvertsRows(t *testing.T) {
	// Disable the stats owner before bootstrap: MockTiKV cannot scan the notifier table.
	t.Cleanup(config.RestoreFunc())
	config.UpdateGlobal(func(conf *config.Config) {
		conf.Instance.TiDBEnableStatsOwner.Store(false)
	})
	store := testkit.CreateMockStore(t, mockstore.WithStoreType(mockstore.MockTiKV))
	for _, format := range []int{1, 2} {
		for _, tc := range []struct {
			name, definition, target, value, wantHex string
		}{
			{"gbk", "c varchar(100) charset gbk", "blob", "你好", "C4E3BAC3"},
			{"gb18030", "c varchar(100) charset gb18030", "blob", "你好𠀀", "C4E3BAC395328236"},
			{"ascii", "c varchar(100) charset ascii", "blob", "hello", "68656C6C6F"},
			{"ascii_invalid", "c varchar(100) charset ascii", "blob", "A😀B", "41"},
			{"wide_utf8", "c varchar(20000) charset utf8", "blob", strings.Repeat("你", 20000), strings.Repeat("E4BDA0", 20000)},
			{"wide_latin1", "c varchar(20000) charset latin1", "blob", strings.Repeat("é", 20000), strings.Repeat("C3A9", 20000)},
			{"blob_limit", "c varchar(65535) charset latin1", "blob", strings.Repeat("a", 65535), strings.Repeat("61", 65535)},
			{"tinyblob_short", "c varchar(1000) charset utf8mb4", "tinyblob", "你好", "E4BDA0E5A5BD"},
			{"tinyblob_limit", "c varchar(64) charset utf8mb4", "tinyblob", strings.Repeat("😀", 63) + "你", strings.Repeat("F09F9880", 63) + "E4BDA0"},
			{"tinyblob_gbk_limit", "c varchar(1000) charset gbk", "tinyblob", strings.Repeat("你", 127) + "a", strings.Repeat("C4E3", 127) + "61"},
			{"mediumblob_gbk", "c varchar(100) charset gbk", "mediumblob", "你好", "C4E3BAC3"},
			{"longblob_gb18030", "c varchar(100) charset gb18030", "longblob", "你好𠀀", "C4E3BAC395328236"},
		} {
			t.Run(fmt.Sprintf("format%d/%s", format, tc.name), func(t *testing.T) {
				tk := testkit.NewTestKit(t, store)
				tk.MustExec("use test")
				tk.MustExec("drop table if exists t")
				tk.MustExec(fmt.Sprintf("set tidb_row_format_version=%d", format))
				tk.MustExec("create table t(id int primary key clustered, " + tc.definition + ")")
				if tc.name == "ascii_invalid" {
					tk.MustExec("set tidb_skip_ascii_check=1")
				}
				tk.MustExec("insert into t values (1, ?), (2, NULL), (3, '')", tc.value)
				tk.MustExec("set tidb_skip_ascii_check=0")
				old := external.GetTableByName(t, tk, "test", "t").Meta()
				var jobID atomic.Int64
				var modifyType atomic.Uint32
				testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/getModifyColumnType", func(tp byte) { modifyType.Store(uint32(tp)) })
				testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/beforeRunOneJobStep", func(job *model.Job) {
					if job.Type == model.ActionModifyColumn && job.TableID == old.ID {
						jobID.Store(job.ID)
					}
				})
				tk.MustExec("alter table t change column c b " + tc.target)
				current := external.GetTableByName(t, tk, "test", "t").Meta()
				require.Len(t, current.Columns, 2)
				require.NotEqual(t, old.Columns[1].ID, current.Columns[1].ID)
				require.Equal(t, uint32(model.ModifyTypeReorg), modifyType.Load())
				history, err := ddl.GetHistoryJobByID(tk.Session(), jobID.Load())
				require.NoError(t, err)
				require.Greater(t, history.RowCount, int64(0))
				tk.MustQuery("select id, hex(b) from t order by id").Check(testkit.Rows("1 "+tc.wantHex, "2 <nil>", "3 "))
				tk.MustExec("admin check table t")
			})
		}
	}
}

func TestVarcharToBlobReorgRebuildsIndexes(t *testing.T) {
	store := testkit.CreateMockStore(t)
	defer ingesttestutil.InjectMockBackendCtx(t, store)()
	for _, tc := range []struct {
		name, definition, target, wantHex string
	}{
		{"prefix", "c varchar(100) collate utf8mb4_general_ci, key idx(c(3))", "blob", "E4BDA0E5A5BD"},
		{"gbk_prefix", "c varchar(100) charset gbk, key idx(c(3))", "blob", "C4E3BAC3"},
		{"unique", "c varchar(100), unique key idx(c(3))", "blob", "E4BDA0E5A5BD"},
		{"composite", "c varchar(100), key idx(c(3),id)", "blob", "E4BDA0E5A5BD"},
		{"invisible", "c varchar(100), key idx(c(3)) invisible", "blob", "E4BDA0E5A5BD"},
		{"tinyblob_prefix", "c varchar(50), key idx(c(3))", "tinyblob", "E4BDA0E5A5BD"},
		{"mediumblob_prefix", "c varchar(100), key idx(c(3))", "mediumblob", "E4BDA0E5A5BD"},
		{"longblob_prefix", "c varchar(100), key idx(c(3))", "longblob", "E4BDA0E5A5BD"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tk := testkit.NewTestKit(t, store)
			tk.MustExec("use test")
			tk.MustExec("set tidb_opt_use_invisible_indexes=1")
			tk.MustExec("drop table if exists t")
			tk.MustExec("create table t(id int, " + tc.definition + ")")
			tk.MustExec("insert into t values (1, '你好')")
			old := external.GetTableByName(t, tk, "test", "t").Meta()
			var modifyType atomic.Uint32
			testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/getModifyColumnType", func(tp byte) { modifyType.Store(uint32(tp)) })
			tk.MustExec("alter table t change column c b " + tc.target)
			current := external.GetTableByName(t, tk, "test", "t").Meta()
			require.NotEqual(t, old.Columns[1].ID, current.Columns[1].ID)
			require.NotEqual(t, old.Indices[0].ID, current.Indices[0].ID)
			require.Equal(t, uint32(model.ModifyTypeReorg), modifyType.Load())
			tk.MustQuery("select hex(b) from t force index(idx) where b = unhex(?)", tc.wantHex).Check(testkit.Rows(tc.wantHex))
			tk.MustExec("admin check table t")
		})
	}
}

func TestVarcharToBlobReorgChecksCapacity(t *testing.T) {
	store := testkit.CreateMockStore(t)
	for _, tc := range []struct {
		name, definition, target, value, lengths string
		capacity                                 int
	}{
		{"blob_utf8", "varchar(20000) charset utf8", "blob", strings.Repeat("😀", 16384), "16384 65536", 65535},
		{"blob_latin1", "varchar(20000) charset latin1", "blob", strings.Repeat("😀", 16384), "16384 65536", 65535},
		{"tinyblob_ascii_value", "varchar(1000) charset utf8mb4", "tinyblob", strings.Repeat("a", 256), "256 256", 255},
		{"tinyblob_utf8mb4", "varchar(64) charset utf8mb4", "tinyblob", strings.Repeat("😀", 64), "64 256", 255},
		{"tinyblob_utf8", "varchar(64) charset utf8", "tinyblob", strings.Repeat("😀", 64), "64 256", 255},
		{"tinyblob_latin1", "varchar(64) charset latin1", "tinyblob", strings.Repeat("😀", 64), "64 256", 255},
		{"tinyblob_gbk", "varchar(1000) charset gbk", "tinyblob", strings.Repeat("你", 128), "128 256", 255},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tk := testkit.NewTestKit(t, store)
			tk.MustExec("use test")
			tk.MustExec("drop table if exists t")
			tk.MustExec("set sql_mode='STRICT_ALL_TABLES'")
			tk.MustExec("set tidb_skip_utf8_check=1")
			tk.MustExec("create table t(c " + tc.definition + ")")
			tk.MustExec("insert into t values (?)", tc.value)
			before := external.GetTableByName(t, tk, "test", "t").Meta().Columns[0].Clone()
			tk.MustGetErrCode("alter table t modify column c "+tc.target, errno.WarnDataTruncated)
			current := external.GetTableByName(t, tk, "test", "t").Meta()
			require.Len(t, current.Columns, 1)
			require.Equal(t, before, current.Columns[0])
			tk.MustQuery("select char_length(c), length(c) from t").Check(testkit.Rows(tc.lengths))
			tk.MustExec("admin check table t")

			tk.MustExec("set sql_mode=''")
			tk.MustExec("alter table t modify column c " + tc.target)
			warnings := tk.MustQuery("show warnings").Rows()
			require.NotEmpty(t, warnings)
			tk.MustQuery("select length(c) from t").Check(testkit.Rows(fmt.Sprint(tc.capacity)))
			tk.MustExec("admin check table t")
		})
	}
}

func TestVarcharToBlobReorgConcurrentDML(t *testing.T) {
	store := testkit.CreateMockStore(t)
	defer ingesttestutil.InjectMockBackendCtx(t, store)()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t(id int primary key)")
	tk.MustExec("insert into t values (1)")
	tk.MustExec("alter table t add column c varchar(100) charset gbk default '你好'")
	tk.MustExec("alter table t add index idx(c(3))")
	tk.MustExec("insert into t values (2, '你'), (3, '你好')")
	tbl := external.GetTableByName(t, tk, "test", "t").Meta()
	var written atomic.Bool
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/beforeRunOneJobStep", func(job *model.Job) {
		if job.TableID == tbl.ID && job.Type == model.ActionModifyColumn && job.SchemaState == model.StateWriteReorganization && written.CompareAndSwap(false, true) {
			tk2 := testkit.NewTestKit(t, store)
			tk2.MustExec("insert into test.t values (4, '你好')")
			tk2.MustExec("update test.t set c='好' where id=2")
			tk2.MustExec("delete from test.t where id=3")
		}
	})
	tk.MustExec("alter table t modify column c blob")
	require.True(t, written.Load())
	tk.MustQuery("select id, hex(c) from t order by id").Check(testkit.Rows("1 C4E3BAC3", "2 BAC3", "4 C4E3BAC3"))
	tk.MustQuery("select id from t force index(idx) where c=x'C4E3BAC3' order by id").Check(testkit.Rows("1", "4"))
	tk.MustExec("admin check table t")
}

func TestVarcharToBlobReorgChecksConcurrentCapacity(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t(id int primary key, c varchar(64) charset utf8mb4)")
	tk.MustExec("insert into t values (1, ?)", strings.Repeat("😀", 63)+"你")
	tableID := external.GetTableByName(t, tk, "test", "t").Meta().ID
	var checked atomic.Bool
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/beforeRunOneJobStep", func(job *model.Job) {
		if job.TableID == tableID && job.Type == model.ActionModifyColumn && job.SchemaState == model.StateWriteReorganization && checked.CompareAndSwap(false, true) {
			tk2 := testkit.NewTestKit(t, store)
			tk2.MustExec("set sql_mode='STRICT_ALL_TABLES'")
			// Both statements fit the source VARCHAR, but exceed TINYBLOB's byte limit.
			tk2.MustGetErrCode("insert into test.t values (2, repeat('😀', 64))", errno.ErrDataTooLong)
			tk2.MustGetErrCode("update test.t set c=repeat('😀', 64) where id=1", errno.ErrDataTooLong)
			tk2.MustExec("insert into test.t values (3, repeat('😀', 63))")
		}
	})
	tk.MustExec("alter table t modify column c tinyblob")
	require.True(t, checked.Load())
	tk.MustQuery("select id, length(c) from t order by id").Check(testkit.Rows("1 255", "3 252"))
	tk.MustExec("admin check table t")
}

func TestVarcharToBlobReorgRejectsDuplicatePrefix(t *testing.T) {
	store := testkit.CreateMockStore(t)
	// The mock ingest backend skips duplicate detection; use transactional reorg.
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t(c varchar(100) charset utf8mb4, unique key idx(c(3)))")
	tk.MustExec("insert into t values ('你a'), ('你b')")
	before := tk.MustQuery("show create table t").Rows()
	tk.MustGetErrCode("alter table t modify column c blob", errno.ErrDupEntry)
	require.Equal(t, before, tk.MustQuery("show create table t").Rows())
	tk.MustQuery("select c from t force index(idx) order by c").Check(testkit.Rows("你a", "你b"))
	tk.MustExec("admin check table t")
}

func TestVarcharToBlobReorgCancellation(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t(c varchar(100) charset gbk)")
	tk.MustExec("insert into t values ('你好')")
	old := external.GetTableByName(t, tk, "test", "t").Meta()
	var cancelled atomic.Bool
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/beforeRunOneJobStep", func(job *model.Job) {
		if job.TableID == old.ID && job.Type == model.ActionModifyColumn && job.SchemaState == model.StateWriteReorganization && job.RowCount > 0 && cancelled.CompareAndSwap(false, true) {
			tk2 := testkit.NewTestKit(t, store)
			tk2.MustQuery(fmt.Sprintf("admin cancel ddl jobs %d", job.ID)).Check(testkit.Rows(fmt.Sprintf("%d successful", job.ID)))
		}
	})
	err := tk.ExecToErr("alter table t modify column c blob")
	require.ErrorContains(t, err, "Cancelled DDL job")
	require.True(t, cancelled.Load())
	require.Equal(t, old.Columns, external.GetTableByName(t, tk, "test", "t").Meta().Columns)
	tk.MustQuery("select c, hex(c) from t").Check(testkit.Rows("你好 C4E3BAC3"))
	tk.MustExec("insert into t values ('好')")
	tk.MustExec("admin check table t")
}

func TestVarcharToBlobOriginDefaultAndDML(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t(id int primary key clustered)")
	tk.MustExec("insert into t values (1)")
	tk.MustExec("alter table t add column c varchar(10) charset utf8mb4 default '你好é'")
	old := external.GetTableByName(t, tk, "test", "t").Meta()
	readRow := func() []byte {
		ver, err := store.CurrentVersion(kv.GlobalTxnScope)
		require.NoError(t, err)
		raw, err := kv.GetValue(t.Context(), store.GetSnapshot(ver), tablecodec.EncodeRowKeyWithHandle(old.ID, kv.IntHandle(1)))
		require.NoError(t, err)
		return raw
	}
	before := readRow()
	decoded, err := tablecodec.DecodeRowToDatumMap(before, map[int64]*types.FieldType{old.Columns[1].ID: &old.Columns[1].FieldType}, time.UTC)
	require.NoError(t, err)
	require.NotContains(t, decoded, old.Columns[1].ID)
	tk.MustExec("alter table t modify column c blob")
	require.Equal(t, before, readRow())
	tk.MustQuery("select hex(c), char_length(c) from t").Check(testkit.Rows("E4BDA0E5A5BDC3A9 8"))
	tk.MustExec("insert into t values (2, x'00FF8041'), (3, NULL), (4, 'a')")
	tk.MustExec("update t set c=concat(c, x'00') where id=2")
	tk.MustExec("delete from t where id=4")
	tk.MustQuery("select id, hex(c) from t order by id").Check(testkit.Rows("1 E4BDA0E5A5BDC3A9", "2 00FF804100", "3 <nil>"))
	tk.MustGetErrCode("insert into t values (5, repeat('a', 65536))", errno.ErrDataTooLong)
	tk.MustExec("admin check table t")
}

func TestVarcharToBlobNotNull(t *testing.T) {
	store := testkit.CreateMockStore(t)
	for _, concurrentNull := range []bool{false, true} {
		t.Run(fmt.Sprintf("concurrent_null=%t", concurrentNull), func(t *testing.T) {
			tk := testkit.NewTestKit(t, store)
			tk.MustExec("use test")
			tk.MustExec("drop table if exists t")
			tk.MustExec("create table t(c varchar(10))")
			tk.MustExec("insert into t values ('你好')")
			old := external.GetTableByName(t, tk, "test", "t").Meta()
			var observed atomic.Bool
			if concurrentNull {
				testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/beforeDoModifyColumnSkipReorgCheck", func() {
					if observed.CompareAndSwap(false, true) {
						tk2 := testkit.NewTestKit(t, store)
						tk2.MustExec("insert into test.t values (NULL)")
					}
				})
			} else {
				testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/afterDoModifyColumnSkipReorgCheck", func() {
					observed.Store(true)
					tk2 := testkit.NewTestKit(t, store)
					tk2.MustExecToErr("insert into test.t values (NULL)")
				})
			}
			var modifyType atomic.Uint32
			testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/getModifyColumnType", func(tp byte) { modifyType.Store(uint32(tp)) })
			if concurrentNull {
				tk.MustExecToErr("alter table t modify column c blob not null")
				tk.MustQuery("select count(*) from t where c is null").Check(testkit.Rows("1"))
				// Existing NULL must also be rejected before submission; rollback clears the temporary flag.
				tk.MustExecToErr("alter table t modify column c blob not null")
				tk.MustExec("insert into t values (NULL)")
			} else {
				tk.MustExec("alter table t modify column c blob not null")
				tk.MustQuery("select hex(c) from t").Check(testkit.Rows("E4BDA0E5A5BD"))
				tk.MustGetErrCode("insert into t values (NULL)", errno.ErrBadNull)
			}
			require.True(t, observed.Load())
			require.Equal(t, uint32(model.ModifyTypeNoReorgWithCheck), modifyType.Load())
			col := external.GetTableByName(t, tk, "test", "t").Meta().Columns[0]
			require.Equal(t, old.Columns[0].ID, col.ID)
			require.Nil(t, col.ChangingFieldType)
			require.Equal(t, !concurrentNull, mysql.HasNotNullFlag(col.GetFlag()))
			if concurrentNull {
				require.Equal(t, mysql.TypeVarchar, col.GetType())
			}
			tk.MustExec("admin check table t")
		})
	}
}

func TestVarcharToBlobPreservesStatisticsAndOtherConversions(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t(c varchar(10) collate utf8mb4_general_ci)")
	tk.MustExec("insert into t values ('a'), ('A'), ('a'), ('b'), ('c'), ('你好')")
	tk.MustExec("analyze table t with 2 topn, 2 buckets")
	tableID := external.GetTableByName(t, tk, "test", "t").Meta().ID
	queries := []string{
		fmt.Sprintf("select * from mysql.stats_histograms where table_id=%d order by hist_id", tableID),
		fmt.Sprintf("select * from mysql.stats_buckets where table_id=%d order by hist_id, bucket_id", tableID),
		fmt.Sprintf("select * from mysql.stats_top_n where table_id=%d order by hist_id, value", tableID),
	}
	before := make([][][]any, len(queries))
	for i, query := range queries {
		before[i] = tk.MustQuery(query).Rows()
		require.NotEmpty(t, before[i])
	}
	tk.MustExec("alter table t modify column c blob")
	for i, query := range queries {
		require.Equal(t, before[i], tk.MustQuery(query).Rows())
	}
	tk.MustQuery("select count(*) from t where c='a'").Check(testkit.Rows("2"))
	for _, prefix := range []string{"tiny", "", "medium", "long"} {
		t.Run(prefix+"blob", func(t *testing.T) {
			tk := testkit.NewTestKit(t, store)
			tk.MustExec("use test")
			tk.MustExec("drop table if exists legacy")
			tk.MustExec("create table legacy(v varchar(10), b varbinary(10), key idx(b(3)))")
			tk.MustExec("insert into legacy values ('你好', x'00FF80')")
			tk.MustExec("alter table legacy modify column v " + prefix + "text")
			tk.MustExec("alter table legacy modify column b " + prefix + "blob")
			tk.MustQuery("select hex(v), hex(b) from legacy").Check(testkit.Rows("E4BDA0E5A5BD 00FF80"))
			tk.MustExec("admin check table legacy")
		})
	}
}

func TestVarcharToTextPreservesExistingConversions(t *testing.T) {
	store := testkit.CreateMockStore(t)
	for _, tc := range []struct {
		name, definition, target, value, lengths string
	}{
		{"prefix_index", "c varchar(100) charset utf8mb4, key idx(c(5))", "text charset utf8mb4", "你好😀", "3 10"},
		{"wide_utf8", "c varchar(20000) charset utf8", "text charset utf8", strings.Repeat("你", 20000), "20000 60000"},
		{"gbk", "c varchar(100) charset gbk", "text charset gbk", "你好", "2 4"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tk := testkit.NewTestKit(t, store)
			tk.MustExec("use test")
			tk.MustExec("drop table if exists t")
			tk.MustExec("create table t(" + tc.definition + ")")
			tk.MustExec("insert into t values (?)", tc.value)
			tk.MustExec("alter table t modify column c " + tc.target)
			tk.MustQuery("select c from t").Check(testkit.Rows(tc.value))
			tk.MustQuery("select char_length(c), length(c) from t").Check(testkit.Rows(tc.lengths))
			if tc.name == "prefix_index" {
				tk.MustQuery("select c from t force index(idx) where c = ?", tc.value).Check(testkit.Rows(tc.value))
			}
			tk.MustExec("admin check table t")
		})
	}

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table t")
	tk.MustExec("create table t(c varchar(100) charset utf8mb4)")
	tk.MustExec("insert into t values ('你好')")
	err := tk.ExecToErr("alter table t modify column c text charset gbk")
	require.ErrorContains(t, err, "Unsupported modify charset from utf8mb4 to gbk")
	tk.MustQuery("select c, charset(c) from t").Check(testkit.Rows("你好 utf8mb4"))
}

func TestVarcharToBlobRechecksQueuedIndex(t *testing.T) {
	store := testkit.CreateMockStore(t)
	for _, tc := range []struct {
		name, addIndex, alterColumn, errorMessage string
	}{
		{"prefix", "add index idx(c(3))", "modify column c blob", ""},
		{"partial_condition_with_rename", "add index idx(id) where c='你好'", "change column c c2 blob", "it is referenced in partial index"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tk := testkit.NewTestKit(t, store)
			tk.MustExec("use test")
			tk.MustExec("drop table if exists t")
			tk.MustExec("create table t(id int, c varchar(10))")
			defer ingesttestutil.InjectMockBackendCtx(t, store)()
			var modifyType atomic.Uint32
			testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/getModifyColumnType", func(tp byte) { modifyType.Store(uint32(tp)) })
			tk.MustExec("insert into t values (1, '你好')")
			before := external.GetTableByName(t, tk, "test", "t").Meta().Columns[1].Clone()
			startSchedule := make(chan struct{})
			var release sync.Once
			defer release.Do(func() { close(startSchedule) })
			submitted := make(chan struct{}, 2)
			testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/beforeLoadAndDeliverJobs", func() { <-startSchedule })
			testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/afterGetJobFromLimitCh", func(chan *ddl.JobWrapper) { submitted <- struct{}{} })
			indexDone, modifyDone := make(chan error, 1), make(chan error, 1)
			go func() {
				tk2 := testkit.NewTestKit(t, store)
				indexDone <- tk2.ExecToErr("alter table test.t " + tc.addIndex)
			}()
			select {
			case <-submitted:
			case <-time.After(10 * time.Second):
				t.Fatal("add index was not submitted")
			}
			go func() {
				tk2 := testkit.NewTestKit(t, store)
				modifyDone <- tk2.ExecToErr("alter table test.t " + tc.alterColumn)
			}()
			require.Eventually(t, func() bool {
				jobs, err := ddl.GetAllDDLJobs(t.Context(), tk.Session())
				require.NoError(t, err)
				return len(jobs) == 2
			}, 10*time.Second, 100*time.Millisecond)
			// NeedReorg is omitted from the serialized job, but the scheduler persists it separately.
			tk.MustQuery(fmt.Sprintf("select reorg from mysql.tidb_ddl_job where type=%d", model.ActionModifyColumn)).Check(testkit.Rows("0"))
			release.Do(func() { close(startSchedule) })
			require.NoError(t, <-indexDone)
			if tc.errorMessage != "" {
				require.ErrorContains(t, <-modifyDone, tc.errorMessage)
				require.Equal(t, before, external.GetTableByName(t, tk, "test", "t").Meta().Columns[1])
			} else {
				require.NoError(t, <-modifyDone)
				require.Equal(t, uint32(model.ModifyTypeReorg), modifyType.Load())
				tk.MustQuery("select hex(c) from t force index(idx) where c = '你好'").Check(testkit.Rows("E4BDA0E5A5BD"))
			}
			tk.MustQuery("select hex(c) from t").Check(testkit.Rows("E4BDA0E5A5BD"))
			tk.MustExec("admin check table t")
		})
	}
}
