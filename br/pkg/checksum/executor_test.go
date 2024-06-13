// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package checksum_test

import (
	"context"
	"math"
	"testing"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/br/pkg/checksum"
	"github.com/pingcap/tidb/br/pkg/metautil"
	"github.com/pingcap/tidb/br/pkg/mock"
	"github.com/pingcap/tidb/pkg/distsql"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

func getTableInfo(t *testing.T, mock *mock.Cluster, db, table string) *model.TableInfo {
	info, err := mock.Domain.GetSnapshotInfoSchema(math.MaxUint64)
	require.NoError(t, err)
	cDBName := model.NewCIStr(db)
	cTableName := model.NewCIStr(table)
	tableInfo, err := info.TableByName(cDBName, cTableName)
	require.NoError(t, err)
	return tableInfo.Meta()
}

func getTableInfo2(t *testing.B, mock *mock.Cluster, db, table string) *model.TableInfo {
	info, err := mock.Domain.GetSnapshotInfoSchema(math.MaxUint64)
	require.NoError(t, err)
	cDBName := model.NewCIStr(db)
	cTableName := model.NewCIStr(table)
	tableInfo, err := info.TableByName(cDBName, cTableName)
	require.NoError(t, err)
	return tableInfo.Meta()
}

func TestChecksumContextDone(t *testing.T) {
	mock, err := mock.NewCluster()
	require.NoError(t, err)
	require.NoError(t, mock.Start())
	defer mock.Stop()

	tk := testkit.NewTestKit(t, mock.Storage)
	tk.MustExec("use test")

	tk.MustExec("drop table if exists t1;")
	tk.MustExec("create table t1 (a int, b int, key i1(a, b), primary key (a));")
	tk.MustExec("insert into t1 values (10, 10);")
	tableInfo1 := getTableInfo(t, mock, "test", "t1")
	exe, err := checksum.NewExecutorBuilder(tableInfo1, math.MaxUint64).
		SetConcurrency(variable.DefChecksumTableConcurrency).
		Build()
	require.NoError(t, err)

	ctx := context.Background()
	cctx, cancel := context.WithCancel(ctx)

	cancel()

	resp, err := exe.Execute(cctx, mock.Storage.GetClient(), func() { t.Log("request done") })
	t.Log(err)
	t.Log(resp)
	require.Error(t, err)
}

func TestChecksum(t *testing.T) {
	mock, err := mock.NewCluster()
	require.NoError(t, err)
	require.NoError(t, mock.Start())
	defer mock.Stop()

	tk := testkit.NewTestKit(t, mock.Storage)
	tk.MustExec("use test")

	tk.MustExec("drop table if exists t1;")
	tk.MustExec("create table t1 (a int);")
	tk.MustExec("insert into t1 values (10);")
	tableInfo1 := getTableInfo(t, mock, "test", "t1")
	exe1, err := checksum.NewExecutorBuilder(tableInfo1, math.MaxUint64).
		SetConcurrency(variable.DefChecksumTableConcurrency).
		Build()
	require.NoError(t, err)
	require.NoError(t, exe1.Each(func(r *kv.Request) error {
		require.True(t, r.NotFillCache)
		require.Equal(t, variable.DefChecksumTableConcurrency, r.Concurrency)
		return nil
	}))
	require.Equal(t, 1, exe1.Len())
	resp, err := exe1.Execute(context.TODO(), mock.Storage.GetClient(), func() {})
	require.NoError(t, err)
	// Cluster returns a dummy checksum (all fields are 1).
	require.Equalf(t, uint64(1), resp.Checksum, "%v", resp)
	require.Equalf(t, uint64(1), resp.TotalKvs, "%v", resp)
	require.Equalf(t, uint64(1), resp.TotalBytes, "%v", resp)

	tk.MustExec("drop table if exists t2;")
	tk.MustExec("create table t2 (a int);")
	tk.MustExec("alter table t2 add index i2(a);")
	tk.MustExec("insert into t2 values (10);")
	tableInfo2 := getTableInfo(t, mock, "test", "t2")
	exe2, err := checksum.NewExecutorBuilder(tableInfo2, math.MaxUint64).Build()
	require.NoError(t, err)
	require.Equalf(t, 2, exe2.Len(), "%v", tableInfo2)
	resp2, err := exe2.Execute(context.TODO(), mock.Storage.GetClient(), func() {})
	require.NoError(t, err)
	require.Equalf(t, uint64(0), resp2.Checksum, "%v", resp2)
	require.Equalf(t, uint64(2), resp2.TotalKvs, "%v", resp2)
	require.Equalf(t, uint64(2), resp2.TotalBytes, "%v", resp2)

	// Test rewrite rules
	tk.MustExec("alter table t1 add index i2(a);")
	tableInfo1 = getTableInfo(t, mock, "test", "t1")
	oldTable := metautil.Table{Info: tableInfo1}
	exe2, err = checksum.NewExecutorBuilder(tableInfo2, math.MaxUint64).
		SetOldTable(&oldTable).Build()
	require.NoError(t, err)
	require.Equal(t, 2, exe2.Len())
	rawReqs, err := exe2.RawRequests()
	require.NoError(t, err)
	require.Len(t, rawReqs, 2)
	for _, rawReq := range rawReqs {
		require.NotNil(t, rawReq.Rule)
	}
	resp2, err = exe2.Execute(context.TODO(), mock.Storage.GetClient(), func() {})
	require.NoError(t, err)
	require.NotNil(t, resp2)

	// Test commonHandle ranges

	tk.MustExec("drop table if exists t3;")
	tk.MustExec("create table t3 (a char(255), b int, primary key(a) CLUSTERED);")
	tk.MustExec("insert into t3 values ('fffffffff', 1), ('010101010', 2), ('394393fj39efefe', 3);")
	tableInfo3 := getTableInfo(t, mock, "test", "t3")
	exe3, err := checksum.NewExecutorBuilder(tableInfo3, math.MaxUint64).Build()
	require.NoError(t, err)
	first := true
	require.NoError(t, exe3.Each(func(req *kv.Request) error {
		if first {
			first = false
			ranges, err := distsql.BuildTableRanges(tableInfo3)
			require.NoError(t, err)
			require.Equalf(t, ranges[:1], req.KeyRanges.FirstPartitionRange(), "%v", req.KeyRanges.FirstPartitionRange())
		}
		return nil
	}))

	exe4, err := checksum.NewExecutorBuilder(tableInfo3, math.MaxUint64).Build()
	require.NoError(t, err)
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/br/pkg/checksum/checksumRetryErr", `1*return(true)`))
	failpoint.Disable("github.com/pingcap/tidb/br/pkg/checksum/checksumRetryErr")
	resp4, err := exe4.Execute(context.TODO(), mock.Storage.GetClient(), func() {})
	require.NoError(t, err)
	require.NotNil(t, resp4)
}

func BenchmarkDecodeTableInfo(b *testing.B) {
	rawJson := `{
 "id": 106,
 "name": {
  "O": "tp",
  "L": "tp"
 },
 "charset": "utf8mb4",
 "collate": "utf8mb4_bin",
 "cols": [
  {
   "id": 1,
   "name": {
    "O": "a",
    "L": "a"
   },
   "offset": 0,
   "origin_default": null,
   "origin_default_bit": null,
   "default": null,
   "default_bit": null,
   "default_is_expr": false,
   "generated_expr_string": "",
   "generated_stored": false,
   "dependences": null,
   "type": {
    "Tp": 3,
    "Flag": 0,
    "Flen": 11,
    "Decimal": 0,
    "Charset": "binary",
    "Collate": "binary",
    "Elems": null,
    "ElemsIsBinaryLit": null,
    "Array": false
   },
   "state": 5,
   "comment": "",
   "hidden": false,
   "change_state_info": null,
   "version": 2
  }
 ],
 "index_info": null,
 "constraint_info": null,
 "fk_info": null,
 "state": 5,
 "pk_is_handle": false,
 "is_common_handle": false,
 "common_handle_version": 0,
 "comment": "",
 "auto_inc_id": 0,
 "auto_id_cache": 0,
 "auto_rand_id": 0,
 "max_col_id": 1,
 "max_idx_id": 0,
 "max_fk_id": 0,
 "max_cst_id": 0,
 "update_timestamp": 450117177897648130,
 "ShardRowIDBits": 0,
 "max_shard_row_id_bits": 0,
 "auto_random_bits": 0,
 "auto_random_range_bits": 0,
 "pre_split_regions": 0,
 "partition": {
  "type": 2,
  "expr": "a",
  "columns": null,
  "enable": true,
  "is_empty_columns": false,
  "definitions": [
   {
    "id": 107,
    "name": {
     "O": "p0",
     "L": "p0"
    },
    "less_than": null,
    "in_values": null,
    "policy_ref_info": null
   },
   {
    "id": 108,
    "name": {
     "O": "p1",
     "L": "p1"
    },
    "less_than": null,
    "in_values": null,
    "policy_ref_info": null
   },
   {
    "id": 109,
    "name": {
     "O": "p2",
     "L": "p2"
    },
    "less_than": null,
    "in_values": null,
    "policy_ref_info": null
   },
   {
    "id": 110,
    "name": {
     "O": "p3",
     "L": "p3"
    },
    "less_than": null,
    "in_values": null,
    "policy_ref_info": null
   }
  ],
  "adding_definitions": null,
  "dropping_definitions": null,
  "NewPartitionIDs": null,
  "states": null,
  "num": 4,
  "ddl_state": 0,
  "new_table_id": 0,
  "ddl_type": 0,
  "ddl_expr": "",
  "ddl_columns": null
 },
 "compression": "",
 "view": null,
 "sequence": null,
 "Lock": null,
 "version": 5,
 "tiflash_replica": null,
 "is_columnar": false,
 "temp_table_type": 0,
 "cache_table_status": 0,
 "policy_ref_info": null,
 "stats_options": null,
 "exchange_partition_info": null,
 "ttl_info": null,
 "revision": 0
}
`

	p := gjson.Parse(rawJson)
	value := p.Get("partition")
	value = p.Get("tiflash_replica")
	value.Exists()
	//idPattern := `"L":\s*"([^"]+)"`
	//idRegex := regexp.MustCompile(idPattern)

	//emptyRegex := regexp.MustCompile(`\"partition\": {"`)

	//var m model.TableInfo
	for i := 0; i < b.N; i++ {
		//nameLMatches = idRegex.FindStringSubmatch(rawJson)
		//if len(nameLMatches) > 1 {
		//	nameL := nameLMatches[1]
		//	//fmt.Println("Name.L:", nameL)
		//}

		//strings.Contains(rawJson, "\"partition\": {")
		//p := gjson.Parse(string(hack.String(rawJson)))
		//value := gjson.Get(string(hack.String(rawJson)), "[partition, tiflash_replica, ttl_info]")
		//value = p.Get("tiflash_replica")
		//_ = emptyRegex.MatchString(rawJson)

		//value := gjson.Get(string(hack.String(rawJson)), "partition")
		//value.Exists()
		//json.Unmarshal(hack.Slice(rawJson), &m)
	}
}
