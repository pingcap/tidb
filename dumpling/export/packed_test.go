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

package export

import (
	"bytes"
	"context"
	"testing"

	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func TestPackedProtocolRows(t *testing.T) {
	testCases := []struct {
		name      string
		encoded   []byte
		key       []byte
		value     []byte
		end       bool
		errorText string
	}{
		{
			name:    "binary row",
			encoded: []byte{2, 0, 0, 0, 2, 0, 0, 0, 'k', 0, 'v', 0xff},
			key:     []byte{'k', 0},
			value:   []byte{'v', 0xff},
		},
		{
			name: "clean stream EOF",
			end:  true,
		},
		{
			name:      "empty key",
			encoded:   []byte{0, 0, 0, 0, 0, 0, 0, 0},
			errorText: "invalid packed row with empty key",
		},
		{
			name:      "truncated key size",
			encoded:   []byte{2, 0},
			errorText: "read packed row key size: unexpected EOF",
		},
		{
			name:      "truncated key",
			encoded:   []byte{2, 0, 0, 0, 0, 0, 0, 0, 'k'},
			errorText: "read packed row key: unexpected EOF",
		},
		{
			name:      "truncated value",
			encoded:   []byte{1, 0, 0, 0, 2, 0, 0, 0, 'k', 'v'},
			errorText: "read packed row value: unexpected EOF",
		},
	}
	for _, testCase := range testCases {
		key, value, end, err := readPackedRow(bytes.NewReader(testCase.encoded), make([]byte, 0, 8), make([]byte, 0, 8))
		if testCase.errorText != "" {
			require.EqualError(t, err, testCase.errorText, testCase.name)
			continue
		}
		require.NoError(t, err, testCase.name)
		require.Equal(t, testCase.key, key, testCase.name)
		require.Equal(t, testCase.value, value, testCase.name)
		require.Equal(t, testCase.end, end, testCase.name)
	}

	baseArgs := []string{
		"dumper",
		"--metadata-url", "s3://bucket/backup.meta",
		"--start-key-hex", "00ff",
		"--end-key-hex", "10",
	}
	require.Equal(
		t,
		baseArgs,
		cseDumperArgs("s3://bucket/backup.meta", false, []byte{0, 0xff}, []byte{0x10}),
	)
	require.Equal(
		t,
		append(baseArgs, "--legacy-encryption"),
		cseDumperArgs("s3://bucket/backup.meta", true, []byte{0, 0xff}, []byte{0x10}),
	)

	stderr := &cseDumperStderr{}
	for _, chunk := range []string{
		"human diag",
		"nostic\nCSE packed ",
		"perf part=setup manifest=1ms\nnot-CSE packed perf part=scan\n",
		"CSE packed perf part=scan total=2ms\nlast diagnostic",
	} {
		_, err := stderr.Write([]byte(chunk))
		require.NoError(t, err)
	}
	require.True(t, isCSEPackedPerfLine([]byte("CSE packed perf part=output total=3ms")))
	require.False(t, isCSEPackedPerfLine([]byte("not-CSE packed perf part=scan")))
	require.Equal(t, "human diagnostic\nnot-CSE packed perf part=scan\nlast diagnostic", stderr.diagnostics())
}

func TestPackedRowsUseTiDBStorageEncoding(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`create table packed_int (
		id bigint primary key clustered,
		name varchar(16) not null,
		note varchar(16),
		description text,
		payload varbinary(8),
		amount decimal(10,2),
		large_float float,
		large_double double,
		year_zero year,
		created datetime(3),
		flags bit(4),
		status enum('new', 'done'),
		labels set('a', 'b'),
		name_len int as (length(name)),
		id_twice bigint as (id * 2) stored
	)`)
	tk.MustExec("insert into packed_int (id, name, note, description, payload, amount, large_float, large_double, year_zero, created, flags, status, labels) values (1, 'alpha', null, 'plain text', x'00ff', -12.30, 1e20, 1e20, 0, '2026-07-16 01:02:03.456', b'1010', 'done', 'a,b')")
	tk.MustExec("insert into packed_int (id, name, note, description, payload, amount, large_float, large_double, year_zero, created, flags, status, labels) values (2, 'beta', '', '', x'', 0, 1, 1, 2000, '2020-01-02 03:04:05.000', b'0001', 'new', '')")
	tk.MustExec("alter table packed_int add column added int not null default 7, add column later_nullable varchar(8)")

	tk.MustExec("create table packed_common (tenant varchar(8), id int, value varchar(16), primary key (tenant, id) clustered)")
	tk.MustExec("insert into packed_common values ('acme', 9, 'common')")
	tk.MustExec("create table packed_common_prefix (tenant varchar(8), id int, value varchar(16), primary key (tenant(2), id) clustered)")
	tk.MustExec("insert into packed_common_prefix values ('acme', 9, 'prefix')")
	tk.MustExec("set tidb_row_format_version = 1")
	tk.MustExec("create table packed_legacy (tenant varchar(8), id int, value varchar(16), primary key (tenant(2), id) clustered)")
	tk.MustExec("insert into packed_legacy values ('legacy', 10, 'old row')")
	tk.MustExec("set tidb_row_format_version = 2")
	tk.MustExec("alter table packed_legacy add column added int not null default 7")
	tk.MustExec("create table packed_decimal_scale (id int primary key clustered, amount decimal(10,4))")
	tk.MustExec("insert into packed_decimal_scale values (1, 1.2350)")
	tk.MustExec("create table packed_partition (id int primary key, value varchar(16)) partition by range (id) (partition p0 values less than (10), partition p1 values less than maxvalue)")
	tk.MustExec("insert into packed_partition values (1, 'first'), (11, 'second')")
	tk.MustExec("create database packed_parent")
	tk.MustExec("create table packed_parent.parent (id int primary key)")
	tk.MustExec("create table packed_child (id int primary key, parent_id int, foreign key (parent_id) references packed_parent.parent(id))")

	txn, err := store.Begin()
	require.NoError(t, err)
	databases, err := loadPackedDatabases(context.Background(), func(
		_ context.Context,
		startKey, endKey []byte,
		emit func(key, value []byte) error,
	) error {
		iterator, err := txn.Iter(kv.Key(startKey), kv.Key(endKey))
		if err != nil {
			return err
		}
		defer iterator.Close()
		for iterator.Valid() {
			if err := emit(iterator.Key(), iterator.Value()); err != nil {
				return err
			}
			if err := iterator.Next(); err != nil {
				return err
			}
		}
		return nil
	})
	require.NoError(t, err)
	require.NoError(t, txn.Rollback())
	var database *model.DBInfo
	for _, candidate := range databases {
		if candidate.Name.L == "test" {
			database = candidate
			break
		}
	}
	require.NotNil(t, database)
	var packedChild *model.TableInfo
	for _, candidate := range database.Deprecated.Tables {
		if candidate.Name.L == "packed_child" {
			packedChild = candidate
			break
		}
	}
	require.NotNil(t, packedChild)
	createSQL, err := packedCreateTableSQL(database.Name, packedChild)
	require.NoError(t, err)
	require.Contains(t, createSQL, "REFERENCES `packed_parent`.`parent`")

	initColTypeRowReceiverMap()
	testCases := []struct {
		table  string
		adjust func(*model.TableInfo)
		rows   []string
	}{
		{
			table: "packed_int",
			rows: []string{
				`1,"alpha",\N,"plain text","00ff",-12.30,1e20,1e20,"0000","2026-07-16 01:02:03.456","0a","done","a,b",7,\N`,
				`2,"beta","","","",0.00,1,1,"2000","2020-01-02 03:04:05.000","01","new","",7,\N`,
			},
		},
		{
			table: "packed_common",
			rows:  []string{`"acme",9,"common"`},
		},
		{
			table: "packed_common_prefix",
			rows:  []string{`"acme",9,"prefix"`},
		},
		{
			table: "packed_legacy",
			rows:  []string{`"legacy",10,"old row",7`},
		},
		{
			table: "packed_decimal_scale",
			adjust: func(table *model.TableInfo) {
				table.Columns[1].SetDecimal(2)
			},
			rows: []string{`1,1.24`},
		},
		{
			table: "packed_partition",
			rows:  []string{`1,"first"`, `11,"second"`},
		},
	}
	for _, testCase := range testCases {
		var table *model.TableInfo
		for _, candidate := range database.Deprecated.Tables {
			if candidate.Name.L == testCase.table {
				table = candidate
				break
			}
		}
		require.NotNil(t, table, testCase.table)
		if testCase.adjust != nil {
			table = table.Clone()
			testCase.adjust(table)
		}
		rows := readPackedTestRows(t, store, table)
		require.Equal(t, testCase.rows, rows, testCase.table)
	}
}

func readPackedTestRows(t *testing.T, store kv.Storage, table *model.TableInfo) []string {
	t.Helper()
	txn, err := store.Begin()
	require.NoError(t, err)
	defer func() {
		require.NoError(t, txn.Rollback())
	}()
	meta := newPackedTableMeta("test", table, "")
	decoder, err := newPackedRowDecoder(table)
	require.NoError(t, err)
	option := &csvOption{
		nullValue:      "\\N",
		separator:      []byte(","),
		delimiter:      []byte(`"`),
		lineTerminator: []byte("\n"),
		binaryFormat:   BinaryFormatHEX,
	}
	rows := make([]string, 0)
	for _, tableID := range packedPhysicalTableIDs(table) {
		prefix := tablecodec.GenTableRecordPrefix(tableID)
		iterator, err := txn.Iter(prefix, prefix.PrefixNext())
		require.NoError(t, err)
		for iterator.Valid() {
			row := MakeRowReceiver(meta.ColumnTypes())
			packed := &packedRowIter{
				table:   table,
				decoder: decoder,
				key:     append([]byte(nil), iterator.Key()...),
				value:   append([]byte(nil), iterator.Value()...),
				args:    make([]any, meta.ColumnCount()),
				hasRow:  true,
			}
			require.NoError(t, packed.Decode(row))
			var output bytes.Buffer
			row.WriteToBufferInCsv(&output, true, option)
			rows = append(rows, output.String())
			require.NoError(t, iterator.Next())
		}
		iterator.Close()
	}
	return rows
}
