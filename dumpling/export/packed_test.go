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
	"github.com/pingcap/tidb/pkg/parser/ast"
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
			name:    "stream terminator",
			encoded: []byte{0, 0, 0, 0, 0, 0, 0, 0},
			end:     true,
		},
		{
			name:      "invalid terminator",
			encoded:   []byte{0, 0, 0, 0, 1, 0, 0, 0},
			errorText: "invalid packed row terminator with value size 1",
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
}

func TestPackedRowsUseTiDBStorageEncoding(t *testing.T) {
	store, domain := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`create table packed_int (
		id bigint primary key clustered,
		name varchar(16) not null,
		note varchar(16),
		payload varbinary(8),
		amount decimal(10,2),
		created datetime(3),
		flags bit(4),
		status enum('new', 'done'),
		labels set('a', 'b'),
		name_len int as (length(name)),
		id_twice bigint as (id * 2) stored
	)`)
	tk.MustExec("insert into packed_int (id, name, note, payload, amount, created, flags, status, labels) values (1, 'alpha', null, x'00ff', -12.30, '2026-07-16 01:02:03.456', b'1010', 'done', 'a,b')")
	tk.MustExec("insert into packed_int (id, name, note, payload, amount, created, flags, status, labels) values (2, 'beta', '', x'', 0, '2020-01-02 03:04:05.000', b'0001', 'new', '')")
	tk.MustExec("alter table packed_int add column added int not null default 7, add column later_nullable varchar(8)")

	tk.MustExec("create table packed_common (tenant varchar(8), id int, value varchar(16), primary key (tenant, id) clustered)")
	tk.MustExec("insert into packed_common values ('acme', 9, 'common')")
	tk.MustExec("create table packed_partition (id int primary key, value varchar(16)) partition by range (id) (partition p0 values less than (10), partition p1 values less than maxvalue)")
	tk.MustExec("insert into packed_partition values (1, 'first'), (11, 'second')")

	initColTypeRowReceiverMap()
	testCases := []struct {
		table string
		rows  []string
	}{
		{
			table: "packed_int",
			rows: []string{
				`1,"alpha",\N,"00ff",-12.30,"2026-07-16 01:02:03.456","0a","done","a,b",7,\N`,
				`2,"beta","","",0.00,"2020-01-02 03:04:05.000","01","new","",7,\N`,
			},
		},
		{
			table: "packed_common",
			rows:  []string{`"acme",9,"common"`},
		},
		{
			table: "packed_partition",
			rows:  []string{`1,"first"`, `11,"second"`},
		},
	}
	for _, testCase := range testCases {
		table, err := domain.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr(testCase.table))
		require.NoError(t, err, testCase.table)
		rows := readPackedTestRows(t, store, table.Meta())
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
				table:  table,
				key:    append([]byte(nil), iterator.Key()...),
				value:  append([]byte(nil), iterator.Value()...),
				args:   make([]any, meta.ColumnCount()),
				hasRow: true,
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
