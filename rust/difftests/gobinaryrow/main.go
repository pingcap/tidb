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
// See the License for the specific language governing permissions and
// limitations under the License.

// Command gobinaryrow emits byte-level fixtures for TiDB's MySQL binary
// protocol result rows by calling the production `column.DumpBinaryRow`
// (pkg/server/internal/column/column.go) on real chunk rows.
//
// Each line of output is `<case name> <hex of the dumped row>`. The Rust
// tidb-protocol encoder is checked against these bytes; nothing here is
// self-round-tripped.
package main

import (
	"encoding/hex"
	"fmt"
	"os"
	"time"

	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/server/internal/column"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
)

func info(tp byte) *column.Info {
	return &column.Info{Type: tp, Charset: uint16(mysql.DefaultCollationID)}
}

func emit(name string, cols []*column.Info, fill func(c *chunk.Chunk)) {
	fieldTypes := make([]*types.FieldType, 0, len(cols))
	for _, c := range cols {
		ft := types.NewFieldType(c.Type)
		ft.SetFlen(types.UnspecifiedLength)
		ft.SetDecimal(types.UnspecifiedLength)
		fieldTypes = append(fieldTypes, ft)
	}
	c := chunk.NewChunkWithCapacity(fieldTypes, 1)
	fill(c)
	out, err := column.DumpBinaryRow(nil, cols, c.GetRow(0), nil)
	if err != nil {
		fmt.Printf("%s ERR:%v\n", name, err)
		return
	}
	fmt.Printf("%s %s\n", name, hex.EncodeToString(out))
}

func mustTime(s string, tp byte, fsp int) types.Time {
	t, err := types.ParseTime(types.DefaultStmtNoWarningContext, s, tp, fsp)
	if err != nil {
		fmt.Fprintf(os.Stderr, "parse %q: %v\n", s, err)
		os.Exit(1)
	}
	return t
}

func main() {
	// --- DATE / DATETIME / TIMESTAMP: every dump.BinaryDateTime shape.
	emit("datetime_micros", []*column.Info{info(mysql.TypeDatetime)}, func(c *chunk.Chunk) {
		c.AppendTime(0, mustTime("2017-01-05 23:59:59.575601", mysql.TypeDatetime, 6))
	})
	emit("datetime_seconds", []*column.Info{info(mysql.TypeDatetime)}, func(c *chunk.Chunk) {
		c.AppendTime(0, mustTime("2017-01-05 23:59:59", mysql.TypeDatetime, 0))
	})
	emit("datetime_midnight", []*column.Info{info(mysql.TypeDatetime)}, func(c *chunk.Chunk) {
		c.AppendTime(0, mustTime("2017-01-05 00:00:00", mysql.TypeDatetime, 0))
	})
	emit("datetime_zero", []*column.Info{info(mysql.TypeDatetime)}, func(c *chunk.Chunk) {
		c.AppendTime(0, types.NewTime(types.ZeroCoreTime, mysql.TypeDatetime, 0))
	})
	emit("timestamp_micros", []*column.Info{info(mysql.TypeTimestamp)}, func(c *chunk.Chunk) {
		c.AppendTime(0, mustTime("2020-06-15 12:34:56.000001", mysql.TypeTimestamp, 6))
	})
	emit("date_plain", []*column.Info{info(mysql.TypeDate)}, func(c *chunk.Chunk) {
		c.AppendTime(0, mustTime("2020-06-15", mysql.TypeDate, 0))
	})
	emit("date_zero", []*column.Info{info(mysql.TypeDate)}, func(c *chunk.Chunk) {
		c.AppendTime(0, types.NewTime(types.ZeroCoreTime, mysql.TypeDate, 0))
	})

	// --- DURATION: every dump.BinaryTime shape.
	for _, tc := range []struct {
		name string
		dur  time.Duration
	}{
		{"duration_zero", 0},
		{"duration_neg_1ns", -1},
		{"duration_1d2h3m4s", 26*time.Hour + 3*time.Minute + 4*time.Second},
		{"duration_2s", 2 * time.Second},
		{"duration_micros", time.Hour + 2*time.Minute + 3*time.Second + 456789*time.Microsecond},
		{"duration_negative", -(10*time.Hour + 20*time.Minute + 30*time.Second)},
	} {
		d := tc.dur
		emit(tc.name, []*column.Info{info(mysql.TypeDuration)}, func(c *chunk.Chunk) {
			c.AppendDuration(0, types.Duration{Duration: d, Fsp: 6})
		})
	}

	// --- string group members the previous encoder rejected.
	emit("blob", []*column.Info{info(mysql.TypeBlob)}, func(c *chunk.Chunk) {
		c.AppendBytes(0, []byte("hello blob"))
	})
	emit("tiny_blob", []*column.Info{info(mysql.TypeTinyBlob)}, func(c *chunk.Chunk) {
		c.AppendBytes(0, []byte("tiny"))
	})
	emit("long_blob", []*column.Info{info(mysql.TypeLongBlob)}, func(c *chunk.Chunk) {
		c.AppendBytes(0, []byte("long"))
	})
	emit("bit", []*column.Info{info(mysql.TypeBit)}, func(c *chunk.Chunk) {
		c.AppendBytes(0, []byte{0x01, 0x02})
	})
	emit("enum", []*column.Info{info(mysql.TypeEnum)}, func(c *chunk.Chunk) {
		c.AppendEnum(0, types.Enum{Name: "green", Value: 2})
	})
	emit("set", []*column.Info{info(mysql.TypeSet)}, func(c *chunk.Chunk) {
		c.AppendSet(0, types.Set{Name: "a,c", Value: 5})
	})
	emit("json", []*column.Info{info(mysql.TypeJSON)}, func(c *chunk.Chunk) {
		j, err := types.ParseBinaryJSONFromString(`{"a": [1, 2]}`)
		if err != nil {
			panic(err)
		}
		c.AppendJSON(0, j)
	})

	// --- mixed row: null bitmap interleaved with a temporal and a blob.
	mixed := []*column.Info{
		info(mysql.TypeLonglong),
		info(mysql.TypeDatetime),
		info(mysql.TypeDuration),
		info(mysql.TypeBlob),
	}
	emit("mixed_row", mixed, func(c *chunk.Chunk) {
		c.AppendInt64(0, 7)
		c.AppendTime(1, mustTime("1999-12-31 23:59:58", mysql.TypeDatetime, 0))
		c.AppendDuration(2, types.Duration{Duration: 90 * time.Second, Fsp: 0})
		c.AppendBytes(3, []byte("tail"))
	})
	emit("mixed_row_nulls", mixed, func(c *chunk.Chunk) {
		c.AppendInt64(0, 7)
		c.AppendNull(1)
		c.AppendDuration(2, types.Duration{Duration: 90 * time.Second, Fsp: 0})
		c.AppendNull(3)
	})
}
