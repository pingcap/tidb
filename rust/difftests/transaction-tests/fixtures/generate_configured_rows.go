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

//go:build ignore

// This generator emits the exact bytes TiDB persists for the campaign-28
// configured table shape: one clustered signed BIGINT primary key carried by
// the record key, and signed BIGINT NOT NULL stored columns carried by a
// version-2 row value.
//
// The authoritative owners are pkg/tablecodec/tablecodec.go
// (EncodeRowKeyWithHandle) and pkg/util/rowcodec/encoder.go (Encoder.Encode).
// The handle column is deliberately absent from every value below because
// pkg/table/tables/tables.go CanSkip skips col.IsPKHandleColumn when building
// the row, so a clustered signed handle exists only in the key.
package main

import (
	"encoding/hex"
	"fmt"
	"math"
	"time"

	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/rowcodec"
)

// The campaign-28 fixture: `campaign28.accounts(id BIGINT PRIMARY KEY
// CLUSTERED, balance BIGINT NOT NULL)`. Column IDs follow TiDB's allocation
// order, so `id` is 1 and `balance` is 2. `id` never reaches a value, so only
// `balance` needs a constant here.
const (
	tableID       = 114
	balanceColumn = 2
)

func rowValue(colIDs []int64, values []types.Datum) []byte {
	var encoder rowcodec.Encoder
	encoded, err := encoder.Encode(time.UTC, colIDs, values, nil, nil)
	if err != nil {
		panic(err)
	}
	return encoded
}

func emit(name string, encoded []byte) {
	fmt.Printf("%s=%s\n", name, hex.EncodeToString(encoded))
}

func main() {
	// Record keys across the signed handle domain.
	for _, handle := range []int64{math.MinInt64, -1, 0, 1, 10, 11, math.MaxInt64} {
		emit(
			fmt.Sprintf("key_%d", handle),
			tablecodec.EncodeRowKeyWithHandle(tableID, kv.IntHandle(handle)),
		)
	}

	// One stored signed column across every compact rowcodec width.
	for _, balance := range []int64{
		math.MinInt64,
		math.MinInt32 - 1,
		math.MinInt32,
		math.MinInt16 - 1,
		math.MinInt16,
		math.MinInt8 - 1,
		math.MinInt8,
		-1,
		0,
		1,
		100,
		math.MaxInt8,
		math.MaxInt8 + 1,
		math.MaxInt16,
		math.MaxInt16 + 1,
		math.MaxInt32,
		math.MaxInt32 + 1,
		math.MaxInt64,
	} {
		emit(
			fmt.Sprintf("value_balance_%d", balance),
			rowValue(
				[]int64{balanceColumn},
				[]types.Datum{types.NewIntDatum(balance)},
			),
		)
	}

	// Two stored signed columns, supplied out of ID order so the generator
	// also pins Go's own not-null column-ID sort.
	emit(
		"value_two_columns_unsorted",
		rowValue(
			[]int64{balanceColumn + 1, balanceColumn},
			[]types.Datum{types.NewIntDatum(-7), types.NewIntDatum(100)},
		),
	)

	// A large column ID forces the row-format large flag and u32 ID metadata.
	emit(
		"value_large_column_id",
		rowValue(
			[]int64{256},
			[]types.Datum{types.NewIntDatum(100)},
		),
	)

	// The handle column never appears in a clustered row value; this vector
	// records what Go writes when only the stored column is supplied for the
	// exact row the campaign-28 live proof inserts.
	emit(
		"value_accounts_id10_balance100",
		rowValue(
			[]int64{balanceColumn},
			[]types.Datum{types.NewIntDatum(100)},
		),
	)
	emit(
		"key_accounts_id10",
		tablecodec.EncodeRowKeyWithHandle(tableID, kv.IntHandle(10)),
	)

	// String column values, stored the way TiDB persists a `CHAR(N)` column at
	// the default `utf8mb4_bin` collation: NeedRestoredData is false for that
	// case (pkg/types/etc.go), so the table encoder passes the plain string
	// datum to rowcodec and no restored-collation bytes are appended. The row
	// therefore carries only the raw value bytes, addressed by the offset table.
	const stringColumn = 5
	for name, text := range map[string]string{
		"value_char_empty":     "",
		"value_char_hello":     "hello",
		"value_char_multibyte": "héllo😀",
		"value_char_spaces":    "ab  ",
	} {
		emit(
			name,
			rowValue(
				[]int64{stringColumn},
				[]types.Datum{types.NewStringDatum(text)},
			),
		)
	}
}
