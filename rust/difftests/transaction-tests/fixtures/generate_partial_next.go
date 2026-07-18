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

//go:build ignore

// This generator deliberately calls the same TiDB codec path and uses the
// same values as pkg/kv/key_test.go TestPartialNext. Its stdout is the reviewed
// fixture stored in partial_next.hex.
package main

import (
	"encoding/hex"
	"fmt"
	"time"

	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/codec"
)

func mustEncode(values ...types.Datum) []byte {
	sc := stmtctx.NewStmtCtxWithTimeZone(time.Local)
	encoded, err := codec.EncodeValue(sc.TimeZone(), nil, values...)
	if err != nil {
		panic(err)
	}
	return encoded
}

func main() {
	keyA := mustEncode(types.NewDatum("abc"), types.NewDatum("def"))
	keyB := mustEncode(types.NewDatum("abca"), types.NewDatum("def"))
	seekKey := mustEncode(types.NewDatum("abc"))

	fmt.Printf("key_a=%s\n", hex.EncodeToString(keyA))
	fmt.Printf("key_b=%s\n", hex.EncodeToString(keyB))
	fmt.Printf("seek_key=%s\n", hex.EncodeToString(seekKey))
}
