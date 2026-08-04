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

// This generator drives the REAL `pkg/util/chunk.DataInDiskByRows` -- the
// row-addressed spill container -- and prints the BYTES GO WROTE TO DISK for
// each case, so the Rust port is compared against Go's framing and never
// against itself.
//
// Per case three lines are printed:
//
//	<case>.data<TAB><hex of the whole data spill file, checksum layer included>
//	<case>.offsets<TAB><hex of the whole offset spill file>
//	<case>.rows<TAB><chkIdx:rowIdx=datum,datum,...|...>   (read back through GetRow)
//
// The data file hex is the file AS IT SITS ON DISK: every 1024-byte payload
// block is preceded by its 4-byte CRC32-Castagnoli checksum, because
// diskFileReaderWriter always wraps the file in checksum.NewWriter. That is
// exactly the layer the Rust port must reproduce.
//
// Reproduce with, from the repository root:
//
//	go run rust/difftests/chunk-tests/fixtures/generate_row_in_disk_vectors.go \
//	  > rust/difftests/chunk-tests/fixtures/row_in_disk_vectors.tsv

package main

import (
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
)

func ft(tp byte) *types.FieldType { return types.NewFieldType(tp) }

func mustJSON(s string) types.BinaryJSON {
	j, err := types.ParseBinaryJSONFromString(s)
	if err != nil {
		panic(err)
	}
	return j
}

// tempDirFiles returns the data file and the offset file the container just
// created. The names are `chunk.DataInDiskByRows<label><random>` and
// `chunk.DataInDiskByRowsOffset<label><random>`, so the offset file must be
// matched FIRST -- its name has the data file's name as a prefix.
func tempDirFiles(dir string) (dataPath, offsetPath string) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		panic(err)
	}
	for _, e := range entries {
		name := e.Name()
		switch {
		case strings.HasPrefix(name, "chunk.DataInDiskByRowsOffset"):
			offsetPath = filepath.Join(dir, name)
		case strings.HasPrefix(name, "chunk.DataInDiskByRows"):
			dataPath = filepath.Join(dir, name)
		}
	}
	if dataPath == "" || offsetPath == "" {
		panic("spill files not found in " + dir)
	}
	return dataPath, offsetPath
}

func readHex(path string) string {
	b, err := os.ReadFile(path)
	if err != nil {
		panic(err)
	}
	return hex.EncodeToString(b)
}

func emit(name string, fields []*types.FieldType, chks []*chunk.Chunk) {
	dir, err := os.MkdirTemp("", "rowindisk")
	if err != nil {
		panic(err)
	}
	conf := config.GetGlobalConfig()
	newConf := *conf
	newConf.TempStoragePath = dir
	config.StoreGlobalConfig(&newConf)

	l := chunk.NewDataInDiskByRows(fields)
	for _, chk := range chks {
		if err := l.Add(chk); err != nil {
			panic(err)
		}
	}

	// Read the files BEFORE Close, which unlinks them. The container's write
	// buffer still holds the tail of the last block; that tail is not on disk
	// and so is not in this hex -- exactly the situation ReaderWithCache
	// exists for, and the Rust port must land in the same state.
	dataPath, offsetPath := tempDirFiles(dir)
	fmt.Printf("%s.data\t%s\n", name, readHex(dataPath))
	fmt.Printf("%s.offsets\t%s\n", name, readHex(offsetPath))

	var rows []string
	for chkIdx := range l.NumChunks() {
		for rowIdx := range l.NumRowsOfChunk(chkIdx) {
			row, err := l.GetRow(chunk.RowPtr{ChkIdx: uint32(chkIdx), RowIdx: uint32(rowIdx)})
			if err != nil {
				panic(err)
			}
			var cells []string
			for colIdx := range fields {
				if row.IsNull(colIdx) {
					cells = append(cells, "NULL")
					continue
				}
				cells = append(cells, hex.EncodeToString(row.GetRaw(colIdx)))
			}
			rows = append(rows, fmt.Sprintf("%d:%d=%s", chkIdx, rowIdx, strings.Join(cells, ",")))
		}
	}
	fmt.Printf("%s.rows\t%s\n", name, strings.Join(rows, "|"))
	fmt.Printf("%s.meta\tnumChunks=%d,len=%d\n", name, l.NumChunks(), l.Len())

	if err := l.Close(); err != nil {
		panic(err)
	}
	if err := os.RemoveAll(dir); err != nil {
		panic(err)
	}
}

func main() {
	// 1. The shape `row_in_disk_test.go` itself uses: a var-string, two
	//    always-null columns, an int, and a JSON column that is null on odd
	//    chunks. Strings are fixed here (the Go test randomises them).
	{
		fields := []*types.FieldType{
			ft(mysql.TypeVarString),
			ft(mysql.TypeLonglong),
			ft(mysql.TypeVarString),
			ft(mysql.TypeLonglong),
			ft(mysql.TypeJSON),
		}
		numChk, numRow := 3, 8
		chks := make([]*chunk.Chunk, 0, numChk)
		for chkIdx := range numChk {
			chk := chunk.NewChunkWithCapacity(fields, numRow)
			for rowIdx := range numRow {
				data := int64(chkIdx*numRow + rowIdx)
				chk.AppendString(0, strings.Repeat("西xi瓜gua", rowIdx+1))
				chk.AppendNull(1)
				chk.AppendNull(2)
				chk.AppendInt64(3, data)
				if chkIdx%2 == 0 {
					chk.AppendJSON(4, mustJSON(`{"a": [1, 2, "b"]}`))
				} else {
					chk.AppendNull(4)
				}
			}
			chks = append(chks, chk)
		}
		emit("mixed", fields, chks)
	}

	// 2. One fixed-length column only: every row is exactly 8 size bytes plus
	//    8 data bytes, so the offsets advance by a constant.
	{
		fields := []*types.FieldType{ft(mysql.TypeLonglong)}
		chk := chunk.NewChunkWithCapacity(fields, 4)
		chk.AppendInt64(0, 1)
		chk.AppendNull(0)
		chk.AppendInt64(0, -2)
		emit("int64_with_null", fields, []*chunk.Chunk{chk})
	}

	// 3. Enough rows to cross the checksum layer's 1024-byte block boundary
	//    several times, so the block headers are part of what is compared.
	{
		fields := []*types.FieldType{ft(mysql.TypeVarchar), ft(mysql.TypeLonglong)}
		chks := make([]*chunk.Chunk, 0, 4)
		for chkIdx := range 4 {
			chk := chunk.NewChunkWithCapacity(fields, 50)
			for rowIdx := range 50 {
				chk.AppendString(0, strings.Repeat("z", (chkIdx*50+rowIdx)%17))
				chk.AppendInt64(1, int64(chkIdx*50+rowIdx))
			}
			chks = append(chks, chk)
		}
		emit("many_blocks", fields, chks)
	}
}
