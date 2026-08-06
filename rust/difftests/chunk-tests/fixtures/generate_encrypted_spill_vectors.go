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

// This generator drives Go's real DataInDiskByRows and DataInDiskByChunks
// through checksum.Writer -> encrypt.Writer -> os.File. crypto/rand.Reader is
// replaced only while each container creates its CtrCipher, making the file
// image reproducible without changing production Go code.
//
// Reproduce from the repository root with:
//
//	go run rust/difftests/chunk-tests/fixtures/generate_encrypted_spill_vectors.go \
//	  > rust/difftests/chunk-tests/fixtures/encrypted_spill_vectors.tsv

package main

import (
	"bytes"
	cryptorand "crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
)

const (
	rowsDataKeyHex     = "000102030405060708090a0b0c0d0e0f"
	rowsDataNonceHex   = "0102030405060708"
	rowsOffsetKeyHex   = "101112131415161718191a1b1c1d1e1f"
	rowsOffsetNonceHex = "1112131415161718"
	chunksKeyHex       = "202122232425262728292a2b2c2d2e2f"
	chunksNonceHex     = "2122232425262728"
)

func mustDecode(value string) []byte {
	decoded, err := hex.DecodeString(value)
	if err != nil {
		panic(err)
	}
	return decoded
}

func fixedRandom(parts ...string) io.Reader {
	var all []byte
	for _, part := range parts {
		all = append(all, mustDecode(part)...)
	}
	return bytes.NewReader(all)
}

func configureEncryptedSpill(dir string) func() {
	old := config.GetGlobalConfig()
	next := *old
	next.TempStoragePath = dir
	next.Security.SpilledFileEncryptionMethod = config.SpilledFileEncryptionMethodAES128CTR
	config.StoreGlobalConfig(&next)
	return func() { config.StoreGlobalConfig(old) }
}

func fileWithPrefix(dir, prefix string) string {
	entries, err := os.ReadDir(dir)
	if err != nil {
		panic(err)
	}
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), prefix) {
			return filepath.Join(dir, entry.Name())
		}
	}
	panic("spill file with prefix " + prefix + " not found")
}

func rowFiles(dir string) (dataPath, offsetPath string) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		panic(err)
	}
	for _, entry := range entries {
		switch {
		case strings.HasPrefix(entry.Name(), "chunk.DataInDiskByRowsOffset"):
			offsetPath = filepath.Join(dir, entry.Name())
		case strings.HasPrefix(entry.Name(), "chunk.DataInDiskByRows"):
			dataPath = filepath.Join(dir, entry.Name())
		}
	}
	if dataPath == "" || offsetPath == "" {
		panic("row spill files not found")
	}
	return dataPath, offsetPath
}

func fileHex(path string) string {
	data, err := os.ReadFile(path)
	if err != nil {
		panic(err)
	}
	return hex.EncodeToString(data)
}

func fieldsAndChunks() ([]*types.FieldType, []*chunk.Chunk) {
	fields := []*types.FieldType{
		types.NewFieldType(mysql.TypeVarchar),
		types.NewFieldType(mysql.TypeLonglong),
	}
	chunks := make([]*chunk.Chunk, 0, 9)
	for chunkIdx := range 9 {
		chk := chunk.NewChunkWithCapacity(fields, 32)
		for rowIdx := range 32 {
			ordinal := chunkIdx*32 + rowIdx
			chk.AppendString(0, strings.Repeat("x", ordinal%31+1))
			if ordinal%11 == 5 {
				chk.AppendNull(1)
			} else {
				chk.AppendInt64(1, int64(ordinal*17-9))
			}
		}
		chunks = append(chunks, chk)
	}
	return fields, chunks
}

func renderRows(fields []*types.FieldType, rows *chunk.DataInDiskByRows) string {
	var rendered []string
	for chunkIdx := range rows.NumChunks() {
		for rowIdx := range rows.NumRowsOfChunk(chunkIdx) {
			row, err := rows.GetRow(chunk.RowPtr{ChkIdx: uint32(chunkIdx), RowIdx: uint32(rowIdx)})
			if err != nil {
				panic(err)
			}
			var cells []string
			for columnIdx := range fields {
				if row.IsNull(columnIdx) {
					cells = append(cells, "NULL")
				} else {
					cells = append(cells, hex.EncodeToString(row.GetRaw(columnIdx)))
				}
			}
			rendered = append(rendered, strings.Join(cells, ","))
		}
	}
	return strings.Join(rendered, "|")
}

func emitRows() {
	dir, err := os.MkdirTemp("", "encrypted-rows")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(dir)
	restoreConfig := configureEncryptedSpill(dir)
	defer restoreConfig()

	oldRandom := cryptorand.Reader
	cryptorand.Reader = fixedRandom(
		rowsDataKeyHex, rowsDataNonceHex,
		rowsOffsetKeyHex, rowsOffsetNonceHex,
	)
	fields, chunks := fieldsAndChunks()
	rows := chunk.NewDataInDiskByRows(fields)
	for _, chk := range chunks {
		if err := rows.Add(chk); err != nil {
			panic(err)
		}
	}
	cryptorand.Reader = oldRandom

	dataPath, offsetPath := rowFiles(dir)
	fmt.Printf("rows.data.cipher\tkey=%s,nonce=%s\n", rowsDataKeyHex, rowsDataNonceHex)
	fmt.Printf("rows.offsets.cipher\tkey=%s,nonce=%s\n", rowsOffsetKeyHex, rowsOffsetNonceHex)
	fmt.Printf("rows.data\t%s\n", fileHex(dataPath))
	fmt.Printf("rows.offsets\t%s\n", fileHex(offsetPath))
	fmt.Printf("rows.readback\t%s\n", renderRows(fields, rows))
	rows.Close()
}

func emitChunks() {
	dir, err := os.MkdirTemp("", "encrypted-chunks")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(dir)
	restoreConfig := configureEncryptedSpill(dir)
	defer restoreConfig()

	oldRandom := cryptorand.Reader
	cryptorand.Reader = fixedRandom(chunksKeyHex, chunksNonceHex)
	fields, chunks := fieldsAndChunks()
	onDisk := chunk.NewDataInDiskByChunks(fields, "oracle")
	for _, chk := range chunks {
		if err := onDisk.Add(chk); err != nil {
			panic(err)
		}
	}
	cryptorand.Reader = oldRandom

	path := fileWithPrefix(dir, "oracle"+chunk.DefaultChunkDataInDiskByChunksPath)
	fmt.Printf("chunks.data.cipher\tkey=%s,nonce=%s\n", chunksKeyHex, chunksNonceHex)
	fmt.Printf("chunks.data\t%s\n", fileHex(path))

	var rendered []string
	for chunkIdx := range onDisk.NumChunks() {
		chk, err := onDisk.GetChunk(chunkIdx)
		if err != nil {
			panic(err)
		}
		for rowIdx := range chk.NumRows() {
			row := chk.GetRow(rowIdx)
			value := "NULL"
			if !row.IsNull(1) {
				value = hex.EncodeToString(row.GetRaw(1))
			}
			rendered = append(rendered, hex.EncodeToString(row.GetRaw(0))+","+value)
		}
	}
	fmt.Printf("chunks.readback\t%s\n", strings.Join(rendered, "|"))
	onDisk.Close()
}

func main() {
	emitRows()
	emitChunks()
}
