//go:build ignore

// Go-authoritative JSON binary-format vectors.
//
// Regenerate with:
//	go run ./rust/difftests/transaction-tests/fixtures/generate_json_vectors.go \
//	    > rust/difftests/transaction-tests/fixtures/json_vectors.hex
//
// Each line pins `name=<hex of TypeCode+Value>` for the binary form that
// `types.ParseBinaryJSONFromString` produces, which is the exact byte
// sequence TiDB stores inside a JSON datum. The Rust consumer
// (crates/tidb-datatype/tests/json_go_vectors.rs) embeds the same document
// list and asserts `BinaryJSON::parse(...).encoded()` byte equality, which
// is what a self-round-trip (parse ours, encode ours) cannot prove.
package main

import (
	"encoding/hex"
	"fmt"

	"github.com/pingcap/tidb/pkg/types"
)

var documents = []string{
	"null",
	"true",
	"false",
	"1",
	"-1",
	"18446744073709551615",
	"1.5",
	"-2.25",
	"\"hello\"",
	"\"中文 ✓ escaped \\u00e9\"",
	"[]",
	"{}",
	"[1, -2, 3.5, \"x\", true, null, [], {\"k\": \"v\"}]",
	"{\"b\": 1, \"a\": 2}",
	"{\"k\": [{\"nested\": true}, 255]}",
	"{\"z\": {\"y\": {\"x\": 1}}}",
	"\"\"",
}

func main() {
	for index, document := range documents {
		parsed, err := types.ParseBinaryJSONFromString(document)
		if err != nil {
			panic(fmt.Sprintf("document %d (%q) does not parse: %v", index, document, err))
		}
		binary := append([]byte{parsed.TypeCode}, parsed.Value...)
		fmt.Printf("doc%02d=%s\n", index, hex.EncodeToString(binary))
	}
}
