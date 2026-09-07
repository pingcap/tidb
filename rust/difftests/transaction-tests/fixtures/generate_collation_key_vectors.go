//go:build ignore

// Go-authoritative collation sort-key vectors.
//
// Regenerate with:
//	go run ./rust/difftests/transaction-tests/fixtures/generate_collation_key_vectors.go \
//	    > rust/difftests/transaction-tests/fixtures/collation_key_vectors.tsv
//
// Each line pins `collation<TAB>sample-index<TAB>hex-of-Key` for the sort
// key Go's collator builds for one sample string. The Rust consumer
// (crates/tidb-datatype/tests/collation_go_vectors.rs) embeds the same
// sample list and asserts `get_collator_by_id(...).key(...)` byte equality.
package main

import (
	"encoding/hex"
	"fmt"

	"github.com/pingcap/tidb/pkg/util/collate"
)

var collations = []string{
	"binary",
	"utf8mb4_bin",
	"ascii_bin",
	"latin1_bin",
	"utf8mb4_general_ci",
	"utf8mb4_unicode_ci",
	"utf8mb4_0900_bin",
}

var samples = []string{
	"",
	"a",
	"A",
	"a ",
	"a  ",
	"a\t",
	"abc",
	"ABC",
	"中文",
	"a中",
	"ß",
	"ﬀ",
}

func main() {
	for _, collation := range collations {
		for index, sample := range samples {
			key := collate.GetCollator(collation).Key(sample)
			fmt.Printf("%s\t%02d\t%s\n", collation, index, hex.EncodeToString(key))
		}
	}
}
