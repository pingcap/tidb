package main

import (
	"bytes"
	"encoding/hex"
	"fmt"

	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/util/codec"
)

func main() {
	for i := 0; i < 1<<10; i++ {
		pfx := tablecodec.EncodeTablePrefix(int64(i))
		enc := codec.EncodeBytes(nil, pfx)

		if bytes.HasPrefix(enc, pfx) {
			fmt.Println(i, hex.EncodeToString(enc), hex.EncodeToString(pfx))
		}
	}
}
