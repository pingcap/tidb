package main

import (
	"encoding/hex"
	"fmt"

	"github.com/pingcap/tidb/util/codec"
)

func encodeRewriteRules(oldPrefix, newPrefix []byte) [2][]byte {
	ungroupedLen := len(oldPrefix) % 8
	encodedPrefix := append([]byte{}, codec.EncodeBytes([]byte{}, oldPrefix[:len(oldPrefix)-ungroupedLen])...)
	oldPrefix = append(encodedPrefix[:len(encodedPrefix)-9], oldPrefix[len(oldPrefix)-ungroupedLen:]...)

	encodedPrefix = append([]byte{}, codec.EncodeBytes([]byte{}, newPrefix[:len(newPrefix)-ungroupedLen])...)
	if (len(newPrefix)-ungroupedLen)%8 == 0 {
		encodedPrefix = encodedPrefix[:len(encodedPrefix)-9]
	}
	newPrefix = append(encodedPrefix, newPrefix[len(newPrefix)-ungroupedLen:]...)

	return [2][]byte{oldPrefix, newPrefix}
}

func dec(s string) []byte {
	b, err := hex.DecodeString(s)
	if err != nil {
		panic(err)
	}
	return b
}

func main() {
	a := encodeRewriteRules(dec("7480000000000000465f72"),
		dec("780000007480000000000000465f72"))

	fmt.Println(hex.EncodeToString(a[0]))
	fmt.Println(hex.EncodeToString(a[1]))
}
