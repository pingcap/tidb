package infoschema

import (
	"fmt"
	"testing"
)

func TestParseSlowLogFile(t *testing.T) {
	rowMap, err := parseSlowLogFile("/Users/cs/code/goread/src/github.com/pingcap/tidb/slow2.log")
	if err != nil {
		fmt.Println(err)
	}
	for k, v := range rowMap {
		fmt.Println(k, v)
	}
}

func BenchmarkCopyString(b *testing.B) {
	str := "abcdefghigklmnopqrst"
	for i := 0; i < b.N; i++ {
		_ = copyString(str)
	}
}

func BenchmarkCopyStringHack(b *testing.B) {
	str := "abcdefghigklmnopqrst"
	for i := 0; i < b.N; i++ {
		_ = copyStringHack(str)
	}
}
