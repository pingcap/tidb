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
	fmt.Println(rowMap)
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
