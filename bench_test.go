package tidb

import (
	"testing"

	"github.com/ngaut/log"
)

func BenchmarkBasic(b *testing.B) {
	store, err := NewStore("memory://bench")
	if err != nil {
		b.Fatal(err)
	}
	log.SetLevel(log.LOG_LEVEL_ERROR)
	se, err := CreateSession(store)
	if err != nil {
		b.Fatal(err)
	}
	for i := 0; i < b.N; i++ {
		rs, err := se.Execute("select 1")
		if err != nil {
			b.Fatal(err)
		}
		row, err := rs[0].Next()
		if err != nil || row == nil {
			b.Fatal(err)
		}
		rs[0].Close()
	}
}
