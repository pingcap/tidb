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

func BenchmarkTable(b *testing.B) {
	b.StopTimer()
	store, err := NewStore("memory://bench")
	if err != nil {
		b.Fatal(err)
	}
	log.SetLevel(log.LOG_LEVEL_ERROR)
	se, err := CreateSession(store)
	if err != nil {
		b.Fatal(err)
	}
	se.Execute("use test")
	se.Execute("create table t (c1 int primary key auto_increment, c2 int)")
	se.Execute("insert t (c2) values (1),(2),(3),(4),(5)")
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		runSelect(b, se)
	}
}

func runSelect(b *testing.B, se Session) {
	rss, err := se.Execute("select * from t")
	if err != nil {
		b.Fatal(err)
	}
	_, err = GetRows(rss[0])
	if err != nil {
		b.Fatal(err)
	}
}
