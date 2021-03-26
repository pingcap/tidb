package set

import (
	"fmt"
	"strconv"
	"testing"
)

func BenchmarkFloat64SetMemoryUsage(b *testing.B) {
	b.ReportAllocs()
	type testCase struct {
		rowNum int
	}
	cases := []testCase{
		{rowNum: 0},
		{rowNum: 100},
		{rowNum: 10000},
		{rowNum: 1000000},
		{rowNum: 851968}, // 6.5 * (1 << 17)
		{rowNum: 851969}, // 6.5 * (1 << 17) + 1
		{rowNum: 425984}, // 6.5 * (1 << 16),
		{rowNum: 425985}, // 6.5 * (1 << 16) + 1
	}

	for _, c := range cases {
		b.Run(fmt.Sprintf("MapRows %v", c.rowNum), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				float64Set, _ := NewFloat64SetWithMemoryUsage()
				for num := 0; num < c.rowNum; num++ {
					float64Set.Insert(float64(num))
				}
			}
		})
	}
}

func BenchmarkInt64SetMemoryUsage(b *testing.B) {
	b.ReportAllocs()
	type testCase struct {
		rowNum int
	}
	cases := []testCase{
		{rowNum: 0},
		{rowNum: 100},
		{rowNum: 10000},
		{rowNum: 1000000},
		{rowNum: 851968}, // 6.5 * (1 << 17)
		{rowNum: 851969}, // 6.5 * (1 << 17) + 1
		{rowNum: 425984}, // 6.5 * (1 << 16)
		{rowNum: 425985}, // 6.5 * (1 << 16) + 1
	}

	for _, c := range cases {
		b.Run(fmt.Sprintf("MapRows %v", c.rowNum), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				int64Set, _ := NewInt64SetWithMemoryUsage()
				for num := 0; num < c.rowNum; num++ {
					int64Set.Insert(int64(num))
				}
			}
		})
	}
}

func BenchmarkStringSetMemoryUsage(b *testing.B) {
	b.ReportAllocs()
	type testCase struct {
		rowNum int
	}
	cases := []testCase{
		{rowNum: 0},
		{rowNum: 100},
		{rowNum: 10000},
		{rowNum: 1000000},
		{rowNum: 851968}, // 6.5 * (1 << 17)
		{rowNum: 851969}, // 6.5 * (1 << 17) + 1
		{rowNum: 425984}, // 6.5 * (1 << 16)
		{rowNum: 425985}, // 6.5 * (1 << 16) + 1
	}

	for _, c := range cases {
		b.Run(fmt.Sprintf("MapRows %v", c.rowNum), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				stringSet, _ := NewStringSetWithMemoryUsage()
				for num := 0; num < c.rowNum; num++ {
					stringSet.Insert(strconv.Itoa(num))
				}
			}
		})
	}
}
