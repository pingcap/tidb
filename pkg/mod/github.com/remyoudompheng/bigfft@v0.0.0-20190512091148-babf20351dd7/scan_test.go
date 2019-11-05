package bigfft

import (
	"math/big"
	"testing"
	"time"
)

func TestScan(t *testing.T) {
	for size := 10; size <= 1e5; size += 191 {
		s := rndStr(size)
		x, ok := new(big.Int).SetString(s, 10)
		if !ok {
			t.Fatal("cannot parse", s)
		}
		t0 := time.Now()
		y := FromDecimalString(s)
		if x.Cmp(y) != 0 {
			t.Errorf("failed at size %d", size)
		} else {
			t.Logf("OK for size %d in %s", size, time.Since(t0))
		}
	}
}

func BenchmarkScanFast1k(b *testing.B)   { benchmarkScanFast(1e3, b) }
func BenchmarkScanFast10k(b *testing.B)  { benchmarkScanFast(10e3, b) }
func BenchmarkScanFast100k(b *testing.B) { benchmarkScanFast(100e3, b) }
func BenchmarkScanFast1M(b *testing.B)   { benchmarkScanFast(1e6, b) }
func BenchmarkScanFast2M(b *testing.B)   { benchmarkScanFast(2e6, b) }
func BenchmarkScanFast5M(b *testing.B)   { benchmarkScanFast(5e6, b) }
func BenchmarkScanFast10M(b *testing.B)  { benchmarkScanFast(10e6, b) }

//func BenchmarkScanFast100M(b *testing.B) { benchmarkScanFast(100e6, b) }

func benchmarkScanFast(n int, b *testing.B) {
	s := rndStr(n)
	var x *big.Int
	for i := 0; i < b.N; i++ {
		x = FromDecimalString(s)
	}
	_ = x
}

func BenchmarkScanBig1k(b *testing.B)   { benchmarkScanBig(1e3, b) }
func BenchmarkScanBig10k(b *testing.B)  { benchmarkScanBig(10e3, b) }
func BenchmarkScanBig100k(b *testing.B) { benchmarkScanBig(100e3, b) }
func BenchmarkScanBig1M(b *testing.B)   { benchmarkScanBig(1e6, b) }
func BenchmarkScanBig2M(b *testing.B)   { benchmarkScanBig(2e6, b) }
func BenchmarkScanBig5M(b *testing.B)   { benchmarkScanBig(5e6, b) }
func BenchmarkScanBig10M(b *testing.B)  { benchmarkScanBig(10e6, b) }

func benchmarkScanBig(n int, b *testing.B) {
	s := rndStr(n)
	var x big.Int
	for i := 0; i < b.N; i++ {
		x.SetString(s, 10)
	}
}

func rndStr(n int) string {
	x := make([]byte, n)
	for i := 0; i < n; i++ {
		x[i] = '0' + byte(rnd.Intn(10))
	}
	return string(x)
}
