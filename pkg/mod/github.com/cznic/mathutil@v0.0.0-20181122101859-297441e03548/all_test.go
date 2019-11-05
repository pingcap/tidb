// Copyright (c) 2014 The mathutil Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package mathutil

import (
	"bytes"
	"fmt"
	"math"
	"math/big"
	"math/rand"
	"os"
	"path"
	"runtime"
	"sort"
	"strings"
	"sync"
	"testing"
)

func caller(s string, va ...interface{}) {
	_, fn, fl, _ := runtime.Caller(2)
	fmt.Fprintf(os.Stderr, "caller: %s:%d: ", path.Base(fn), fl)
	fmt.Fprintf(os.Stderr, s, va...)
	fmt.Fprintln(os.Stderr)
	_, fn, fl, _ = runtime.Caller(1)
	fmt.Fprintf(os.Stderr, "\tcallee: %s:%d: ", path.Base(fn), fl)
	fmt.Fprintln(os.Stderr)
}

func dbg(s string, va ...interface{}) {
	if s == "" {
		s = strings.Repeat("%v ", len(va))
	}
	_, fn, fl, _ := runtime.Caller(1)
	fmt.Fprintf(os.Stderr, "dbg %s:%d: ", path.Base(fn), fl)
	fmt.Fprintf(os.Stderr, s, va...)
	fmt.Fprintln(os.Stderr)
}

func TODO(...interface{}) string {
	_, fn, fl, _ := runtime.Caller(1)
	return fmt.Sprintf("TODO: %s:%d:\n", path.Base(fn), fl)
}

func use(...interface{}) {}

func init() {
	use(caller, TODO)
}

func intPtr(a int) *int {
	return &a
}

func uIntPtr(a uint) *uint {
	return &a
}

func bytePtr(a byte) *byte {
	return &a
}

func int8Ptr(a int8) *int8 {
	return &a
}

func int16Ptr(a int16) *int16 {
	return &a
}

func uInt16Ptr(a uint16) *uint16 {
	return &a
}

func int32Ptr(a int32) *int32 {
	return &a
}

func uInt32Ptr(a uint32) *uint32 {
	return &a
}

func int64Ptr(a int64) *int64 {
	return &a
}

func uInt64Ptr(a uint64) *uint64 {
	return &a
}

// ============================================================================

func r32() *FC32 {
	r, err := NewFC32(math.MinInt32, math.MaxInt32, true)
	if err != nil {
		panic(err)
	}

	return r
}

var (
	r64lo          = big.NewInt(math.MinInt64)
	r64hi          = big.NewInt(math.MaxInt64)
	MinIntM1       = MinInt
	MaxIntP1       = MaxInt
	MaxUintP1 uint = MaxUint
)

func init() {
	MinIntM1--
	MaxIntP1++
	MaxUintP1++
}

func r64() *FCBig {
	r, err := NewFCBig(r64lo, r64hi, true)
	if err != nil {
		panic(err)
	}

	return r
}

func benchmark1eN(b *testing.B, r *FC32) {
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		r.Next()
	}
}

func BenchmarkFC1e3(b *testing.B) {
	b.StopTimer()
	r, _ := NewFC32(0, 1e3, false)
	benchmark1eN(b, r)
}

func BenchmarkFC1e6(b *testing.B) {
	b.StopTimer()
	r, _ := NewFC32(0, 1e6, false)
	benchmark1eN(b, r)
}

func BenchmarkFC1e9(b *testing.B) {
	b.StopTimer()
	r, _ := NewFC32(0, 1e9, false)
	benchmark1eN(b, r)
}

func Test0(t *testing.T) {
	const N = 10000
	for n := 1; n < N; n++ {
		lo, hi := 0, n-1
		period := int64(hi) - int64(lo) + 1
		r, err := NewFC32(lo, hi, false)
		if err != nil {
			t.Fatal(err)
		}
		if r.Cycle()-period > period {
			t.Fatalf("Cycle exceeds 2 * period")
		}
	}
	for n := 1; n < N; n++ {
		lo, hi := 0, n-1
		period := int64(hi) - int64(lo) + 1
		r, err := NewFC32(lo, hi, true)
		if err != nil {
			t.Fatal(err)
		}
		if r.Cycle()-2*period > period {
			t.Fatalf("Cycle exceeds 3 * period")
		}
	}
}

func Test1(t *testing.T) {
	const (
		N = 360
		S = 3
	)
	for hq := 0; hq <= 1; hq++ {
		for n := 1; n < N; n++ {
			for seed := 0; seed < S; seed++ {
				lo, hi := -n, 2*n
				period := int64(hi - lo + 1)
				r, err := NewFC32(lo, hi, hq == 1)
				if err != nil {
					t.Fatal(err)
				}
				r.Seed(int64(seed))
				m := map[int]bool{}
				v := make([]int, period)
				p := make([]int64, period)
				for i := lo; i <= hi; i++ {
					x := r.Next()
					p[i-lo] = r.Pos()
					if x < lo || x > hi {
						t.Fatal("t1.0")
					}
					if m[x] {
						t.Fatal("t1.1")
					}
					m[x] = true
					v[i-lo] = x
				}
				for i := lo; i <= hi; i++ {
					x := r.Next()
					if x < lo || x > hi {
						t.Fatal("t1.2")
					}
					if !m[x] {
						t.Fatal("t1.3")
					}
					if x != v[i-lo] {
						t.Fatal("t1.4")
					}
					if r.Pos() != p[i-lo] {
						t.Fatal("t1.5")
					}
					m[x] = false
				}
				for i := lo; i <= hi; i++ {
					r.Seek(p[i-lo] + 1)
					x := r.Prev()
					if x < lo || x > hi {
						t.Fatal("t1.6")
					}
					if x != v[i-lo] {
						t.Fatal("t1.7")
					}
				}
			}
		}
	}
}

func Test2(t *testing.T) {
	const (
		N = 370
		S = 3
	)
	for hq := 0; hq <= 1; hq++ {
		for n := 1; n < N; n++ {
			for seed := 0; seed < S; seed++ {
				lo, hi := -n, 2*n
				period := int64(hi - lo + 1)
				r, err := NewFC32(lo, hi, hq == 1)
				if err != nil {
					t.Fatal(err)
				}
				r.Seed(int64(seed))
				m := map[int]bool{}
				v := make([]int, period)
				p := make([]int64, period)
				for i := lo; i <= hi; i++ {
					x := r.Prev()
					p[i-lo] = r.Pos()
					if x < lo || x > hi {
						t.Fatal("t2.0")
					}
					if m[x] {
						t.Fatal("t2.1")
					}
					m[x] = true
					v[i-lo] = x
				}
				for i := lo; i <= hi; i++ {
					x := r.Prev()
					if x < lo || x > hi {
						t.Fatal("t2.2")
					}
					if !m[x] {
						t.Fatal("t2.3")
					}
					if x != v[i-lo] {
						t.Fatal("t2.4")
					}
					if r.Pos() != p[i-lo] {
						t.Fatal("t2.5")
					}
					m[x] = false
				}
				for i := lo; i <= hi; i++ {
					s := p[i-lo] - 1
					if s < 0 {
						s = r.Cycle() - 1
					}
					r.Seek(s)
					x := r.Next()
					if x < lo || x > hi {
						t.Fatal("t2.6")
					}
					if x != v[i-lo] {
						t.Fatal("t2.7")
					}
				}
			}
		}
	}
}

func benchmarkBig1eN(b *testing.B, r *FCBig) {
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		r.Next()
	}
}

func BenchmarkFCBig1e3(b *testing.B) {
	b.StopTimer()
	hi := big.NewInt(0).SetInt64(1e3)
	r, _ := NewFCBig(big0, hi, false)
	benchmarkBig1eN(b, r)
}

func BenchmarkFCBig1e6(b *testing.B) {
	b.StopTimer()
	hi := big.NewInt(0).SetInt64(1e6)
	r, _ := NewFCBig(big0, hi, false)
	benchmarkBig1eN(b, r)
}

func BenchmarkFCBig1e9(b *testing.B) {
	b.StopTimer()
	hi := big.NewInt(0).SetInt64(1e9)
	r, _ := NewFCBig(big0, hi, false)
	benchmarkBig1eN(b, r)
}

func BenchmarkFCBig1e12(b *testing.B) {
	b.StopTimer()
	hi := big.NewInt(0).SetInt64(1e12)
	r, _ := NewFCBig(big0, hi, false)
	benchmarkBig1eN(b, r)
}

func BenchmarkFCBig1e15(b *testing.B) {
	b.StopTimer()
	hi := big.NewInt(0).SetInt64(1e15)
	r, _ := NewFCBig(big0, hi, false)
	benchmarkBig1eN(b, r)
}

func BenchmarkFCBig1e18(b *testing.B) {
	b.StopTimer()
	hi := big.NewInt(0).SetInt64(1e18)
	r, _ := NewFCBig(big0, hi, false)
	benchmarkBig1eN(b, r)
}

var (
	big0 = big.NewInt(0)
)

func TestBig0(t *testing.T) {
	const N = 7400
	lo := big.NewInt(0)
	hi := big.NewInt(0)
	period := big.NewInt(0)
	c := big.NewInt(0)
	for n := int64(1); n < N; n++ {
		hi.SetInt64(n - 1)
		period.Set(hi)
		period.Sub(period, lo)
		period.Add(period, _1)
		r, err := NewFCBig(lo, hi, false)
		if err != nil {
			t.Fatal(err)
		}
		if r.cycle.Cmp(period) < 0 {
			t.Fatalf("Period exceeds cycle")
		}
		c.Set(r.Cycle())
		c.Sub(c, period)
		if c.Cmp(period) > 0 {
			t.Fatalf("Cycle exceeds 2 * period")
		}
	}
	for n := int64(1); n < N; n++ {
		hi.SetInt64(n - 1)
		period.Set(hi)
		period.Sub(period, lo)
		period.Add(period, _1)
		r, err := NewFCBig(lo, hi, true)
		if err != nil {
			t.Fatal(err)
		}
		if r.cycle.Cmp(period) < 0 {
			t.Fatalf("Period exceeds cycle")
		}
		c.Set(r.Cycle())
		c.Sub(c, period)
		c.Sub(c, period)
		if c.Cmp(period) > 0 {
			t.Fatalf("Cycle exceeds 3 * period")
		}
	}
}

func TestBig1(t *testing.T) {
	const (
		N = 120
		S = 3
	)
	lo := big.NewInt(0)
	hi := big.NewInt(0)
	seek := big.NewInt(0)
	for hq := 0; hq <= 1; hq++ {
		for n := int64(1); n < N; n++ {
			for seed := 0; seed < S; seed++ {
				lo64 := -n
				hi64 := 2 * n
				lo.SetInt64(lo64)
				hi.SetInt64(hi64)
				period := hi64 - lo64 + 1
				r, err := NewFCBig(lo, hi, hq == 1)
				if err != nil {
					t.Fatal(err)
				}
				r.Seed(int64(seed))
				m := map[int64]bool{}
				v := make([]int64, period)
				p := make([]int64, period)
				for i := lo64; i <= hi64; i++ {
					x := r.Next().Int64()
					p[i-lo64] = r.Pos().Int64()
					if x < lo64 || x > hi64 {
						t.Fatal("tb1.0")
					}
					if m[x] {
						t.Fatal("tb1.1")
					}
					m[x] = true
					v[i-lo64] = x
				}
				for i := lo64; i <= hi64; i++ {
					x := r.Next().Int64()
					if x < lo64 || x > hi64 {
						t.Fatal("tb1.2")
					}
					if !m[x] {
						t.Fatal("tb1.3")
					}
					if x != v[i-lo64] {
						t.Fatal("tb1.4")
					}
					if r.Pos().Int64() != p[i-lo64] {
						t.Fatal("tb1.5")
					}
					m[x] = false
				}
				for i := lo64; i <= hi64; i++ {
					r.Seek(seek.SetInt64(p[i-lo64] + 1))
					x := r.Prev().Int64()
					if x < lo64 || x > hi64 {
						t.Fatal("tb1.6")
					}
					if x != v[i-lo64] {
						t.Fatal("tb1.7")
					}
				}
			}
		}
	}
}

func TestBig2(t *testing.T) {
	const (
		N = 120
		S = 3
	)
	lo := big.NewInt(0)
	hi := big.NewInt(0)
	seek := big.NewInt(0)
	for hq := 0; hq <= 1; hq++ {
		for n := int64(1); n < N; n++ {
			for seed := 0; seed < S; seed++ {
				lo64, hi64 := -n, 2*n
				lo.SetInt64(lo64)
				hi.SetInt64(hi64)
				period := hi64 - lo64 + 1
				r, err := NewFCBig(lo, hi, hq == 1)
				if err != nil {
					t.Fatal(err)
				}
				r.Seed(int64(seed))
				m := map[int64]bool{}
				v := make([]int64, period)
				p := make([]int64, period)
				for i := lo64; i <= hi64; i++ {
					x := r.Prev().Int64()
					p[i-lo64] = r.Pos().Int64()
					if x < lo64 || x > hi64 {
						t.Fatal("tb2.0")
					}
					if m[x] {
						t.Fatal("tb2.1")
					}
					m[x] = true
					v[i-lo64] = x
				}
				for i := lo64; i <= hi64; i++ {
					x := r.Prev().Int64()
					if x < lo64 || x > hi64 {
						t.Fatal("tb2.2")
					}
					if !m[x] {
						t.Fatal("tb2.3")
					}
					if x != v[i-lo64] {
						t.Fatal("tb2.4")
					}
					if r.Pos().Int64() != p[i-lo64] {
						t.Fatal("tb2.5")
					}
					m[x] = false
				}
				for i := lo64; i <= hi64; i++ {
					s := p[i-lo64] - 1
					if s < 0 {
						s = r.Cycle().Int64() - 1
					}
					r.Seek(seek.SetInt64(s))
					x := r.Next().Int64()
					if x < lo64 || x > hi64 {
						t.Fatal("tb2.6")
					}
					if x != v[i-lo64] {
						t.Fatal("tb2.7")
					}
				}
			}
		}
	}
}

func TestPermutations(t *testing.T) {
	data := sort.IntSlice{3, 2, 1}
	check := [][]int{
		{1, 2, 3},
		{1, 3, 2},
		{2, 1, 3},
		{2, 3, 1},
		{3, 1, 2},
		{3, 2, 1},
	}
	i := 0
	for PermutationFirst(data); ; i++ {
		if i >= len(check) {
			t.Fatalf("too much permutations generated: %d > %d", i+1, len(check))
		}

		for j, v := range check[i] {
			got := data[j]
			if got != v {
				t.Fatalf("permutation %d:\ndata: %v\ncheck: %v\nexpected data[%d] == %d, got %d", i, data, check[i], j, v, got)
			}
		}

		if !PermutationNext(data) {
			if i != len(check)-1 {
				t.Fatal("permutations generated", i, "expected", len(check))
			}
			break
		}
	}
}

func TestIsPrime(t *testing.T) {
	const p4M = 283146 // # of primes < 4e6
	n := 0
	for i := uint32(0); i <= 4e6; i++ {
		if IsPrime(i) {
			n++
		}
	}
	t.Log(n)
	if n != p4M {
		t.Fatal(n)
	}
}

func BenchmarkIsPrime(b *testing.B) {
	b.StopTimer()
	n := make([]uint32, b.N)
	rng := rand.New(rand.NewSource(1))
	for i := 0; i < b.N; i++ {
		n[i] = rng.Uint32()
	}
	b.StartTimer()
	for _, n := range n {
		IsPrime(n)
	}
}

func BenchmarkNextPrime(b *testing.B) {
	b.StopTimer()
	n := make([]uint32, b.N)
	rng := rand.New(rand.NewSource(1))
	for i := 0; i < b.N; i++ {
		n[i] = rng.Uint32()
	}
	b.StartTimer()
	for _, n := range n {
		NextPrime(n)
	}
}

func BenchmarkIsPrimeUint64(b *testing.B) {
	const N = 1 << 16
	b.StopTimer()
	a := make([]uint64, N)
	r := r64()
	for i := range a {
		a[i] = uint64(r.Next().Int64())
	}
	runtime.GC()
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		IsPrimeUint64(a[i&(N-1)])
	}
}

func BenchmarkNextPrimeUint64(b *testing.B) {
	b.StopTimer()
	n := make([]uint64, b.N)
	rng := rand.New(rand.NewSource(1))
	for i := 0; i < b.N; i++ {
		n[i] = uint64(rng.Int63())
		if i&1 == 0 {
			n[i] ^= 1 << 63
		}
	}
	b.StartTimer()
	for _, n := range n {
		NextPrimeUint64(n)
	}
}

func TestNextPrime(t *testing.T) {
	const p4M = 283146 // # of primes < 4e6
	n := 0
	var p uint32
	for {
		p, _ = NextPrime(p)
		if p >= 4e6 {
			break
		}
		n++
	}
	t.Log(n)
	if n != p4M {
		t.Fatal(n)
	}
}

func TestNextPrime2(t *testing.T) {
	type data struct {
		x  uint32
		y  uint32
		ok bool
	}
	tests := []data{
		{0, 2, true},
		{1, 2, true},
		{2, 3, true},
		{3, 5, true},
		{math.MaxUint32, 0, false},
		{math.MaxUint32 - 1, 0, false},
		{math.MaxUint32 - 2, 0, false},
		{math.MaxUint32 - 3, 0, false},
		{math.MaxUint32 - 4, 0, false},
		{math.MaxUint32 - 5, math.MaxUint32 - 4, true},
	}

	for _, test := range tests {
		y, ok := NextPrime(test.x)
		if ok != test.ok || ok && y != test.y {
			t.Fatalf("x %d, got y %d ok %t, expected y %d ok %t", test.x, y, ok, test.y, test.ok)
		}
	}
}

func TestNextPrimeUint64(t *testing.T) {
	const (
		lo = 2000000000000000000
		hi = 2000000000000100000
		k  = 2346 // PrimePi(hi)-PrimePi(lo)
	)
	n := 0
	p := uint64(lo) - 1
	var ok bool
	for {
		p0 := p
		p, ok = NextPrimeUint64(p)
		if !ok {
			t.Fatal(p0)
		}

		if p > hi {
			break
		}

		n++
	}
	if n != k {
		t.Fatal(n, k)
	}
}

func TestISqrt(t *testing.T) {
	for n := int64(0); n < 5e6; n++ {
		x := int64(ISqrt(uint32(n)))
		if x2 := x * x; x2 > n {
			t.Fatalf("got ISqrt(%d) == %d, too big", n, x)
		}
		if x2 := x*x + 2*x + 1; x2 < n {
			t.Fatalf("got ISqrt(%d) == %d, too low", n, x)
		}
	}
	for n := int64(math.MaxUint32); n > math.MaxUint32-5e6; n-- {
		x := int64(ISqrt(uint32(n)))
		if x2 := x * x; x2 > n {
			t.Fatalf("got ISqrt(%d) == %d, too big", n, x)
		}
		if x2 := x*x + 2*x + 1; x2 < n {
			t.Fatalf("got ISqrt(%d) == %d, too low", n, x)
		}
	}
	rng := rand.New(rand.NewSource(1))
	for i := 0; i < 5e6; i++ {
		n := int64(rng.Uint32())
		x := int64(ISqrt(uint32(n)))
		if x2 := x * x; x2 > n {
			t.Fatalf("got ISqrt(%d) == %d, too big", n, x)
		}
		if x2 := x*x + 2*x + 1; x2 < n {
			t.Fatalf("got ISqrt(%d) == %d, too low", n, x)
		}
	}
}

func TestSqrtUint64(t *testing.T) {
	for n := uint64(0); n < 2e6; n++ {
		x := SqrtUint64(n)
		if x > math.MaxUint32 {
			t.Fatalf("got Sqrt(%d) == %d, too big", n, x)
		}
		if x2 := x * x; x2 > n {
			t.Fatalf("got Sqrt(%d) == %d, too big", n, x)
		}
		if x2 := x*x + 2*x + 1; x2 < n {
			t.Fatalf("got Sqrt(%d) == %d, too low", n, x)
		}
	}
	const H = uint64(18446744056529682436)
	for n := H; n > H-2e6; n-- {
		x := SqrtUint64(n)
		if x > math.MaxUint32 {
			t.Fatalf("got Sqrt(%d) == %d, too big", n, x)
		}
		if x2 := x * x; x2 > n {
			t.Fatalf("got Sqrt(%d) == %d, too big", n, x)
		}
		if x2 := x*x + 2*x + 1; x2 < n {
			t.Fatalf("got Sqrt(%d) == %d, too low", n, x)
		}
	}
	rng := rand.New(rand.NewSource(1))
	for i := 0; i < 2e6; i++ {
		n := uint64(rng.Uint32())<<31 | uint64(rng.Uint32())
		x := SqrtUint64(n)
		if x2 := x * x; x2 > n {
			t.Fatalf("got Sqrt(%d) == %d, too big", n, x)
		}
		if x2 := x*x + 2*x + 1; x2 < n {
			t.Fatalf("got Sqrt(%d) == %d, too low", n, x)
		}
	}
}

func TestSqrtBig(t *testing.T) {
	const N = 3e4
	var n, lim, x2 big.Int
	lim.SetInt64(N)
	for n.Cmp(&lim) != 0 {
		x := SqrtBig(&n)
		x2.Mul(x, x)
		if x.Cmp(&n) > 0 {
			t.Fatalf("got sqrt(%s) == %s, too big", &n, x)
		}
		x2.Add(&x2, x)
		x2.Add(&x2, x)
		x2.Add(&x2, _1)
		if x2.Cmp(&n) < 0 {
			t.Fatalf("got sqrt(%s) == %s, too low", &n, x)
		}
		n.Add(&n, _1)
	}
	rng := rand.New(rand.NewSource(1))
	var h big.Int
	h.SetBit(&h, 1e3, 1)
	for i := 0; i < N; i++ {
		n.Rand(rng, &h)
		x := SqrtBig(&n)
		x2.Mul(x, x)
		if x.Cmp(&n) > 0 {
			t.Fatalf("got sqrt(%s) == %s, too big", &n, x)
		}
		x2.Add(&x2, x)
		x2.Add(&x2, x)
		x2.Add(&x2, _1)
		if x2.Cmp(&n) < 0 {
			t.Fatalf("got sqrt(%s) == %s, too low", &n, x)
		}
	}
}

func TestFactorInt(t *testing.T) {
	chk := func(n uint64, f []FactorTerm) bool {
		if n < 2 {
			return len(f) == 0
		}

		for i := 1; i < len(f); i++ { // verify ordering
			if t, u := f[i-1], f[i]; t.Prime >= u.Prime {
				return false
			}
		}

		x := uint64(1)
		for _, v := range f {
			if p := v.Prime; p < 0 || !IsPrime(v.Prime) {
				return false
			}

			for i := uint32(0); i < v.Power; i++ {
				x *= uint64(v.Prime)
				if x > math.MaxUint32 {
					return false
				}
			}
		}
		return x == n
	}

	for n := uint64(0); n < 3e5; n++ {
		f := FactorInt(uint32(n))
		if !chk(n, f) {
			t.Fatalf("bad FactorInt(%d): %v", n, f)
		}
	}
	for n := uint64(math.MaxUint32); n > math.MaxUint32-12e4; n-- {
		f := FactorInt(uint32(n))
		if !chk(n, f) {
			t.Fatalf("bad FactorInt(%d): %v", n, f)
		}
	}
	rng := rand.New(rand.NewSource(1))
	for i := 0; i < 13e4; i++ {
		n := rng.Uint32()
		f := FactorInt(n)
		if !chk(uint64(n), f) {
			t.Fatalf("bad FactorInt(%d): %v", n, f)
		}
	}
}

func TestFactorIntB(t *testing.T) {
	const N = 3e5 // must be < math.MaxInt32
	factors := make([][]FactorTerm, N+1)
	// set up the divisors
	for prime := uint32(2); prime <= N; prime, _ = NextPrime(prime) {
		for n := int(prime); n <= N; n += int(prime) {
			factors[n] = append(factors[n], FactorTerm{prime, 0})
		}
	}
	// set up the powers
	for n := 2; n <= N; n++ {
		f := factors[n]
		m := uint32(n)
		for i, v := range f {
			for m%v.Prime == 0 {
				m /= v.Prime
				v.Power++
			}
			f[i] = v
		}
		factors[n] = f
	}
	// check equal
	for n, e := range factors {
		g := FactorInt(uint32(n))
		if len(e) != len(g) {
			t.Fatal(n, "len", g, "!=", e)
		}

		for i, ev := range e {
			gv := g[i]
			if ev.Prime != gv.Prime {
				t.Fatal(n, "prime", gv, ev)
			}

			if ev.Power != gv.Power {
				t.Fatal(n, "power", gv, ev)
			}
		}
	}
}

func BenchmarkISqrt(b *testing.B) {
	b.StopTimer()
	n := make([]uint32, b.N)
	rng := rand.New(rand.NewSource(1))
	for i := 0; i < b.N; i++ {
		n[i] = rng.Uint32()
	}
	b.StartTimer()
	for _, n := range n {
		ISqrt(n)
	}
}

func BenchmarkSqrtUint64(b *testing.B) {
	b.StopTimer()
	n := make([]uint64, b.N)
	rng := rand.New(rand.NewSource(1))
	for i := 0; i < b.N; i++ {
		n[i] = uint64(rng.Uint32())<<32 | uint64(rng.Uint32())
	}
	b.StartTimer()
	for _, n := range n {
		SqrtUint64(n)
	}
}

func benchmarkSqrtBig(b *testing.B, bits int) {
	b.StopTimer()
	n := make([]*big.Int, b.N)
	rng := rand.New(rand.NewSource(1))
	var nn, h big.Int
	h.SetBit(&h, bits, 1)
	for i := 0; i < b.N; i++ {
		n[i] = nn.Rand(rng, &h)
	}
	runtime.GC()
	b.StartTimer()
	for _, n := range n {
		SqrtBig(n)
	}
}

func BenchmarkSqrtBig2e1e1(b *testing.B) {
	benchmarkSqrtBig(b, 1e1)
}

func BenchmarkSqrtBig2e1e2(b *testing.B) {
	benchmarkSqrtBig(b, 1e2)
}

func BenchmarkSqrtBig2e1e3(b *testing.B) {
	benchmarkSqrtBig(b, 1e3)
}

func BenchmarkSqrtBig2e1e4(b *testing.B) {
	benchmarkSqrtBig(b, 1e4)
}

func BenchmarkSqrtBig2e1e5(b *testing.B) {
	benchmarkSqrtBig(b, 1e5)
}

func BenchmarkFactorInt(b *testing.B) {
	b.StopTimer()
	n := make([]uint32, b.N)
	rng := rand.New(rand.NewSource(1))
	for i := 0; i < b.N; i++ {
		n[i] = rng.Uint32()
	}
	b.StartTimer()
	for _, n := range n {
		FactorInt(n)
	}
}

func TestIsPrimeUint16(t *testing.T) {
	for n := 0; n <= math.MaxUint16; n++ {
		if IsPrimeUint16(uint16(n)) != IsPrime(uint32(n)) {
			t.Fatal(n)
		}
	}
}

func BenchmarkIsPrimeUint16(b *testing.B) {
	b.StopTimer()
	n := make([]uint16, b.N)
	rng := rand.New(rand.NewSource(1))
	for i := 0; i < b.N; i++ {
		n[i] = uint16(rng.Uint32())
	}
	b.StartTimer()
	for _, n := range n {
		IsPrimeUint16(n)
	}
}

func TestNextPrimeUint16(t *testing.T) {
	for n := 0; n <= math.MaxUint16; n++ {
		p, ok := NextPrimeUint16(uint16(n))
		p2, ok2 := NextPrime(uint32(n))
		switch {
		case ok:
			if !ok2 || uint32(p) != p2 {
				t.Fatal(n, p, ok)
			}
		case !ok && ok2:
			if p2 < 65536 {
				t.Fatal(n, p, ok)
			}
		}
	}
}

func BenchmarkNextPrimeUint16(b *testing.B) {
	b.StopTimer()
	n := make([]uint16, b.N)
	rng := rand.New(rand.NewSource(1))
	for i := 0; i < b.N; i++ {
		n[i] = uint16(rng.Uint32())
	}
	b.StartTimer()
	for _, n := range n {
		NextPrimeUint16(n)
	}
}

/*

From: http://graphics.stanford.edu/~seander/bithacks.html#CountBitsSetKernighan

Counting bits set, Brian Kernighan's way

unsigned int v; // count the number of bits set in v
unsigned int c; // c accumulates the total bits set in v
for (c = 0; v; c++)
{
  v &= v - 1; // clear the least significant bit set
}

Brian Kernighan's method goes through as many iterations as there are set bits.
So if we have a 32-bit word with only the high bit set, then it will only go
once through the loop.

Published in 1988, the C Programming Language 2nd Ed. (by Brian W. Kernighan
and Dennis M. Ritchie) mentions this in exercise 2-9. On April 19, 2006 Don
Knuth pointed out to me that this method "was first published by Peter Wegner
in CACM 3 (1960), 322. (Also discovered independently by Derrick Lehmer and
published in 1964 in a book edited by Beckenbach.)"
*/
func bcnt(v uint64) (c int) {
	for ; v != 0; c++ {
		v &= v - 1
	}
	return
}

func TestPopCount(t *testing.T) {
	const N = 4e5
	maxUint64 := big.NewInt(0)
	maxUint64.SetBit(maxUint64, 64, 1)
	maxUint64.Sub(maxUint64, big.NewInt(1))
	rng := r64()
	for i := 0; i < N; i++ {
		n := uint64(rng.Next().Int64())
		if g, e := PopCountByte(byte(n)), bcnt(uint64(byte(n))); g != e {
			t.Fatal(n, g, e)
		}

		if g, e := PopCountUint16(uint16(n)), bcnt(uint64(uint16(n))); g != e {
			t.Fatal(n, g, e)
		}

		if g, e := PopCountUint32(uint32(n)), bcnt(uint64(uint32(n))); g != e {
			t.Fatal(n, g, e)
		}

		if g, e := PopCount(int(n)), bcnt(uint64(uint(n))); g != e {
			t.Fatal(n, g, e)
		}

		if g, e := PopCountUint(uint(n)), bcnt(uint64(uint(n))); g != e {
			t.Fatal(n, g, e)
		}

		if g, e := PopCountUint64(n), bcnt(n); g != e {
			t.Fatal(n, g, e)
		}

		if g, e := PopCountUintptr(uintptr(n)), bcnt(uint64(uintptr(n))); g != e {
			t.Fatal(n, g, e)
		}
	}
}

var gcds = []struct{ a, b, gcd uint64 }{
	{8, 12, 4},
	{12, 18, 6},
	{42, 56, 14},
	{54, 24, 6},
	{252, 105, 21},
	{1989, 867, 51},
	{1071, 462, 21},
	{2 * 3 * 5 * 7 * 11, 5 * 7 * 11 * 13 * 17, 5 * 7 * 11},
	{2 * 3 * 5 * 7 * 7 * 11, 5 * 7 * 7 * 11 * 13 * 17, 5 * 7 * 7 * 11},
	{2 * 3 * 5 * 7 * 7 * 11, 5 * 7 * 7 * 13 * 17, 5 * 7 * 7},
	{2 * 3 * 5 * 7 * 11, 13 * 17 * 19, 1},
}

func TestGCD(t *testing.T) {
	for i, v := range gcds {
		if v.a <= math.MaxUint16 && v.b <= math.MaxUint16 {
			if g, e := uint64(GCDUint16(uint16(v.a), uint16(v.b))), v.gcd; g != e {
				t.Errorf("%d: got gcd(%d, %d) %d, exp %d", i, v.a, v.b, g, e)
			}
			if g, e := uint64(GCDUint16(uint16(v.b), uint16(v.a))), v.gcd; g != e {
				t.Errorf("%d: got gcd(%d, %d) %d, exp %d", i, v.b, v.a, g, e)
			}
		}
		if v.a <= math.MaxUint32 && v.b <= math.MaxUint32 {
			if g, e := uint64(GCDUint32(uint32(v.a), uint32(v.b))), v.gcd; g != e {
				t.Errorf("%d: got gcd(%d, %d) %d, exp %d", i, v.a, v.b, g, e)
			}
			if g, e := uint64(GCDUint32(uint32(v.b), uint32(v.a))), v.gcd; g != e {
				t.Errorf("%d: got gcd(%d, %d) %d, exp %d", i, v.b, v.a, g, e)
			}
		}
		if g, e := GCDUint64(v.a, v.b), v.gcd; g != e {
			t.Errorf("%d: got gcd(%d, %d) %d, exp %d", i, v.a, v.b, g, e)
		}
		if g, e := GCDUint64(v.b, v.a), v.gcd; g != e {
			t.Errorf("%d: got gcd(%d, %d) %d, exp %d", i, v.b, v.a, g, e)
		}
	}
}

func lg2(n uint64) (lg int) {
	if n == 0 {
		return -1
	}

	for n >>= 1; n != 0; n >>= 1 {
		lg++
	}
	return
}

func TestLog2(t *testing.T) {
	if g, e := Log2Byte(0), -1; g != e {
		t.Error(g, e)
	}
	if g, e := Log2Uint16(0), -1; g != e {
		t.Error(g, e)
	}
	if g, e := Log2Uint32(0), -1; g != e {
		t.Error(g, e)
	}
	if g, e := Log2Uint64(0), -1; g != e {
		t.Error(g, e)
	}
	const N = 1e6
	rng := r64()
	for i := 0; i < N; i++ {
		n := uint64(rng.Next().Int64())
		if g, e := Log2Uint64(n), lg2(n); g != e {
			t.Fatalf("%b %d %d", n, g, e)
		}
		if g, e := Log2Uint32(uint32(n)), lg2(n&0xffffffff); g != e {
			t.Fatalf("%b %d %d", n, g, e)
		}
		if g, e := Log2Uint16(uint16(n)), lg2(n&0xffff); g != e {
			t.Fatalf("%b %d %d", n, g, e)
		}
		if g, e := Log2Byte(byte(n)), lg2(n&0xff); g != e {
			t.Fatalf("%b %d %d", n, g, e)
		}
	}
}

func TestBitLen(t *testing.T) {
	if g, e := BitLenByte(0), 0; g != e {
		t.Error(g, e)
	}
	if g, e := BitLenUint16(0), 0; g != e {
		t.Error(g, e)
	}
	if g, e := BitLenUint32(0), 0; g != e {
		t.Error(g, e)
	}
	if g, e := BitLenUint64(0), 0; g != e {
		t.Error(g, e)
	}
	if g, e := BitLenUintptr(0), 0; g != e {
		t.Error(g, e)
	}
	const N = 1e6
	rng := r64()
	for i := 0; i < N; i++ {
		n := uint64(rng.Next().Int64())
		if g, e := BitLenUintptr(uintptr(n)), lg2(uint64(uintptr(n)))+1; g != e {
			t.Fatalf("%b %d %d", n, g, e)
		}
		if g, e := BitLenUint64(n), lg2(n)+1; g != e {
			t.Fatalf("%b %d %d", n, g, e)
		}
		if g, e := BitLenUint32(uint32(n)), lg2(n&0xffffffff)+1; g != e {
			t.Fatalf("%b %d %d", n, g, e)
		}
		if g, e := BitLen(int(n)), lg2(uint64(uint(n)))+1; g != e {
			t.Fatalf("%b %d %d", n, g, e)
		}
		if g, e := BitLenUint(uint(n)), lg2(uint64(uint(n)))+1; g != e {
			t.Fatalf("%b %d %d", n, g, e)
		}
		if g, e := BitLenUint16(uint16(n)), lg2(n&0xffff)+1; g != e {
			t.Fatalf("%b %d %d", n, g, e)
		}
		if g, e := BitLenByte(byte(n)), lg2(n&0xff)+1; g != e {
			t.Fatalf("%b %d %d", n, g, e)
		}
	}
}

func BenchmarkGCDByte(b *testing.B) {
	const N = 1 << 16
	type t byte
	type u struct{ a, b t }
	b.StopTimer()
	rng := r32()
	a := make([]u, N)
	for i := range a {
		a[i] = u{t(rng.Next()), t(rng.Next())}
	}
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		v := a[i&(N-1)]
		GCDByte(byte(v.a), byte(v.b))
	}
}

func BenchmarkGCDUint16(b *testing.B) {
	const N = 1 << 16
	type t uint16
	type u struct{ a, b t }
	b.StopTimer()
	rng := r32()
	a := make([]u, N)
	for i := range a {
		a[i] = u{t(rng.Next()), t(rng.Next())}
	}
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		v := a[i&(N-1)]
		GCDUint16(uint16(v.a), uint16(v.b))
	}
}

func BenchmarkGCDUint32(b *testing.B) {
	const N = 1 << 16
	type t uint32
	type u struct{ a, b t }
	b.StopTimer()
	rng := r32()
	a := make([]u, N)
	for i := range a {
		a[i] = u{t(rng.Next()), t(rng.Next())}
	}
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		v := a[i&(N-1)]
		GCDUint32(uint32(v.a), uint32(v.b))
	}
}

func BenchmarkGCDUint64(b *testing.B) {
	const N = 1 << 16
	type t uint64
	type u struct{ a, b t }
	b.StopTimer()
	rng := r64()
	a := make([]u, N)
	for i := range a {
		a[i] = u{t(rng.Next().Int64()), t(rng.Next().Int64())}
	}
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		v := a[i&(N-1)]
		GCDUint64(uint64(v.a), uint64(v.b))
	}
}

func BenchmarkLog2Byte(b *testing.B) {
	const N = 1 << 16
	type t byte
	b.StopTimer()
	rng := r32()
	a := make([]t, N)
	for i := range a {
		a[i] = t(rng.Next())
	}
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		Log2Byte(byte(a[i&(N-1)]))
	}
}

func BenchmarkLog2Uint16(b *testing.B) {
	const N = 1 << 16
	type t uint16
	b.StopTimer()
	rng := r32()
	a := make([]t, N)
	for i := range a {
		a[i] = t(rng.Next())
	}
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		Log2Uint16(uint16(a[i&(N-1)]))
	}
}

func BenchmarkLog2Uint32(b *testing.B) {
	const N = 1 << 16
	type t uint32
	b.StopTimer()
	rng := r32()
	a := make([]t, N)
	for i := range a {
		a[i] = t(rng.Next())
	}
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		Log2Uint32(uint32(a[i&(N-1)]))
	}
}

func BenchmarkLog2Uint64(b *testing.B) {
	const N = 1 << 16
	type t uint64
	b.StopTimer()
	rng := r64()
	a := make([]t, N)
	for i := range a {
		a[i] = t(rng.Next().Int64())
	}
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		Log2Uint64(uint64(a[i&(N-1)]))
	}
}
func BenchmarkBitLenByte(b *testing.B) {
	const N = 1 << 16
	type t byte
	b.StopTimer()
	rng := r32()
	a := make([]t, N)
	for i := range a {
		a[i] = t(rng.Next())
	}
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		BitLenByte(byte(a[i&(N-1)]))
	}
}

func BenchmarkBitLenUint16(b *testing.B) {
	const N = 1 << 16
	type t uint16
	b.StopTimer()
	rng := r32()
	a := make([]t, N)
	for i := range a {
		a[i] = t(rng.Next())
	}
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		BitLenUint16(uint16(a[i&(N-1)]))
	}
}

func BenchmarkBitLenUint32(b *testing.B) {
	const N = 1 << 16
	type t uint32
	b.StopTimer()
	rng := r32()
	a := make([]t, N)
	for i := range a {
		a[i] = t(rng.Next())
	}
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		BitLenUint32(uint32(a[i&(N-1)]))
	}
}

func BenchmarkBitLen(b *testing.B) {
	const N = 1 << 16
	type t int
	b.StopTimer()
	rng := r64()
	a := make([]t, N)
	for i := range a {
		a[i] = t(rng.Next().Int64())
	}
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		BitLen(int(a[i&(N-1)]))
	}
}

func BenchmarkBitLenUint(b *testing.B) {
	const N = 1 << 16
	type t uint
	b.StopTimer()
	rng := r64()
	a := make([]t, N)
	for i := range a {
		a[i] = t(rng.Next().Int64())
	}
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		BitLenUint(uint(a[i&(N-1)]))
	}
}

func BenchmarkBitLenUintptr(b *testing.B) {
	const N = 1 << 16
	type t uintptr
	b.StopTimer()
	rng := r64()
	a := make([]t, N)
	for i := range a {
		a[i] = t(rng.Next().Int64())
	}
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		BitLenUintptr(uintptr(a[i&(N-1)]))
	}
}

func BenchmarkBitLenUint64(b *testing.B) {
	const N = 1 << 16
	type t uint64
	b.StopTimer()
	rng := r64()
	a := make([]t, N)
	for i := range a {
		a[i] = t(rng.Next().Int64())
	}
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		BitLenUint64(uint64(a[i&(N-1)]))
	}
}

func BenchmarkPopCountByte(b *testing.B) {
	const N = 1 << 16
	type t byte
	b.StopTimer()
	rng := r32()
	a := make([]t, N)
	for i := range a {
		a[i] = t(rng.Next())
	}
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		PopCountByte(byte(a[i&(N-1)]))
	}
}

func BenchmarkPopCountUint16(b *testing.B) {
	const N = 1 << 16
	type t uint16
	b.StopTimer()
	rng := r32()
	a := make([]t, N)
	for i := range a {
		a[i] = t(rng.Next())
	}
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		PopCountUint16(uint16(a[i&(N-1)]))
	}
}

func BenchmarkPopCountUint32(b *testing.B) {
	const N = 1 << 16
	type t uint32
	b.StopTimer()
	rng := r32()
	a := make([]t, N)
	for i := range a {
		a[i] = t(rng.Next())
	}
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		PopCountUint32(uint32(a[i&(N-1)]))
	}
}

func BenchmarkPopCount(b *testing.B) {
	const N = 1 << 16
	type t int
	b.StopTimer()
	rng := r64()
	a := make([]t, N)
	for i := range a {
		a[i] = t(rng.Next().Int64())
	}
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		PopCount(int(a[i&(N-1)]))
	}
}

func BenchmarkPopCountUint(b *testing.B) {
	const N = 1 << 16
	type t uint
	b.StopTimer()
	rng := r64()
	a := make([]t, N)
	for i := range a {
		a[i] = t(rng.Next().Int64())
	}
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		PopCountUint(uint(a[i&(N-1)]))
	}
}

func BenchmarkPopCountUintptr(b *testing.B) {
	const N = 1 << 16
	type t uintptr
	b.StopTimer()
	rng := r64()
	a := make([]t, N)
	for i := range a {
		a[i] = t(rng.Next().Int64())
	}
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		PopCountUintptr(uintptr(a[i&(N-1)]))
	}
}

func BenchmarkPopCountUint64(b *testing.B) {
	const N = 1 << 16
	type t uint64
	b.StopTimer()
	rng := r64()
	a := make([]t, N)
	for i := range a {
		a[i] = t(rng.Next().Int64())
	}
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		PopCountUint64(uint64(a[i&(N-1)]))
	}
}

func TestUintptrBits(t *testing.T) {
	switch g := UintptrBits(); g {
	case 32, 64:
		// ok
		t.Log(g)
	default:
		t.Fatalf("got %d, expected 32 or 64", g)
	}
}

func BenchmarkUintptrBits(b *testing.B) {
	for i := 0; i < b.N; i++ {
		UintptrBits()
	}
}

func TestModPowByte(t *testing.T) {
	data := []struct{ b, e, m, r byte }{
		{0, 1, 1, 0},
		{0, 2, 1, 0},
		{0, 3, 1, 0},

		{1, 0, 1, 0},
		{1, 1, 1, 0},
		{1, 2, 1, 0},
		{1, 3, 1, 0},

		{2, 0, 1, 0},
		{2, 1, 1, 0},
		{2, 2, 1, 0},
		{2, 3, 1, 0},

		{2, 11, 23, 1}, // 23|M11
		{2, 11, 89, 1}, // 89|M11
		{2, 23, 47, 1}, // 47|M23
		{5, 3, 13, 8},
	}

	for _, v := range data {
		if g, e := ModPowByte(v.b, v.e, v.m), v.r; g != e {
			t.Errorf("b %d e %d m %d: got %d, exp %d", v.b, v.e, v.m, g, e)
		}
	}
}

func TestModPowUint16(t *testing.T) {
	data := []struct{ b, e, m, r uint16 }{
		{0, 1, 1, 0},
		{0, 2, 1, 0},
		{0, 3, 1, 0},

		{1, 0, 1, 0},
		{1, 1, 1, 0},
		{1, 2, 1, 0},
		{1, 3, 1, 0},

		{2, 0, 1, 0},
		{2, 1, 1, 0},
		{2, 2, 1, 0},
		{2, 3, 1, 0},

		{2, 11, 23, 1},     // 23|M11
		{2, 11, 89, 1},     // 89|M11
		{2, 23, 47, 1},     // 47|M23
		{2, 929, 13007, 1}, // 13007|M929
		{4, 13, 497, 445},
		{5, 3, 13, 8},
	}

	for _, v := range data {
		if g, e := ModPowUint16(v.b, v.e, v.m), v.r; g != e {
			t.Errorf("b %d e %d m %d: got %d, exp %d", v.b, v.e, v.m, g, e)
		}
	}
}

func TestModPowUint32(t *testing.T) {
	data := []struct{ b, e, m, r uint32 }{
		{0, 1, 1, 0},
		{0, 2, 1, 0},
		{0, 3, 1, 0},

		{1, 0, 1, 0},
		{1, 1, 1, 0},
		{1, 2, 1, 0},
		{1, 3, 1, 0},

		{2, 0, 1, 0},
		{2, 1, 1, 0},
		{2, 2, 1, 0},
		{2, 3, 1, 0},

		{2, 23, 47, 1},        // 47|M23
		{2, 67, 193707721, 1}, // 193707721|M67
		{2, 929, 13007, 1},    // 13007|M929
		{4, 13, 497, 445},
		{5, 3, 13, 8},
		{2, 500471, 264248689, 1},
		{2, 1000249, 112027889, 1},
		{2, 2000633, 252079759, 1},
		{2, 3000743, 222054983, 1},
		{2, 4000741, 1920355681, 1},
		{2, 5000551, 330036367, 1},
		{2, 6000479, 1020081431, 1},
		{2, 7000619, 840074281, 1},
		{2, 8000401, 624031279, 1},
		{2, 9000743, 378031207, 1},
		{2, 10000961, 380036519, 1},
		{2, 20000723, 40001447, 1},
	}

	for _, v := range data {
		if g, e := ModPowUint32(v.b, v.e, v.m), v.r; g != e {
			t.Errorf("b %d e %d m %d: got %d, exp %d", v.b, v.e, v.m, g, e)
		}
	}
}

func TestModPowUint64(t *testing.T) {
	data := []struct{ b, e, m, r uint64 }{
		{0, 1, 1, 0},
		{0, 2, 1, 0},
		{0, 3, 1, 0},

		{1, 0, 1, 0},
		{1, 1, 1, 0},
		{1, 2, 1, 0},
		{1, 3, 1, 0},

		{2, 0, 1, 0},
		{2, 1, 1, 0},
		{2, 2, 1, 0},
		{2, 3, 1, 0},

		{2, 23, 47, 1},        // 47|M23
		{2, 67, 193707721, 1}, // 193707721|M67
		{2, 929, 13007, 1},    // 13007|M929
		{4, 13, 497, 445},
		{5, 3, 13, 8},
		{2, 500471, 264248689, 1}, // m|Me ...
		{2, 1000249, 112027889, 1},
		{2, 2000633, 252079759, 1},
		{2, 3000743, 222054983, 1},
		{2, 4000741, 1920355681, 1},
		{2, 5000551, 330036367, 1},
		{2, 6000479, 1020081431, 1},
		{2, 7000619, 840074281, 1},
		{2, 8000401, 624031279, 1},
		{2, 9000743, 378031207, 1},
		{2, 10000961, 380036519, 1},
		{2, 20000723, 40001447, 1},
		{2, 1000099, 1872347344039, 1},

		{9223372036854775919, 9223372036854776030, 9223372036854776141, 7865333882915297658},
	}

	for _, v := range data {
		if g, e := ModPowUint64(v.b, v.e, v.m), v.r; g != e {
			t.Errorf("b %d e %d m %d: got %d, exp %d", v.b, v.e, v.m, g, e)
		}
	}
}

func TestModPowBigInt(t *testing.T) {
	data := []struct {
		b, e int64
		m    interface{}
		r    int64
	}{
		{0, 1, 1, 0},
		{0, 2, 1, 0},
		{0, 3, 1, 0},

		{1, 0, 1, 0},
		{1, 1, 1, 0},
		{1, 2, 1, 0},
		{1, 3, 1, 0},

		{2, 0, 1, 0},
		{2, 1, 1, 0},
		{2, 2, 1, 0},
		{2, 3, 1, 0},

		{2, 23, 47, 1},        // 47|M23
		{2, 67, 193707721, 1}, // 193707721|M67
		{2, 929, 13007, 1},    // 13007|M929
		{4, 13, 497, 445},
		{5, 3, 13, 8},
		{2, 500471, 264248689, 1}, // m|Me ...
		{2, 1000249, 112027889, 1},
		{2, 2000633, 252079759, 1},
		{2, 3000743, 222054983, 1},
		{2, 4000741, 1920355681, 1},
		{2, 5000551, 330036367, 1},
		{2, 6000479, 1020081431, 1},
		{2, 7000619, 840074281, 1},
		{2, 8000401, 624031279, 1},
		{2, 9000743, 378031207, 1},
		{2, 10000961, 380036519, 1},
		{2, 20000723, 40001447, 1},
		{2, 100279, "11502865265922183403581252152383", 1},

		{2, 7293457, "533975545077050000610542659519277030089249998649", 1},
	}

	for _, v := range data {
		var m big.Int
		switch x := v.m.(type) {
		case int:
			m.SetInt64(int64(x))
		case string:
			m.SetString(x, 10)
		}
		b, e, r := big.NewInt(v.b), big.NewInt(v.e), big.NewInt(v.r)
		if g, e := ModPowBigInt(b, e, &m), r; g.Cmp(e) != 0 {
			t.Errorf("b %s e %s m %v: got %s, exp %s", b, e, m, g, e)
		}
	}

	s := func(n string) *big.Int {
		i, ok := big.NewInt(0).SetString(n, 10)
		if !ok {
			t.Fatal(ok)
		}

		return i
	}

	if g, e := ModPowBigInt(
		s("36893488147419103343"), s("36893488147419103454"), s("36893488147419103565")), s("34853007610367449339"); g.Cmp(e) != 0 {
		t.Fatal(g, e)
	}
}

func BenchmarkModPowByte(b *testing.B) {
	const N = 1 << 16
	b.StopTimer()
	type t struct{ b, e, m byte }
	a := make([]t, N)
	r := r32()
	for i := range a {
		a[i] = t{
			byte(r.Next() | 2),
			byte(r.Next() | 2),
			byte(r.Next() | 2),
		}
	}
	runtime.GC()
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		v := a[i&(N-1)]
		ModPowByte(v.b, v.e, v.m)
	}
}

func BenchmarkModPowUint16(b *testing.B) {
	const N = 1 << 16
	b.StopTimer()
	type t struct{ b, e, m uint16 }
	a := make([]t, N)
	r := r32()
	for i := range a {
		a[i] = t{
			uint16(r.Next() | 2),
			uint16(r.Next() | 2),
			uint16(r.Next() | 2),
		}
	}
	runtime.GC()
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		v := a[i&(N-1)]
		ModPowUint16(v.b, v.e, v.m)
	}
}

func BenchmarkModPowUint32(b *testing.B) {
	const N = 1 << 16
	b.StopTimer()
	type t struct{ b, e, m uint32 }
	a := make([]t, N)
	r := r32()
	for i := range a {
		a[i] = t{
			uint32(r.Next() | 2),
			uint32(r.Next() | 2),
			uint32(r.Next() | 2),
		}
	}
	runtime.GC()
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		v := a[i&(N-1)]
		ModPowUint32(v.b, v.e, v.m)
	}
}

func BenchmarkModPowUint64(b *testing.B) {
	const N = 1 << 16
	b.StopTimer()
	type t struct{ b, e, m uint64 }
	a := make([]t, N)
	r := r64()
	for i := range a {
		a[i] = t{
			uint64(r.Next().Int64() | 2),
			uint64(r.Next().Int64() | 2),
			uint64(r.Next().Int64() | 2),
		}
	}
	runtime.GC()
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		v := a[i&(N-1)]
		ModPowUint64(v.b, v.e, v.m)
	}
}

func BenchmarkModPowBigInt(b *testing.B) {
	const N = 1 << 16
	b.StopTimer()
	type t struct{ b, e, m *big.Int }
	a := make([]t, N)
	mx := big.NewInt(math.MaxInt64)
	mx.Mul(mx, mx)
	r, err := NewFCBig(big.NewInt(2), mx, true)
	if err != nil {
		b.Fatal(err)
	}
	for i := range a {
		a[i] = t{
			r.Next(),
			r.Next(),
			r.Next(),
		}
	}
	runtime.GC()
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		v := a[i&(N-1)]
		ModPowBigInt(v.b, v.e, v.m)
	}
}

func TestAdd128(t *testing.T) {
	const N = 1e5
	r := r64()
	var mm big.Int
	for i := 0; i < N; i++ {
		a, b := uint64(r.Next().Int64()), uint64(r.Next().Int64())
		aa, bb := big.NewInt(0).SetUint64(a), big.NewInt(0).SetUint64(b)
		mhi, mlo := AddUint128_64(a, b)
		m := big.NewInt(0).SetUint64(mhi)
		m.Lsh(m, 64)
		m.Add(m, big.NewInt(0).SetUint64(mlo))
		mm.Add(aa, bb)
		if m.Cmp(&mm) != 0 {
			t.Fatalf("%d\na %40d\nb %40d\ng %40s %032x\ne %40s %032x", i, a, b, m, m, &mm, &mm)
		}
	}
}

func TestMul128(t *testing.T) {
	const N = 1e5
	r := r64()
	var mm big.Int
	f := func(a, b uint64) {
		aa, bb := big.NewInt(0).SetUint64(a), big.NewInt(0).SetUint64(b)
		mhi, mlo := MulUint128_64(a, b)
		m := big.NewInt(0).SetUint64(mhi)
		m.Lsh(m, 64)
		m.Add(m, big.NewInt(0).SetUint64(mlo))
		mm.Mul(aa, bb)
		if m.Cmp(&mm) != 0 {
			t.Fatalf("\na %40d\nb %40d\ng %40s %032x\ne %40s %032x", a, b, m, m, &mm, &mm)
		}
	}
	for i := 0; i < N; i++ {
		f(uint64(r.Next().Int64()), uint64(r.Next().Int64()))
	}
	for x := 0; x <= 1<<9; x++ {
		for y := 0; y <= 1<<9; y++ {
			f(math.MaxUint64-uint64(x), math.MaxUint64-uint64(y))
		}
	}
}

func BenchmarkMul128(b *testing.B) {
	const N = 1 << 16
	b.StopTimer()
	type t struct{ a, b uint64 }
	a := make([]t, N)
	r := r64()
	for i := range a {
		a[i] = t{
			uint64(r.Next().Int64()),
			uint64(r.Next().Int64()),
		}
	}
	runtime.GC()
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		v := a[i&(N-1)]
		MulUint128_64(v.a, v.b)
	}
}

func BenchmarkMul128Big(b *testing.B) {
	const N = 1 << 16
	b.StopTimer()
	type t struct{ a, b *big.Int }
	a := make([]t, N)
	r := r64()
	for i := range a {
		a[i] = t{
			big.NewInt(r.Next().Int64()),
			big.NewInt(r.Next().Int64()),
		}
	}
	var x big.Int
	runtime.GC()
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		v := a[i&(N-1)]
		x.Mul(v.a, v.b)
	}
}

func TestIsPrimeUint64(t *testing.T) {
	f := func(lo, hi uint64, exp int) {
		got := 0
		for n := lo; n <= hi; {
			if IsPrimeUint64(n) {
				got++
			}
			n0 := n
			n++
			if n < n0 {
				break
			}
		}
		if got != exp {
			t.Fatal(lo, hi, got, exp)
		}
	}

	// lo, hi, PrimePi(hi)-PrimePi(lo)
	f(0, 1e4, 1229)
	f(1e5, 1e5+1e4, 861)
	f(1e6, 1e6+1e4, 753)
	f(1e7, 1e7+1e4, 614)
	f(1e8, 1e8+1e4, 551)
	f(1e9, 1e9+1e4, 487)
	f(1e10, 1e10+1e4, 406)
	f(1e11, 1e11+1e4, 394)
	f(1e12, 1e12+1e4, 335)
	f(1e13, 1e13+1e4, 354)
	f(1e14, 1e14+1e4, 304)
	f(1e15, 1e15+1e4, 263)
	f(1e16, 1e16+1e4, 270)
	f(1e17, 1e17+1e4, 265)
	f(1e18, 1e18+1e4, 241)
	f(1e19, 1e19+1e4, 255)
	f(1<<64-1e4, 1<<64-1, 218)
}

func TestProbablyPrimeUint32(t *testing.T) {
	f := func(n, firstFail uint32, primes []uint32) {
		for ; n <= firstFail; n += 2 {
			prp := true
			for _, a := range primes {
				if !ProbablyPrimeUint32(n, a) {
					prp = false
					break
				}
			}
			if prp != IsPrime(n) && n != firstFail {
				t.Fatal(n)
			}
		}
	}
	if !ProbablyPrimeUint32(5, 2) {
		t.Fatal(false)
	}
	if !ProbablyPrimeUint32(7, 2) {
		t.Fatal(false)
	}
	if ProbablyPrimeUint32(9, 2) {
		t.Fatal(true)
	}
	if !ProbablyPrimeUint32(11, 2) {
		t.Fatal(false)
	}
	// http://oeis.org/A014233
	f(5, 2047, []uint32{2})
	f(2047, 1373653, []uint32{2, 3})
	f(1373653, 25326001, []uint32{2, 3, 5})
}

func BenchmarkProbablyPrimeUint32(b *testing.B) {
	const N = 1 << 16
	b.StopTimer()
	type t struct{ n, a uint32 }
	data := make([]t, N)
	r := r32()
	for i := range data {
		n := uint32(r.Next()) | 1
		if n <= 3 {
			n += 5
		}
		a := uint32(r.Next())
		if a <= 1 {
			a += 2
		}
		data[i] = t{n, a}
	}
	runtime.GC()
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		v := data[i&(N-1)]
		ProbablyPrimeUint32(v.n, v.a)
	}
}

func TestProbablyPrimeUint64_32(t *testing.T) {
	f := func(n, firstFail uint64, primes []uint32) {
		for ; n <= firstFail; n += 2 {
			prp := true
			for _, a := range primes {
				if !ProbablyPrimeUint64_32(n, a) {
					prp = false
					break
				}
			}
			if prp != IsPrimeUint64(n) && n != firstFail {
				t.Fatal(n)
			}
		}
	}
	if !ProbablyPrimeUint64_32(5, 2) {
		t.Fatal(false)
	}
	if !ProbablyPrimeUint64_32(7, 2) {
		t.Fatal(false)
	}
	if ProbablyPrimeUint64_32(9, 2) {
		t.Fatal(true)
	}
	if !ProbablyPrimeUint64_32(11, 2) {
		t.Fatal(false)
	}
	// http://oeis.org/A014233
	f(5, 2047, []uint32{2})
	f(2047, 1373653, []uint32{2, 3})
}

func BenchmarkProbablyPrimeUint64_32(b *testing.B) {
	const N = 1 << 16
	b.StopTimer()
	type t struct {
		n uint64
		a uint32
	}
	data := make([]t, N)
	r := r32()
	r2 := r64()
	for i := range data {
		var n uint64
		for n <= 3 {
			n = uint64(r2.Next().Int64()) | 1
		}
		var a uint32
		for a <= 1 {
			a = uint32(r.Next())
		}
		data[i] = t{n, a}
	}
	runtime.GC()
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		v := data[i&(N-1)]
		ProbablyPrimeUint64_32(v.n, v.a)
	}
}

func TestProbablyPrimeBigInt_32(t *testing.T) {
	f := func(n0, firstFail0 uint64, primes []uint32) {
		n, firstFail := big.NewInt(0).SetUint64(n0), big.NewInt(0).SetUint64(firstFail0)
		for ; n.Cmp(firstFail) <= 0; n.Add(n, _2) {
			prp := true
			for _, a := range primes {
				if !ProbablyPrimeBigInt_32(n, a) {
					prp = false
					break
				}
			}
			if prp != IsPrimeUint64(n0) && n0 != firstFail0 {
				t.Fatal(n)
			}
			n0 += 2
		}
	}
	if !ProbablyPrimeBigInt_32(big.NewInt(5), 2) {
		t.Fatal(false)
	}
	if !ProbablyPrimeBigInt_32(big.NewInt(7), 2) {
		t.Fatal(false)
	}
	if ProbablyPrimeBigInt_32(big.NewInt(9), 2) {
		t.Fatal(true)
	}
	if !ProbablyPrimeBigInt_32(big.NewInt(11), 2) {
		t.Fatal(false)
	}
	// http://oeis.org/A014233
	f(5, 2047, []uint32{2})
	f(2047, 1373653, []uint32{2, 3})
}

func BenchmarkProbablyPrimeBigInt_32(b *testing.B) {
	const N = 1 << 16
	b.StopTimer()
	type t struct {
		n *big.Int
		a uint32
	}
	data := make([]t, N)
	r := r32()
	r2 := r64()
	for i := range data {
		var n uint64
		for n <= 3 {
			n = uint64(r2.Next().Int64()) | 1
		}
		var a uint32
		for a <= 1 {
			a = uint32(r.Next())
		}
		data[i] = t{big.NewInt(0).SetUint64(n), a}
	}
	runtime.GC()
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		v := data[i&(N-1)]
		ProbablyPrimeBigInt_32(v.n, v.a)
	}
}

func TestProbablyPrimeBigInt(t *testing.T) {
	f := func(n0, firstFail0 uint64, primes []uint32) {
		n, firstFail := big.NewInt(0).SetUint64(n0), big.NewInt(0).SetUint64(firstFail0)
		for ; n.Cmp(firstFail) <= 0; n.Add(n, _2) {
			prp := true
			var a big.Int
			for _, a0 := range primes {
				a.SetInt64(int64(a0))
				if !ProbablyPrimeBigInt(n, &a) {
					prp = false
					break
				}
			}
			if prp != IsPrimeUint64(n0) && n0 != firstFail0 {
				t.Fatal(n)
			}
			n0 += 2
		}
	}
	if !ProbablyPrimeBigInt(big.NewInt(5), _2) {
		t.Fatal(false)
	}
	if !ProbablyPrimeBigInt(big.NewInt(7), _2) {
		t.Fatal(false)
	}
	if ProbablyPrimeBigInt(big.NewInt(9), _2) {
		t.Fatal(true)
	}
	if !ProbablyPrimeBigInt(big.NewInt(11), _2) {
		t.Fatal(false)
	}
	// http://oeis.org/A014233
	f(5, 2047, []uint32{2})
	f(2047, 1373653, []uint32{2, 3})
}

var once2059 sync.Once

func BenchmarkProbablyPrimeBigInt64(b *testing.B) {
	const N = 1 << 16
	b.StopTimer()
	once2059.Do(func() { b.Log("64 bit n, 64 bit a\n") })
	type t struct {
		n, a *big.Int
	}
	data := make([]t, N)
	r := r64()
	for i := range data {
		var n uint64
		for n <= 3 {
			n = uint64(r.Next().Int64()) | 1
		}
		var a uint64
		for a <= 1 {
			a = uint64(r.Next().Int64())
		}
		data[i] = t{big.NewInt(0).SetUint64(n), big.NewInt(0).SetUint64(a)}
	}
	runtime.GC()
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		v := data[i&(N-1)]
		ProbablyPrimeBigInt(v.n, v.a)
	}
}

var once2090 sync.Once

func BenchmarkProbablyPrimeBigInt128(b *testing.B) {
	const N = 1 << 16
	b.StopTimer()
	once2090.Do(func() { b.Log("128 bit n, 128 bit a\n") })
	type t struct {
		n, a *big.Int
	}
	data := make([]t, N)
	r := r64()
	for i := range data {
		n := big.NewInt(0).SetUint64(uint64(r.Next().Int64()))
		n.Lsh(n, 64)
		n.Add(n, big.NewInt(0).SetUint64(uint64(r.Next().Int64())|1))
		a := big.NewInt(0).SetUint64(uint64(r.Next().Int64()))
		a.Lsh(a, 64)
		a.Add(a, big.NewInt(0).SetUint64(uint64(r.Next().Int64())))
		data[i] = t{n, a}
	}
	runtime.GC()
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		v := data[i&(N-1)]
		ProbablyPrimeBigInt(v.n, v.a)
	}
}

func TestQCmpUint32(t *testing.T) {
	const N = 6e4
	r := r32()
	var x, y big.Rat
	for i := 0; i < N; i++ {
		a, b, c, d := uint32(r.Next()), uint32(r.Next()), uint32(r.Next()), uint32(r.Next())
		x.SetFrac64(int64(a), int64(b))
		y.SetFrac64(int64(c), int64(d))
		if g, e := QCmpUint32(a, b, c, d), x.Cmp(&y); g != e {
			t.Fatal(a, b, c, d, g, e)
		}
	}
}

func TestQScaleUint32(t *testing.T) {
	const N = 4e4
	r := r32()
	var x, y big.Rat
	var a uint64
	var b, c, d uint32
	for i := 0; i < N; i++ {
		for {
			b, c, d = uint32(r.Next()), uint32(r.Next()), uint32(r.Next())
			a = QScaleUint32(b, c, d)
			if a <= math.MaxInt64 {
				break
			}
		}
		x.SetFrac64(int64(a), int64(b))
		y.SetFrac64(int64(c), int64(d))
		if g := x.Cmp(&y); g < 0 {
			t.Fatal(a, b, c, d, g, "expexted 1 or 0")
		}

		if a != 0 {
			x.SetFrac64(int64(a-1), int64(b))
			if g := x.Cmp(&y); g > 0 {
				t.Fatal(a, b, c, d, g, "expected -1 or 0")
			}
		}
	}
}

var smalls = []uint32{2, 3, 5, 7, 11, 13, 17, 19, 23, 29}

func isPrimorialProduct(t FactorTerms, maxp uint32) bool {
	if len(t) == 0 {
		return false
	}

	pmax := uint32(32)
	for i, v := range t {
		if v.Prime != smalls[i] || v.Power > pmax || v.Power > maxp {
			return false
		}
		pmax = v.Power
	}
	return true
}

func TestPrimorialProductsUint32(t *testing.T) {
	r := PrimorialProductsUint32(2*3*5*7*11*13*17*19+1, math.MaxUint32, 1)
	if len(r) != 1 {
		t.Fatal(len(r))
	}

	if r[0] != 2*3*5*7*11*13*17*19*23 {
		t.Fatal(r[0])
	}

	r = PrimorialProductsUint32(0, math.MaxUint32, math.MaxUint32)
	if g, e := len(r), 1679; g != e {
		t.Fatal(g, e)
	}

	m := map[uint32]struct{}{}
	for _, v := range r {
		if _, ok := m[v]; ok {
			t.Fatal(v)
		}

		m[v] = struct{}{}
	}

	for lo := uint32(0); lo < 5e4; lo += 1e3 {
		hi := 1e2 * lo
		for max := uint32(0); max <= 32; max++ {
			m := map[uint32]struct{}{}
			for i, v := range PrimorialProductsUint32(lo, hi, max) {
				f := FactorInt(v)
				if v < lo || v > hi {
					t.Fatal(lo, hi, max, v)
				}

				if _, ok := m[v]; ok {
					t.Fatal(i, lo, hi, max, v, f)
				}

				m[v] = struct{}{}
				if !isPrimorialProduct(f, max) {
					t.Fatal(i, v)
				}

				for _, v := range f {
					if v.Power > max {
						t.Fatal(i, v, f)
					}
				}
			}
		}
	}
}

func BenchmarkPrimorialProductsUint32(b *testing.B) {
	for i := 0; i < b.N; i++ {
		PrimorialProductsUint32(0, math.MaxUint32, math.MaxUint32)
	}
}

func powerizeUint32BigInt(b uint32, n *big.Int) (e uint32, p *big.Int) {
	p = big.NewInt(1)
	bb := big.NewInt(int64(b))
	for p.Cmp(n) < 0 {
		p.Mul(p, bb)
		e++
	}
	return
}

func TestPowerizeUint32BigInt(t *testing.T) {
	var data = []struct{ b, n, e, p int }{
		{0, 10, 0, -1},
		{1, 10, 0, -1},
		{2, -1, 0, -1},
		{2, 0, 0, 1},
		{2, 1, 0, 1},
		{2, 2, 1, 2},
		{2, 3, 2, 4},
		{3, 0, 0, 1},
		{3, 1, 0, 1},
		{3, 2, 1, 3},
		{3, 3, 1, 3},
		{3, 4, 2, 9},
		{3, 8, 2, 9},
		{3, 9, 2, 9},
		{3, 10, 3, 27},
		{3, 80, 4, 81},
	}

	var n big.Int
	for _, v := range data {
		b := v.b
		n.SetInt64(int64(v.n))
		e, p := PowerizeUint32BigInt(uint32(b), &n)
		if e != uint32(v.e) {
			t.Fatal(b, &n, e, p, v.e, v.p)
		}

		if v.p < 0 {
			if p != nil {
				t.Fatal(b, &n, e, p, v.e, v.p)
			}
			continue
		}

		if p.Int64() != int64(v.p) {
			t.Fatal(b, &n, e, p, v.e, v.p)
		}
	}
	const N = 1e5
	var nn big.Int
	for _, base := range []uint32{2, 3, 15, 17} {
		for n := 0; n <= N; n++ {
			nn.SetInt64(int64(n))
			ge, gp := PowerizeUint32BigInt(base, &nn)
			ee, ep := powerizeUint32BigInt(base, &nn)
			if ge != ee || gp.Cmp(ep) != 0 {
				t.Fatal(base, n, ge, gp, ee, ep)
			}

			gp.Div(gp, big.NewInt(int64(base)))
			if gp.Sign() > 0 && gp.Cmp(&nn) >= 0 {
				t.Fatal(gp.Sign(), gp.Cmp(&nn))
			}
		}
	}
}

func benchmarkPowerizeUint32BigInt(b *testing.B, base uint32, exp int) {
	b.StopTimer()
	var n big.Int
	n.SetBit(&n, exp, 1)
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		PowerizeUint32BigInt(base, &n)
	}
}

func BenchmarkPowerizeUint32BigInt_2_2e1e1(b *testing.B) {
	benchmarkPowerizeUint32BigInt(b, 2, 1e1)
}

func BenchmarkPowerizeUint32BigInt_2_2e1e2(b *testing.B) {
	benchmarkPowerizeUint32BigInt(b, 2, 1e2)
}

func BenchmarkPowerizeUint32BigInt_2_2e1e3(b *testing.B) {
	benchmarkPowerizeUint32BigInt(b, 2, 1e3)
}

func BenchmarkPowerizeUint32BigInt_2_2e1e4(b *testing.B) {
	benchmarkPowerizeUint32BigInt(b, 2, 1e4)
}

func BenchmarkPowerizeUint32BigInt_2_2e1e5(b *testing.B) {
	benchmarkPowerizeUint32BigInt(b, 2, 1e5)
}

func BenchmarkPowerizeUint32BigInt_2_2e1e6(b *testing.B) {
	benchmarkPowerizeUint32BigInt(b, 2, 1e6)
}

func BenchmarkPowerizeUint32BigInt_2_2e1e7(b *testing.B) {
	benchmarkPowerizeUint32BigInt(b, 2, 1e7)
}

func BenchmarkPowerizeUint32BigInt_3_2e1e1(b *testing.B) {
	benchmarkPowerizeUint32BigInt(b, 3, 1e1)
}

func BenchmarkPowerizeUint32BigInt_3_2e1e2(b *testing.B) {
	benchmarkPowerizeUint32BigInt(b, 3, 1e2)
}

func BenchmarkPowerizeUint32BigInt_3_2e1e3(b *testing.B) {
	benchmarkPowerizeUint32BigInt(b, 3, 1e3)
}

func BenchmarkPowerizeUint32BigInt_3_2e1e4(b *testing.B) {
	benchmarkPowerizeUint32BigInt(b, 3, 1e4)
}

func BenchmarkPowerizeUint32BigInt_3_2e1e5(b *testing.B) {
	benchmarkPowerizeUint32BigInt(b, 3, 1e5)
}

func BenchmarkPowerizeUint32BigInt_3_2e1e6(b *testing.B) {
	benchmarkPowerizeUint32BigInt(b, 3, 1e6)
}

func BenchmarkPowerizeUint32BigInt_15_2e1e1(b *testing.B) {
	benchmarkPowerizeUint32BigInt(b, 15, 1e1)
}

func BenchmarkPowerizeUint32BigInt_15_2e1e2(b *testing.B) {
	benchmarkPowerizeUint32BigInt(b, 15, 1e2)
}

func BenchmarkPowerizeUint32BigInt_15_2e1e3(b *testing.B) {
	benchmarkPowerizeUint32BigInt(b, 15, 1e3)
}

func BenchmarkPowerizeUint32BigInt_15_2e1e4(b *testing.B) {
	benchmarkPowerizeUint32BigInt(b, 15, 1e4)
}

func BenchmarkPowerizeUint32BigInt_15_2e1e5(b *testing.B) {
	benchmarkPowerizeUint32BigInt(b, 15, 1e5)
}

func BenchmarkPowerizeUint32BigInt_15_2e1e6(b *testing.B) {
	benchmarkPowerizeUint32BigInt(b, 15, 1e6)
}

func BenchmarkPowerizeUint32BigInt_17_2e1e1(b *testing.B) {
	benchmarkPowerizeUint32BigInt(b, 17, 1e1)
}

func BenchmarkPowerizeUint32BigInt_17_2e1e2(b *testing.B) {
	benchmarkPowerizeUint32BigInt(b, 17, 1e2)
}

func BenchmarkPowerizeUint32BigInt_17_2e1e3(b *testing.B) {
	benchmarkPowerizeUint32BigInt(b, 17, 1e3)
}

func BenchmarkPowerizeUint32BigInt_17_2e1e4(b *testing.B) {
	benchmarkPowerizeUint32BigInt(b, 17, 1e4)
}

func BenchmarkPowerizeUint32BigInt_17_2e1e5(b *testing.B) {
	benchmarkPowerizeUint32BigInt(b, 17, 1e5)
}

func BenchmarkPowerizeUint32BigInt_17_2e1e6(b *testing.B) {
	benchmarkPowerizeUint32BigInt(b, 17, 1e6)
}

func TestPowerizeBigInt(t *testing.T) {
	var data = []struct{ b, n, e, p int }{
		{0, 10, 0, -1},
		{1, 10, 0, -1},
		{2, -1, 0, -1},
		{2, 0, 0, 1},
		{2, 1, 0, 1},
		{2, 2, 1, 2},
		{2, 3, 2, 4},
		{3, 0, 0, 1},
		{3, 1, 0, 1},
		{3, 2, 1, 3},
		{3, 3, 1, 3},
		{3, 4, 2, 9},
		{3, 8, 2, 9},
		{3, 9, 2, 9},
		{3, 10, 3, 27},
		{3, 80, 4, 81},
	}

	var b, n big.Int
	for _, v := range data {
		b.SetInt64(int64(v.b))
		n.SetInt64(int64(v.n))
		e, p := PowerizeBigInt(&b, &n)
		if e != uint32(v.e) {
			t.Fatal(&b, &n, e, p, v.e, v.p)
		}

		if v.p < 0 {
			if p != nil {
				t.Fatal(&b, &n, e, p, v.e, v.p)
			}
			continue
		}

		if p.Int64() != int64(v.p) {
			t.Fatal(&b, &n, e, p, v.e, v.p)
		}
	}
	const N = 1e5
	var nn big.Int
	for _, base := range []uint32{2, 3, 15, 17} {
		b.SetInt64(int64(base))
		for n := 0; n <= N; n++ {
			nn.SetInt64(int64(n))
			ge, gp := PowerizeBigInt(&b, &nn)
			ee, ep := powerizeUint32BigInt(base, &nn)
			if ge != ee || gp.Cmp(ep) != 0 {
				t.Fatal(base, n, ge, gp, ee, ep)
			}

			gp.Div(gp, &b)
			if gp.Sign() > 0 && gp.Cmp(&nn) >= 0 {
				t.Fatal(gp.Sign(), gp.Cmp(&nn))
			}
		}
	}
}

func benchmarkPowerizeBigInt(b *testing.B, base uint32, exp int) {
	b.StopTimer()
	var bb, n big.Int
	n.SetBit(&n, exp, 1)
	bb.SetInt64(int64(base))
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		PowerizeBigInt(&bb, &n)
	}
}

func BenchmarkPowerizeBigInt_2_2e1e1(b *testing.B) {
	benchmarkPowerizeBigInt(b, 2, 1e1)
}

func BenchmarkPowerizeBigInt_2_2e1e2(b *testing.B) {
	benchmarkPowerizeBigInt(b, 2, 1e2)
}

func BenchmarkPowerizeBigInt_2_2e1e3(b *testing.B) {
	benchmarkPowerizeBigInt(b, 2, 1e3)
}

func BenchmarkPowerizeBigInt_2_2e1e4(b *testing.B) {
	benchmarkPowerizeBigInt(b, 2, 1e4)
}

func BenchmarkPowerizeBigInt_2_2e1e5(b *testing.B) {
	benchmarkPowerizeBigInt(b, 2, 1e5)
}

func BenchmarkPowerizeBigInt_2_2e1e6(b *testing.B) {
	benchmarkPowerizeBigInt(b, 2, 1e6)
}

func BenchmarkPowerizeBigInt_2_2e1e7(b *testing.B) {
	benchmarkPowerizeBigInt(b, 2, 1e7)
}

func BenchmarkPowerizeBigInt_3_2e1e1(b *testing.B) {
	benchmarkPowerizeBigInt(b, 3, 1e1)
}

func BenchmarkPowerizeBigInt_3_2e1e2(b *testing.B) {
	benchmarkPowerizeBigInt(b, 3, 1e2)
}

func BenchmarkPowerizeBigInt_3_2e1e3(b *testing.B) {
	benchmarkPowerizeBigInt(b, 3, 1e3)
}

func BenchmarkPowerizeBigInt_3_2e1e4(b *testing.B) {
	benchmarkPowerizeBigInt(b, 3, 1e4)
}

func BenchmarkPowerizeBigInt_3_2e1e5(b *testing.B) {
	benchmarkPowerizeBigInt(b, 3, 1e5)
}

func BenchmarkPowerizeBigInt_3_2e1e6(b *testing.B) {
	benchmarkPowerizeBigInt(b, 3, 1e6)
}

func BenchmarkPowerizeBigInt_15_2e1e1(b *testing.B) {
	benchmarkPowerizeBigInt(b, 15, 1e1)
}

func BenchmarkPowerizeBigInt_15_2e1e2(b *testing.B) {
	benchmarkPowerizeBigInt(b, 15, 1e2)
}

func BenchmarkPowerizeBigInt_15_2e1e3(b *testing.B) {
	benchmarkPowerizeBigInt(b, 15, 1e3)
}

func BenchmarkPowerizeBigInt_15_2e1e4(b *testing.B) {
	benchmarkPowerizeBigInt(b, 15, 1e4)
}

func BenchmarkPowerizeBigInt_15_2e1e5(b *testing.B) {
	benchmarkPowerizeBigInt(b, 15, 1e5)
}

func BenchmarkPowerizeBigInt_15_2e1e6(b *testing.B) {
	benchmarkPowerizeBigInt(b, 15, 1e6)
}

func BenchmarkPowerizeBigInt_17_2e1e1(b *testing.B) {
	benchmarkPowerizeBigInt(b, 17, 1e1)
}

func BenchmarkPowerizeBigInt_17_2e1e2(b *testing.B) {
	benchmarkPowerizeBigInt(b, 17, 1e2)
}

func BenchmarkPowerizeBigInt_17_2e1e3(b *testing.B) {
	benchmarkPowerizeBigInt(b, 17, 1e3)
}

func BenchmarkPowerizeBigInt_17_2e1e4(b *testing.B) {
	benchmarkPowerizeBigInt(b, 17, 1e4)
}

func BenchmarkPowerizeBigInt_17_2e1e5(b *testing.B) {
	benchmarkPowerizeBigInt(b, 17, 1e5)
}

func BenchmarkPowerizeBigInt_17_2e1e6(b *testing.B) {
	benchmarkPowerizeBigInt(b, 17, 1e6)
}

func TestEnvelope(t *testing.T) {
	const prec = 1e-3
	type check struct {
		approx Approximation
		x, y   float64
	}
	data := []struct {
		points []float64
		checks []check
	}{
		{
			[]float64{0, 1},
			[]check{
				{Linear, 0, 0},
				{Linear, 0.25, 0.25},
				{Linear, 0.5, 0.5},
				{Linear, 0.75, 0.75},
				{Linear, 0.9999, 1},
			},
		},
		{
			[]float64{-1, 0},
			[]check{
				{Linear, 0, -1},
				{Linear, 0.25, -0.75},
				{Linear, 0.5, -0.5},
				{Linear, 0.75, -0.25},
				{Linear, 0.9999, 0},
			},
		},
		{
			[]float64{-1, 1},
			[]check{
				{Linear, 0, -1},
				{Linear, 0.25, -0.5},
				{Linear, 0.5, 0},
				{Linear, 0.75, 0.5},
				{Linear, 0.9999, 1},
			},
		},
		{
			[]float64{-1, 1, -2},
			[]check{
				{Linear, 0, -1},
				{Linear, 0.25, 0},
				{Linear, 0.5, 1},
				{Linear, 0.75, -0.5},
				{Linear, 0.9, -1.4},
				{Linear, 0.9999, -2},
			},
		},
		{
			[]float64{-1, 1},
			[]check{
				{Sinusoidal, 0, -1},
				{Sinusoidal, 0.25, -math.Sqrt2 / 2},
				{Sinusoidal, 0.5, 0},
				{Sinusoidal, 0.75, math.Sqrt2 / 2},
				{Sinusoidal, 0.9999, 1},
			},
		},
		{
			[]float64{-1, 1, -2},
			[]check{
				{Sinusoidal, 0, -1},
				{Sinusoidal, 1. / 8, -math.Sqrt2 / 2},
				{Sinusoidal, 2. / 8, 0},
				{Sinusoidal, 3. / 8, math.Sqrt2 / 2},
				{Sinusoidal, 4. / 8, 1},
				{Sinusoidal, 5. / 8, (3*math.Sqrt2 - 2) / 4},
				{Sinusoidal, 6. / 8, -0.5},
				{Sinusoidal, 7. / 8, (-3*math.Sqrt2 - 2) / 4},
				{Sinusoidal, 0.9999, -2},
			},
		},
	}
	for i, suite := range data {
		for j, test := range suite.checks {
			e, g := test.y, Envelope(test.x, suite.points, test.approx)
			d := math.Abs(e - g)
			if d > prec {
				t.Errorf(
					"i %d, j %d, x %v, e %v, g %v, d %v, prec %v",
					i, j, test.x, e, g, d, prec,
				)
			}
		}
	}
}

func TestMaxInt(t *testing.T) {
	n := int64(MaxInt)
	if n != math.MaxInt32 && n != math.MaxInt64 {
		t.Error(n)
	}

	t.Logf("64 bit ints: %t, MaxInt: %d", n == math.MaxInt64, n)
}

func TestMinInt(t *testing.T) {
	n := int64(MinInt)
	if n != math.MinInt32 && n != math.MinInt64 {
		t.Error(n)
	}

	t.Logf("64 bit ints: %t. MinInt: %d", n == math.MinInt64, n)
}

func TestMaxUint(t *testing.T) {
	n := uint64(MaxUint)
	if n != math.MaxUint32 && n != math.MaxUint64 {
		t.Error(n)
	}

	t.Logf("64 bit uints: %t. MaxUint: %d", n == math.MaxUint64, n)
}

func TestMax(t *testing.T) {
	tests := []struct{ a, b, e int }{
		{MinInt, MinIntM1, MaxInt},
		{MinIntM1, MinInt, MaxInt},
		{MinIntM1, MinIntM1, MaxInt},

		{MinInt, MinInt, MinInt},
		{MinInt + 1, MinInt, MinInt + 1},
		{MinInt, MinInt + 1, MinInt + 1},

		{-1, -1, -1},
		{-1, 0, 0},
		{-1, 1, 1},

		{0, -1, 0},
		{0, 0, 0},
		{0, 1, 1},

		{1, -1, 1},
		{1, 0, 1},
		{1, 1, 1},

		{MaxInt, MaxInt, MaxInt},
		{MaxInt - 1, MaxInt, MaxInt},
		{MaxInt, MaxInt - 1, MaxInt},

		{MaxIntP1, MaxInt, MaxInt},
		{MaxInt, MaxIntP1, MaxInt},
		{MaxIntP1, MaxIntP1, MinInt},
	}

	for _, test := range tests {
		if g, e := Max(test.a, test.b), test.e; g != e {
			t.Fatal(test.a, test.b, g, e)
		}
	}
}

func TestMin(t *testing.T) {
	tests := []struct{ a, b, e int }{
		{MinIntM1, MinInt, MinInt},
		{MinInt, MinIntM1, MinInt},
		{MinIntM1, MinIntM1, MaxInt},

		{MinInt, MinInt, MinInt},
		{MinInt + 1, MinInt, MinInt},
		{MinInt, MinInt + 1, MinInt},

		{-1, -1, -1},
		{-1, 0, -1},
		{-1, 1, -1},

		{0, -1, -1},
		{0, 0, 0},
		{0, 1, 0},

		{1, -1, -1},
		{1, 0, 0},
		{1, 1, 1},

		{MaxInt, MaxInt, MaxInt},
		{MaxInt - 1, MaxInt, MaxInt - 1},
		{MaxInt, MaxInt - 1, MaxInt - 1},

		{MaxIntP1, MaxInt, MinInt},
		{MaxInt, MaxIntP1, MinInt},
		{MaxIntP1, MaxIntP1, MinInt},
	}

	for _, test := range tests {
		if g, e := Min(test.a, test.b), test.e; g != e {
			t.Fatal(test.a, test.b, g, e)
		}
	}
}

func TestMaxPtr(t *testing.T) {
	tests := []struct{ a, b, e *int }{
		{intPtr(MinInt), intPtr(MinIntM1), intPtr(MaxInt)},
		{intPtr(MinIntM1), intPtr(MinInt), intPtr(MaxInt)},
		{intPtr(MinIntM1), intPtr(MinIntM1), intPtr(MaxInt)},
		{nil, intPtr(MinIntM1), intPtr(MaxInt)},

		{intPtr(MinInt), intPtr(MinInt), intPtr(MinInt)},
		{intPtr(MinInt + 1), intPtr(MinInt), intPtr(MinInt + 1)},
		{intPtr(MinInt), intPtr(MinInt + 1), intPtr(MinInt + 1)},
		{nil, intPtr(MinInt + 1), intPtr(MinInt + 1)},

		{intPtr(-1), intPtr(-1), intPtr(-1)},
		{intPtr(-1), intPtr(0), intPtr(0)},
		{intPtr(-1), intPtr(1), intPtr(1)},
		{intPtr(-1), nil, intPtr(-1)},

		{intPtr(0), intPtr(-1), intPtr(0)},
		{intPtr(0), intPtr(0), intPtr(0)},
		{intPtr(0), intPtr(1), intPtr(1)},
		{intPtr(0), nil, intPtr(0)},

		{intPtr(1), intPtr(-1), intPtr(1)},
		{intPtr(1), intPtr(0), intPtr(1)},
		{intPtr(1), intPtr(1), intPtr(1)},
		{nil, nil, nil},

		{intPtr(MaxInt), intPtr(MaxInt), intPtr(MaxInt)},
		{intPtr(MaxInt - 1), intPtr(MaxInt), intPtr(MaxInt)},
		{intPtr(MaxInt), intPtr(MaxInt - 1), intPtr(MaxInt)},
		{intPtr(MaxInt), nil, intPtr(MaxInt)},

		{intPtr(MaxIntP1), intPtr(MaxInt), intPtr(MaxInt)},
		{intPtr(MaxInt), intPtr(MaxIntP1), intPtr(MaxInt)},
		{intPtr(MaxIntP1), intPtr(MaxIntP1), intPtr(MinInt)},
		{nil, intPtr(MaxIntP1), intPtr(MinInt)},
	}

	for c, test := range tests {
		g, e := MaxPtr(test.a, test.b), test.e
		if e != nil {
			if *g != *e {
				t.Fatal(*test.a, *test.b, *g, *e, c)
			}
		} else {
			if e != g {
				t.Fatal(*test.a, *test.b, *g, e, c)
			}
		}
	}
}

func TestMinPtr(t *testing.T) {
	tests := []struct{ a, b, e *int }{
		{intPtr(MinIntM1), intPtr(MinInt), intPtr(MinInt)},
		{intPtr(MinInt), intPtr(MinIntM1), intPtr(MinInt)},
		{intPtr(MinIntM1), intPtr(MinIntM1), intPtr(MaxInt)},
		{nil, intPtr(MinIntM1), intPtr(MinIntM1)},

		{intPtr(MinInt), intPtr(MinInt), intPtr(MinInt)},
		{intPtr(MinInt + 1), intPtr(MinInt), intPtr(MinInt)},
		{intPtr(MinInt), intPtr(MinInt + 1), intPtr(MinInt)},
		{nil, intPtr(MinInt + 1), intPtr(MinInt + 1)},

		{intPtr(-1), intPtr(-1), intPtr(-1)},
		{intPtr(-1), intPtr(0), intPtr(-1)},
		{intPtr(-1), intPtr(1), intPtr(-1)},
		{intPtr(-1), nil, intPtr(-1)},

		{intPtr(0), intPtr(-1), intPtr(-1)},
		{intPtr(0), intPtr(0), intPtr(0)},
		{intPtr(0), intPtr(1), intPtr(0)},
		{intPtr(0), nil, intPtr(0)},

		{intPtr(1), intPtr(-1), intPtr(-1)},
		{intPtr(1), intPtr(0), intPtr(0)},
		{intPtr(1), intPtr(1), intPtr(1)},
		{nil, nil, nil},

		{intPtr(MaxInt), intPtr(MaxInt), intPtr(MaxInt)},
		{intPtr(MaxInt - 1), intPtr(MaxInt), intPtr(MaxInt - 1)},
		{intPtr(MaxInt), intPtr(MaxInt - 1), intPtr(MaxInt - 1)},
		{intPtr(MaxInt), nil, intPtr(MaxInt)},

		{intPtr(MaxIntP1), intPtr(MaxInt), intPtr(MinInt)},
		{intPtr(MaxInt), intPtr(MaxIntP1), intPtr(MinInt)},
		{intPtr(MaxIntP1), intPtr(MaxIntP1), intPtr(MinInt)},
		{nil, intPtr(MaxIntP1), intPtr(MinInt)},
	}

	for c, test := range tests {
		g, e := MinPtr(test.a, test.b), test.e
		if e != nil {
			if *g != *e {
				t.Fatal(*test.a, *test.b, *g, *e, c)
			}
		} else {
			if e != g {
				t.Fatal(*test.a, *test.b, *g, e, c)
			}
		}
	}
}

func TestMaxVal(t *testing.T) {
	tests := []struct{ a, b, c, e int }{
		{MinInt, MinInt, MinIntM1, MaxInt},
		{MinIntM1, MinIntM1, MinInt, MaxInt},
		{MinIntM1, MinIntM1, MinIntM1, MaxInt},

		{MinInt, MinInt, MinInt, MinInt},
		{MinInt + 1, MinInt + 1, MinInt, MinInt + 1},
		{MinInt, MinInt, MinInt + 1, MinInt + 1},

		{-1, -1, -1, -1},
		{-1, -1, 0, 0},
		{-1, -1, 1, 1},

		{0, 0, -1, 0},
		{0, 0, 0, 0},
		{0, 0, 1, 1},

		{1, 1, -1, 1},
		{1, 1, 0, 1},
		{1, 1, 1, 1},

		{MaxInt, MaxInt, MaxInt, MaxInt},
		{MaxInt - 1, MaxInt - 1, MaxInt, MaxInt},
		{MaxInt, MaxInt, MaxInt - 1, MaxInt},

		{MaxIntP1, MaxIntP1, MaxInt, MaxInt},
		{MaxInt, MaxInt, MaxIntP1, MaxInt},
		{MaxIntP1, MaxIntP1, MaxIntP1, MinInt},
	}

	for i, test := range tests {
		if g, e := MaxVal(test.a, test.b, test.c), test.e; g != e {
			t.Fatal(i, test.a, test.b, test.c, g, e)
		}
		if g, e := MaxVal(test.a), test.a; g != e {
			t.Fatal(i, test.a, g, e)
		}
	}
}

func TestMinVal(t *testing.T) {
	tests := []struct{ a, b, c, e int }{
		{MinIntM1, MinIntM1, MinInt, MinInt},
		{MinInt, MinInt, MinIntM1, MinInt},
		{MinIntM1, MinIntM1, MinIntM1, MaxInt},

		{MinInt, MinInt, MinInt, MinInt},
		{MinInt + 1, MinInt + 1, MinInt, MinInt},
		{MinInt, MinInt, MinInt + 1, MinInt},

		{-1, -1, -1, -1},
		{-1, -1, 0, -1},
		{-1, -1, 1, -1},

		{0, 0, -1, -1},
		{0, 0, 0, 0},
		{0, 0, 1, 0},

		{1, 1, -1, -1},
		{1, 1, 0, 0},
		{1, 1, 1, 1},

		{MaxInt, MaxInt, MaxInt, MaxInt},
		{MaxInt - 1, MaxInt - 1, MaxInt, MaxInt - 1},
		{MaxInt, MaxInt, MaxInt - 1, MaxInt - 1},

		{MaxIntP1, MaxIntP1, MaxInt, MinInt},
		{MaxInt, MaxInt, MaxIntP1, MinInt},
		{MaxIntP1, MaxIntP1, MaxIntP1, MinInt},
	}

	for i, test := range tests {
		if g, e := MinVal(test.a, test.b, test.c), test.e; g != e {
			t.Fatal(i, test.a, test.b, test.c, g, e)
		}
		if g, e := MinVal(test.a), test.a; g != e {
			t.Fatal(i, test.a, g, e)
		}
	}
}

func TestClamp(t *testing.T) {
	tests := []struct{ v, lo, hi, e int }{
		{0, 0, 0, 0},
		{5, 10, 20, 10},
		{10, 10, 20, 10},
		{15, 10, 20, 15},
		{20, 10, 20, 20},
		{25, 10, 20, 20},
	}

	for _, test := range tests {
		if g, e := Clamp(test.v, test.lo, test.hi), test.e; g != e {
			t.Fatal(test.v, test.lo, test.hi, g, e)
		}
	}
}

func TestUMax(t *testing.T) {
	tests := []struct{ a, b, e uint }{
		{0, 0, 0},
		{0, 1, 1},
		{1, 0, 1},

		{10, 10, 10},
		{10, 11, 11},
		{11, 10, 11},
		{11, 11, 11},

		{MaxUint, MaxUint, MaxUint},
		{MaxUint, MaxUint - 1, MaxUint},
		{MaxUint - 1, MaxUint, MaxUint},
		{MaxUint - 1, MaxUint - 1, MaxUint - 1},

		{MaxUint, MaxUintP1, MaxUint},
		{MaxUintP1, MaxUint, MaxUint},
		{MaxUintP1, MaxUintP1, 0},
	}

	for _, test := range tests {
		if g, e := UMax(test.a, test.b), test.e; g != e {
			t.Fatal(test.a, test.b, g, e)
		}
	}
}

func TestUMin(t *testing.T) {
	tests := []struct{ a, b, e uint }{
		{0, 0, 0},
		{0, 1, 0},
		{1, 0, 0},

		{10, 10, 10},
		{10, 11, 10},
		{11, 10, 10},
		{11, 11, 11},

		{MaxUint, MaxUint, MaxUint},
		{MaxUint, MaxUint - 1, MaxUint - 1},
		{MaxUint - 1, MaxUint, MaxUint - 1},
		{MaxUint - 1, MaxUint - 1, MaxUint - 1},

		{MaxUint, MaxUintP1, 0},
		{MaxUintP1, MaxUint, 0},
		{MaxUintP1, MaxUintP1, 0},
	}

	for _, test := range tests {
		if g, e := UMin(test.a, test.b), test.e; g != e {
			t.Fatal(test.a, test.b, g, e)
		}
	}
}

func TestUMaxPtr(t *testing.T) {
	tests := []struct{ a, b, e *uint }{
		{uIntPtr(0), uIntPtr(0), uIntPtr(0)},
		{uIntPtr(0), uIntPtr(1), uIntPtr(1)},
		{uIntPtr(1), uIntPtr(0), uIntPtr(1)},
		{nil, uIntPtr(1), uIntPtr(1)},
		{nil, uIntPtr(0), uIntPtr(0)},

		{uIntPtr(10), uIntPtr(10), uIntPtr(10)},
		{uIntPtr(10), uIntPtr(11), uIntPtr(11)},
		{uIntPtr(11), uIntPtr(10), uIntPtr(11)},
		{uIntPtr(11), uIntPtr(11), uIntPtr(11)},
		{nil, nil, nil},

		{uIntPtr(MaxUint), uIntPtr(MaxUint), uIntPtr(MaxUint)},
		{uIntPtr(MaxUint), uIntPtr(MaxUint - 1), uIntPtr(MaxUint)},
		{uIntPtr(MaxUint - 1), nil, uIntPtr(MaxUint - 1)},
		{nil, uIntPtr(MaxUint - 1), uIntPtr(MaxUint - 1)},
		{uIntPtr(MaxUint - 1), nil, uIntPtr(MaxUint - 1)},

		{uIntPtr(MaxUint), uIntPtr(MaxUintP1), uIntPtr(MaxUint)},
		{uIntPtr(MaxUintP1), uIntPtr(MaxUint), uIntPtr(MaxUint)},
		{uIntPtr(MaxUintP1), uIntPtr(MaxUintP1), uIntPtr(0)},
		{uIntPtr(MaxUintP1), nil, uIntPtr(0)},
	}

	for c, test := range tests {
		g, e := UMaxPtr(test.a, test.b), test.e
		if e != nil {
			if *g != *e {
				t.Fatal(*test.a, *test.b, *g, *e, c)
			}
		} else {
			if e != g {
				t.Fatal(*test.a, *test.b, *g, e, c)
			}
		}
	}
}

func TestUMinPtr(t *testing.T) {
	tests := []struct{ a, b, e *uint }{
		{uIntPtr(0), uIntPtr(0), uIntPtr(0)},
		{uIntPtr(0), uIntPtr(1), uIntPtr(0)},
		{uIntPtr(1), uIntPtr(0), uIntPtr(0)},
		{nil, uIntPtr(1), uIntPtr(1)},
		{nil, uIntPtr(0), uIntPtr(0)},

		{uIntPtr(10), uIntPtr(10), uIntPtr(10)},
		{uIntPtr(10), uIntPtr(11), uIntPtr(10)},
		{uIntPtr(11), uIntPtr(10), uIntPtr(10)},
		{uIntPtr(11), uIntPtr(11), uIntPtr(11)},
		{nil, nil, nil},

		{uIntPtr(MaxUint), uIntPtr(MaxUint), uIntPtr(MaxUint)},
		{uIntPtr(MaxUint), uIntPtr(MaxUint - 1), uIntPtr(MaxUint - 1)},
		{uIntPtr(MaxUint - 1), nil, uIntPtr(MaxUint - 1)},
		{nil, uIntPtr(MaxUint - 1), uIntPtr(MaxUint - 1)},
		{uIntPtr(MaxUint - 1), nil, uIntPtr(MaxUint - 1)},

		{uIntPtr(MaxUint), uIntPtr(MaxUintP1), uIntPtr(0)},
		{uIntPtr(MaxUintP1), uIntPtr(MaxUint), uIntPtr(0)},
		{uIntPtr(MaxUintP1), uIntPtr(MaxUintP1), uIntPtr(0)},
		{uIntPtr(MaxUintP1), nil, uIntPtr(0)},
	}

	for c, test := range tests {
		g, e := UMinPtr(test.a, test.b), test.e
		if e != nil {
			if *g != *e {
				t.Fatal(*test.a, *test.b, *g, *e, c)
			}
		} else {
			if e != g {
				t.Fatal(*test.a, *test.b, *g, e, c)
			}
		}
	}
}

func TestUMaxVal(t *testing.T) {
	tests := []struct{ a, b, c, e uint }{
		{0, 0, 0, 0},
		{0, 0, 1, 1},
		{1, 1, 0, 1},

		{10, 10, 10, 10},
		{10, 10, 11, 11},
		{11, 11, 10, 11},
		{11, 11, 11, 11},

		{MaxUint, MaxUint, MaxUint, MaxUint},
		{MaxUint, MaxUint, MaxUint - 1, MaxUint},
		{MaxUint - 1, MaxUint - 1, MaxUint, MaxUint},
		{MaxUint - 1, MaxUint - 1, MaxUint - 1, MaxUint - 1},

		{MaxUint, MaxUint, MaxUintP1, MaxUint},
		{MaxUintP1, MaxUintP1, MaxUint, MaxUint},
		{MaxUintP1, MaxUintP1, MaxUintP1, 0},
	}

	for i, test := range tests {
		if g, e := UMaxVal(test.a, test.b, test.c), test.e; g != e {
			t.Fatal(i, test.a, test.b, test.c, g, e)
		}
		if g, e := UMaxVal(test.a), test.a; g != e {
			t.Fatal(i, test.a, g, e)
		}
	}
}

func TestUMinVal(t *testing.T) {
	tests := []struct{ a, b, c, e uint }{
		{0, 0, 0, 0},
		{0, 0, 1, 0},
		{1, 1, 0, 0},

		{10, 10, 10, 10},
		{10, 10, 11, 10},
		{11, 11, 10, 10},
		{11, 11, 11, 11},

		{MaxUint, MaxUint, MaxUint, MaxUint},
		{MaxUint, MaxUint, MaxUint - 1, MaxUint - 1},
		{MaxUint - 1, MaxUint - 1, MaxUint, MaxUint - 1},
		{MaxUint - 1, MaxUint - 1, MaxUint - 1, MaxUint - 1},

		{MaxUint, MaxUint, MaxUintP1, 0},
		{MaxUintP1, MaxUintP1, MaxUint, 0},
		{MaxUintP1, MaxUintP1, MaxUintP1, 0},
	}

	for i, test := range tests {
		if g, e := UMinVal(test.a, test.b, test.c), test.e; g != e {
			t.Fatal(i, test.a, test.b, test.c, g, e)
		}
		if g, e := UMinVal(test.a), test.a; g != e {
			t.Fatal(i, test.a, g, e)
		}
	}
}

func TestUClamp(t *testing.T) {
	tests := []struct{ v, lo, hi, e uint }{
		{0, 0, 0, 0},
		{5, 10, 20, 10},
		{10, 10, 20, 10},
		{15, 10, 20, 15},
		{20, 10, 20, 20},
		{25, 10, 20, 20},
	}

	for _, test := range tests {
		if g, e := UClamp(test.v, test.lo, test.hi), test.e; g != e {
			t.Fatal(test.v, test.lo, test.hi, g, e)
		}
	}
}

func TestMaxByte(t *testing.T) {
	tests := []struct{ a, b, e byte }{
		{0, 0, 0},
		{0, 1, 1},
		{1, 0, 1},

		{10, 10, 10},
		{10, 11, 11},
		{11, 10, 11},
		{11, 11, 11},

		{math.MaxUint8, math.MaxUint8, math.MaxUint8},
		{math.MaxUint8, math.MaxUint8 - 1, math.MaxUint8},
		{math.MaxUint8 - 1, math.MaxUint8, math.MaxUint8},
		{math.MaxUint8 - 1, math.MaxUint8 - 1, math.MaxUint8 - 1},
	}

	for _, test := range tests {
		if g, e := MaxByte(test.a, test.b), test.e; g != e {
			t.Fatal(test.a, test.b, g, e)
		}
	}
}

func TestMinByte(t *testing.T) {
	tests := []struct{ a, b, e byte }{
		{0, 0, 0},
		{0, 1, 0},
		{1, 0, 0},

		{10, 10, 10},
		{10, 11, 10},
		{11, 10, 10},
		{11, 11, 11},

		{math.MaxUint8, math.MaxUint8, math.MaxUint8},
		{math.MaxUint8, math.MaxUint8 - 1, math.MaxUint8 - 1},
		{math.MaxUint8 - 1, math.MaxUint8, math.MaxUint8 - 1},
		{math.MaxUint8 - 1, math.MaxUint8 - 1, math.MaxUint8 - 1},
	}

	for _, test := range tests {
		if g, e := MinByte(test.a, test.b), test.e; g != e {
			t.Fatal(test.a, test.b, g, e)
		}
	}
}

func TestMaxBytePtr(t *testing.T) {
	tests := []struct{ a, b, e *byte }{
		{bytePtr(0), bytePtr(0), bytePtr(0)},
		{bytePtr(0), bytePtr(1), bytePtr(1)},
		{bytePtr(1), bytePtr(0), bytePtr(1)},
		{nil, bytePtr(0), bytePtr(0)},
		{bytePtr(1), nil, bytePtr(1)},

		{bytePtr(10), bytePtr(10), bytePtr(10)},
		{bytePtr(10), bytePtr(11), bytePtr(11)},
		{bytePtr(11), bytePtr(10), bytePtr(11)},
		{bytePtr(11), bytePtr(11), bytePtr(11)},
		{nil, nil, nil},

		{bytePtr(math.MaxUint8), bytePtr(math.MaxUint8), bytePtr(math.MaxUint8)},
		{bytePtr(math.MaxUint8), bytePtr(math.MaxUint8 - 1), bytePtr(math.MaxUint8)},
		{bytePtr(math.MaxUint8 - 1), bytePtr(math.MaxUint8), bytePtr(math.MaxUint8)},
		{bytePtr(math.MaxUint8 - 1), bytePtr(math.MaxUint8 - 1), bytePtr(math.MaxUint8 - 1)},
		{nil, bytePtr(math.MaxUint8 - 1), bytePtr(math.MaxUint8 - 1)},
		{bytePtr(math.MaxUint8), nil, bytePtr(math.MaxUint8)},
	}

	for c, test := range tests {
		g, e := MaxBytePtr(test.a, test.b), test.e
		if e != nil {
			if *g != *e {
				t.Fatal(*test.a, *test.b, *g, *e, c)
			}
		} else {
			if e != g {
				t.Fatal(*test.a, *test.b, *g, e, c)
			}
		}
	}
}

func TestMinBytePtr(t *testing.T) {
	tests := []struct{ a, b, e *byte }{
		{bytePtr(0), bytePtr(0), bytePtr(0)},
		{bytePtr(0), bytePtr(1), bytePtr(0)},
		{bytePtr(1), bytePtr(0), bytePtr(0)},
		{nil, bytePtr(0), bytePtr(0)},
		{bytePtr(1), nil, bytePtr(1)},

		{bytePtr(10), bytePtr(10), bytePtr(10)},
		{bytePtr(10), bytePtr(11), bytePtr(10)},
		{bytePtr(11), bytePtr(10), bytePtr(10)},
		{bytePtr(11), bytePtr(11), bytePtr(11)},
		{nil, nil, nil},

		{bytePtr(math.MaxUint8), bytePtr(math.MaxUint8), bytePtr(math.MaxUint8)},
		{bytePtr(math.MaxUint8), bytePtr(math.MaxUint8 - 1), bytePtr(math.MaxUint8 - 1)},
		{bytePtr(math.MaxUint8 - 1), bytePtr(math.MaxUint8), bytePtr(math.MaxUint8 - 1)},
		{bytePtr(math.MaxUint8 - 1), bytePtr(math.MaxUint8 - 1), bytePtr(math.MaxUint8 - 1)},
		{nil, bytePtr(math.MaxUint8 - 1), bytePtr(math.MaxUint8 - 1)},
		{bytePtr(math.MaxUint8), nil, bytePtr(math.MaxUint8)},
	}

	for c, test := range tests {
		g, e := MinBytePtr(test.a, test.b), test.e
		if e != nil {
			if *g != *e {
				t.Fatal(*test.a, *test.b, *g, *e, c)
			}
		} else {
			if e != g {
				t.Fatal(*test.a, *test.b, *g, e, c)
			}
		}
	}
}

func TestMaxByteVal(t *testing.T) {
	tests := []struct{ a, b, c, e byte }{
		{0, 0, 0, 0},
		{0, 0, 1, 1},
		{1, 1, 0, 1},

		{10, 10, 10, 10},
		{10, 10, 11, 11},
		{11, 11, 10, 11},
		{11, 11, 11, 11},

		{math.MaxUint8, math.MaxUint8, math.MaxUint8, math.MaxUint8},
		{math.MaxUint8, math.MaxUint8, math.MaxUint8 - 1, math.MaxUint8},
		{math.MaxUint8 - 1, math.MaxUint8 - 1, math.MaxUint8, math.MaxUint8},
		{math.MaxUint8 - 1, math.MaxUint8 - 1, math.MaxUint8 - 1, math.MaxUint8 - 1},
	}

	for i, test := range tests {
		if g, e := MaxByteVal(test.a, test.b, test.c), test.e; g != e {
			t.Fatal(i, test.a, test.b, test.c, g, e)
		}
		if g, e := MaxByteVal(test.a), test.a; g != e {
			t.Fatal(i, test.a, g, e)
		}
	}
}

func TestMinByteVal(t *testing.T) {
	tests := []struct{ a, b, c, e byte }{
		{0, 0, 0, 0},
		{0, 0, 1, 0},
		{1, 1, 0, 0},

		{10, 10, 10, 10},
		{10, 10, 11, 10},
		{11, 11, 10, 10},
		{11, 11, 11, 11},

		{math.MaxUint8, math.MaxUint8, math.MaxUint8, math.MaxUint8},
		{math.MaxUint8, math.MaxUint8, math.MaxUint8 - 1, math.MaxUint8 - 1},
		{math.MaxUint8 - 1, math.MaxUint8 - 1, math.MaxUint8, math.MaxUint8 - 1},
		{math.MaxUint8 - 1, math.MaxUint8 - 1, math.MaxUint8 - 1, math.MaxUint8 - 1},
	}

	for i, test := range tests {
		if g, e := MinByteVal(test.a, test.b, test.c), test.e; g != e {
			t.Fatal(i, test.a, test.b, test.c, g, e)
		}
		if g, e := MinByteVal(test.a), test.a; g != e {
			t.Fatal(i, test.a, g, e)
		}
	}
}

func TestClampByte(t *testing.T) {
	tests := []struct{ v, lo, hi, e byte }{
		{0, 0, 0, 0},
		{5, 10, 20, 10},
		{10, 10, 20, 10},
		{15, 10, 20, 15},
		{20, 10, 20, 20},
		{25, 10, 20, 20},
	}

	for _, test := range tests {
		if g, e := ClampByte(test.v, test.lo, test.hi), test.e; g != e {
			t.Fatal(test.v, test.lo, test.hi, g, e)
		}
	}
}

func TestMaxUint16(t *testing.T) {
	tests := []struct{ a, b, e uint16 }{
		{0, 0, 0},
		{0, 1, 1},
		{1, 0, 1},

		{10, 10, 10},
		{10, 11, 11},
		{11, 10, 11},
		{11, 11, 11},

		{math.MaxUint16, math.MaxUint16, math.MaxUint16},
		{math.MaxUint16, math.MaxUint16 - 1, math.MaxUint16},
		{math.MaxUint16 - 1, math.MaxUint16, math.MaxUint16},
		{math.MaxUint16 - 1, math.MaxUint16 - 1, math.MaxUint16 - 1},
	}

	for _, test := range tests {
		if g, e := MaxUint16(test.a, test.b), test.e; g != e {
			t.Fatal(test.a, test.b, g, e)
		}
	}
}

func TestMinUint16(t *testing.T) {
	tests := []struct{ a, b, e uint16 }{
		{0, 0, 0},
		{0, 1, 0},
		{1, 0, 0},

		{10, 10, 10},
		{10, 11, 10},
		{11, 10, 10},
		{11, 11, 11},

		{math.MaxUint16, math.MaxUint16, math.MaxUint16},
		{math.MaxUint16, math.MaxUint16 - 1, math.MaxUint16 - 1},
		{math.MaxUint16 - 1, math.MaxUint16, math.MaxUint16 - 1},
		{math.MaxUint16 - 1, math.MaxUint16 - 1, math.MaxUint16 - 1},
	}

	for _, test := range tests {
		if g, e := MinUint16(test.a, test.b), test.e; g != e {
			t.Fatal(test.a, test.b, g, e)
		}
	}
}

func TestMaxUint16Ptr(t *testing.T) {
	tests := []struct{ a, b, e *uint16 }{
		{uInt16Ptr(0), uInt16Ptr(0), uInt16Ptr(0)},
		{uInt16Ptr(0), uInt16Ptr(1), uInt16Ptr(1)},
		{uInt16Ptr(1), uInt16Ptr(0), uInt16Ptr(1)},
		{nil, uInt16Ptr(0), uInt16Ptr(0)},
		{uInt16Ptr(1), nil, uInt16Ptr(1)},

		{uInt16Ptr(10), uInt16Ptr(10), uInt16Ptr(10)},
		{uInt16Ptr(10), uInt16Ptr(11), uInt16Ptr(11)},
		{uInt16Ptr(11), uInt16Ptr(10), uInt16Ptr(11)},
		{uInt16Ptr(11), uInt16Ptr(11), uInt16Ptr(11)},
		{nil, nil, nil},

		{uInt16Ptr(math.MaxUint16), uInt16Ptr(math.MaxUint16), uInt16Ptr(math.MaxUint16)},
		{uInt16Ptr(math.MaxUint16), uInt16Ptr(math.MaxUint16 - 1), uInt16Ptr(math.MaxUint16)},
		{uInt16Ptr(math.MaxUint16 - 1), uInt16Ptr(math.MaxUint16), uInt16Ptr(math.MaxUint16)},
		{uInt16Ptr(math.MaxUint16 - 1), uInt16Ptr(math.MaxUint16 - 1), uInt16Ptr(math.MaxUint16 - 1)},
		{nil, uInt16Ptr(math.MaxUint16 - 1), uInt16Ptr(math.MaxUint16 - 1)},
		{uInt16Ptr(math.MaxUint16), nil, uInt16Ptr(math.MaxUint16)},
	}

	for c, test := range tests {
		g, e := MaxUint16Ptr(test.a, test.b), test.e
		if e != nil {
			if *g != *e {
				t.Fatal(*test.a, *test.b, *g, *e, c)
			}
		} else {
			if e != g {
				t.Fatal(*test.a, *test.b, *g, e, c)
			}
		}
	}
}

func TestMinUint16Ptr(t *testing.T) {
	tests := []struct{ a, b, e *uint16 }{
		{uInt16Ptr(0), uInt16Ptr(0), uInt16Ptr(0)},
		{uInt16Ptr(0), uInt16Ptr(1), uInt16Ptr(0)},
		{uInt16Ptr(1), uInt16Ptr(0), uInt16Ptr(0)},
		{nil, uInt16Ptr(0), uInt16Ptr(0)},
		{uInt16Ptr(1), nil, uInt16Ptr(1)},

		{uInt16Ptr(10), uInt16Ptr(10), uInt16Ptr(10)},
		{uInt16Ptr(10), uInt16Ptr(11), uInt16Ptr(10)},
		{uInt16Ptr(11), uInt16Ptr(10), uInt16Ptr(10)},
		{uInt16Ptr(11), uInt16Ptr(11), uInt16Ptr(11)},
		{nil, nil, nil},

		{uInt16Ptr(math.MaxUint16), uInt16Ptr(math.MaxUint16), uInt16Ptr(math.MaxUint16)},
		{uInt16Ptr(math.MaxUint16), uInt16Ptr(math.MaxUint16 - 1), uInt16Ptr(math.MaxUint16 - 1)},
		{uInt16Ptr(math.MaxUint16 - 1), uInt16Ptr(math.MaxUint16), uInt16Ptr(math.MaxUint16 - 1)},
		{uInt16Ptr(math.MaxUint16 - 1), uInt16Ptr(math.MaxUint16 - 1), uInt16Ptr(math.MaxUint16 - 1)},
		{nil, uInt16Ptr(math.MaxUint16 - 1), uInt16Ptr(math.MaxUint16 - 1)},
		{uInt16Ptr(math.MaxUint16), nil, uInt16Ptr(math.MaxUint16)},
	}

	for c, test := range tests {
		g, e := MinUint16Ptr(test.a, test.b), test.e
		if e != nil {
			if *g != *e {
				t.Fatal(*test.a, *test.b, *g, *e, c)
			}
		} else {
			if e != g {
				t.Fatal(*test.a, *test.b, *g, e, c)
			}
		}
	}
}

func TestMaxUint16Val(t *testing.T) {
	tests := []struct{ a, b, c, e uint16 }{
		{0, 0, 0, 0},
		{0, 0, 1, 1},
		{1, 1, 0, 1},

		{10, 10, 10, 10},
		{10, 10, 11, 11},
		{11, 11, 10, 11},
		{11, 11, 11, 11},

		{math.MaxUint16, math.MaxUint16, math.MaxUint16, math.MaxUint16},
		{math.MaxUint16, math.MaxUint16, math.MaxUint16 - 1, math.MaxUint16},
		{math.MaxUint16 - 1, math.MaxUint16 - 1, math.MaxUint16, math.MaxUint16},
		{math.MaxUint16 - 1, math.MaxUint16 - 1, math.MaxUint16 - 1, math.MaxUint16 - 1},
	}

	for i, test := range tests {
		if g, e := MaxUint16Val(test.a, test.b, test.c), test.e; g != e {
			t.Fatal(i, test.a, test.b, test.c, g, e)
		}
		if g, e := MaxUint16Val(test.a), test.a; g != e {
			t.Fatal(i, test.a, g, e)
		}
	}
}

func TestMinUint16Val(t *testing.T) {
	tests := []struct{ a, b, c, e uint16 }{
		{0, 0, 0, 0},
		{0, 0, 1, 0},
		{1, 1, 0, 0},

		{10, 10, 10, 10},
		{10, 10, 11, 10},
		{11, 11, 10, 10},
		{11, 11, 11, 11},

		{math.MaxUint16, math.MaxUint16, math.MaxUint16, math.MaxUint16},
		{math.MaxUint16, math.MaxUint16, math.MaxUint16 - 1, math.MaxUint16 - 1},
		{math.MaxUint16 - 1, math.MaxUint16 - 1, math.MaxUint16, math.MaxUint16 - 1},
		{math.MaxUint16 - 1, math.MaxUint16 - 1, math.MaxUint16 - 1, math.MaxUint16 - 1},
	}

	for i, test := range tests {
		if g, e := MinUint16Val(test.a, test.b, test.c), test.e; g != e {
			t.Fatal(i, test.a, test.b, test.c, g, e)
		}
		if g, e := MinUint16Val(test.a), test.a; g != e {
			t.Fatal(i, test.a, g, e)
		}
	}
}

func TestClampUint16(t *testing.T) {
	tests := []struct{ v, lo, hi, e uint16 }{
		{0, 0, 0, 0},
		{5, 10, 20, 10},
		{10, 10, 20, 10},
		{15, 10, 20, 15},
		{20, 10, 20, 20},
		{25, 10, 20, 20},
	}

	for _, test := range tests {
		if g, e := ClampUint16(test.v, test.lo, test.hi), test.e; g != e {
			t.Fatal(test.v, test.lo, test.hi, g, e)
		}
	}
}

func TestMaxUint32(t *testing.T) {
	tests := []struct{ a, b, e uint32 }{
		{0, 0, 0},
		{0, 1, 1},
		{1, 0, 1},

		{10, 10, 10},
		{10, 11, 11},
		{11, 10, 11},
		{11, 11, 11},

		{math.MaxUint32, math.MaxUint32, math.MaxUint32},
		{math.MaxUint32, math.MaxUint32 - 1, math.MaxUint32},
		{math.MaxUint32 - 1, math.MaxUint32, math.MaxUint32},
		{math.MaxUint32 - 1, math.MaxUint32 - 1, math.MaxUint32 - 1},
	}

	for _, test := range tests {
		if g, e := MaxUint32(test.a, test.b), test.e; g != e {
			t.Fatal(test.a, test.b, g, e)
		}
	}
}

func TestMinUint32(t *testing.T) {
	tests := []struct{ a, b, e uint32 }{
		{0, 0, 0},
		{0, 1, 0},
		{1, 0, 0},

		{10, 10, 10},
		{10, 11, 10},
		{11, 10, 10},
		{11, 11, 11},

		{math.MaxUint32, math.MaxUint32, math.MaxUint32},
		{math.MaxUint32, math.MaxUint32 - 1, math.MaxUint32 - 1},
		{math.MaxUint32 - 1, math.MaxUint32, math.MaxUint32 - 1},
		{math.MaxUint32 - 1, math.MaxUint32 - 1, math.MaxUint32 - 1},
	}

	for _, test := range tests {
		if g, e := MinUint32(test.a, test.b), test.e; g != e {
			t.Fatal(test.a, test.b, g, e)
		}
	}
}

func TestMaxUint32Ptr(t *testing.T) {
	tests := []struct{ a, b, e *uint32 }{
		{uInt32Ptr(0), uInt32Ptr(0), uInt32Ptr(0)},
		{uInt32Ptr(0), uInt32Ptr(1), uInt32Ptr(1)},
		{uInt32Ptr(1), uInt32Ptr(0), uInt32Ptr(1)},
		{nil, uInt32Ptr(0), uInt32Ptr(0)},
		{uInt32Ptr(1), nil, uInt32Ptr(1)},

		{uInt32Ptr(10), uInt32Ptr(10), uInt32Ptr(10)},
		{uInt32Ptr(10), uInt32Ptr(11), uInt32Ptr(11)},
		{uInt32Ptr(11), uInt32Ptr(10), uInt32Ptr(11)},
		{uInt32Ptr(11), uInt32Ptr(11), uInt32Ptr(11)},
		{nil, nil, nil},

		{uInt32Ptr(math.MaxInt32), uInt32Ptr(math.MaxInt32), uInt32Ptr(math.MaxInt32)},
		{uInt32Ptr(math.MaxInt32), uInt32Ptr(math.MaxInt32 - 1), uInt32Ptr(math.MaxInt32)},
		{uInt32Ptr(math.MaxInt32 - 1), uInt32Ptr(math.MaxInt32), uInt32Ptr(math.MaxInt32)},
		{uInt32Ptr(math.MaxInt32 - 1), uInt32Ptr(math.MaxInt32 - 1), uInt32Ptr(math.MaxInt32 - 1)},
		{nil, uInt32Ptr(math.MaxInt32 - 1), uInt32Ptr(math.MaxInt32 - 1)},
		{uInt32Ptr(math.MaxInt32), nil, uInt32Ptr(math.MaxInt32)},
	}

	for c, test := range tests {
		g, e := MaxUint32Ptr(test.a, test.b), test.e
		if e != nil {
			if *g != *e {
				t.Fatal(*test.a, *test.b, *g, *e, c)
			}
		} else {
			if e != g {
				t.Fatal(*test.a, *test.b, *g, e, c)
			}
		}
	}
}

func TestMinUint32Ptr(t *testing.T) {
	tests := []struct{ a, b, e *uint32 }{
		{uInt32Ptr(0), uInt32Ptr(0), uInt32Ptr(0)},
		{uInt32Ptr(0), uInt32Ptr(1), uInt32Ptr(0)},
		{uInt32Ptr(1), uInt32Ptr(0), uInt32Ptr(0)},
		{nil, uInt32Ptr(0), uInt32Ptr(0)},
		{uInt32Ptr(1), nil, uInt32Ptr(1)},

		{uInt32Ptr(10), uInt32Ptr(10), uInt32Ptr(10)},
		{uInt32Ptr(10), uInt32Ptr(11), uInt32Ptr(10)},
		{uInt32Ptr(11), uInt32Ptr(10), uInt32Ptr(10)},
		{uInt32Ptr(11), uInt32Ptr(11), uInt32Ptr(11)},
		{nil, nil, nil},

		{uInt32Ptr(math.MaxInt32), uInt32Ptr(math.MaxInt32), uInt32Ptr(math.MaxInt32)},
		{uInt32Ptr(math.MaxInt32), uInt32Ptr(math.MaxInt32 - 1), uInt32Ptr(math.MaxInt32 - 1)},
		{uInt32Ptr(math.MaxInt32 - 1), uInt32Ptr(math.MaxInt32), uInt32Ptr(math.MaxInt32 - 1)},
		{uInt32Ptr(math.MaxInt32 - 1), uInt32Ptr(math.MaxInt32 - 1), uInt32Ptr(math.MaxInt32 - 1)},
		{nil, uInt32Ptr(math.MaxInt32 - 1), uInt32Ptr(math.MaxInt32 - 1)},
		{uInt32Ptr(math.MaxInt32), nil, uInt32Ptr(math.MaxInt32)},
	}

	for c, test := range tests {
		g, e := MinUint32Ptr(test.a, test.b), test.e
		if e != nil {
			if *g != *e {
				t.Fatal(*test.a, *test.b, *g, *e, c)
			}
		} else {
			if e != g {
				t.Fatal(*test.a, *test.b, *g, e, c)
			}
		}
	}
}

func TestMaxUint32Val(t *testing.T) {
	tests := []struct{ a, b, c, e uint32 }{
		{0, 0, 0, 0},
		{0, 0, 1, 1},
		{1, 1, 0, 1},

		{10, 10, 10, 10},
		{10, 10, 11, 11},
		{11, 11, 10, 11},
		{11, 11, 11, 11},

		{math.MaxUint32, math.MaxUint32, math.MaxUint32, math.MaxUint32},
		{math.MaxUint32, math.MaxUint32, math.MaxUint32 - 1, math.MaxUint32},
		{math.MaxUint32 - 1, math.MaxUint32 - 1, math.MaxUint32, math.MaxUint32},
		{math.MaxUint32 - 1, math.MaxUint32 - 1, math.MaxUint32 - 1, math.MaxUint32 - 1},
	}

	for i, test := range tests {
		if g, e := MaxUint32Val(test.a, test.b, test.c), test.e; g != e {
			t.Fatal(i, test.a, test.b, test.c, g, e)
		}
		if g, e := MaxUint32Val(test.a), test.a; g != e {
			t.Fatal(i, test.a, g, e)
		}
	}
}

func TestMinUint32Val(t *testing.T) {
	tests := []struct{ a, b, c, e uint32 }{
		{0, 0, 0, 0},
		{0, 0, 1, 0},
		{1, 1, 0, 0},

		{10, 10, 10, 10},
		{10, 10, 11, 10},
		{11, 11, 10, 10},
		{11, 11, 11, 11},

		{math.MaxUint32, math.MaxUint32, math.MaxUint32, math.MaxUint32},
		{math.MaxUint32, math.MaxUint32, math.MaxUint32 - 1, math.MaxUint32 - 1},
		{math.MaxUint32 - 1, math.MaxUint32 - 1, math.MaxUint32, math.MaxUint32 - 1},
		{math.MaxUint32 - 1, math.MaxUint32 - 1, math.MaxUint32 - 1, math.MaxUint32 - 1},
	}

	for i, test := range tests {
		if g, e := MinUint32Val(test.a, test.b, test.c), test.e; g != e {
			t.Fatal(i, test.a, test.b, test.c, g, e)
		}
		if g, e := MinUint32Val(test.a), test.a; g != e {
			t.Fatal(i, test.a, g, e)
		}
	}
}

func TestClampUint32(t *testing.T) {
	tests := []struct{ v, lo, hi, e uint32 }{
		{0, 0, 0, 0},
		{5, 10, 20, 10},
		{10, 10, 20, 10},
		{15, 10, 20, 15},
		{20, 10, 20, 20},
		{25, 10, 20, 20},
	}

	for _, test := range tests {
		if g, e := ClampUint32(test.v, test.lo, test.hi), test.e; g != e {
			t.Fatal(test.v, test.lo, test.hi, g, e)
		}
	}
}

func TestMaxUint64(t *testing.T) {
	tests := []struct{ a, b, e uint64 }{
		{0, 0, 0},
		{0, 1, 1},
		{1, 0, 1},

		{10, 10, 10},
		{10, 11, 11},
		{11, 10, 11},
		{11, 11, 11},

		{math.MaxUint64, math.MaxUint64, math.MaxUint64},
		{math.MaxUint64, math.MaxUint64 - 1, math.MaxUint64},
		{math.MaxUint64 - 1, math.MaxUint64, math.MaxUint64},
		{math.MaxUint64 - 1, math.MaxUint64 - 1, math.MaxUint64 - 1},
	}

	for _, test := range tests {
		if g, e := MaxUint64(test.a, test.b), test.e; g != e {
			t.Fatal(test.a, test.b, g, e)
		}
	}
}

func TestMinUint64(t *testing.T) {
	tests := []struct{ a, b, e uint64 }{
		{0, 0, 0},
		{0, 1, 0},
		{1, 0, 0},

		{10, 10, 10},
		{10, 11, 10},
		{11, 10, 10},
		{11, 11, 11},

		{math.MaxUint64, math.MaxUint64, math.MaxUint64},
		{math.MaxUint64, math.MaxUint64 - 1, math.MaxUint64 - 1},
		{math.MaxUint64 - 1, math.MaxUint64, math.MaxUint64 - 1},
		{math.MaxUint64 - 1, math.MaxUint64 - 1, math.MaxUint64 - 1},
	}

	for _, test := range tests {
		if g, e := MinUint64(test.a, test.b), test.e; g != e {
			t.Fatal(test.a, test.b, g, e)
		}
	}
}

func TestMaxUint64Ptr(t *testing.T) {
	tests := []struct{ a, b, e *uint64 }{
		{uInt64Ptr(0), uInt64Ptr(0), uInt64Ptr(0)},
		{uInt64Ptr(0), uInt64Ptr(1), uInt64Ptr(1)},
		{uInt64Ptr(1), uInt64Ptr(0), uInt64Ptr(1)},
		{nil, uInt64Ptr(0), uInt64Ptr(0)},
		{uInt64Ptr(1), nil, uInt64Ptr(1)},

		{uInt64Ptr(10), uInt64Ptr(10), uInt64Ptr(10)},
		{uInt64Ptr(10), uInt64Ptr(11), uInt64Ptr(11)},
		{uInt64Ptr(11), uInt64Ptr(10), uInt64Ptr(11)},
		{uInt64Ptr(11), uInt64Ptr(11), uInt64Ptr(11)},
		{nil, nil, nil},

		{uInt64Ptr(math.MaxUint64), uInt64Ptr(math.MaxUint64), uInt64Ptr(math.MaxUint64)},
		{uInt64Ptr(math.MaxUint64), uInt64Ptr(math.MaxUint64 - 1), uInt64Ptr(math.MaxUint64)},
		{uInt64Ptr(math.MaxUint64 - 1), uInt64Ptr(math.MaxUint64), uInt64Ptr(math.MaxUint64)},
		{uInt64Ptr(math.MaxUint64 - 1), uInt64Ptr(math.MaxUint64 - 1), uInt64Ptr(math.MaxUint64 - 1)},
		{nil, uInt64Ptr(math.MaxUint64 - 1), uInt64Ptr(math.MaxUint64 - 1)},
		{uInt64Ptr(math.MaxUint64), nil, uInt64Ptr(math.MaxUint64)},
	}

	for c, test := range tests {
		g, e := MaxUint64Ptr(test.a, test.b), test.e
		if e != nil {
			if *g != *e {
				t.Fatal(*test.a, *test.b, *g, *e, c)
			}
		} else {
			if e != g {
				t.Fatal(*test.a, *test.b, *g, e, c)
			}
		}
	}
}

func TestMinUint64Ptr(t *testing.T) {
	tests := []struct{ a, b, e *uint64 }{
		{uInt64Ptr(0), uInt64Ptr(0), uInt64Ptr(0)},
		{uInt64Ptr(0), uInt64Ptr(1), uInt64Ptr(0)},
		{uInt64Ptr(1), uInt64Ptr(0), uInt64Ptr(0)},
		{nil, uInt64Ptr(0), uInt64Ptr(0)},
		{uInt64Ptr(1), nil, uInt64Ptr(1)},

		{uInt64Ptr(10), uInt64Ptr(10), uInt64Ptr(10)},
		{uInt64Ptr(10), uInt64Ptr(11), uInt64Ptr(10)},
		{uInt64Ptr(11), uInt64Ptr(10), uInt64Ptr(10)},
		{uInt64Ptr(11), uInt64Ptr(11), uInt64Ptr(11)},
		{nil, nil, nil},

		{uInt64Ptr(math.MaxUint64), uInt64Ptr(math.MaxUint64), uInt64Ptr(math.MaxUint64)},
		{uInt64Ptr(math.MaxUint64), uInt64Ptr(math.MaxUint64 - 1), uInt64Ptr(math.MaxUint64 - 1)},
		{uInt64Ptr(math.MaxUint64 - 1), uInt64Ptr(math.MaxUint64), uInt64Ptr(math.MaxUint64 - 1)},
		{uInt64Ptr(math.MaxUint64 - 1), uInt64Ptr(math.MaxUint64 - 1), uInt64Ptr(math.MaxUint64 - 1)},
		{nil, uInt64Ptr(math.MaxUint64 - 1), uInt64Ptr(math.MaxUint64 - 1)},
		{uInt64Ptr(math.MaxUint64), nil, uInt64Ptr(math.MaxUint64)},
	}

	for c, test := range tests {
		g, e := MinUint64Ptr(test.a, test.b), test.e
		if e != nil {
			if *g != *e {
				t.Fatal(*test.a, *test.b, *g, *e, c)
			}
		} else {
			if e != g {
				t.Fatal(*test.a, *test.b, *g, e, c)
			}
		}
	}
}

func TestMaxUint64Val(t *testing.T) {
	tests := []struct{ a, b, c, e uint64 }{
		{0, 0, 0, 0},
		{0, 0, 1, 1},
		{1, 1, 0, 1},

		{10, 10, 10, 10},
		{10, 10, 11, 11},
		{11, 11, 10, 11},
		{11, 11, 11, 11},

		{math.MaxUint64, math.MaxUint64, math.MaxUint64, math.MaxUint64},
		{math.MaxUint64, math.MaxUint64, math.MaxUint64 - 1, math.MaxUint64},
		{math.MaxUint64 - 1, math.MaxUint64 - 1, math.MaxUint64, math.MaxUint64},
		{math.MaxUint64 - 1, math.MaxUint64 - 1, math.MaxUint64 - 1, math.MaxUint64 - 1},
	}

	for i, test := range tests {
		if g, e := MaxUint64Val(test.a, test.b, test.c), test.e; g != e {
			t.Fatal(i, test.a, test.b, test.c, g, e)
		}
		if g, e := MaxUint64Val(test.a), test.a; g != e {
			t.Fatal(i, test.a, g, e)
		}
	}
}

func TestMinUint64Val(t *testing.T) {
	tests := []struct{ a, b, c, e uint64 }{
		{0, 0, 0, 0},
		{0, 0, 1, 0},
		{1, 1, 0, 0},

		{10, 10, 10, 10},
		{10, 10, 11, 10},
		{11, 11, 10, 10},
		{11, 11, 11, 11},

		{math.MaxUint64, math.MaxUint64, math.MaxUint64, math.MaxUint64},
		{math.MaxUint64, math.MaxUint64, math.MaxUint64 - 1, math.MaxUint64 - 1},
		{math.MaxUint64 - 1, math.MaxUint64 - 1, math.MaxUint64, math.MaxUint64 - 1},
		{math.MaxUint64 - 1, math.MaxUint64 - 1, math.MaxUint64 - 1, math.MaxUint64 - 1},
	}

	for i, test := range tests {
		if g, e := MinUint64Val(test.a, test.b, test.c), test.e; g != e {
			t.Fatal(i, test.a, test.b, test.c, g, e)
		}
		if g, e := MinUint64Val(test.a), test.a; g != e {
			t.Fatal(i, test.a, g, e)
		}
	}
}

func TestClampUint64(t *testing.T) {
	tests := []struct{ v, lo, hi, e uint64 }{
		{0, 0, 0, 0},
		{5, 10, 20, 10},
		{10, 10, 20, 10},
		{15, 10, 20, 15},
		{20, 10, 20, 20},
		{25, 10, 20, 20},
	}

	for _, test := range tests {
		if g, e := ClampUint64(test.v, test.lo, test.hi), test.e; g != e {
			t.Fatal(test.v, test.lo, test.hi, g, e)
		}
	}
}

func TestMaxInt8(t *testing.T) {
	tests := []struct{ a, b, e int8 }{
		{math.MinInt8, math.MinInt8, math.MinInt8},
		{math.MinInt8 + 1, math.MinInt8, math.MinInt8 + 1},
		{math.MinInt8, math.MinInt8 + 1, math.MinInt8 + 1},

		{-1, -1, -1},
		{-1, 0, 0},
		{-1, 1, 1},

		{0, -1, 0},
		{0, 0, 0},
		{0, 1, 1},

		{1, -1, 1},
		{1, 0, 1},
		{1, 1, 1},

		{math.MaxInt8, math.MaxInt8, math.MaxInt8},
		{math.MaxInt8 - 1, math.MaxInt8, math.MaxInt8},
		{math.MaxInt8, math.MaxInt8 - 1, math.MaxInt8},
	}

	for _, test := range tests {
		if g, e := MaxInt8(test.a, test.b), test.e; g != e {
			t.Fatal(test.a, test.b, g, e)
		}
	}
}

func TestMinInt8(t *testing.T) {
	tests := []struct{ a, b, e int8 }{
		{math.MinInt8, math.MinInt8, math.MinInt8},
		{math.MinInt8 + 1, math.MinInt8, math.MinInt8},
		{math.MinInt8, math.MinInt8 + 1, math.MinInt8},

		{-1, -1, -1},
		{-1, 0, -1},
		{-1, 1, -1},

		{0, -1, -1},
		{0, 0, 0},
		{0, 1, 0},

		{1, -1, -1},
		{1, 0, 0},
		{1, 1, 1},

		{math.MaxInt8, math.MaxInt8, math.MaxInt8},
		{math.MaxInt8 - 1, math.MaxInt8, math.MaxInt8 - 1},
		{math.MaxInt8, math.MaxInt8 - 1, math.MaxInt8 - 1},
	}

	for _, test := range tests {
		if g, e := MinInt8(test.a, test.b), test.e; g != e {
			t.Fatal(test.a, test.b, g, e)
		}
	}
}

func TestMaxInt8Ptr(t *testing.T) {
	tests := []struct{ a, b, e *int8 }{
		{int8Ptr(0), int8Ptr(0), int8Ptr(0)},
		{int8Ptr(0), int8Ptr(1), int8Ptr(1)},
		{int8Ptr(1), int8Ptr(0), int8Ptr(1)},
		{nil, int8Ptr(0), int8Ptr(0)},
		{int8Ptr(1), nil, int8Ptr(1)},

		{int8Ptr(10), int8Ptr(10), int8Ptr(10)},
		{int8Ptr(10), int8Ptr(11), int8Ptr(11)},
		{int8Ptr(11), int8Ptr(10), int8Ptr(11)},
		{int8Ptr(11), int8Ptr(11), int8Ptr(11)},
		{nil, nil, nil},

		{int8Ptr(math.MaxInt8), int8Ptr(math.MaxInt8), int8Ptr(math.MaxInt8)},
		{int8Ptr(math.MaxInt8), int8Ptr(math.MaxInt8 - 1), int8Ptr(math.MaxInt8)},
		{int8Ptr(math.MaxInt8 - 1), int8Ptr(math.MaxInt8), int8Ptr(math.MaxInt8)},
		{int8Ptr(math.MaxInt8 - 1), int8Ptr(math.MaxInt8 - 1), int8Ptr(math.MaxInt8 - 1)},
		{nil, int8Ptr(math.MaxInt8 - 1), int8Ptr(math.MaxInt8 - 1)},
		{int8Ptr(math.MaxInt8), nil, int8Ptr(math.MaxInt8)},
	}

	for c, test := range tests {
		g, e := MaxInt8Ptr(test.a, test.b), test.e
		if e != nil {
			if *g != *e {
				t.Fatal(*test.a, *test.b, *g, *e, c)
			}
		} else {
			if e != g {
				t.Fatal(*test.a, *test.b, *g, e, c)
			}
		}
	}
}

func TestMinInt8Ptr(t *testing.T) {
	tests := []struct{ a, b, e *int8 }{
		{int8Ptr(0), int8Ptr(0), int8Ptr(0)},
		{int8Ptr(0), int8Ptr(1), int8Ptr(0)},
		{int8Ptr(1), int8Ptr(0), int8Ptr(0)},
		{nil, int8Ptr(0), int8Ptr(0)},
		{int8Ptr(1), nil, int8Ptr(1)},

		{int8Ptr(10), int8Ptr(10), int8Ptr(10)},
		{int8Ptr(10), int8Ptr(11), int8Ptr(10)},
		{int8Ptr(11), int8Ptr(10), int8Ptr(10)},
		{int8Ptr(11), int8Ptr(11), int8Ptr(11)},
		{nil, nil, nil},

		{int8Ptr(math.MaxInt8), int8Ptr(math.MaxInt8), int8Ptr(math.MaxInt8)},
		{int8Ptr(math.MaxInt8), int8Ptr(math.MaxInt8 - 1), int8Ptr(math.MaxInt8 - 1)},
		{int8Ptr(math.MaxInt8 - 1), int8Ptr(math.MaxInt8), int8Ptr(math.MaxInt8 - 1)},
		{int8Ptr(math.MaxInt8 - 1), int8Ptr(math.MaxInt8 - 1), int8Ptr(math.MaxInt8 - 1)},
		{nil, int8Ptr(math.MaxInt8 - 1), int8Ptr(math.MaxInt8 - 1)},
		{int8Ptr(math.MaxInt8), nil, int8Ptr(math.MaxInt8)},
	}

	for c, test := range tests {
		g, e := MinInt8Ptr(test.a, test.b), test.e
		if e != nil {
			if *g != *e {
				t.Fatal(*test.a, *test.b, *g, *e, c)
			}
		} else {
			if e != g {
				t.Fatal(*test.a, *test.b, *g, e, c)
			}
		}
	}
}

func TestMaxInt8Val(t *testing.T) {
	tests := []struct{ a, b, c, e int8 }{
		{math.MinInt8, math.MinInt8, math.MinInt8, math.MinInt8},
		{math.MinInt8, math.MinInt8 + 1, math.MinInt8, math.MinInt8 + 1},
		{math.MinInt8, math.MinInt8, math.MinInt8 + 1, math.MinInt8 + 1},

		{-1, -1, -1, -1},
		{-1, -1, 0, 0},
		{-1, -1, 1, 1},

		{0, 0, -1, 0},
		{0, 0, 0, 0},
		{0, 0, 1, 1},

		{1, 1, -1, 1},
		{1, 1, 0, 1},
		{1, 1, 1, 1},

		{math.MaxInt8, math.MaxInt8, math.MaxInt8, math.MaxInt8},
		{math.MaxInt8 - 1, math.MaxInt8 - 1, math.MaxInt8, math.MaxInt8},
		{math.MaxInt8, math.MaxInt8, math.MaxInt8 - 1, math.MaxInt8},
	}

	for i, test := range tests {
		if g, e := MaxInt8Val(test.a, test.b, test.c), test.e; g != e {
			t.Fatal(i, test.a, test.b, test.c, g, e)
		}
		if g, e := MaxInt8Val(test.a), test.a; g != e {
			t.Fatal(i, test.a, g, e)
		}
	}
}

func TestMinInt8Val(t *testing.T) {
	tests := []struct{ a, b, c, e int8 }{
		{math.MinInt8, math.MinInt8, math.MinInt8, math.MinInt8},
		{math.MinInt8, math.MinInt8 + 1, math.MinInt8, math.MinInt8},
		{math.MinInt8, math.MinInt8, math.MinInt8 + 1, math.MinInt8},

		{-1, -1, -1, -1},
		{-1, -1, 0, -1},
		{-1, -1, 1, -1},

		{0, 0, -1, -1},
		{0, 0, 0, 0},
		{0, 0, 1, 0},

		{1, 1, -1, -1},
		{1, 1, 0, 0},
		{1, 1, 1, 1},

		{math.MaxInt8, math.MaxInt8, math.MaxInt8, math.MaxInt8},
		{math.MaxInt8 - 1, math.MaxInt8 - 1, math.MaxInt8, math.MaxInt8 - 1},
		{math.MaxInt8, math.MaxInt8, math.MaxInt8 - 1, math.MaxInt8 - 1},
	}

	for i, test := range tests {
		if g, e := MinInt8Val(test.a, test.b, test.c), test.e; g != e {
			t.Fatal(i, test.a, test.b, test.c, g, e)
		}
		if g, e := MinInt8Val(test.a), test.a; g != e {
			t.Fatal(i, test.a, g, e)
		}
	}
}

func TestClampInt8(t *testing.T) {
	tests := []struct{ v, lo, hi, e int8 }{
		{0, 0, 0, 0},
		{5, 10, 20, 10},
		{10, 10, 20, 10},
		{15, 10, 20, 15},
		{20, 10, 20, 20},
		{25, 10, 20, 20},
	}

	for _, test := range tests {
		if g, e := ClampInt8(test.v, test.lo, test.hi), test.e; g != e {
			t.Fatal(test.v, test.lo, test.hi, g, e)
		}
	}
}

func TestMaxInt16(t *testing.T) {
	tests := []struct{ a, b, e int16 }{
		{math.MinInt16, math.MinInt16, math.MinInt16},
		{math.MinInt16 + 1, math.MinInt16, math.MinInt16 + 1},
		{math.MinInt16, math.MinInt16 + 1, math.MinInt16 + 1},

		{-1, -1, -1},
		{-1, 0, 0},
		{-1, 1, 1},

		{0, -1, 0},
		{0, 0, 0},
		{0, 1, 1},

		{1, -1, 1},
		{1, 0, 1},
		{1, 1, 1},

		{math.MaxInt16, math.MaxInt16, math.MaxInt16},
		{math.MaxInt16 - 1, math.MaxInt16, math.MaxInt16},
		{math.MaxInt16, math.MaxInt16 - 1, math.MaxInt16},
	}

	for _, test := range tests {
		if g, e := MaxInt16(test.a, test.b), test.e; g != e {
			t.Fatal(test.a, test.b, g, e)
		}
	}
}

func TestMinInt16(t *testing.T) {
	tests := []struct{ a, b, e int16 }{
		{math.MinInt16, math.MinInt16, math.MinInt16},
		{math.MinInt16 + 1, math.MinInt16, math.MinInt16},
		{math.MinInt16, math.MinInt16 + 1, math.MinInt16},

		{-1, -1, -1},
		{-1, 0, -1},
		{-1, 1, -1},

		{0, -1, -1},
		{0, 0, 0},
		{0, 1, 0},

		{1, -1, -1},
		{1, 0, 0},
		{1, 1, 1},

		{math.MaxInt16, math.MaxInt16, math.MaxInt16},
		{math.MaxInt16 - 1, math.MaxInt16, math.MaxInt16 - 1},
		{math.MaxInt16, math.MaxInt16 - 1, math.MaxInt16 - 1},
	}

	for _, test := range tests {
		if g, e := MinInt16(test.a, test.b), test.e; g != e {
			t.Fatal(test.a, test.b, g, e)
		}
	}
}

func TestMaxInt16Ptr(t *testing.T) {
	tests := []struct{ a, b, e *int16 }{
		{int16Ptr(0), int16Ptr(0), int16Ptr(0)},
		{int16Ptr(0), int16Ptr(1), int16Ptr(1)},
		{int16Ptr(1), int16Ptr(0), int16Ptr(1)},
		{nil, int16Ptr(0), int16Ptr(0)},
		{int16Ptr(1), nil, int16Ptr(1)},

		{int16Ptr(10), int16Ptr(10), int16Ptr(10)},
		{int16Ptr(10), int16Ptr(11), int16Ptr(11)},
		{int16Ptr(11), int16Ptr(10), int16Ptr(11)},
		{int16Ptr(11), int16Ptr(11), int16Ptr(11)},
		{nil, nil, nil},

		{int16Ptr(math.MaxInt8), int16Ptr(math.MaxInt8), int16Ptr(math.MaxInt8)},
		{int16Ptr(math.MaxInt8), int16Ptr(math.MaxInt8 - 1), int16Ptr(math.MaxInt8)},
		{int16Ptr(math.MaxInt8 - 1), int16Ptr(math.MaxInt8), int16Ptr(math.MaxInt8)},
		{int16Ptr(math.MaxInt8 - 1), int16Ptr(math.MaxInt8 - 1), int16Ptr(math.MaxInt8 - 1)},
		{nil, int16Ptr(math.MaxInt8 - 1), int16Ptr(math.MaxInt8 - 1)},
		{int16Ptr(math.MaxInt8), nil, int16Ptr(math.MaxInt8)},
	}

	for c, test := range tests {
		g, e := MaxInt16Ptr(test.a, test.b), test.e
		if e != nil {
			if *g != *e {
				t.Fatal(*test.a, *test.b, *g, *e, c)
			}
		} else {
			if e != g {
				t.Fatal(*test.a, *test.b, *g, e, c)
			}
		}
	}
}

func TestMinInt16Ptr(t *testing.T) {
	tests := []struct{ a, b, e *int16 }{
		{int16Ptr(0), int16Ptr(0), int16Ptr(0)},
		{int16Ptr(0), int16Ptr(1), int16Ptr(0)},
		{int16Ptr(1), int16Ptr(0), int16Ptr(0)},
		{nil, int16Ptr(0), int16Ptr(0)},
		{int16Ptr(1), nil, int16Ptr(1)},

		{int16Ptr(10), int16Ptr(10), int16Ptr(10)},
		{int16Ptr(10), int16Ptr(11), int16Ptr(10)},
		{int16Ptr(11), int16Ptr(10), int16Ptr(10)},
		{int16Ptr(11), int16Ptr(11), int16Ptr(11)},
		{nil, nil, nil},

		{int16Ptr(math.MaxInt8), int16Ptr(math.MaxInt8), int16Ptr(math.MaxInt8)},
		{int16Ptr(math.MaxInt8), int16Ptr(math.MaxInt8 - 1), int16Ptr(math.MaxInt8 - 1)},
		{int16Ptr(math.MaxInt8 - 1), int16Ptr(math.MaxInt8), int16Ptr(math.MaxInt8 - 1)},
		{int16Ptr(math.MaxInt8 - 1), int16Ptr(math.MaxInt8 - 1), int16Ptr(math.MaxInt8 - 1)},
		{nil, int16Ptr(math.MaxInt8 - 1), int16Ptr(math.MaxInt8 - 1)},
		{int16Ptr(math.MaxInt8), nil, int16Ptr(math.MaxInt8)},
	}

	for c, test := range tests {
		g, e := MinInt16Ptr(test.a, test.b), test.e
		if e != nil {
			if *g != *e {
				t.Fatal(*test.a, *test.b, *g, *e, c)
			}
		} else {
			if e != g {
				t.Fatal(*test.a, *test.b, *g, e, c)
			}
		}
	}
}

func TestMaxInt16Val(t *testing.T) {
	tests := []struct{ a, b, c, e int16 }{
		{math.MinInt16, math.MinInt16, math.MinInt16, math.MinInt16},
		{math.MinInt16, math.MinInt16 + 1, math.MinInt16, math.MinInt16 + 1},
		{math.MinInt16, math.MinInt16, math.MinInt16 + 1, math.MinInt16 + 1},

		{-1, -1, -1, -1},
		{-1, -1, 0, 0},
		{-1, -1, 1, 1},

		{0, 0, -1, 0},
		{0, 0, 0, 0},
		{0, 0, 1, 1},

		{1, 1, -1, 1},
		{1, 1, 0, 1},
		{1, 1, 1, 1},

		{math.MaxInt16, math.MaxInt16, math.MaxInt16, math.MaxInt16},
		{math.MaxInt16 - 1, math.MaxInt16 - 1, math.MaxInt16, math.MaxInt16},
		{math.MaxInt16, math.MaxInt16, math.MaxInt16 - 1, math.MaxInt16},
	}

	for i, test := range tests {
		if g, e := MaxInt16Val(test.a, test.b, test.c), test.e; g != e {
			t.Fatal(i, test.a, test.b, test.c, g, e)
		}
		if g, e := MaxInt16Val(test.a), test.a; g != e {
			t.Fatal(i, test.a, g, e)
		}
	}
}

func TestMinInt16Val(t *testing.T) {
	tests := []struct{ a, b, c, e int16 }{
		{math.MinInt16, math.MinInt16, math.MinInt16, math.MinInt16},
		{math.MinInt16, math.MinInt16 + 1, math.MinInt16, math.MinInt16},
		{math.MinInt16, math.MinInt16, math.MinInt16 + 1, math.MinInt16},

		{-1, -1, -1, -1},
		{-1, -1, 0, -1},
		{-1, -1, 1, -1},

		{0, 0, -1, -1},
		{0, 0, 0, 0},
		{0, 0, 1, 0},

		{1, 1, -1, -1},
		{1, 1, 0, 0},
		{1, 1, 1, 1},

		{math.MaxInt16, math.MaxInt16, math.MaxInt16, math.MaxInt16},
		{math.MaxInt16 - 1, math.MaxInt16 - 1, math.MaxInt16, math.MaxInt16 - 1},
		{math.MaxInt16, math.MaxInt16, math.MaxInt16 - 1, math.MaxInt16 - 1},
	}

	for i, test := range tests {
		if g, e := MinInt16Val(test.a, test.b, test.c), test.e; g != e {
			t.Fatal(i, test.a, test.b, test.c, g, e)
		}
		if g, e := MinInt16Val(test.a), test.a; g != e {
			t.Fatal(i, test.a, g, e)
		}
	}
}

func TestClampInt16(t *testing.T) {
	tests := []struct{ v, lo, hi, e int16 }{
		{0, 0, 0, 0},
		{5, 10, 20, 10},
		{10, 10, 20, 10},
		{15, 10, 20, 15},
		{20, 10, 20, 20},
		{25, 10, 20, 20},
	}

	for _, test := range tests {
		if g, e := ClampInt16(test.v, test.lo, test.hi), test.e; g != e {
			t.Fatal(test.v, test.lo, test.hi, g, e)
		}
	}
}

func TestMaxInt32(t *testing.T) {
	tests := []struct{ a, b, e int32 }{
		{math.MinInt32, math.MinInt32, math.MinInt32},
		{math.MinInt32 + 1, math.MinInt32, math.MinInt32 + 1},
		{math.MinInt32, math.MinInt32 + 1, math.MinInt32 + 1},

		{-1, -1, -1},
		{-1, 0, 0},
		{-1, 1, 1},

		{0, -1, 0},
		{0, 0, 0},
		{0, 1, 1},

		{1, -1, 1},
		{1, 0, 1},
		{1, 1, 1},

		{math.MaxInt32, math.MaxInt32, math.MaxInt32},
		{math.MaxInt32 - 1, math.MaxInt32, math.MaxInt32},
		{math.MaxInt32, math.MaxInt32 - 1, math.MaxInt32},
	}

	for _, test := range tests {
		if g, e := MaxInt32(test.a, test.b), test.e; g != e {
			t.Fatal(test.a, test.b, g, e)
		}
	}
}

func TestMinInt32(t *testing.T) {
	tests := []struct{ a, b, e int32 }{
		{math.MinInt32, math.MinInt32, math.MinInt32},
		{math.MinInt32 + 1, math.MinInt32, math.MinInt32},
		{math.MinInt32, math.MinInt32 + 1, math.MinInt32},

		{-1, -1, -1},
		{-1, 0, -1},
		{-1, 1, -1},

		{0, -1, -1},
		{0, 0, 0},
		{0, 1, 0},

		{1, -1, -1},
		{1, 0, 0},
		{1, 1, 1},

		{math.MaxInt32, math.MaxInt32, math.MaxInt32},
		{math.MaxInt32 - 1, math.MaxInt32, math.MaxInt32 - 1},
		{math.MaxInt32, math.MaxInt32 - 1, math.MaxInt32 - 1},
	}

	for _, test := range tests {
		if g, e := MinInt32(test.a, test.b), test.e; g != e {
			t.Fatal(test.a, test.b, g, e)
		}
	}
}

func TestMaxInt32Ptr(t *testing.T) {
	tests := []struct{ a, b, e *int32 }{
		{int32Ptr(0), int32Ptr(0), int32Ptr(0)},
		{int32Ptr(0), int32Ptr(1), int32Ptr(1)},
		{int32Ptr(1), int32Ptr(0), int32Ptr(1)},
		{nil, int32Ptr(0), int32Ptr(0)},
		{int32Ptr(1), nil, int32Ptr(1)},

		{int32Ptr(10), int32Ptr(10), int32Ptr(10)},
		{int32Ptr(10), int32Ptr(11), int32Ptr(11)},
		{int32Ptr(11), int32Ptr(10), int32Ptr(11)},
		{int32Ptr(11), int32Ptr(11), int32Ptr(11)},
		{nil, nil, nil},

		{int32Ptr(math.MaxInt32), int32Ptr(math.MaxInt32), int32Ptr(math.MaxInt32)},
		{int32Ptr(math.MaxInt32), int32Ptr(math.MaxInt32 - 1), int32Ptr(math.MaxInt32)},
		{int32Ptr(math.MaxInt32 - 1), int32Ptr(math.MaxInt32), int32Ptr(math.MaxInt32)},
		{int32Ptr(math.MaxInt32 - 1), int32Ptr(math.MaxInt32 - 1), int32Ptr(math.MaxInt32 - 1)},
		{nil, int32Ptr(math.MaxInt32 - 1), int32Ptr(math.MaxInt32 - 1)},
		{int32Ptr(math.MaxInt32), nil, int32Ptr(math.MaxInt32)},
	}

	for c, test := range tests {
		g, e := MaxInt32Ptr(test.a, test.b), test.e
		if e != nil {
			if *g != *e {
				t.Fatal(*test.a, *test.b, *g, *e, c)
			}
		} else {
			if e != g {
				t.Fatal(*test.a, *test.b, *g, e, c)
			}
		}
	}
}

func TestMinInt32Ptr(t *testing.T) {
	tests := []struct{ a, b, e *int32 }{
		{int32Ptr(0), int32Ptr(0), int32Ptr(0)},
		{int32Ptr(0), int32Ptr(1), int32Ptr(0)},
		{int32Ptr(1), int32Ptr(0), int32Ptr(0)},
		{nil, int32Ptr(0), int32Ptr(0)},
		{int32Ptr(1), nil, int32Ptr(1)},

		{int32Ptr(10), int32Ptr(10), int32Ptr(10)},
		{int32Ptr(10), int32Ptr(11), int32Ptr(10)},
		{int32Ptr(11), int32Ptr(10), int32Ptr(10)},
		{int32Ptr(11), int32Ptr(11), int32Ptr(11)},
		{nil, nil, nil},

		{int32Ptr(math.MaxInt32), int32Ptr(math.MaxInt32), int32Ptr(math.MaxInt32)},
		{int32Ptr(math.MaxInt32), int32Ptr(math.MaxInt32 - 1), int32Ptr(math.MaxInt32 - 1)},
		{int32Ptr(math.MaxInt32 - 1), int32Ptr(math.MaxInt32), int32Ptr(math.MaxInt32 - 1)},
		{int32Ptr(math.MaxInt32 - 1), int32Ptr(math.MaxInt32 - 1), int32Ptr(math.MaxInt32 - 1)},
		{nil, int32Ptr(math.MaxInt32 - 1), int32Ptr(math.MaxInt32 - 1)},
		{int32Ptr(math.MaxInt32), nil, int32Ptr(math.MaxInt32)},
	}

	for c, test := range tests {
		g, e := MinInt32Ptr(test.a, test.b), test.e
		if e != nil {
			if *g != *e {
				t.Fatal(*test.a, *test.b, *g, *e, c)
			}
		} else {
			if e != g {
				t.Fatal(*test.a, *test.b, *g, e, c)
			}
		}
	}
}

func TestMaxInt32Val(t *testing.T) {
	tests := []struct{ a, b, c, e int32 }{
		{math.MinInt32, math.MinInt32, math.MinInt32, math.MinInt32},
		{math.MinInt32, math.MinInt32 + 1, math.MinInt32, math.MinInt32 + 1},
		{math.MinInt32, math.MinInt32, math.MinInt32 + 1, math.MinInt32 + 1},

		{-1, -1, -1, -1},
		{-1, -1, 0, 0},
		{-1, -1, 1, 1},

		{0, 0, -1, 0},
		{0, 0, 0, 0},
		{0, 0, 1, 1},

		{1, 1, -1, 1},
		{1, 1, 0, 1},
		{1, 1, 1, 1},

		{math.MaxInt32, math.MaxInt32, math.MaxInt32, math.MaxInt32},
		{math.MaxInt32 - 1, math.MaxInt32 - 1, math.MaxInt32, math.MaxInt32},
		{math.MaxInt32, math.MaxInt32, math.MaxInt32 - 1, math.MaxInt32},
	}

	for i, test := range tests {
		if g, e := MaxInt32Val(test.a, test.b, test.c), test.e; g != e {
			t.Fatal(i, test.a, test.b, test.c, g, e)
		}
		if g, e := MaxInt32Val(test.a), test.a; g != e {
			t.Fatal(i, test.a, g, e)
		}
	}
}

func TestMinInt32Val(t *testing.T) {
	tests := []struct{ a, b, c, e int32 }{
		{math.MinInt32, math.MinInt32, math.MinInt32, math.MinInt32},
		{math.MinInt32, math.MinInt32 + 1, math.MinInt32, math.MinInt32},
		{math.MinInt32, math.MinInt32, math.MinInt32 + 1, math.MinInt32},

		{-1, -1, -1, -1},
		{-1, -1, 0, -1},
		{-1, -1, 1, -1},

		{0, 0, -1, -1},
		{0, 0, 0, 0},
		{0, 0, 1, 0},

		{1, 1, -1, -1},
		{1, 1, 0, 0},
		{1, 1, 1, 1},

		{math.MaxInt32, math.MaxInt32, math.MaxInt32, math.MaxInt32},
		{math.MaxInt32 - 1, math.MaxInt32 - 1, math.MaxInt32, math.MaxInt32 - 1},
		{math.MaxInt32, math.MaxInt32, math.MaxInt32 - 1, math.MaxInt32 - 1},
	}

	for i, test := range tests {
		if g, e := MinInt32Val(test.a, test.b, test.c), test.e; g != e {
			t.Fatal(i, test.a, test.b, test.c, g, e)
		}
		if g, e := MinInt32Val(test.a), test.a; g != e {
			t.Fatal(i, test.a, g, e)
		}
	}
}

func TestClampInt32(t *testing.T) {
	tests := []struct{ v, lo, hi, e int32 }{
		{0, 0, 0, 0},
		{5, 10, 20, 10},
		{10, 10, 20, 10},
		{15, 10, 20, 15},
		{20, 10, 20, 20},
		{25, 10, 20, 20},
	}

	for _, test := range tests {
		if g, e := ClampInt32(test.v, test.lo, test.hi), test.e; g != e {
			t.Fatal(test.v, test.lo, test.hi, g, e)
		}
	}
}

func TestMaxInt64(t *testing.T) {
	tests := []struct{ a, b, e int64 }{
		{math.MinInt64, math.MinInt64, math.MinInt64},
		{math.MinInt64 + 1, math.MinInt64, math.MinInt64 + 1},
		{math.MinInt64, math.MinInt64 + 1, math.MinInt64 + 1},

		{-1, -1, -1},
		{-1, 0, 0},
		{-1, 1, 1},

		{0, -1, 0},
		{0, 0, 0},
		{0, 1, 1},

		{1, -1, 1},
		{1, 0, 1},
		{1, 1, 1},

		{math.MaxInt64, math.MaxInt64, math.MaxInt64},
		{math.MaxInt64 - 1, math.MaxInt64, math.MaxInt64},
		{math.MaxInt64, math.MaxInt64 - 1, math.MaxInt64},
	}

	for _, test := range tests {
		if g, e := MaxInt64(test.a, test.b), test.e; g != e {
			t.Fatal(test.a, test.b, g, e)
		}
	}
}

func TestMinInt64(t *testing.T) {
	tests := []struct{ a, b, e int64 }{
		{math.MinInt64, math.MinInt64, math.MinInt64},
		{math.MinInt64 + 1, math.MinInt64, math.MinInt64},
		{math.MinInt64, math.MinInt64 + 1, math.MinInt64},

		{-1, -1, -1},
		{-1, 0, -1},
		{-1, 1, -1},

		{0, -1, -1},
		{0, 0, 0},
		{0, 1, 0},

		{1, -1, -1},
		{1, 0, 0},
		{1, 1, 1},

		{math.MaxInt64, math.MaxInt64, math.MaxInt64},
		{math.MaxInt64 - 1, math.MaxInt64, math.MaxInt64 - 1},
		{math.MaxInt64, math.MaxInt64 - 1, math.MaxInt64 - 1},
	}

	for _, test := range tests {
		if g, e := MinInt64(test.a, test.b), test.e; g != e {
			t.Fatal(test.a, test.b, g, e)
		}
	}
}

func TestMaxInt64Ptr(t *testing.T) {
	tests := []struct{ a, b, e *int64 }{
		{int64Ptr(0), int64Ptr(0), int64Ptr(0)},
		{int64Ptr(0), int64Ptr(1), int64Ptr(1)},
		{int64Ptr(1), int64Ptr(0), int64Ptr(1)},
		{nil, int64Ptr(0), int64Ptr(0)},
		{int64Ptr(1), nil, int64Ptr(1)},

		{int64Ptr(10), int64Ptr(10), int64Ptr(10)},
		{int64Ptr(10), int64Ptr(11), int64Ptr(11)},
		{int64Ptr(11), int64Ptr(10), int64Ptr(11)},
		{int64Ptr(11), int64Ptr(11), int64Ptr(11)},
		{nil, nil, nil},

		{int64Ptr(math.MaxInt64), int64Ptr(math.MaxInt64), int64Ptr(math.MaxInt64)},
		{int64Ptr(math.MaxInt64), int64Ptr(math.MaxInt64 - 1), int64Ptr(math.MaxInt64)},
		{int64Ptr(math.MaxInt64 - 1), int64Ptr(math.MaxInt64), int64Ptr(math.MaxInt64)},
		{int64Ptr(math.MaxInt64 - 1), int64Ptr(math.MaxInt64 - 1), int64Ptr(math.MaxInt64 - 1)},
		{nil, int64Ptr(math.MaxInt64 - 1), int64Ptr(math.MaxInt64 - 1)},
		{int64Ptr(math.MaxInt64), nil, int64Ptr(math.MaxInt64)},
	}

	for c, test := range tests {
		g, e := MaxInt64Ptr(test.a, test.b), test.e
		if e != nil {
			if *g != *e {
				t.Fatal(*test.a, *test.b, *g, *e, c)
			}
		} else {
			if e != g {
				t.Fatal(*test.a, *test.b, *g, e, c)
			}
		}
	}
}

func TestMinInt64Ptr(t *testing.T) {
	tests := []struct{ a, b, e *int64 }{
		{int64Ptr(0), int64Ptr(0), int64Ptr(0)},
		{int64Ptr(0), int64Ptr(1), int64Ptr(0)},
		{int64Ptr(1), int64Ptr(0), int64Ptr(0)},
		{nil, int64Ptr(0), int64Ptr(0)},
		{int64Ptr(1), nil, int64Ptr(1)},

		{int64Ptr(10), int64Ptr(10), int64Ptr(10)},
		{int64Ptr(10), int64Ptr(11), int64Ptr(10)},
		{int64Ptr(11), int64Ptr(10), int64Ptr(10)},
		{int64Ptr(11), int64Ptr(11), int64Ptr(11)},
		{nil, nil, nil},

		{int64Ptr(math.MaxInt64), int64Ptr(math.MaxInt64), int64Ptr(math.MaxInt64)},
		{int64Ptr(math.MaxInt64), int64Ptr(math.MaxInt64 - 1), int64Ptr(math.MaxInt64 - 1)},
		{int64Ptr(math.MaxInt64 - 1), int64Ptr(math.MaxInt64), int64Ptr(math.MaxInt64 - 1)},
		{int64Ptr(math.MaxInt64 - 1), int64Ptr(math.MaxInt64 - 1), int64Ptr(math.MaxInt64 - 1)},
		{nil, int64Ptr(math.MaxInt64 - 1), int64Ptr(math.MaxInt64 - 1)},
		{int64Ptr(math.MaxInt64), nil, int64Ptr(math.MaxInt64)},
	}

	for c, test := range tests {
		g, e := MinInt64Ptr(test.a, test.b), test.e
		if e != nil {
			if *g != *e {
				t.Fatal(*test.a, *test.b, *g, *e, c)
			}
		} else {
			if e != g {
				t.Fatal(*test.a, *test.b, *g, e, c)
			}
		}
	}
}

func TestMaxInt64Val(t *testing.T) {
	tests := []struct{ a, b, c, e int64 }{
		{math.MinInt64, math.MinInt64, math.MinInt64, math.MinInt64},
		{math.MinInt64, math.MinInt64 + 1, math.MinInt64, math.MinInt64 + 1},
		{math.MinInt64, math.MinInt64, math.MinInt64 + 1, math.MinInt64 + 1},

		{-1, -1, -1, -1},
		{-1, -1, 0, 0},
		{-1, -1, 1, 1},

		{0, 0, -1, 0},
		{0, 0, 0, 0},
		{0, 0, 1, 1},

		{1, 1, -1, 1},
		{1, 1, 0, 1},
		{1, 1, 1, 1},

		{math.MaxInt64, math.MaxInt64, math.MaxInt64, math.MaxInt64},
		{math.MaxInt64 - 1, math.MaxInt64 - 1, math.MaxInt64, math.MaxInt64},
		{math.MaxInt64, math.MaxInt64, math.MaxInt64 - 1, math.MaxInt64},
	}

	for i, test := range tests {
		if g, e := MaxInt64Val(test.a, test.b, test.c), test.e; g != e {
			t.Fatal(i, test.a, test.b, test.c, g, e)
		}
		if g, e := MaxInt64Val(test.a), test.a; g != e {
			t.Fatal(i, test.a, g, e)
		}
	}
}

func TestMinInt64Val(t *testing.T) {
	tests := []struct{ a, b, c, e int64 }{
		{math.MinInt64, math.MinInt64, math.MinInt64, math.MinInt64},
		{math.MinInt64, math.MinInt64 + 1, math.MinInt64, math.MinInt64},
		{math.MinInt64, math.MinInt64, math.MinInt64 + 1, math.MinInt64},

		{-1, -1, -1, -1},
		{-1, -1, 0, -1},
		{-1, -1, 1, -1},

		{0, 0, -1, -1},
		{0, 0, 0, 0},
		{0, 0, 1, 0},

		{1, 1, -1, -1},
		{1, 1, 0, 0},
		{1, 1, 1, 1},

		{math.MaxInt64, math.MaxInt64, math.MaxInt64, math.MaxInt64},
		{math.MaxInt64 - 1, math.MaxInt64 - 1, math.MaxInt64, math.MaxInt64 - 1},
		{math.MaxInt64, math.MaxInt64, math.MaxInt64 - 1, math.MaxInt64 - 1},
	}

	for i, test := range tests {
		if g, e := MinInt64Val(test.a, test.b, test.c), test.e; g != e {
			t.Fatal(i, test.a, test.b, test.c, g, e)
		}
		if g, e := MinInt64Val(test.a), test.a; g != e {
			t.Fatal(i, test.a, g, e)
		}
	}
}

func TestClampInt64(t *testing.T) {
	tests := []struct{ v, lo, hi, e int64 }{
		{0, 0, 0, 0},
		{5, 10, 20, 10},
		{10, 10, 20, 10},
		{15, 10, 20, 15},
		{20, 10, 20, 20},
		{25, 10, 20, 20},
	}

	for _, test := range tests {
		if g, e := ClampInt64(test.v, test.lo, test.hi), test.e; g != e {
			t.Fatal(test.v, test.lo, test.hi, g, e)
		}
	}
}

func TestPopCountBigInt(t *testing.T) {
	const N = 1e4
	rng := rand.New(rand.NewSource(42))
	lim := big.NewInt(0)
	lim.SetBit(lim, 1e3, 1)
	z := big.NewInt(0)
	m1 := big.NewInt(-1)
	for i := 0; i < N; i++ {
		z.Rand(rng, lim)
		g := PopCountBigInt(z)
		e := 0
		for bit := 0; bit < z.BitLen(); bit++ {
			if z.Bit(bit) != 0 {
				e++
			}
		}
		if g != e {
			t.Fatal(g, e)
		}

		z.Mul(z, m1)
		if g := PopCountBigInt(z); g != e {
			t.Fatal(g, e)
		}
	}
}

func benchmarkPopCountBigInt(b *testing.B, bits int) {
	lim := big.NewInt(0)
	lim.SetBit(lim, bits, 1)
	z := big.NewInt(0)
	z.Rand(rand.New(rand.NewSource(42)), lim)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		PopCountBigInt(z)
	}
}

func BenchmarkPopCountBigInt1e1(b *testing.B) {
	benchmarkPopCountBigInt(b, 1e1)
}

func BenchmarkPopCountBigInt1e2(b *testing.B) {
	benchmarkPopCountBigInt(b, 1e2)
}

func BenchmarkPopCountBigInt1e3(b *testing.B) {
	benchmarkPopCountBigInt(b, 1e3)
}

func BenchmarkPopCountBigIbnt1e4(b *testing.B) {
	benchmarkPopCountBigInt(b, 1e4)
}

func BenchmarkPopCountBigInt1e5(b *testing.B) {
	benchmarkPopCountBigInt(b, 1e5)
}

func BenchmarkPopCountBigInt1e6(b *testing.B) {
	benchmarkPopCountBigInt(b, 1e6)
}

func TestToBase(t *testing.T) {
	x := ToBase(big.NewInt(0), 42)
	e := []int{0}
	if g, e := len(x), len(e); g != e {
		t.Fatal(g, e)
	}

	for i, g := range x {
		if e := e[i]; g != e {
			t.Fatal(i, g, e)
		}
	}

	x = ToBase(big.NewInt(2047), 22)
	e = []int{1, 5, 4}
	if g, e := len(x), len(e); g != e {
		t.Fatal(g, e)
	}

	for i, g := range x {
		if e := e[i]; g != e {
			t.Fatal(i, g, e)
		}
	}

	x = ToBase(big.NewInt(-2047), 22)
	e = []int{-1, -5, -4}
	if g, e := len(x), len(e); g != e {
		t.Fatal(g, e)
	}

	for i, g := range x {
		if e := e[i]; g != e {
			t.Fatal(i, g, e)
		}
	}
}

func TestBug(t *testing.T) {
	if BitLenUint(MaxUint) != 64 {
		t.Logf("Bug reproducible only on 64 bit architecture")
		return
	}

	_, err := NewFC32(MinInt, MaxInt, true)
	if err == nil {
		t.Fatal("Expected non nil err")
	}
}

func poly(a ...int) string {
	var b bytes.Buffer
	for i, v := range a {
		p := len(a) - i - 1
		if v == 0 && p != 0 {
			continue
		}

		if v == 0 && p == 0 && b.Len() != 0 {
			continue
		}

		if av := abs(v); av == 1 && p != 0 {
			if b.Len() != 0 {
				if v == 1 {
					b.WriteByte('+')
				} else {
					b.WriteByte('-')
				}
			} else if v == -1 {
				b.WriteByte('-')
			}
		} else {
			switch {
			case b.Len() == 0:
				fmt.Fprintf(&b, "%d", v)
			default:
				fmt.Fprintf(&b, "%+d", v)
			}
		}

		if p == 0 {
			continue
		}

		if p == 1 {
			fmt.Fprintf(&b, "x")
			continue
		}

		fmt.Fprintf(&b, "x^%d", p)
	}
	return b.String()
}

func polyBig(a ...*big.Int) string {
	var b bytes.Buffer
	for i, v := range a {
		p := len(a) - i - 1
		if v.Sign() == 0 && p != 0 {
			continue
		}

		if v.Sign() == 0 && p == 0 && b.Len() != 0 {
			continue
		}

		if av := bigAbs(v); av.Cmp(_1) == 0 && p != 0 {
			if b.Len() != 0 {
				if v.Cmp(_1) == 0 {
					b.WriteByte('+')
				} else {
					b.WriteByte('-')
				}
			} else if v.Cmp(_m1) == 0 {
				b.WriteByte('-')
			}
		} else {
			switch {
			case b.Len() == 0:
				fmt.Fprintf(&b, "%d", v)
			default:
				fmt.Fprintf(&b, "%+d", v)
			}
		}

		if p == 0 {
			continue
		}

		if p == 1 {
			fmt.Fprintf(&b, "x")
			continue
		}

		fmt.Fprintf(&b, "x^%d", p)
	}
	return b.String()
}

func polyK(k int) string {
	switch {
	case k == -1:
		return "-"
	case k == 1:
		return ""
	default:
		return fmt.Sprint(k)
	}
}

func polyKBig(k *big.Int) string {
	switch {
	case k.Cmp(_m1) == 0:
		return "-"
	case k.Cmp(_1) == 0:
		return ""
	default:
		return fmt.Sprint(k)
	}
}

func TestQuadPolyDiscriminantBig(t *testing.T) {
	for i, test := range []struct {
		a, b, c, ds, d int
	}{
		{-1, -5, 6, 49, 7},
		{-1, 5, 6, 49, 7},
		{1, -5, -6, 49, 7},
		{1, 5, -6, 49, 7},
		{1, 5, 6, 1, 1},
		{2, 3, 5, -31, -1},
		{2, 7, 3, 25, 5},
		{3, 8, 5, 4, 2},
		{3, 9, 5, 21, -1},
		{4, 5, 1, 9, 3},
		{5, 3, 2, -31, -1},
	} {
		ds, d := QuadPolyDiscriminantBig(big.NewInt(int64(test.a)), big.NewInt(int64(test.b)), big.NewInt(int64(test.c)))
		if g, e := ds, big.NewInt(int64(test.ds)); g.Cmp(e) != 0 {
			t.Fatal(i, g, e)
		}

		switch {
		case test.d < 0:
			if d != nil {
				t.Fatal(i, d, nil)
			}
		default:
			if g, e := d, big.NewInt(int64(test.d)); g.Cmp(e) != 0 {
				t.Fatal(i, g, e)
			}
		}
	}
}

func testQuadPolyFactorsBig(t *testing.T, p1, q1, p2, q2, k *big.Int, cases int) {
	a := bigMul(k, bigMul(p1, p2))
	b := bigMul(k, bigAdd(bigMul(p1, q2), bigMul(q1, p2)))
	c := bigMul(k, bigMul(q1, q2))
	con, f := QuadPolyFactorsBig(a, b, c)

	switch {
	case a.Sign() == 0:
		if g, e := len(f), 1; g != e {
			t.Fatalf(
				"%d: %s(%s)(%s) = %s -> got %v factors, expected %v",
				cases, polyKBig(k), polyBig(p1, q1), polyBig(p2, q2), polyBig(a, b, c),
				g, e,
			)
		}

		a2 := big.NewInt(0)
		b2 := bigMul(con, f[0].P)
		c2 := bigMul(con, f[0].Q)
		if a.Cmp(a2) != 0 || b.Cmp(b2) != 0 || c.Cmp(c2) != 0 {
			t.Fatalf(
				"%d: %s(%s)(%s) = %s -> %s(%s) = %s",
				cases, polyKBig(k), polyBig(p1, q1), polyBig(p2, q2), polyBig(a, b, c),
				polyKBig(con), polyBig(f[0].P, f[0].Q), polyBig(a2, b2, c2),
			)
		}

		t.Logf(
			"%d: %s(%s)(%s) = %s -> %s(%s) = %s",
			cases, polyKBig(k), polyBig(p1, q1), polyBig(p2, q2), polyBig(a, b, c),
			polyKBig(con), polyBig(f[0].P, f[0].Q), polyBig(a2, b2, c2),
		)
	default:
		if g, e := len(f), 2; g != e {
			t.Fatalf(
				"%d: %s(%s)(%s) = %s -> got %v factors, expected %v",
				cases, polyKBig(k), polyBig(p1, q1), polyBig(p2, q2), polyBig(a, b, c),
				g, e,
			)
		}

		a2 := bigMul(con, bigMul(f[0].P, f[1].P))
		b2 := bigMul(con, bigAdd(bigMul(f[0].P, f[1].Q), bigMul(f[0].Q, f[1].P)))
		c2 := bigMul(con, bigMul(f[0].Q, f[1].Q))
		if a.Cmp(a2) != 0 || b.Cmp(b2) != 0 || c.Cmp(c2) != 0 {
			t.Fatalf(
				"%d: %s(%s)(%s) = %s -> %s(%s)(%s) = %s",
				cases, polyKBig(k), polyBig(p1, q1), polyBig(p2, q2), polyBig(a, b, c),
				polyKBig(con), polyBig(f[0].P, f[0].Q), polyBig(f[1].P, f[1].Q), polyBig(a2, b2, c2),
			)
		}

		t.Logf(
			"%d: %s(%s)(%s) = %s -> %s(%s)(%s) = %s",
			cases, polyKBig(k), polyBig(p1, q1), polyBig(p2, q2), polyBig(a, b, c),
			polyKBig(con), polyBig(f[0].P, f[0].Q), polyBig(f[1].P, f[1].Q), polyBig(a2, b2, c2),
		)
	}
}

func TestQuadPolyFactorsBig(t *testing.T) {
	cases := 0

	const N = 1e4
	rng := rand.New(rand.NewSource(42))
	for i := 0; i < N; i++ {
		p1 := big.NewInt(rng.Int63())
		q1 := big.NewInt(rng.Int63())
		p2 := big.NewInt(rng.Int63())
		q2 := big.NewInt(rng.Int63())
		k := big.NewInt(rng.Int63())
		testQuadPolyFactorsBig(t, p1, q1, p2, q2, k, cases)
		cases++
	}

	cons := []int{-1, 1}
	const lim = 7
	for p1 := -lim; p1 <= lim; p1++ {
		for q1 := -lim; q1 <= lim; q1++ {
			for p2 := -lim; p2 <= lim; p2++ {
				for q2 := -lim; q2 <= lim; q2++ {
					for _, k := range cons {
						testQuadPolyFactorsBig(
							t,
							big.NewInt(int64(p1)),
							big.NewInt(int64(q1)),
							big.NewInt(int64(p2)),
							big.NewInt(int64(q2)),
							big.NewInt(int64(k)),
							cases,
						)
						cases++
					}
				}
			}
		}
	}
}

func TestQuadPolyDiscriminant(t *testing.T) {
	for i, test := range []struct {
		a, b, c, ds, d int
	}{
		{-1, -5, 6, 49, 7},
		{-1, 5, 6, 49, 7},
		{1, -5, -6, 49, 7},
		{1, 5, -6, 49, 7},
		{1, 5, 6, 1, 1},
		{2, 3, 5, -31, -1},
		{2, 7, 3, 25, 5},
		{3, 8, 5, 4, 2},
		{3, 9, 5, 21, -1},
		{4, 5, 1, 9, 3},
		{5, 3, 2, -31, -1},
	} {
		ds, d, err := QuadPolyDiscriminant(test.a, test.b, test.c)
		if err != nil {
			t.Fatal(i, err)
		}

		if g, e := ds, test.ds; g != e {
			t.Fatal(i, g, e)
		}

		if g, e := d, test.d; g != e {
			t.Fatal(i, g, e)
		}
	}
}

func testQuadPolyFactors(t *testing.T, p1, q1, p2, q2, k, cases int) {
	a := k * p1 * p2
	b := k * (p1*q2 + q1*p2)
	c := k * (q1 * q2)
	con, f, err := QuadPolyFactors(a, b, c)
	if err != nil {
		t.Fatalf(
			"%d: %s(%s)(%s) = %s -> %v",
			cases, polyK(k), poly(p1, q1), poly(p2, q2), poly(a, b, c),
			err,
		)
	}

	switch {
	case a == 0:
		if g, e := len(f), 1; g != e {
			t.Fatalf(
				"%d: %s(%s)(%s) = %s -> got %v factors, expected %v",
				cases, polyK(k), poly(p1, q1), poly(p2, q2), poly(a, b, c),
				g, e,
			)
		}

		a2 := 0
		b2 := con * f[0].P
		c2 := con * f[0].Q
		if a != a2 || b != b2 || c != c2 {
			t.Fatalf(
				"%d: %s(%s)(%s) = %s -> %s(%s) = %s",
				cases, polyK(k), poly(p1, q1), poly(p2, q2), poly(a, b, c),
				polyK(con), poly(f[0].P, f[0].Q), poly(a2, b2, c2),
			)
		}

		t.Logf(
			"%d: %s(%s)(%s) = %s -> %s(%s) = %s",
			cases, polyK(k), poly(p1, q1), poly(p2, q2), poly(a, b, c),
			polyK(con), poly(f[0].P, f[0].Q), poly(a2, b2, c2),
		)
	default:
		if g, e := len(f), 2; g != e {
			t.Fatalf(
				"%d: %s(%s)(%s) = %s -> got %v factors, expected %v",
				cases, polyK(k), poly(p1, q1), poly(p2, q2), poly(a, b, c),
				g, e,
			)
		}

		a2 := con * f[0].P * f[1].P
		b2 := con * (f[0].P*f[1].Q + f[0].Q*f[1].P)
		c2 := con * f[0].Q * f[1].Q
		if a != a2 || b != b2 || c != c2 {
			t.Fatalf(
				"%d: %s(%s)(%s) = %s -> %s(%s)(%s) = %s",
				cases, polyK(k), poly(p1, q1), poly(p2, q2), poly(a, b, c),
				polyK(con), poly(f[0].P, f[0].Q), poly(f[1].P, f[1].Q), poly(a2, b2, c2),
			)
		}

		t.Logf(
			"%d: %s(%s)(%s) = %s -> %s(%s)(%s) = %s",
			cases, polyK(k), poly(p1, q1), poly(p2, q2), poly(a, b, c),
			polyK(con), poly(f[0].P, f[0].Q), poly(f[1].P, f[1].Q), poly(a2, b2, c2),
		)
	}
}

func TestQuadPolyFactors(t *testing.T) {
	cases := 0

	const N = 1e4
	mask := 1<<14 - 1
	if IntBits < 64 {
		mask = 1<<7 - 1
	}
	rng := rand.New(rand.NewSource(42))
	for i := 0; i < N; i++ {
		p1 := int(rng.Int63()) & mask
		q1 := int(rng.Int63()) & mask
		p2 := int(rng.Int63()) & mask
		q2 := int(rng.Int63()) & mask
		k := int(rng.Int63()) & mask
		testQuadPolyFactors(t, p1, q1, p2, q2, k, cases)
		cases++
	}

	cons := []int{-1, 1}
	const lim = 7
	for p1 := -lim; p1 <= lim; p1++ {
		for q1 := -lim; q1 <= lim; q1++ {
			for p2 := -lim; p2 <= lim; p2++ {
				for q2 := -lim; q2 <= lim; q2++ {
					for _, k := range cons {
						testQuadPolyFactors(t, p1, q1, p2, q2, k, cases)
						cases++
					}
				}
			}
		}
	}
}

// https://github.com/cznic/sqlite/issues/12#issuecomment-310155204
func TestFCPRNG(t *testing.T) {
	const N = 131072
	rng, err := NewFC32(1, N, true)
	if err != nil {
		t.Fatal(err)
	}

	var mods, exp [3]int
	m := make(map[int]byte, N)
	for i := 1; i <= N; i++ {
		n := rng.Next()
		if _, ok := m[n]; ok {
			t.Fatal(i, n)
		}

		m[n] = 1
		mods[n%len(mods)]++
	}
	if g, e := len(m), N; g != e {
		t.Fatal(g, e)
	}

	for i := 1; i <= N; i++ {
		n := rng.Next()
		if m[n] != 1 {
			t.Fatal(i, n)
		}

		m[n] = 0
	}

	for i := 1; i <= N; i++ {
		exp[i%len(mods)]++
	}

	for i, g := range mods {
		if e := exp[i]; g != e {
			t.Fatal(g, e)
		}
	}

	t.Log(mods)
}
