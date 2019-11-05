package bigfft

import (
	"fmt"
	. "math/big"
	"math/rand"
	"testing"
)

func cmpnat(t *testing.T, x, y nat) int {
	var a, b Int
	a.SetBits(x)
	b.SetBits(y)
	c := a.Cmp(&b)
	if c != 0 {
		t.Logf("a.len=%d, b.len=%d", a.BitLen(), b.BitLen())
		for i := 0; i < len(x) || i < len(y); i++ {
			var u, v Word
			if i < len(x) {
				u = x[i]
			}
			if i < len(y) {
				v = y[i]
			}
			if diff := u ^ v; diff != 0 {
				t.Logf("diff at word %d: %x", i, diff)
			}
		}
	}
	return c
}

func TestRoundTripIntPoly(t *testing.T) {
	N := 4
	step := 500
	if testing.Short() {
		N = 2
	}
	// Sizes 12800 and 34300 may cause problems.
	for size := 300; size < 50000; size += step {
		n := make(nat, size)
		for i := 0; i < N; i++ {
			for p := range n {
				n[p] = Word(rand.Int63())
			}
			k, m := fftSize(n, nil)
			pol := polyFromNat(n, k, m)
			n2 := pol.Int()
			if cmpnat(t, n, n2) != 0 {
				t.Errorf("different n and n2, size=%d, iter=%d", size, i)
			}
		}
	}
}

func TestFourierSizes(t *testing.T) {
	sizes := []int{
		2e3, 3e3, 5e3, 7e3, 10e3, 14e3,
		2e4, 3e4, 5e4, 7e4, 10e4, 14e4,
		2e5, 3e5, 5e5, 7e5, 10e5, 14e5,
		2e6, 3e6, 5e6, 7e6, 10e6, 14e6,
		2e7, 3e7, 5e7, 7e7, 10e7, 14e7,
		2e8, 3e8, 5e8, 7e8, 10e8, 14e8,
	}
	for _, s := range sizes {
		k, m := fftSize(make(nat, s/_W), make(nat, s/_W))
		v := valueSize(k, m, 2)
		t.Logf("bits=%d => FFT size %d, chunk size = %d, value size = %d",
			s, 1<<k, m, v)
		needed := 2*m*_W + int(k)
		got := v * _W
		t.Logf("inefficiency: value/chunk_product=%.2f, fftsize/inputsize=%.2f",
			float64(got)/float64(needed), float64(v<<k)/float64(2*s/_W))
		if v > 3*m {
			t.Errorf("FFT word size %d >> input word size %d", v, m)
		}
	}
}

func testFourier(t *testing.T, N int, k uint) {
	// Random coefficients
	src := make([]fermat, 1<<k)
	for i := range src {
		src[i] = make(fermat, N+1)
		for p := 0; p < N; p++ {
			src[i][p] = Word(rnd.Int63())
		}
	}
	cmpFourier(t, N, k, src, false)
	cmpFourier(t, N, k, src, true)

	// Saturated coefficients (b^N-1)
	for i := range src {
		for p := 0; p < N; p++ {
			src[i][p] = ^Word(0)
		}
	}
	cmpFourier(t, N, k, src, false)
	cmpFourier(t, N, k, src, true)
}

// cmpFourier computes the Fourier transform of src
// and compares it to the FFT result.
func cmpFourier(t *testing.T, N int, k uint, src []fermat, inverse bool) {
	t.Logf("testFourier(t, %d, %d, inverse=%v)", N, k, inverse)
	ωshift := (4 * N * _W) >> k
	if inverse {
		ωshift = -ωshift
	}
	dst1 := make([]fermat, 1<<k)
	dst2 := make([]fermat, 1<<k)
	for i := range src {
		dst1[i] = make(fermat, N+1)
		dst2[i] = make(fermat, N+1)
	}

	// naive transform
	tmp := make(fermat, N+1)
	tmp2 := make(fermat, N+1)
	for i := range src {
		for j := range dst1 {
			tmp.ShiftHalf(src[i], i*j*ωshift, tmp2)
			dst1[j].Add(dst1[j], tmp)
		}
	}

	// fast transform
	fourier(dst2, src, inverse, N, k)

	for i := range src {
		if cmpnat(t, nat(dst1[i]), nat(dst2[i])) != 0 {
			var x, y Int
			x.SetBits(dst1[i])
			y.SetBits(dst2[i])
			t.Errorf("difference in dst[%d]: %x %x", i, &x, &y)
		}
	}
}

func TestFourier(t *testing.T) {
	// 1-word transforms.
	testFourier(t, 1, 2)
	testFourier(t, 1, 3)
	testFourier(t, 1, 4)

	// 2-word transforms
	testFourier(t, 2, 2)
	testFourier(t, 2, 3)
	testFourier(t, 2, 4)
	testFourier(t, 2, 8)

	testFourier(t, 4, 4)
	testFourier(t, 4, 5)
	testFourier(t, 4, 6)
	testFourier(t, 4, 8)

	// Test a few limit cases. This is when
	// N*WordSize is a multiple of 1<<(k-2) but not 1<<(k-1)
	if _W == 64 {
		testFourier(t, 1, 8)
		testFourier(t, 3, 8)
		testFourier(t, 5, 8)
		testFourier(t, 7, 8)
		testFourier(t, 9, 8)
		testFourier(t, 11, 8)
	}
}

// Tests Fourier transform and its reciprocal.
func TestRoundTripPolyValues(t *testing.T) {
	Size := 100000
	if testing.Short() {
		Size = 50
	}
	// Build a polynomial from an integer.
	n := make(nat, Size)
	for p := range n {
		n[p] = Word(rand.Int63())
	}
	k, m := fftSize(n, nil)
	pol := polyFromNat(n, k, m)

	// Transform it.
	f := valueSize(k, m, 1)
	values := pol.Transform(f)

	// Inverse transform.
	pol2 := values.InvTransform()
	pol2.m = m

	t.Logf("k=%d, m=%d", k, m)

	// Evaluate and compare.
	n2 := pol2.Int()
	if cmpnat(t, n, n2) != 0 {
		t.Errorf("different n and n2")
	}
}

var rnd = rand.New(rand.NewSource(0x43de683f473542af))

func rndNat(n int) nat {
	x := make(nat, n)
	for i := 0; i < n; i++ {
		x[i] = Word(rnd.Int63()<<1 + rnd.Int63n(2))
	}
	return x
}

func TestMul(t *testing.T) {
	sizes := []int{1e3, 5e3, 15e3, 25e3, 70e3, 200e3, 500e3}
	iters := 10
	if testing.Short() {
		iters = 1
	}

	var x, y Int
	for i := 0; i < iters; i++ {
		for _, size1 := range sizes {
			for _, size2 := range sizes {
				x.SetBits(rndNat(size1 / _W))
				y.SetBits(rndNat(size2 / _W))
				z := new(Int).Mul(&x, &y)
				z2 := Mul(&x, &y)
				if z.Cmp(z2) != 0 {
					t.Errorf("z (%d bits) != z2 (%d bits)", z.BitLen(), z2.BitLen())
					logbig(t, new(Int).Xor(z, z2))
				}
			}
		}
	}
}

func logbig(t *testing.T, n *Int) {
	s := fmt.Sprintf("%x", n)
	for len(s) > 64 {
		t.Log(s[:64])
		s = s[64:]
	}
	t.Log(s)
}

func benchmarkMulBig(b *testing.B, sizex, sizey int) {
	mulx := rndNat(sizex / _W)
	muly := rndNat(sizey / _W)
	b.ResetTimer()
	var x, y, z Int
	x.SetBits(mulx)
	y.SetBits(muly)
	for i := 0; i < b.N; i++ {
		z.Mul(&x, &y)
	}
}

func benchmarkMulFFT(b *testing.B, sizex, sizey int) {
	mulx := rndNat(sizex / _W)
	muly := rndNat(sizey / _W)
	b.ResetTimer()
	var x, y Int
	x.SetBits(mulx)
	y.SetBits(muly)
	for i := 0; i < b.N; i++ {
		_ = mulFFT(&x, &y)
	}
}

func BenchmarkMulBig_1kb(b *testing.B)   { benchmarkMulBig(b, 1e3, 1e3) }
func BenchmarkMulBig_10kb(b *testing.B)  { benchmarkMulBig(b, 1e4, 1e4) }
func BenchmarkMulBig_50kb(b *testing.B)  { benchmarkMulBig(b, 5e4, 5e4) }
func BenchmarkMulBig_100kb(b *testing.B) { benchmarkMulBig(b, 1e5, 1e5) }
func BenchmarkMulBig_200kb(b *testing.B) { benchmarkMulBig(b, 2e5, 2e5) }
func BenchmarkMulBig_500kb(b *testing.B) { benchmarkMulBig(b, 5e5, 5e5) }
func BenchmarkMulBig_1Mb(b *testing.B)   { benchmarkMulBig(b, 1e6, 1e6) }
func BenchmarkMulBig_2Mb(b *testing.B)   { benchmarkMulBig(b, 2e6, 2e6) }
func BenchmarkMulBig_5Mb(b *testing.B)   { benchmarkMulBig(b, 5e6, 5e6) }
func BenchmarkMulBig_10Mb(b *testing.B)  { benchmarkMulBig(b, 10e6, 10e6) }
func BenchmarkMulBig_20Mb(b *testing.B)  { benchmarkMulBig(b, 20e6, 20e6) }
func BenchmarkMulBig_50Mb(b *testing.B)  { benchmarkMulBig(b, 50e6, 50e6) }
func BenchmarkMulBig_100Mb(b *testing.B) { benchmarkMulBig(b, 100e6, 100e6) }

func BenchmarkMulFFT_1kb(b *testing.B)   { benchmarkMulFFT(b, 1e3, 1e3) }
func BenchmarkMulFFT_10kb(b *testing.B)  { benchmarkMulFFT(b, 1e4, 1e4) }
func BenchmarkMulFFT_50kb(b *testing.B)  { benchmarkMulFFT(b, 5e4, 5e4) }
func BenchmarkMulFFT_100kb(b *testing.B) { benchmarkMulFFT(b, 1e5, 1e5) }
func BenchmarkMulFFT_200kb(b *testing.B) { benchmarkMulFFT(b, 2e5, 2e5) }
func BenchmarkMulFFT_500kb(b *testing.B) { benchmarkMulFFT(b, 5e5, 5e5) }
func BenchmarkMulFFT_1Mb(b *testing.B)   { benchmarkMulFFT(b, 1e6, 1e6) }
func BenchmarkMulFFT_2Mb(b *testing.B)   { benchmarkMulFFT(b, 2e6, 2e6) }
func BenchmarkMulFFT_5Mb(b *testing.B)   { benchmarkMulFFT(b, 5e6, 5e6) }
func BenchmarkMulFFT_10Mb(b *testing.B)  { benchmarkMulFFT(b, 10e6, 10e6) }
func BenchmarkMulFFT_20Mb(b *testing.B)  { benchmarkMulFFT(b, 20e6, 20e6) }
func BenchmarkMulFFT_50Mb(b *testing.B)  { benchmarkMulFFT(b, 50e6, 50e6) }
func BenchmarkMulFFT_100Mb(b *testing.B) { benchmarkMulFFT(b, 100e6, 100e6) }
func BenchmarkMulFFT_200Mb(b *testing.B) { benchmarkMulFFT(b, 200e6, 200e6) }
func BenchmarkMulFFT_500Mb(b *testing.B) { benchmarkMulFFT(b, 500e6, 500e6) }
func BenchmarkMulFFT_1Gb(b *testing.B)   { benchmarkMulFFT(b, 1e9, 1e9) }

func benchmarkMul(b *testing.B, sizex, sizey int) {
	mulx := rndNat(sizex / _W)
	muly := rndNat(sizey / _W)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var x, y Int
		x.SetBits(mulx)
		y.SetBits(muly)
		_ = Mul(&x, &y)
	}
}

func BenchmarkMul_50kb(b *testing.B)  { benchmarkMul(b, 5e4, 5e4) }
func BenchmarkMul_100kb(b *testing.B) { benchmarkMul(b, 1e5, 1e5) }
func BenchmarkMul_200kb(b *testing.B) { benchmarkMul(b, 2e5, 2e5) }
func BenchmarkMul_500kb(b *testing.B) { benchmarkMul(b, 5e5, 5e5) }
func BenchmarkMul_1Mb(b *testing.B)   { benchmarkMul(b, 1e6, 1e6) }
func BenchmarkMul_2Mb(b *testing.B)   { benchmarkMul(b, 2e6, 2e6) }
func BenchmarkMul_5Mb(b *testing.B)   { benchmarkMul(b, 5e6, 5e6) }
func BenchmarkMul_10Mb(b *testing.B)  { benchmarkMul(b, 10e6, 10e6) }
func BenchmarkMul_20Mb(b *testing.B)  { benchmarkMul(b, 20e6, 20e6) }
func BenchmarkMul_50Mb(b *testing.B)  { benchmarkMul(b, 50e6, 50e6) }
func BenchmarkMul_100Mb(b *testing.B) { benchmarkMul(b, 100e6, 100e6) }

// Unbalanced multiplication benchmarks
func BenchmarkMul_1x5Mb(b *testing.B)  { benchmarkMul(b, 1e6, 5e6) }
func BenchmarkMul_1x10Mb(b *testing.B) { benchmarkMul(b, 1e6, 10e6) }
func BenchmarkMul_1x20Mb(b *testing.B) { benchmarkMul(b, 1e6, 20e6) }
func BenchmarkMul_1x50Mb(b *testing.B) { benchmarkMul(b, 1e6, 50e6) }
func BenchmarkMul_5x20Mb(b *testing.B) { benchmarkMul(b, 5e6, 20e6) }
func BenchmarkMul_5x50Mb(b *testing.B) { benchmarkMul(b, 5e6, 50e6) }

func BenchmarkMulBig_1x5Mb(b *testing.B)  { benchmarkMulBig(b, 1e6, 5e6) }
func BenchmarkMulBig_1x10Mb(b *testing.B) { benchmarkMulBig(b, 1e6, 10e6) }
func BenchmarkMulBig_1x20Mb(b *testing.B) { benchmarkMulBig(b, 1e6, 20e6) }
func BenchmarkMulBig_1x50Mb(b *testing.B) { benchmarkMulBig(b, 1e6, 50e6) }
func BenchmarkMulBig_5x20Mb(b *testing.B) { benchmarkMulBig(b, 5e6, 20e6) }
func BenchmarkMulBig_5x50Mb(b *testing.B) { benchmarkMulBig(b, 5e6, 50e6) }

func BenchmarkMulFFT_1x5Mb(b *testing.B)  { benchmarkMulFFT(b, 1e6, 5e6) }
func BenchmarkMulFFT_1x10Mb(b *testing.B) { benchmarkMulFFT(b, 1e6, 10e6) }
func BenchmarkMulFFT_1x20Mb(b *testing.B) { benchmarkMulFFT(b, 1e6, 20e6) }
func BenchmarkMulFFT_1x50Mb(b *testing.B) { benchmarkMulFFT(b, 1e6, 50e6) }
func BenchmarkMulFFT_5x20Mb(b *testing.B) { benchmarkMulFFT(b, 5e6, 20e6) }
func BenchmarkMulFFT_5x50Mb(b *testing.B) { benchmarkMulFFT(b, 5e6, 50e6) }

func TestIssue1(t *testing.T) {
	e := NewInt(1)
	e.SetBit(e, 132048, 1)
	e.Sub(e, NewInt(4)) // e == 1<<132048 - 4
	g := NewInt(0).Set(e)
	e.Mul(e, e)
	g = Mul(g, g)
	if g.Cmp(e) != 0 {
		t.Fatal("incorrect Mul result")
	}
}
