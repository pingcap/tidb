// Copyright (c) 2016 The mathutil Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package mathutil

import (
	"fmt"
	"math/big"
	"testing"
)

func TestNewFloat(t *testing.T) {
	for i, v := range []struct {
		n0, f0 int
		n, f   int
	}{
		{1, 0, 1, 0},
		{2, 0, 2, 0},
		{2, 1, 1, 0},
		{3, 0, 3, 0},
		{3, 1, 3, 1},
		{4, 0, 4, 0},
		{4, 1, 2, 0},
		{4, 2, 1, 0},
	} {
		f := newFloat(big.NewInt(int64(v.n0)), v.f0, 10)
		if g, e := f.n.Int64(), int64(v.n); g != e {
			t.Fatal(i, "n", g, e)
		}

		if g, e := f.fracBits, v.f; g != e {
			t.Fatal(i, "fracBits", g, e)
		}
	}
}

func TestFloatDiv2(t *testing.T) {
	for i, v := range []struct {
		n0, f0 int
		n, f   int
	}{
		{1, 0, 1, 1},
		{2, 0, 1, 0},
		{2, 1, 1, 1},
		{3, 0, 3, 1},
		{3, 1, 3, 2},
		{4, 0, 2, 0},
		{4, 1, 1, 0},
		{4, 2, 1, 1},
		{5, 0, 5, 1},
		{5, 1, 5, 2},
		{5, 2, 5, 3},
		{6, 0, 3, 0},
		{6, 1, 3, 1},
		{6, 2, 3, 2},
		{7, 0, 7, 1},
		{7, 1, 7, 2},
		{7, 2, 7, 3},
		{8, 0, 4, 0},
		{8, 1, 2, 0},
		{8, 2, 1, 0},
		{8, 3, 1, 1},
	} {
		f := newFloat(big.NewInt(int64(v.n0)), v.f0, 10)
		f.div2()
		if g, e := f.n.Int64(), int64(v.n); g != e {
			t.Fatal(i, "n", g, e)
		}

		if g, e := f.fracBits, v.f; g != e {
			t.Fatal(i, "fracBits", g, e)
		}
	}
}

func TestFloatSqr(t *testing.T) {
	for i, v := range []struct {
		n0, f0 int
		n, f   int
	}{
		{1, 1, 1, 2},
		{1, 0, 1, 0},
		{2, 0, 4, 0},
		{2, 1, 1, 0},
		{3, 0, 9, 0},
		{3, 1, 9, 2},
		{4, 0, 16, 0},
		{4, 1, 4, 0},
		{4, 2, 1, 0},
		{5, 0, 25, 0},
		{5, 1, 25, 2},
		{5, 2, 25, 4},
		{6, 0, 36, 0},
		{6, 1, 9, 0},
		{6, 2, 9, 2},
		{7, 0, 49, 0},
		{7, 1, 49, 2},
		{7, 2, 49, 4},
		{8, 0, 64, 0},
		{8, 1, 16, 0},
		{8, 2, 4, 0},
		{8, 3, 1, 0},
		{9, 0, 81, 0},
		{9, 1, 81, 2},
	} {
		f := newFloat(big.NewInt(int64(v.n0)), v.f0, 10)
		f.sqr()
		if g, e := f.n.Int64(), int64(v.n); g != e {
			t.Fatal(i, "n", g, e)
		}

		if g, e := f.fracBits, v.f; g != e {
			t.Fatal(i, "fracBits", g, e)
		}
	}
}

func TestFloatEq1(t *testing.T) {
	for i, v := range []struct {
		n0, f0 int
		b      bool
	}{
		{1, 1, false},
		{1, 0, true},
		{2, 0, false},
		{2, 1, true},
		{3, 0, false},
		{3, 1, false},
		{4, 0, false},
		{4, 1, false},
		{4, 2, true},
		{5, 0, false},
		{5, 1, false},
		{5, 2, false},
	} {
		f := newFloat(big.NewInt(int64(v.n0)), v.f0, 10)
		if g, e := f.eq1(), v.b; g != e {
			t.Fatal(i, g, e)
		}
	}
}

func TestFloatGe2(t *testing.T) {
	for i, v := range []struct {
		n0, f0 int
		b      bool
	}{
		{1, 1, false},
		{1, 0, false},
		{2, 0, true},
		{2, 1, false},
		{3, 0, true},
		{3, 1, false},
		{4, 0, true},
		{4, 1, true},
		{4, 2, false},
		{5, 0, true},
		{5, 1, true},
		{5, 2, false},
	} {
		f := newFloat(big.NewInt(int64(v.n0)), v.f0, 10)
		if g, e := f.ge2(), v.b; g != e {
			t.Fatal(i, g, e)
		}
	}
}

func TestFloatMaxFracBits(t *testing.T) {
	for i, v := range []struct {
		n0, f0 int
		n, f   int
	}{
		{1, 0, 1, 0},
		{2, 0, 4, 0},
		{3, 0, 9, 0},
	} {
		f := newFloat(big.NewInt(int64(v.n0)), v.f0, 0)
		f.sqr()
		if g, e := f.n.Int64(), int64(v.n); g != e {
			t.Fatal(i, "n", g, e)
		}

		if g, e := f.fracBits, v.f; g != e {
			t.Fatal(i, "fracBits", g, e)
		}
	}
	for i, v := range []struct {
		n0, f0 int
		n, f   int
	}{
		{1, 0, 1, 0},
		{2, 0, 4, 0},
		{2, 1, 1, 0},
		{3, 0, 9, 0},
		{3, 1, 5, 1},
		{4, 0, 16, 0},
		{4, 1, 4, 0},
		{5, 0, 25, 0},
		{5, 1, 13, 1},
		{6, 0, 36, 0},
		{6, 1, 9, 0},
		{7, 0, 49, 0},
		{7, 1, 25, 1},
		{8, 0, 64, 0},
		{8, 1, 16, 0},
	} {
		f := newFloat(big.NewInt(int64(v.n0)), v.f0, 1)
		f.sqr()
		if g, e := f.n.Int64(), int64(v.n); g != e {
			t.Fatal(i, "n", g, e)
		}

		if g, e := f.fracBits, v.f; g != e {
			t.Fatal(i, "fracBits", g, e)
		}
	}
	for i, v := range []struct {
		n0, f0 int
		n, f   int
	}{
		{1, 0, 1, 0},
		{2, 0, 4, 0},
		{2, 1, 1, 0},
		{3, 0, 9, 0},
		{3, 1, 9, 2},
		{4, 0, 16, 0},
		{4, 1, 4, 0},
		{4, 2, 1, 0},
		{5, 0, 25, 0},
		{5, 1, 25, 2},
		{5, 2, 3, 1},
		{6, 0, 36, 0},
		{6, 1, 9, 0},
		{6, 2, 9, 2},
		{7, 0, 49, 0},
		{7, 1, 49, 2},
		{7, 2, 3, 0},
		{8, 0, 64, 0},
		{8, 1, 16, 0},
		{8, 2, 4, 0},
		{9, 0, 81, 0},
		{9, 1, 81, 2},
		{9, 2, 5, 0},
	} {
		f := newFloat(big.NewInt(int64(v.n0)), v.f0, 2)
		f.sqr()
		if g, e := f.n.Int64(), int64(v.n); g != e {
			t.Fatal(i, "n", g, e)
		}

		if g, e := f.fracBits, v.f; g != e {
			t.Fatal(i, "fracBits", g, e)
		}
	}
	for i, v := range []struct {
		n0, f0 int
		n, f   int
	}{
		{1, 0, 1, 0},
		{2, 0, 4, 0},
		{2, 1, 1, 0},
		{3, 0, 9, 0},
		{3, 1, 9, 2},
		{4, 0, 16, 0},
		{4, 1, 4, 0},
		{4, 2, 1, 0},
		{5, 0, 25, 0},
		{5, 1, 25, 2},
		{5, 2, 13, 3},
		{6, 0, 36, 0},
		{6, 1, 9, 0},
		{6, 2, 9, 2},
		{7, 0, 49, 0},
		{7, 1, 49, 2},
		{7, 2, 25, 3},
		{8, 0, 64, 0},
		{8, 1, 16, 0},
		{8, 2, 4, 0},
		{8, 3, 1, 0},
		{9, 0, 81, 0},
		{9, 1, 81, 2},
		{9, 2, 41, 3},
		{9, 3, 5, 2},
		{10, 0, 100, 0},
		{10, 1, 25, 0},
		{10, 2, 25, 2},
		{10, 3, 13, 3},
		{11, 0, 121, 0},
		{11, 1, 121, 2},
		{11, 2, 61, 3},
		{11, 3, 15, 3},
	} {
		f := newFloat(big.NewInt(int64(v.n0)), v.f0, 3)
		f.sqr()
		if g, e := f.n.Int64(), int64(v.n); g != e {
			t.Fatal(i, "n", g, e)
		}

		if g, e := f.fracBits, v.f; g != e {
			t.Fatal(i, "fracBits", g, e)
		}
	}
}

func TestBinaryLog(t *testing.T) {
	for i, v := range []struct {
		n, b int
		c    int
		m    string
	}{
		{1, 0, 0, "0"},
		{1, 1, 0, "0"},
		{2, 0, 1, "0"},
		{2, 1, 1, "0"},
		{3, 0, 1, "0"},
		{3, 1, 1, "1"},
		{3, 2, 1, "10"},
		{3, 3, 1, "100"},
		{3, 4, 1, "1001"},
		{3, 20, 1, "10010101110000000001"},
		{42, 20, 5, "1100100011011101110"},
		{700, 20, 9, "1110011100000101001"},
	} {
		c, m := BinaryLog(big.NewInt(int64(v.n)), v.b)
		if g, e := c, v.c; g != e {
			t.Fatalf("characteristic[%v]: %v %v", i, g, e)
		}

		if g, e := fmt.Sprintf("%b", m), v.m; g != e {
			t.Fatalf("mantissa[%v]: %v %v", i, g, e)
		}
	}
}

func ExampleBinaryLog() {
	const mantBits = 257
	x, _ := big.NewInt(0).SetString("89940344608680314083397671686667731393131665861770496634981932531495305005604", 10)
	c, m := BinaryLog(x, mantBits)
	f := big.NewFloat(0).SetPrec(mantBits).SetInt(m)
	f = f.SetMantExp(f, -mantBits)
	f.Add(f, big.NewFloat(float64(c)))
	f.Quo(f, big.NewFloat(4))
	fmt.Printf("%.75f", f)
	// Output:
	// 63.908875905794799149011030723455843229394283193466612998787786375106246936971
}
