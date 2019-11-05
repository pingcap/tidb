// Copyright (c) 2018 The mathutil Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package mathutil

import (
	"math/big"
	"math/rand"
	"testing"
)

const bits128 = 128

var (
	rnd    = rand.New(rand.NewSource(42))
	off128 *big.Int
	mod128 *big.Int
)

func init() {
	mod128 = big.NewInt(0)
	mod128.SetBit(mod128, bits128, 1)
	off128 = big.NewInt(0)
	off128.SetBit(off128, bits128-1, 1)
}

func rnd128() *big.Int {
	var n big.Int
	n.SetBit(&n, bits128, 1)
	n.Rand(rnd, &n)
	n.Sub(&n, off128)
	return &n
}

func TestMaxInt128(t *testing.T) {
	if g, e := MaxInt128.String(), "170141183460469231731687303715884105727"; g != e {
		t.Fatal(g, e)
	}
}

func TestMinInt128(t *testing.T) {
	if g, e := MinInt128.String(), "-170141183460469231731687303715884105728"; g != e {
		t.Fatal(g, e)
	}
}

func testInt128Add(t *testing.T, a, b *big.Int) {
	var x, y Int128
	if _, err := x.SetBigInt(a); err != nil {
		t.Fatal(a, err)
	}

	if _, err := y.SetBigInt(b); err != nil {
		t.Fatal(b, err)
	}

	z, gc := x.Add(y)
	g := z.BigInt()
	e := big.NewInt(0)
	e.Add(a, b)
	ec := e.Cmp(MaxInt128) > 0 || e.Cmp(MinInt128) < 0
	if gc != ec {
		t.Fatal(a, b, g, e, gc, ec)
	}

	if gc {
		switch {
		case e.Cmp(MaxInt128) > 0:
			g.Add(g, mod128)
		case e.Cmp(MinInt128) < 0:
			g.Sub(g, mod128)
		default:
			t.Fatal()
		}
	}

	if g.Cmp(e) != 0 {
		t.Fatal(a, b, g, e, gc, ec)
	}
}

func TestInt128Add(t *testing.T) {
	a := []string{
		"0x0",
		"0x1",
		"0x3fffffffffffffff",
		"0x7fffffffffffffff",
		"0xffffffffffffffff",
		"0x3fffffffffffffffffffffffffffffff",
		"0x7fffffffffffffffffffffffffffffff",
	}
	for _, i := range []int{-1, 1} {
		bi := big.NewInt(int64(i))
		for _, j := range []int{-1, 1} {
			bj := big.NewInt(int64(j))
			for _, k := range a {
				bk, ok := big.NewInt(0).SetString(k, 0)
				if !ok {
					t.Fatal()
				}

				for _, l := range a {
					bl, ok := big.NewInt(0).SetString(l, 0)
					if !ok {
						t.Fatal()
					}

					testInt128Add(t, bk.Mul(bk, bi), bl.Mul(bl, bj))
					continue
				}
			}
		}
	}
}

func TestInt128Add2(t *testing.T) {
	const n = 500000
	for i := 0; i < n; i++ {
		testInt128Add(t, rnd128(), rnd128())
	}
}

func TestInt128BigInt(t *testing.T) {
	for _, v := range []int64{-128, -1, 0, 1, 127} {
		a := big.NewInt(v)
		var b Int128
		if _, err := b.SetBigInt(a); err != nil {
			t.Fatal(v)
		}

		c := b.BigInt()
		if a.Cmp(c) != 0 {
			t.Fatal(a, c)
		}
	}
}

func TestInt128BigInt2(t *testing.T) {
	const n = 1000000
	for i := 0; i < n; i++ {
		a := rnd128()
		var b Int128
		if _, err := b.SetBigInt(a); err != nil {
			t.Fatal(a)
		}

		c := b.BigInt()
		if a.Cmp(c) != 0 {
			t.Fatal(a, c)
		}
	}
}

func testInt128Cmp(t *testing.T, a, b *big.Int) {
	var x, y Int128
	if _, err := x.SetBigInt(a); err != nil {
		t.Fatal(a, err)
	}

	if _, err := y.SetBigInt(b); err != nil {
		t.Fatal(b, err)
	}

	if g, e := x.Cmp(y), a.Cmp(b); g != e {
		t.Fatal(a, b, g, e)
	}
}

func TestInt128Cmp(t *testing.T) {
	a := []string{
		"0x0",
		"0x1",
		"0x3fffffffffffffff",
		"0x7fffffffffffffff",
		"0xffffffffffffffff",
		"0x3fffffffffffffffffffffffffffffff",
		"0x7fffffffffffffffffffffffffffffff",
	}
	for _, i := range []int{-1, 1} {
		bi := big.NewInt(int64(i))
		for _, j := range []int{-1, 1} {
			bj := big.NewInt(int64(j))
			for _, k := range a {
				bk, ok := big.NewInt(0).SetString(k, 0)
				if !ok {
					t.Fatal()
				}

				for _, l := range a {
					bl, ok := big.NewInt(0).SetString(l, 0)
					if !ok {
						t.Fatal()
					}

					testInt128Cmp(t, bk.Mul(bk, bi), bl.Mul(bl, bj))
				}
			}
		}
	}
}

func TestInt128Cmp2(t *testing.T) {
	const n = 1000000
	for i := 0; i < n; i++ {
		testInt128Cmp(t, rnd128(), rnd128())
	}
}

func testInt128Neg(t *testing.T, a *big.Int) {
	var x Int128
	if _, err := x.SetBigInt(a); err != nil {
		t.Fatal(a, err)
	}

	eok := a.Cmp(MinInt128) != 0
	y, gok := x.Neg()
	if g, e := gok, eok; g != e {
		t.Fatal(a, x, y, gok, eok)
	}

	if gok {
		a.Neg(a)
	}
	if g, e := y.BigInt(), a; g.Cmp(e) != 0 {
		t.Fatal(g, e, gok, eok)
	}
}

func TestInt128Neg(t *testing.T) {
	a := []string{
		"0x0",
		"0x1",
		"0x3fffffffffffffff",
		"0x7fffffffffffffff",
		"0xffffffffffffffff",
		"0x3fffffffffffffffffffffffffffffff",
		"0x7fffffffffffffffffffffffffffffff",
	}
	for _, i := range []int{-1, 1} {
		bi := big.NewInt(int64(i))
		for _, k := range a {
			bk, ok := big.NewInt(0).SetString(k, 0)
			if !ok {
				t.Fatal()
			}

			testInt128Neg(t, bk.Mul(bk, bi))
		}
	}
}

func TestInt128Neg2(t *testing.T) {
	const n = 1000000
	for i := 0; i < n; i++ {
		testInt128Neg(t, rnd128())
	}
}

func TestInt128SetInt64(t *testing.T) {
	const n = 1000000
	for i := 0; i < n; i++ {
		r := big.NewInt(0).SetInt64(rnd128().Int64())
		var x Int128
		y := x.SetInt64(r.Int64())
		if x.Cmp(y) != 0 {
			t.Fatal(r, x, y)
		}

		if g, e := x.BigInt(), r; g.Cmp(e) != 0 {
			t.Fatal(r, x, g)
		}
	}
}

func TestInt128SetUint64(t *testing.T) {
	const n = 1000000
	for i := 0; i < n; i++ {
		r := big.NewInt(0).SetUint64(rnd128().Uint64())
		var x Int128
		y := x.SetUint64(r.Uint64())
		if x.Cmp(y) != 0 {
			t.Fatal(r, x, y)
		}

		if g, e := x.BigInt(), r; g.Cmp(e) != 0 {
			t.Fatal(r, x, g)
		}
	}
}
