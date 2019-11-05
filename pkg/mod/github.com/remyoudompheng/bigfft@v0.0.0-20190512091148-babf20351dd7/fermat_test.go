package bigfft

import (
	"fmt"
	. "math/big"
	"testing"
)

// parseHex reads an hex-formatted number modulo 2^bits+1.
func parseHex(s string, bits int) fermat {
	z := new(Int)
	z, ok := z.SetString(s, 0)
	if !ok {
		panic(s)
	}
	f := fermat(z.Bits())
	for len(f)*_W <= bits {
		f = append(f, 0)
	}
	return f
}

func compare(t *testing.T, prefix string, a, b fermat) {
	var x, y Int
	x.SetBits(a)
	y.SetBits(b)
	if x.Cmp(&y) != 0 {
		t.Errorf("%s: %x != %x", prefix, &x, &y)
	}
}

func TestFermatShift(t *testing.T) {
	const n = 4
	f := make(fermat, n+1)
	for i := 0; i < n; i++ {
		f[i] = Word(rnd.Int63())
	}
	b := NewInt(1)
	b = b.Lsh(b, uint(n*_W))
	b = b.Add(b, NewInt(1))
	z := make(fermat, len(f)) // Test with uninitialized z.
	for shift := -2048; shift < 2048; shift++ {
		z.Shift(f, shift)

		z2 := new(Int)
		z2.SetBits(f)
		if shift < 0 {
			s2 := (-shift) % (2 * n * _W)
			z2 = z2.Lsh(z2, uint(2*n*_W-s2))
		} else {
			z2 = z2.Lsh(z2, uint(shift))
		}
		z2 = z2.Mod(z2, b)
		compare(t, fmt.Sprintf("shift %d", shift), z, z2.Bits())
	}
}

func TestFermatShiftHalf(t *testing.T) {
	const n = 3
	f := make(fermat, n+1)
	for i := 0; i < n; i++ {
		f[i] = ^Word(0)
	}
	b := NewInt(1)
	b = b.Lsh(b, uint(n*_W))
	b = b.Add(b, NewInt(1))
	z := make(fermat, len(f)) // Test with uninitialized z.
	tmp := make(fermat, len(f))
	tmp2 := make(fermat, len(f))
	for shift := 0; shift < 16384; shift++ {
		// Shift twice by shift/2
		z.ShiftHalf(f, shift, tmp)
		copy(tmp, z)
		z.ShiftHalf(tmp, shift, tmp2)

		z2 := new(Int)
		z2 = z2.Lsh(new(Int).SetBits(f), uint(shift))
		z2 = z2.Mod(z2, b)
		compare(t, fmt.Sprintf("shift %d", shift), z, z2.Bits())
	}
}

type test struct{ a, b, c fermat }

// addTests is a series of mod 2^256+1 tests.
var addTests = []test{
	{
		parseHex("0x5555555555555555555555555555555555555555555555555555555555555555", 256),
		parseHex("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab", 256),
		parseHex("0x10000000000000000000000000000000000000000000000000000000000000000", 256),
	},
	{
		parseHex("0x5555555555555555555555555555555555555555555555555555555555555555", 256),
		parseHex("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 256),
		parseHex("0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff", 256),
	},
	{
		parseHex("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 256),
		parseHex("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 256),
		parseHex("0x5555555555555555555555555555555555555555555555555555555555555553", 256),
	},
}

func TestFermatAdd(t *testing.T) {
	for i, item := range addTests {
		z := make(fermat, len(item.a))
		z = z.Add(item.a, item.b)
		compare(t, fmt.Sprintf("addTests[%d]", i), z, item.c)
	}
}

var mulTests = []test{
	{ // 3^400 = 3^200 * 3^200
		parseHex("0xc21a937a76f3432ffd73d97e447606b683ecf6f6e4a7ae223c2578e26c486a03", 256),
		parseHex("0xc21a937a76f3432ffd73d97e447606b683ecf6f6e4a7ae223c2578e26c486a03", 256),
		parseHex("0x0e65f4d3508036eaca8faa2b8194ace009c863e44bdc040c459a7127bf8bcc62", 256),
	},
	{ // 2^256 * 2^256 mod (2^256+1) = 1.
		parseHex("0x10000000000000000000000000000000000000000000000000000000000000000", 256),
		parseHex("0x10000000000000000000000000000000000000000000000000000000000000000", 256),
		parseHex("0x1", 256),
	},
	{ // (2^256-1) * (2^256-1) mod (2^256+1) = 4.
		parseHex("0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff", 256),
		parseHex("0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff", 256),
		parseHex("0x4", 256),
	},
	{ // 1<<(64W) * 1<<(64W) mod (1<<64W+1) = 1
		fermat{64: 1},
		fermat{64: 1},
		fermat{0: 1},
	},
	{
		// Test case from issue 1. One of the squares of the Fourier
		// transforms was miscomputed.
		// The input number is made of 18 words, but we are working modulo 2^1280+1
		parseHex("0xfffffffffffffffffffffffeffffffffffffffffffffffffffffffffffff00000000000000000000000100000000000000000000000000000000000000000000000000000000fffeffffffffffffffffffffffffffffffffffffffffffffffffffffffff000100000000000000000000000100000000000000000000000000000000fffefffffffffffffffffffd", 1280),
		parseHex("0xfffffffffffffffffffffffeffffffffffffffffffffffffffffffffffff00000000000000000000000100000000000000000000000000000000000000000000000000000000fffeffffffffffffffffffffffffffffffffffffffffffffffffffffffff000100000000000000000000000100000000000000000000000000000000fffefffffffffffffffffffd", 1280),
		parseHex("0xfffe00000003fffc0000000000000000fff40003000000000000000000060001fffffffd0001fffffffffffffffe000dfffbfffffffffffffffffffafffe0000000200000000000000000002fff60002fffffffffffffffa00060001ffffffff0000000000000000fffc0007fffe0000000000000007fff8fffdfffffffffffffffffffa00000004fffa0000fffffffffffffff600080000000000000000000a", 1280),
	},
}

func TestFermatMul(t *testing.T) {
	for i, item := range mulTests {
		z := make(fermat, 3*len(item.a))
		z = z.Mul(item.a, item.b)
		compare(t, fmt.Sprintf("mulTests[%d]", i), z, item.c)
	}
}
