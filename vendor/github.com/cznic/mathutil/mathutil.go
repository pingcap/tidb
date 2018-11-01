// Copyright (c) 2014 The mathutil Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package mathutil provides utilities supplementing the standard 'math' and
// 'math/rand' packages.
//
// Release history and compatibility issues
//
// 2018-10-21 Added BinaryLog
//
// 2018-04-25: New functions for determinig Max/Min of nullable values. Ex:
//  func MaxPtr(a, b *int) *int {
//  func MinPtr(a, b *int) *int {
//  func MaxBytePtr(a, b *byte) *byte {
//  func MinBytePtr(a, b *byte) *byte {
//  ...
//
// 2017-10-14: New variadic functions for Max/Min. Ex:
//  func MaxVal(val int, vals ...int) int {
//  func MinVal(val int, vals ...int) int {
//  func MaxByteVal(val byte, vals ...byte) byte {
//  func MinByteVal(val byte, vals ...byte) byte {
//  ...
//
// 2016-10-10: New functions QuadPolyDiscriminant and QuadPolyFactors.
//
// 2013-12-13: The following functions have been REMOVED
//
// 	func Uint64ToBigInt(n uint64) *big.Int
// 	func Uint64FromBigInt(n *big.Int) (uint64, bool)
//
// 2013-05-13: The following functions are now DEPRECATED
//
// 	func Uint64ToBigInt(n uint64) *big.Int
// 	func Uint64FromBigInt(n *big.Int) (uint64, bool)
//
// These functions will be REMOVED with Go release 1.1+1.
//
// 2013-01-21: The following functions have been REMOVED
//
// 	func MaxInt() int
// 	func MinInt() int
// 	func MaxUint() uint
// 	func UintPtrBits() int
//
// They are now replaced by untyped constants
//
// 	MaxInt
// 	MinInt
// 	MaxUint
// 	UintPtrBits
//
// Additionally one more untyped constant was added
//
// 	IntBits
//
// This change breaks any existing code depending on the above removed
// functions.  They should have not been published in the first place, that was
// unfortunate. Instead, defining such architecture and/or implementation
// specific integer limits and bit widths as untyped constants improves
// performance and allows for static dead code elimination if it depends on
// these values. Thanks to minux for pointing it out in the mail list
// (https://groups.google.com/d/msg/golang-nuts/tlPpLW6aJw8/NT3mpToH-a4J).
//
// 2012-12-12: The following functions will be DEPRECATED with Go release
// 1.0.3+1 and REMOVED with Go release 1.0.3+2, b/c of
// http://code.google.com/p/go/source/detail?r=954a79ee3ea8
//
// 	func Uint64ToBigInt(n uint64) *big.Int
// 	func Uint64FromBigInt(n *big.Int) (uint64, bool)
package mathutil

import (
	"math"
	"math/big"
)

// Architecture and/or implementation specific integer limits and bit widths.
const (
	MaxInt      = 1<<(IntBits-1) - 1
	MinInt      = -MaxInt - 1
	MaxUint     = 1<<IntBits - 1
	IntBits     = 1 << (^uint(0)>>32&1 + ^uint(0)>>16&1 + ^uint(0)>>8&1 + 3)
	UintPtrBits = 1 << (^uintptr(0)>>32&1 + ^uintptr(0)>>16&1 + ^uintptr(0)>>8&1 + 3)
)

var (
	_m1 = big.NewInt(-1)
	_1  = big.NewInt(1)
	_2  = big.NewInt(2)
)

// GCDByte returns the greatest common divisor of a and b. Based on:
// http://en.wikipedia.org/wiki/Euclidean_algorithm#Implementations
func GCDByte(a, b byte) byte {
	for b != 0 {
		a, b = b, a%b
	}
	return a
}

// GCDUint16 returns the greatest common divisor of a and b.
func GCDUint16(a, b uint16) uint16 {
	for b != 0 {
		a, b = b, a%b
	}
	return a
}

// GCDUint32 returns the greatest common divisor of a and b.
func GCDUint32(a, b uint32) uint32 {
	for b != 0 {
		a, b = b, a%b
	}
	return a
}

// GCDUint64 returns the greatest common divisor of a and b.
func GCDUint64(a, b uint64) uint64 {
	for b != 0 {
		a, b = b, a%b
	}
	return a
}

// ISqrt returns floor(sqrt(n)). Typical run time is few hundreds of ns.
func ISqrt(n uint32) (x uint32) {
	if n == 0 {
		return
	}

	if n >= math.MaxUint16*math.MaxUint16 {
		return math.MaxUint16
	}

	var px, nx uint32
	for x = n; ; px, x = x, nx {
		nx = (x + n/x) / 2
		if nx == x || nx == px {
			break
		}
	}
	return
}

// SqrtUint64 returns floor(sqrt(n)). Typical run time is about 0.5 µs.
func SqrtUint64(n uint64) (x uint64) {
	if n == 0 {
		return
	}

	if n >= math.MaxUint32*math.MaxUint32 {
		return math.MaxUint32
	}

	var px, nx uint64
	for x = n; ; px, x = x, nx {
		nx = (x + n/x) / 2
		if nx == x || nx == px {
			break
		}
	}
	return
}

// SqrtBig returns floor(sqrt(n)). It panics on n < 0.
func SqrtBig(n *big.Int) (x *big.Int) {
	switch n.Sign() {
	case -1:
		panic(-1)
	case 0:
		return big.NewInt(0)
	}

	var px, nx big.Int
	x = big.NewInt(0)
	x.SetBit(x, n.BitLen()/2+1, 1)
	for {
		nx.Rsh(nx.Add(x, nx.Div(n, x)), 1)
		if nx.Cmp(x) == 0 || nx.Cmp(&px) == 0 {
			break
		}
		px.Set(x)
		x.Set(&nx)
	}
	return
}

// Log2Byte returns log base 2 of n. It's the same as index of the highest
// bit set in n.  For n == 0 -1 is returned.
func Log2Byte(n byte) int {
	return log2[n]
}

// Log2Uint16 returns log base 2 of n. It's the same as index of the highest
// bit set in n.  For n == 0 -1 is returned.
func Log2Uint16(n uint16) int {
	if b := n >> 8; b != 0 {
		return log2[b] + 8
	}

	return log2[n]
}

// Log2Uint32 returns log base 2 of n. It's the same as index of the highest
// bit set in n.  For n == 0 -1 is returned.
func Log2Uint32(n uint32) int {
	if b := n >> 24; b != 0 {
		return log2[b] + 24
	}

	if b := n >> 16; b != 0 {
		return log2[b] + 16
	}

	if b := n >> 8; b != 0 {
		return log2[b] + 8
	}

	return log2[n]
}

// Log2Uint64 returns log base 2 of n. It's the same as index of the highest
// bit set in n.  For n == 0 -1 is returned.
func Log2Uint64(n uint64) int {
	if b := n >> 56; b != 0 {
		return log2[b] + 56
	}

	if b := n >> 48; b != 0 {
		return log2[b] + 48
	}

	if b := n >> 40; b != 0 {
		return log2[b] + 40
	}

	if b := n >> 32; b != 0 {
		return log2[b] + 32
	}

	if b := n >> 24; b != 0 {
		return log2[b] + 24
	}

	if b := n >> 16; b != 0 {
		return log2[b] + 16
	}

	if b := n >> 8; b != 0 {
		return log2[b] + 8
	}

	return log2[n]
}

// ModPowByte computes (b^e)%m. It panics for m == 0 || b == e == 0.
//
// See also: http://en.wikipedia.org/wiki/Modular_exponentiation#Right-to-left_binary_method
func ModPowByte(b, e, m byte) byte {
	if b == 0 && e == 0 {
		panic(0)
	}

	if m == 1 {
		return 0
	}

	r := uint16(1)
	for b, m := uint16(b), uint16(m); e > 0; b, e = b*b%m, e>>1 {
		if e&1 == 1 {
			r = r * b % m
		}
	}
	return byte(r)
}

// ModPowUint16 computes (b^e)%m. It panics for m == 0 || b == e == 0.
func ModPowUint16(b, e, m uint16) uint16 {
	if b == 0 && e == 0 {
		panic(0)
	}

	if m == 1 {
		return 0
	}

	r := uint32(1)
	for b, m := uint32(b), uint32(m); e > 0; b, e = b*b%m, e>>1 {
		if e&1 == 1 {
			r = r * b % m
		}
	}
	return uint16(r)
}

// ModPowUint32 computes (b^e)%m. It panics for m == 0 || b == e == 0.
func ModPowUint32(b, e, m uint32) uint32 {
	if b == 0 && e == 0 {
		panic(0)
	}

	if m == 1 {
		return 0
	}

	r := uint64(1)
	for b, m := uint64(b), uint64(m); e > 0; b, e = b*b%m, e>>1 {
		if e&1 == 1 {
			r = r * b % m
		}
	}
	return uint32(r)
}

// ModPowUint64 computes (b^e)%m. It panics for m == 0 || b == e == 0.
func ModPowUint64(b, e, m uint64) (r uint64) {
	if b == 0 && e == 0 {
		panic(0)
	}

	if m == 1 {
		return 0
	}

	return modPowBigInt(big.NewInt(0).SetUint64(b), big.NewInt(0).SetUint64(e), big.NewInt(0).SetUint64(m)).Uint64()
}

func modPowBigInt(b, e, m *big.Int) (r *big.Int) {
	r = big.NewInt(1)
	for i, n := 0, e.BitLen(); i < n; i++ {
		if e.Bit(i) != 0 {
			r.Mod(r.Mul(r, b), m)
		}
		b.Mod(b.Mul(b, b), m)
	}
	return
}

// ModPowBigInt computes (b^e)%m. Returns nil for e < 0. It panics for m == 0 || b == e == 0.
func ModPowBigInt(b, e, m *big.Int) (r *big.Int) {
	if b.Sign() == 0 && e.Sign() == 0 {
		panic(0)
	}

	if m.Cmp(_1) == 0 {
		return big.NewInt(0)
	}

	if e.Sign() < 0 {
		return
	}

	return modPowBigInt(big.NewInt(0).Set(b), big.NewInt(0).Set(e), m)
}

var uint64ToBigIntDelta big.Int

func init() {
	uint64ToBigIntDelta.SetBit(&uint64ToBigIntDelta, 63, 1)
}

var uintptrBits int

func init() {
	x := uint64(math.MaxUint64)
	uintptrBits = BitLenUintptr(uintptr(x))
}

// UintptrBits returns the bit width of an uintptr at the executing machine.
func UintptrBits() int {
	return uintptrBits
}

// AddUint128_64 returns the uint128 sum of uint64 a and b.
func AddUint128_64(a, b uint64) (hi uint64, lo uint64) {
	lo = a + b
	if lo < a {
		hi = 1
	}
	return hi, lo
}

// MulUint128_64 returns the uint128 bit product of uint64 a and b.
func MulUint128_64(a, b uint64) (hi, lo uint64) {
	/*
		2^(2 W) ahi bhi + 2^W alo bhi + 2^W ahi blo + alo blo

		FEDCBA98 76543210 FEDCBA98 76543210
		                  ---- alo*blo ----
		         ---- alo*bhi ----
		         ---- ahi*blo ----
		---- ahi*bhi ----
	*/
	const w = 32
	const m = 1<<w - 1
	ahi, bhi, alo, blo := a>>w, b>>w, a&m, b&m
	lo = alo * blo
	mid1 := alo * bhi
	mid2 := ahi * blo
	c1, lo := AddUint128_64(lo, mid1<<w)
	c2, lo := AddUint128_64(lo, mid2<<w)
	_, hi = AddUint128_64(ahi*bhi, mid1>>w+mid2>>w+c1+c2)
	return
}

// PowerizeBigInt returns (e, p) such that e is the smallest number for which p
// == b^e is greater or equal n. For n < 0 or b < 2 (0, nil) is returned.
//
// NOTE: Run time for large values of n (above about 2^1e6 ~= 1e300000) can be
// significant and/or unacceptabe.  For any smaller values of n the function
// typically performs in sub second time.  For "small" values of n (cca bellow
// 2^1e3 ~= 1e300) the same can be easily below 10 µs.
//
// A special (and trivial) case of b == 2 is handled separately and performs
// much faster.
func PowerizeBigInt(b, n *big.Int) (e uint32, p *big.Int) {
	switch {
	case b.Cmp(_2) < 0 || n.Sign() < 0:
		return
	case n.Sign() == 0 || n.Cmp(_1) == 0:
		return 0, big.NewInt(1)
	case b.Cmp(_2) == 0:
		p = big.NewInt(0)
		e = uint32(n.BitLen() - 1)
		p.SetBit(p, int(e), 1)
		if p.Cmp(n) < 0 {
			p.Mul(p, _2)
			e++
		}
		return
	}

	bw := b.BitLen()
	nw := n.BitLen()
	p = big.NewInt(1)
	var bb, r big.Int
	for {
		switch p.Cmp(n) {
		case -1:
			x := uint32((nw - p.BitLen()) / bw)
			if x == 0 {
				x = 1
			}
			e += x
			switch x {
			case 1:
				p.Mul(p, b)
			default:
				r.Set(_1)
				bb.Set(b)
				e := x
				for {
					if e&1 != 0 {
						r.Mul(&r, &bb)
					}
					if e >>= 1; e == 0 {
						break
					}

					bb.Mul(&bb, &bb)
				}
				p.Mul(p, &r)
			}
		case 0, 1:
			return
		}
	}
}

// PowerizeUint32BigInt returns (e, p) such that e is the smallest number for
// which p == b^e is greater or equal n. For n < 0 or b < 2 (0, nil) is
// returned.
//
// More info: see PowerizeBigInt.
func PowerizeUint32BigInt(b uint32, n *big.Int) (e uint32, p *big.Int) {
	switch {
	case b < 2 || n.Sign() < 0:
		return
	case n.Sign() == 0 || n.Cmp(_1) == 0:
		return 0, big.NewInt(1)
	case b == 2:
		p = big.NewInt(0)
		e = uint32(n.BitLen() - 1)
		p.SetBit(p, int(e), 1)
		if p.Cmp(n) < 0 {
			p.Mul(p, _2)
			e++
		}
		return
	}

	var bb big.Int
	bb.SetInt64(int64(b))
	return PowerizeBigInt(&bb, n)
}

/*
ProbablyPrimeUint32 returns true if n is prime or n is a pseudoprime to base a.
It implements the Miller-Rabin primality test for one specific value of 'a' and
k == 1.

Wrt pseudocode shown at
http://en.wikipedia.org/wiki/Miller-Rabin_primality_test#Algorithm_and_running_time

 Input: n > 3, an odd integer to be tested for primality;
 Input: k, a parameter that determines the accuracy of the test
 Output: composite if n is composite, otherwise probably prime
 write n − 1 as 2^s·d with d odd by factoring powers of 2 from n − 1
 LOOP: repeat k times:
    pick a random integer a in the range [2, n − 2]
    x ← a^d mod n
    if x = 1 or x = n − 1 then do next LOOP
    for r = 1 .. s − 1
       x ← x^2 mod n
       if x = 1 then return composite
       if x = n − 1 then do next LOOP
    return composite
 return probably prime

... this function behaves like passing 1 for 'k' and additionally a
fixed/non-random 'a'.  Otherwise it's the same algorithm.

See also: http://mathworld.wolfram.com/Rabin-MillerStrongPseudoprimeTest.html
*/
func ProbablyPrimeUint32(n, a uint32) bool {
	d, s := n-1, 0
	for ; d&1 == 0; d, s = d>>1, s+1 {
	}
	x := uint64(ModPowUint32(a, d, n))
	if x == 1 || uint32(x) == n-1 {
		return true
	}

	for ; s > 1; s-- {
		if x = x * x % uint64(n); x == 1 {
			return false
		}

		if uint32(x) == n-1 {
			return true
		}
	}
	return false
}

// ProbablyPrimeUint64_32 returns true if n is prime or n is a pseudoprime to
// base a. It implements the Miller-Rabin primality test for one specific value
// of 'a' and k == 1.  See also ProbablyPrimeUint32.
func ProbablyPrimeUint64_32(n uint64, a uint32) bool {
	d, s := n-1, 0
	for ; d&1 == 0; d, s = d>>1, s+1 {
	}
	x := ModPowUint64(uint64(a), d, n)
	if x == 1 || x == n-1 {
		return true
	}

	bx, bn := big.NewInt(0).SetUint64(x), big.NewInt(0).SetUint64(n)
	for ; s > 1; s-- {
		if x = bx.Mod(bx.Mul(bx, bx), bn).Uint64(); x == 1 {
			return false
		}

		if x == n-1 {
			return true
		}
	}
	return false
}

// ProbablyPrimeBigInt_32 returns true if n is prime or n is a pseudoprime to
// base a. It implements the Miller-Rabin primality test for one specific value
// of 'a' and k == 1.  See also ProbablyPrimeUint32.
func ProbablyPrimeBigInt_32(n *big.Int, a uint32) bool {
	var d big.Int
	d.Set(n)
	d.Sub(&d, _1) // d <- n-1
	s := 0
	for ; d.Bit(s) == 0; s++ {
	}
	nMinus1 := big.NewInt(0).Set(&d)
	d.Rsh(&d, uint(s))

	x := ModPowBigInt(big.NewInt(int64(a)), &d, n)
	if x.Cmp(_1) == 0 || x.Cmp(nMinus1) == 0 {
		return true
	}

	for ; s > 1; s-- {
		if x = x.Mod(x.Mul(x, x), n); x.Cmp(_1) == 0 {
			return false
		}

		if x.Cmp(nMinus1) == 0 {
			return true
		}
	}
	return false
}

// ProbablyPrimeBigInt returns true if n is prime or n is a pseudoprime to base
// a. It implements the Miller-Rabin primality test for one specific value of
// 'a' and k == 1.  See also ProbablyPrimeUint32.
func ProbablyPrimeBigInt(n, a *big.Int) bool {
	var d big.Int
	d.Set(n)
	d.Sub(&d, _1) // d <- n-1
	s := 0
	for ; d.Bit(s) == 0; s++ {
	}
	nMinus1 := big.NewInt(0).Set(&d)
	d.Rsh(&d, uint(s))

	x := ModPowBigInt(a, &d, n)
	if x.Cmp(_1) == 0 || x.Cmp(nMinus1) == 0 {
		return true
	}

	for ; s > 1; s-- {
		if x = x.Mod(x.Mul(x, x), n); x.Cmp(_1) == 0 {
			return false
		}

		if x.Cmp(nMinus1) == 0 {
			return true
		}
	}
	return false
}

// Max returns the larger of a and b.
func Max(a, b int) int {
	if a > b {
		return a
	}

	return b
}

// Min returns the smaller of a and b.
func Min(a, b int) int {
	if a < b {
		return a
	}

	return b
}

// MaxPtr returns a pointer to the larger of a and b, or nil.
func MaxPtr(a, b *int) *int {
	if a == nil {
		return b
	}
	if b == nil {
		return a
	}
	if *a > *b {
		return a
	}

	return b
}

// MinPtr returns a pointer to the smaller of a and b, or nil.
func MinPtr(a, b *int) *int {
	if a == nil {
		return b
	}
	if b == nil {
		return a
	}
	if *a < *b {
		return a
	}

	return b
}

// MaxVal returns the largest argument passed.
func MaxVal(val int, vals ...int) int {
	res := val
	for _, v := range vals {
		if v > res {
			res = v
		}
	}
	return res
}

// MinVal returns the smallest argument passed.
func MinVal(val int, vals ...int) int {
	res := val
	for _, v := range vals {
		if v < res {
			res = v
		}
	}
	return res
}

// Clamp returns a value restricted between lo and hi.
func Clamp(v, lo, hi int) int {
	return Min(Max(v, lo), hi)
}

// UMax returns the larger of a and b.
func UMax(a, b uint) uint {
	if a > b {
		return a
	}

	return b
}

// UMin returns the smaller of a and b.
func UMin(a, b uint) uint {
	if a < b {
		return a
	}

	return b
}

// UMaxPtr returns a pointer to the larger of a and b, or nil.
func UMaxPtr(a, b *uint) *uint {
	if a == nil {
		return b
	}
	if b == nil {
		return a
	}
	if *a > *b {
		return a
	}

	return b
}

// UMinPtr returns a pointer to the smaller of a and b, or nil.
func UMinPtr(a, b *uint) *uint {
	if a == nil {
		return b
	}
	if b == nil {
		return a
	}
	if *a < *b {
		return a
	}

	return b
}

// UMaxVal returns the largest argument passed.
func UMaxVal(val uint, vals ...uint) uint {
	res := val
	for _, v := range vals {
		if v > res {
			res = v
		}
	}
	return res
}

// UMinVal returns the smallest argument passed.
func UMinVal(val uint, vals ...uint) uint {
	res := val
	for _, v := range vals {
		if v < res {
			res = v
		}
	}
	return res
}

// UClamp returns a value restricted between lo and hi.
func UClamp(v, lo, hi uint) uint {
	return UMin(UMax(v, lo), hi)
}

// MaxByte returns the larger of a and b.
func MaxByte(a, b byte) byte {
	if a > b {
		return a
	}

	return b
}

// MinByte returns the smaller of a and b.
func MinByte(a, b byte) byte {
	if a < b {
		return a
	}

	return b
}

// MaxBytePtr returns a pointer to the larger of a and b, or nil.
func MaxBytePtr(a, b *byte) *byte {
	if a == nil {
		return b
	}
	if b == nil {
		return a
	}
	if *a > *b {
		return a
	}

	return b
}

// MinBytePtr returns a pointer to the smaller of a and b, or nil.
func MinBytePtr(a, b *byte) *byte {
	if a == nil {
		return b
	}
	if b == nil {
		return a
	}
	if *a < *b {
		return a
	}

	return b
}

// MaxByteVal returns the largest argument passed.
func MaxByteVal(val byte, vals ...byte) byte {
	res := val
	for _, v := range vals {
		if v > res {
			res = v
		}
	}
	return res
}

// MinByteVal returns the smallest argument passed.
func MinByteVal(val byte, vals ...byte) byte {
	res := val
	for _, v := range vals {
		if v < res {
			res = v
		}
	}
	return res
}

// ClampByte returns a value restricted between lo and hi.
func ClampByte(v, lo, hi byte) byte {
	return MinByte(MaxByte(v, lo), hi)
}

// MaxInt8 returns the larger of a and b.
func MaxInt8(a, b int8) int8 {
	if a > b {
		return a
	}

	return b
}

// MinInt8 returns the smaller of a and b.
func MinInt8(a, b int8) int8 {
	if a < b {
		return a
	}

	return b
}

// MaxInt8Ptr returns a pointer to the larger of a and b, or nil.
func MaxInt8Ptr(a, b *int8) *int8 {
	if a == nil {
		return b
	}
	if b == nil {
		return a
	}
	if *a > *b {
		return a
	}

	return b
}

// MinInt8Ptr returns a pointer to the smaller of a and b, or nil.
func MinInt8Ptr(a, b *int8) *int8 {
	if a == nil {
		return b
	}
	if b == nil {
		return a
	}
	if *a < *b {
		return a
	}

	return b
}

// MaxInt8Val returns the largest argument passed.
func MaxInt8Val(val int8, vals ...int8) int8 {
	res := val
	for _, v := range vals {
		if v > res {
			res = v
		}
	}
	return res
}

// MinInt8Val returns the smallest argument passed.
func MinInt8Val(val int8, vals ...int8) int8 {
	res := val
	for _, v := range vals {
		if v < res {
			res = v
		}
	}
	return res
}

// ClampInt8 returns a value restricted between lo and hi.
func ClampInt8(v, lo, hi int8) int8 {
	return MinInt8(MaxInt8(v, lo), hi)
}

// MaxUint16 returns the larger of a and b.
func MaxUint16(a, b uint16) uint16 {
	if a > b {
		return a
	}

	return b
}

// MinUint16 returns the smaller of a and b.
func MinUint16(a, b uint16) uint16 {
	if a < b {
		return a
	}

	return b
}

// MaxUint16Ptr returns a pointer to the larger of a and b, or nil.
func MaxUint16Ptr(a, b *uint16) *uint16 {
	if a == nil {
		return b
	}
	if b == nil {
		return a
	}
	if *a > *b {
		return a
	}

	return b
}

// MinUint16Ptr returns a pointer to the smaller of a and b, or nil.
func MinUint16Ptr(a, b *uint16) *uint16 {
	if a == nil {
		return b
	}
	if b == nil {
		return a
	}
	if *a < *b {
		return a
	}

	return b
}

// MaxUint16Val returns the largest argument passed.
func MaxUint16Val(val uint16, vals ...uint16) uint16 {
	res := val
	for _, v := range vals {
		if v > res {
			res = v
		}
	}
	return res
}

// MinUint16Val returns the smallest argument passed.
func MinUint16Val(val uint16, vals ...uint16) uint16 {
	res := val
	for _, v := range vals {
		if v < res {
			res = v
		}
	}
	return res
}

// ClampUint16 returns a value restricted between lo and hi.
func ClampUint16(v, lo, hi uint16) uint16 {
	return MinUint16(MaxUint16(v, lo), hi)
}

// MaxInt16 returns the larger of a and b.
func MaxInt16(a, b int16) int16 {
	if a > b {
		return a
	}

	return b
}

// MinInt16 returns the smaller of a and b.
func MinInt16(a, b int16) int16 {
	if a < b {
		return a
	}

	return b
}

// MaxInt16Ptr returns a pointer to the larger of a and b, or nil.
func MaxInt16Ptr(a, b *int16) *int16 {
	if a == nil {
		return b
	}
	if b == nil {
		return a
	}
	if *a > *b {
		return a
	}

	return b
}

// MinInt16Ptr returns a pointer to the smaller of a and b, or nil.
func MinInt16Ptr(a, b *int16) *int16 {
	if a == nil {
		return b
	}
	if b == nil {
		return a
	}
	if *a < *b {
		return a
	}

	return b
}

// MaxInt16Val returns the largest argument passed.
func MaxInt16Val(val int16, vals ...int16) int16 {
	res := val
	for _, v := range vals {
		if v > res {
			res = v
		}
	}
	return res
}

// MinInt16Val returns the smallest argument passed.
func MinInt16Val(val int16, vals ...int16) int16 {
	res := val
	for _, v := range vals {
		if v < res {
			res = v
		}
	}
	return res
}

// ClampInt16 returns a value restricted between lo and hi.
func ClampInt16(v, lo, hi int16) int16 {
	return MinInt16(MaxInt16(v, lo), hi)
}

// MaxUint32 returns the larger of a and b.
func MaxUint32(a, b uint32) uint32 {
	if a > b {
		return a
	}

	return b
}

// MinUint32 returns the smaller of a and b.
func MinUint32(a, b uint32) uint32 {
	if a < b {
		return a
	}

	return b
}

// MaxUint32Ptr returns a pointer to the larger of a and b, or nil.
func MaxUint32Ptr(a, b *uint32) *uint32 {
	if a == nil {
		return b
	}
	if b == nil {
		return a
	}
	if *a > *b {
		return a
	}

	return b
}

// MinUint32Ptr returns a pointer to the smaller of a and b, or nil.
func MinUint32Ptr(a, b *uint32) *uint32 {
	if a == nil {
		return b
	}
	if b == nil {
		return a
	}
	if *a < *b {
		return a
	}

	return b
}

// MaxUint32Val returns the largest argument passed.
func MaxUint32Val(val uint32, vals ...uint32) uint32 {
	res := val
	for _, v := range vals {
		if v > res {
			res = v
		}
	}
	return res
}

// MinUint32Val returns the smallest argument passed.
func MinUint32Val(val uint32, vals ...uint32) uint32 {
	res := val
	for _, v := range vals {
		if v < res {
			res = v
		}
	}
	return res
}

// ClampUint32 returns a value restricted between lo and hi.
func ClampUint32(v, lo, hi uint32) uint32 {
	return MinUint32(MaxUint32(v, lo), hi)
}

// MaxInt32 returns the larger of a and b.
func MaxInt32(a, b int32) int32 {
	if a > b {
		return a
	}

	return b
}

// MinInt32 returns the smaller of a and b.
func MinInt32(a, b int32) int32 {
	if a < b {
		return a
	}

	return b
}

// MaxInt32Ptr returns a pointer to the larger of a and b, or nil.
func MaxInt32Ptr(a, b *int32) *int32 {
	if a == nil {
		return b
	}
	if b == nil {
		return a
	}
	if *a > *b {
		return a
	}

	return b
}

// MinInt32Ptr returns a pointer to the smaller of a and b, or nil.
func MinInt32Ptr(a, b *int32) *int32 {
	if a == nil {
		return b
	}
	if b == nil {
		return a
	}
	if *a < *b {
		return a
	}

	return b
}

// MaxInt32Val returns the largest argument passed.
func MaxInt32Val(val int32, vals ...int32) int32 {
	res := val
	for _, v := range vals {
		if v > res {
			res = v
		}
	}
	return res
}

// MinInt32Val returns the smallest argument passed.
func MinInt32Val(val int32, vals ...int32) int32 {
	res := val
	for _, v := range vals {
		if v < res {
			res = v
		}
	}
	return res
}

// ClampInt32 returns a value restricted between lo and hi.
func ClampInt32(v, lo, hi int32) int32 {
	return MinInt32(MaxInt32(v, lo), hi)
}

// MaxUint64 returns the larger of a and b.
func MaxUint64(a, b uint64) uint64 {
	if a > b {
		return a
	}

	return b
}

// MinUint64 returns the smaller of a and b.
func MinUint64(a, b uint64) uint64 {
	if a < b {
		return a
	}

	return b
}

// MaxUint64Ptr returns a pointer to the larger of a and b, or nil.
func MaxUint64Ptr(a, b *uint64) *uint64 {
	if a == nil {
		return b
	}
	if b == nil {
		return a
	}
	if *a > *b {
		return a
	}

	return b
}

// MinUint64Ptr returns a pointer to the smaller of a and b, or nil.
func MinUint64Ptr(a, b *uint64) *uint64 {
	if a == nil {
		return b
	}
	if b == nil {
		return a
	}
	if *a < *b {
		return a
	}

	return b
}

// MaxUint64Val returns the largest argument passed.
func MaxUint64Val(val uint64, vals ...uint64) uint64 {
	res := val
	for _, v := range vals {
		if v > res {
			res = v
		}
	}
	return res
}

// MinUint64Val returns the smallest argument passed.
func MinUint64Val(val uint64, vals ...uint64) uint64 {
	res := val
	for _, v := range vals {
		if v < res {
			res = v
		}
	}
	return res
}

// ClampUint64 returns a value restricted between lo and hi.
func ClampUint64(v, lo, hi uint64) uint64 {
	return MinUint64(MaxUint64(v, lo), hi)
}

// MaxInt64 returns the larger of a and b.
func MaxInt64(a, b int64) int64 {
	if a > b {
		return a
	}

	return b
}

// MinInt64 returns the smaller of a and b.
func MinInt64(a, b int64) int64 {
	if a < b {
		return a
	}

	return b
}

// MaxInt64Ptr returns a pointer to the larger of a and b, or nil.
func MaxInt64Ptr(a, b *int64) *int64 {
	if a == nil {
		return b
	}
	if b == nil {
		return a
	}
	if *a > *b {
		return a
	}

	return b
}

// MinInt64Ptr returns a pointer to the smaller of a and b, or nil.
func MinInt64Ptr(a, b *int64) *int64 {
	if a == nil {
		return b
	}
	if b == nil {
		return a
	}
	if *a < *b {
		return a
	}

	return b
}

// MaxInt64Val returns the largest argument passed.
func MaxInt64Val(val int64, vals ...int64) int64 {
	res := val
	for _, v := range vals {
		if v > res {
			res = v
		}
	}
	return res
}

// MinInt64Val returns the smallest argument passed.
func MinInt64Val(val int64, vals ...int64) int64 {
	res := val
	for _, v := range vals {
		if v < res {
			res = v
		}
	}
	return res
}

// ClampInt64 returns a value restricted between lo and hi.
func ClampInt64(v, lo, hi int64) int64 {
	return MinInt64(MaxInt64(v, lo), hi)
}

// ToBase produces n in base b. For example
//
// 	ToBase(2047, 22) -> [1, 5, 4]
//
//	1 * 22^0           1
//	5 * 22^1         110
//	4 * 22^2        1936
//	                ----
//	                2047
//
// ToBase panics for bases < 2.
func ToBase(n *big.Int, b int) []int {
	var nn big.Int
	nn.Set(n)
	if b < 2 {
		panic("invalid base")
	}

	k := 1
	switch nn.Sign() {
	case -1:
		nn.Neg(&nn)
		k = -1
	case 0:
		return []int{0}
	}

	bb := big.NewInt(int64(b))
	var r []int
	rem := big.NewInt(0)
	for nn.Sign() != 0 {
		nn.QuoRem(&nn, bb, rem)
		r = append(r, k*int(rem.Int64()))
	}
	return r
}
