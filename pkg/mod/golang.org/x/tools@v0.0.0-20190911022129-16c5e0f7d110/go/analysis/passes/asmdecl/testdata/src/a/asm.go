// Copyright 2010 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// This file contains declarations to test the assembly in test_asm.s.

package a

type S struct {
	i int32
	b bool
	s string
}

func arg1(x int8, y uint8)
func arg2(x int16, y uint16)
func arg4(x int32, y uint32)
func arg8(x int64, y uint64)
func argint(x int, y uint)
func argptr(x *byte, y *byte, c chan int, m map[int]int, f func())
func argstring(x, y string)
func argslice(x, y []string)
func argiface(x interface{}, y interface {
	m()
})
func argcomplex(x complex64, y complex128)
func argstruct(x S, y struct{})
func argarray(x [2]S)
func returnint() int
func returnbyte(x int) byte
func returnnamed(x byte) (r1 int, r2 int16, r3 string, r4 byte)
func returnintmissing() int
func leaf(x, y int) int

func noprof(x int)
func dupok(x int)
func nosplit(x int)
func rodata(x int)
func noptr(x int)
func wrapper(x int)

func f15271() (x uint32)
func f17584(x float32, y complex64)
func f29318(x [2][2]uint64)

func noframe1(x int32)
func noframe2(x int32)

func fvariadic(int, ...int)
