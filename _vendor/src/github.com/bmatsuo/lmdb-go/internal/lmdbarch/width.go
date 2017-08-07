// Package lmdbarch contains some architecture detection constants.  The
// primary reason the package exists is because the constant definitions are
// scary and some will not pass linters.
package lmdbarch

// Width64 is 1 for 64-bit architectures and 0 otherwise.
const Width64 = 1 << (^uintptr(0) >> 63) / 2
