// Package types provides various type size constants
package types

// Sizes
const (
	MaxUint = ^uint(0)           // Maximum Uint size
	MinUint = 0                  // Minimum Uint size
	MaxInt  = int(^uint(0) >> 1) // Maximum Int size
	MinInt  = -MaxInt - 1        // Minimum Int size
)
