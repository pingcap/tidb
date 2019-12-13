package mathutil

// The maximum number uint can record
const (
	MaxUint = ^uint(0)
	MinUint = 0
	MaxInt  = int(MaxUint >> 1)
	MinInt  = -MaxInt - 1
)

// MaxUint64 returns the larger of a and b.
func MaxUint64(x, y uint64) uint64 {
	if x > y {
		return x
	}
	return y

}

// MinUint64 returns the smaller of a and b.
func MinUint64(x, y uint64) uint64 {
	if x < y {
		return x
	}
	return y
}

// MaxUint32 returns the larger of a and b.
func MaxUint32(x, y uint32) uint32 {
	if x > y {
		return x
	}
	return y
}

// MinUint32 returns the smaller of a and b.
func MinUint32(x, y uint32) uint32 {
	if x < y {
		return x
	}
	return y
}

// MaxInt64 returns the larger of a and b.
func MaxInt64(x, y int64) int64 {
	if x > y {
		return x
	}
	return y
}

// MinInt64 returns the smaller of a and b.
func MinInt64(x, y int64) int64 {
	if x < y {
		return x
	}
	return y
}

// MaxInt32 returns the larger of a and b.
func MaxInt32(x, y int32) int32 {
	if x > y {
		return x
	}
	return y
}

// MinInt32 returns the smaller of a and b.
func MinInt32(x, y int32) int32 {
	if x < y {
		return x
	}
	return y
}

// MaxInt8 returns the larger of a and b.
func MaxInt8(x, y int8) int8 {
	if x > y {
		return x
	}
	return y
}

// MinInt8 returns the smaller of a and b.
func MinInt8(x, y int8) int8 {
	if x < y {
		return x
	}
	return y
}

// Max returns the larger of a and b.
func Max(x, y int) int {
	if x > y {
		return x
	}
	return y
}

// Min returns the smaller of a and b.
func Min(x, y int) int {
	if x < y {
		return x
	}
	return y
}
