package mathutil

const MaxUint = ^uint(0) 
const MinUint = 0 
const MaxInt = int(MaxUint >> 1) 
const MinInt = -MaxInt - 1

func MaxUint64(x, y uint64) uint64 {
	if x > y {
		return x
	} else {
		return y
	}
}

func MinUint64(x, y uint64) uint64 {
	if x < y {
		return x
	} else {
		return y
	}
}

func MaxUint32(x, y uint32) uint32 {
	if x > y {
		return x
	} else {
		return y
	}
}

func MinUint32(x, y uint32) uint32 {
	if x < y {
		return x
	} else {
		return y
	}
}

func MaxInt64(x, y int64) int64 {
	if x > y {
		return x
	} else {
		return y
	}
}

func MinInt64(x, y int64) int64 {
	if x < y {
		return x
	} else {
		return y
	}
}

func MaxInt32(x, y int32) int32 {
	if x > y {
		return x
	} else {
		return y
	}
}

func MinInt32(x, y int32) int32 {
	if x < y {
		return x
	} else {
		return y
	}
}

func MaxInt8(x, y int8) int8 {
	if x > y {
		return x
	} else {
		return y
	}
}

func MinInt8(x, y int8) int8 {
	if x < y {
		return x
	} else {
		return y
	}
}

func Max(x, y int) int {
	if x > y {
		return x
	} else {
		return y
	}
}

func Min(x, y int) int {
	if x < y {
		return x
	} else {
		return y
	}
}