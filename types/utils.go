package types

// http://cavaliercoder.com/blog/optimized-abs-for-int64-in-go.html
func abs(n int64) int64 {
	y := n >> 63
	return (n ^ y) - y
}

// uintSizeTable is used as a table to do comparison to get uint length is faster than doing loop on division with 10
var uintSizeTable = [21]uint64{
	0, // redundant 0 here, so to make function lenOfUint64Fast to count from 1 and return i directly
	9, 99, 999, 9999, 99999,
	999999, 9999999, 99999999, 999999999, 9999999999,
	99999999999, 999999999999, 9999999999999, 99999999999999, 999999999999999,
	9999999999999999, 99999999999999999, 999999999999999999, 9999999999999999999,
	maxUint,
} // maxUint is 18446744073709551615 and it has 20 digits

// LenOfUint64Fast efficiently calculate the string character lengths of an uint64 as input
func LenOfUint64Fast(x uint64) int {
	i := 1
	// Reduce comparison time for large numbers
	// Optimized for input x mostly range between 0 to 99999
	if x > 9999 {
		i = 5
	} else if x > 99 {
		i = 3
	}
	for ; ; i++ {
		if x <= uintSizeTable[i] {
			return i
		}
	}
}

// LenOfInt64Fast efficiently calculate the string character lengths of an int64 as input
func LenOfInt64Fast(x int64) int {
	size := 0
	if x < 0 {
		size = 1 // add "-" sign on the length count
	}
	return size + LenOfUint64Fast(uint64(abs(x)))
}
