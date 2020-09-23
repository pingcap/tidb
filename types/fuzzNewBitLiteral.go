package types

// FuzzNewBitLiteral implements the fuzzer
func FuzzNewBitLiteral(data []byte) int {
	_, err := NewBitLiteral(string(data))
	if err != nil {
		return 0
	}
	return 1
}
