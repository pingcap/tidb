package types

// FuzzNewHexLiteral implements the fuzzer
func FuzzNewHexLiteral(data []byte) int {
	_, err := NewHexLiteral(string(data))
	if err != nil {
		return 0
	}
	return 1
}
