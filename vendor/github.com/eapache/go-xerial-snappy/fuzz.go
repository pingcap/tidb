// +build gofuzz

package snappy

func Fuzz(data []byte) int {
	decode, err := Decode(data)
	if decode == nil && err == nil {
		panic("nil error with nil result")
	}

	if err != nil {
		return 0
	}

	return 1
}
