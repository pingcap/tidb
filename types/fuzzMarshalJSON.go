package types

import "github.com/pingcap/tidb/types/json"

// FuzzMarshalJSON implements the fuzzer
func FuzzMarshalJSON(data []byte) int {
	bj, err := json.ParseBinaryFromString(string(data))
	if err != nil {
		return -1
	}
	_, err = bj.MarshalJSON()
	if err != nil {
		return 0
	}
	return 1
}
