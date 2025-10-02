package executor

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestConvertReadableSizeToByteSize(t *testing.T) {
	var ci configInspection

	testcases := []struct {
		input  string
		output uint64
		err    bool
	}{
		{"100", 100, false},
		{"abc", 0, true},
		{"1KiB", 1024, false},
		{"1MiB", 1048576, false},
		{"1GiB", 1073741824, false},
		{"1TiB", 1099511627776, false},
		{"1PiB", 1125899906842624, false},
		{"100B", 100, false},
	}

	for _, tc := range testcases {
		r, err := ci.convertReadableSizeToByteSize(tc.input)
		if tc.err {
			require.Error(t, err)
		} else {
			require.NoError(t, err)
			require.Equal(t, tc.output, r)
		}
	}
}
