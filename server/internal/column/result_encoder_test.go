package column

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestResultEncoder(t *testing.T) {
	// Encode bytes to utf-8.
	d := NewResultEncoder("utf-8")
	src := []byte("test_string")
	result := d.EncodeMeta(src)
	require.Equal(t, src, result)

	// Encode bytes to GBK.
	d = NewResultEncoder("gbk")
	result = d.EncodeMeta([]byte("一"))
	require.Equal(t, []byte{0xd2, 0xbb}, result)

	// Encode bytes to binary.
	d = NewResultEncoder("binary")
	result = d.EncodeMeta([]byte("一"))
	require.Equal(t, "一", string(result))
}
