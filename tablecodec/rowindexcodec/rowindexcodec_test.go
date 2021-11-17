package rowindexcodec

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGetKeyKind(t *testing.T) {
	require.Equal(t, KeyKindRow, GetKeyKind([]byte{116, 128, 0, 0, 0, 0, 0, 0, 0, 95, 114}))
	require.Equal(t, KeyKindIndex, GetKeyKind([]byte{116, 128, 0, 0, 0, 0, 0, 0, 0, 95, 105, 128, 0, 0, 0, 0, 0, 0, 0}))
	require.Equal(t, KeyKindUnknown, GetKeyKind([]byte("")))
	require.Equal(t, KeyKindUnknown, GetKeyKind(nil))
}
