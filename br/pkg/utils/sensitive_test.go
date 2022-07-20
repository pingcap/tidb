package utils

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestHideSensitive(t *testing.T) {
	strs := []struct {
		old string
		new string
	}{
		{
			`host = "127.0.0.1"\n  user = "root"\n  password = "/Q7B9DizNLLTTfiZHv9WoEAKamfpIUs="\n  port = 3306\n`,
			`host = "127.0.0.1"\n  user = "root"\n  password = ******\n  port = 3306\n`,
		},
		{
			`host = "127.0.0.1"\n  user = "root"\n  password = ""\n  port = 3306\n`,
			`host = "127.0.0.1"\n  user = "root"\n  password = ******\n  port = 3306\n`,
		},
	}
	for _, str := range strs {
		require.Equal(t, str.new, HideSensitive(str.old))
	}
}
