// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestReadPasswordRequiresTerminal(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "password")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, file.Close()) })
	_, err = file.WriteString("PASSWORD_SECRET\n")
	require.NoError(t, err)
	_, err = file.Seek(0, 0)
	require.NoError(t, err)

	password, err := readPassword(file)
	require.EqualError(t, err, "--ask-password requires a terminal")
	require.Empty(t, password)
	require.NotContains(t, err.Error(), "PASSWORD_SECRET")
	pos, err := file.Seek(0, 1)
	require.NoError(t, err)
	require.Zero(t, pos, "non-terminal input must not be consumed")
}
