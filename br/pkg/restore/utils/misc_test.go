// Copyright 2024 PingCAP, Inc.
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

package utils_test

import (
	"testing"

	"github.com/pingcap/tidb/br/pkg/restore/utils"
	"github.com/stretchr/testify/require"
)

func TestTruncateTS(t *testing.T) {
	keyWithTS := []byte{'1', '2', '1', '2', '1', '2', '1', '2', '1', '2', '1', '2', '1', '2', '1', '2'}
	ts := utils.TruncateTS(keyWithTS)
	require.Equal(t, []byte{'1', '2', '1', '2', '1', '2', '1', '2'}, ts)
	keyWithTS = []byte{'1', '2'}
	ts = utils.TruncateTS(keyWithTS)
	require.Equal(t, []byte{'1', '2'}, ts)
}

func TestEncodeKeyPrefix(t *testing.T) {
	keyPrefix := []byte{'1', '2', '1', '2', '1', '2', '1', '2', '1', '2', '1', '2', '1', '2', '1', '2'}
	encodeKey := utils.EncodeKeyPrefix(keyPrefix)
	require.Equal(t, []byte{'1', '2', '1', '2', '1', '2', '1', '2', 0xff, '1', '2', '1', '2', '1', '2', '1', '2', 0xff}, encodeKey)

	keyPrefix = []byte{'1', '2', '1', '2', '1', '2', '1', '2', '1', '2', '1', '2', '1', '2', '1'}
	encodeKey = utils.EncodeKeyPrefix(keyPrefix)
	require.Equal(t, []byte{'1', '2', '1', '2', '1', '2', '1', '2', 0xff, '1', '2', '1', '2', '1', '2', '1'}, encodeKey)

	keyPrefix = []byte{'1', '2'}
	encodeKey = utils.EncodeKeyPrefix(keyPrefix)
	require.Equal(t, []byte{'1', '2'}, encodeKey)
}
