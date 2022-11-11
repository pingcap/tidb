// Copyright 2022 PingCAP, Inc.
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

package validator

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestUpdateDictionaryFile(t *testing.T) {
	tooLargeDict, err := CreateTmpDictWithSize("1.dict", 2*1024*1024)
	require.NoError(t, err)
	err = UpdateDictionaryFile(tooLargeDict)
	require.ErrorContains(t, err, "Too Large Dictionary. The maximum permitted file size is 1MB")

	dict, err := CreateTmpDictWithContent("2.dict", []byte("abc\n1234\n5678"))
	require.NoError(t, err)
	require.NoError(t, UpdateDictionaryFile(dict))
	_, ok := dictionary.cache["1234"]
	require.True(t, ok)
	_, ok = dictionary.cache["5678"]
	require.True(t, ok)
	_, ok = dictionary.cache["abc"]
	require.False(t, ok)
}

func TestValidateDictionaryPassword(t *testing.T) {
	dict, err := CreateTmpDictWithContent("3.dict", []byte("1234\n5678"))
	require.NoError(t, err)
	require.NoError(t, UpdateDictionaryFile(dict))
	require.True(t, validateDictionaryPassword("abcdefg"))
	require.True(t, validateDictionaryPassword("abcd123efg"))
	require.False(t, validateDictionaryPassword("abcd1234efg"))
	require.False(t, validateDictionaryPassword("abcd12345efg"))
}
