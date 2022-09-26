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

package util

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseHostPortAddr(t *testing.T) {
	urls := []string{
		"127.0.0.1:2379",
		"127.0.0.1:2379,127.0.0.2:2379",
		"localhost:2379",
		"pump-1:8250,pump-2:8250",
		"http://127.0.0.1:2379",
		"https://127.0.0.1:2379",
		"http://127.0.0.1:2379,http://127.0.0.2:2379",
		"https://127.0.0.1:2379,https://127.0.0.2:2379",
		"unix:///home/tidb/tidb.sock",
	}

	expectUrls := [][]string{
		{"127.0.0.1:2379"},
		{"127.0.0.1:2379", "127.0.0.2:2379"},
		{"localhost:2379"},
		{"pump-1:8250", "pump-2:8250"},
		{"http://127.0.0.1:2379"},
		{"https://127.0.0.1:2379"},
		{"http://127.0.0.1:2379", "http://127.0.0.2:2379"},
		{"https://127.0.0.1:2379", "https://127.0.0.2:2379"},
		{"unix:///home/tidb/tidb.sock"},
	}

	for i, url := range urls {
		urlList, err := ParseHostPortAddr(url)
		require.Equal(t, nil, err)
		require.Equal(t, len(expectUrls[i]), len(urlList))
		for j, u := range urlList {
			require.Equal(t, expectUrls[i][j], u)
		}
	}

	inValidUrls := []string{
		"127.0.0.1",
		"http:///127.0.0.1:2379",
		"htt://127.0.0.1:2379",
	}

	for _, url := range inValidUrls {
		_, err := ParseHostPortAddr(url)
		require.Error(t, err)
	}
}
