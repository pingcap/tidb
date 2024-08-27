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

package external

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSplitDataFiles(t *testing.T) {
	allPaths := make([]string, 0, 110)
	for i := 0; i < cap(allPaths); i++ {
		allPaths = append(allPaths, fmt.Sprintf("%d", i))
	}
	cases := []struct {
		paths       []string
		concurrency int
		result      [][]string
	}{
		{
			paths:       allPaths[:1],
			concurrency: 1,
			result:      [][]string{allPaths[:1]},
		},
		{
			paths:       allPaths[:2],
			concurrency: 1,
			result:      [][]string{allPaths[:2]},
		},
		{
			paths:       allPaths[:2],
			concurrency: 4,
			result:      [][]string{allPaths[:2]},
		},
		{
			paths:       allPaths[:3],
			concurrency: 4,
			result:      [][]string{allPaths[:3]},
		},
		{
			paths:       allPaths[:4],
			concurrency: 4,
			result:      [][]string{allPaths[:2], allPaths[2:4]},
		},
		{
			paths:       allPaths[:5],
			concurrency: 4,
			result:      [][]string{allPaths[:3], allPaths[3:5]},
		},
		{
			paths:       allPaths[:6],
			concurrency: 4,
			result:      [][]string{allPaths[:2], allPaths[2:4], allPaths[4:6]},
		},
		{
			paths:       allPaths[:7],
			concurrency: 4,
			result:      [][]string{allPaths[:3], allPaths[3:5], allPaths[5:7]},
		},
		{
			paths:       allPaths[:15],
			concurrency: 4,
			result:      [][]string{allPaths[:4], allPaths[4:8], allPaths[8:12], allPaths[12:15]},
		},
		{
			paths:       allPaths[:83],
			concurrency: 4,
			result:      [][]string{allPaths[:21], allPaths[21:42], allPaths[42:63], allPaths[63:83]},
		},
		{
			paths:       allPaths[:100],
			concurrency: 4,
			result:      [][]string{allPaths[:25], allPaths[25:50], allPaths[50:75], allPaths[75:100]},
		},
		{
			paths:       allPaths[:100],
			concurrency: 8,
			result: [][]string{
				allPaths[:13], allPaths[13:26], allPaths[26:39], allPaths[39:52],
				allPaths[52:64], allPaths[64:76], allPaths[76:88], allPaths[88:100],
			},
		},
	}
	for i, c := range cases {
		t.Run(fmt.Sprintf("case-%d", i), func(t *testing.T) {
			result := splitDataFiles(c.paths, c.concurrency)
			require.Equal(t, c.result, result)
		})
	}

	bak := MaxMergingFilesPerThread
	t.Cleanup(func() {
		MaxMergingFilesPerThread = bak
	})
	MaxMergingFilesPerThread = 10
	require.Equal(t, [][]string{
		allPaths[:10], allPaths[10:19], allPaths[19:28], allPaths[28:37],
		allPaths[37:46], allPaths[46:55], allPaths[55:64], allPaths[64:73],
		allPaths[73:82], allPaths[82:91],
	}, splitDataFiles(allPaths[:91], 8))
	require.Equal(t, [][]string{
		allPaths[:10], allPaths[10:20], allPaths[20:30], allPaths[30:40],
		allPaths[40:50], allPaths[50:60], allPaths[60:70], allPaths[70:80],
		allPaths[80:90], allPaths[90:99],
	}, splitDataFiles(allPaths[:99], 8))
	require.Equal(t, [][]string{
		allPaths[:10], allPaths[10:20], allPaths[20:29], allPaths[29:38],
		allPaths[38:47], allPaths[47:56], allPaths[56:65], allPaths[65:74],
		allPaths[74:83], allPaths[83:92], allPaths[92:101],
	}, splitDataFiles(allPaths[:101], 8))
}
