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

package logclient_test

import (
	"fmt"
	"math/rand"
	"testing"

	logclient "github.com/pingcap/tidb/br/pkg/restore/log_client"
	"github.com/stretchr/testify/require"
)

func TestLogFilesSkipMap(t *testing.T) {
	var (
		metaNum  = 2
		groupNum = 4
		fileNum  = 1000

		ratio = 0.1
	)

	for ratio < 1 {
		skipmap := logclient.NewLogFilesSkipMap()
		nativemap := make(map[string]map[int]map[int]struct{})
		count := 0
		for i := 0; i < int(ratio*float64(metaNum*groupNum*fileNum)); i++ {
			metaKey := fmt.Sprint(rand.Intn(metaNum))
			groupOff := rand.Intn(groupNum)
			fileOff := rand.Intn(fileNum)

			mp, exists := nativemap[metaKey]
			if !exists {
				mp = make(map[int]map[int]struct{})
				nativemap[metaKey] = mp
			}
			gp, exists := mp[groupOff]
			if !exists {
				gp = make(map[int]struct{})
				mp[groupOff] = gp
			}
			if _, exists := gp[fileOff]; !exists {
				gp[fileOff] = struct{}{}
				skipmap.Insert(metaKey, groupOff, fileOff)
				count += 1
			}
		}

		ncount := 0
		for metaKey, mp := range nativemap {
			for groupOff, gp := range mp {
				for fileOff := range gp {
					require.True(t, skipmap.NeedSkip(metaKey, groupOff, fileOff))
					ncount++
				}
			}
		}

		require.Equal(t, count, ncount)

		continueFunc := func(metaKey string, groupi, filei int) bool {
			mp, exists := nativemap[metaKey]
			if !exists {
				return false
			}
			gp, exists := mp[groupi]
			if !exists {
				return false
			}
			_, exists = gp[filei]
			return exists
		}

		for metai := 0; metai < metaNum; metai++ {
			metaKey := fmt.Sprint(metai)
			for groupi := 0; groupi < groupNum; groupi++ {
				for filei := 0; filei < fileNum; filei++ {
					if continueFunc(metaKey, groupi, filei) {
						continue
					}
					require.False(t, skipmap.NeedSkip(metaKey, groupi, filei))
				}
			}
		}

		ratio = ratio * 2
	}
}
