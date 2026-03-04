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

package stream

import (
	"fmt"
	"strings"
	"testing"

	"github.com/pingcap/tidb/br/pkg/stream/backupmetas"
	"github.com/stretchr/testify/require"
)

func FuzzParseBackupMetaFileNameRoundTrip(f *testing.F) {
	f.Add(uint64(10), uint64(11), uint64(5), uint64(10), uint64(30), byte('x'))

	f.Fuzz(func(t *testing.T, flushTs, storeID, minDefaultTs, minTs, maxTs uint64, extraTag byte) {
		if !isASCIIAlphanumeric(extraTag) ||
			extraTag == backupmetas.NameMinBeginTSTag ||
			extraTag == backupmetas.NameMinTSTag ||
			extraTag == backupmetas.NameMaxTSTag {
			extraTag = 'x'
		}

		legacyFileName := fmt.Sprintf("%016x-%016x-%016x-%016x", flushTs, minDefaultTs, minTs, maxTs)
		legacyParsed, err := backupmetas.ParseName(legacyFileName)
		require.NoError(t, err)
		require.Equal(t, backupmetas.ParsedName{
			FlushTS:      flushTs,
			MinDefaultTS: minDefaultTs,
			MinTS:        minTs,
			MaxTS:        maxTs,
		}, legacyParsed)

		taggedFileName := fmt.Sprintf(
			"%016X%016X-%c%016Xd%016Xu%016Xl%016X",
			flushTs, storeID, extraTag, uint64(0), minDefaultTs, maxTs, minTs,
		)
		taggedParsed, err := backupmetas.ParseName(taggedFileName)
		require.NoError(t, err)
		require.Equal(t, backupmetas.ParsedName{
			FlushTS:      flushTs,
			StoreID:      storeID,
			MinDefaultTS: minDefaultTs,
			MinTS:        minTs,
			MaxTS:        maxTs,
		}, taggedParsed)

		tagValues := map[byte]uint64{
			backupmetas.NameMinBeginTSTag: minDefaultTs,
			backupmetas.NameMinTSTag:      minTs,
			backupmetas.NameMaxTSTag:      maxTs,
		}
		tagOrder := []byte{backupmetas.NameMinBeginTSTag, backupmetas.NameMaxTSTag, backupmetas.NameMinTSTag}
		for _, missingTag := range tagOrder {
			segments := make([]string, 0, len(tagOrder)-1)
			for _, tag := range tagOrder {
				if tag == missingTag {
					continue
				}
				segments = append(segments, fmt.Sprintf("%c%016X", tag, tagValues[tag]))
			}
			missingTagFileName := fmt.Sprintf("%016X%016X-%s", flushTs, storeID, strings.Join(segments, ""))
			_, err := backupmetas.ParseName(missingTagFileName)
			require.Error(t, err)
			require.Contains(t, err.Error(), fmt.Sprintf("missing %q tag", missingTag))
		}
	})
}

func isASCIIAlphanumeric(ch byte) bool {
	return '0' <= ch && ch <= '9' || 'a' <= ch && ch <= 'z' || 'A' <= ch && ch <= 'Z'
}
