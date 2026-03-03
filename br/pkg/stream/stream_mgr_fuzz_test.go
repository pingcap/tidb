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
	"testing"

	"github.com/stretchr/testify/require"
)

func FuzzParseBackupMetaFileNameRoundTrip(f *testing.F) {
	f.Add(uint64(10), uint64(11), uint64(5), uint64(10), uint64(30), byte('x'))

	f.Fuzz(func(t *testing.T, flushTs, storeID, minDefaultTs, minTs, maxTs uint64, extraTag byte) {
		if !isASCIIAlphanumeric(extraTag) || extraTag == backupMetaMinBeginTSTag || extraTag == backupMetaMinTSTag || extraTag == backupMetaMaxTSTag {
			extraTag = 'x'
		}

		legacyFileName := fmt.Sprintf("%016x-%016x-%016x-%016x", flushTs, minDefaultTs, minTs, maxTs)
		legacyParsed, err := parseBackupMetaFileName(legacyFileName)
		require.NoError(t, err)
		require.Equal(t, parsedBackupMetaFileName{
			flushTs:      flushTs,
			minDefaultTs: minDefaultTs,
			minTs:        minTs,
			maxTs:        maxTs,
		}, legacyParsed)

		taggedFileName := fmt.Sprintf(
			"%016X%016X-%c%016Xd%016Xu%016Xl%016X",
			flushTs, storeID, extraTag, uint64(0), minDefaultTs, maxTs, minTs,
		)
		taggedParsed, err := parseBackupMetaFileName(taggedFileName)
		require.NoError(t, err)
		require.Equal(t, parsedBackupMetaFileName{
			flushTs:      flushTs,
			storeID:      storeID,
			minDefaultTs: minDefaultTs,
			minTs:        minTs,
			maxTs:        maxTs,
		}, taggedParsed)
	})
}
