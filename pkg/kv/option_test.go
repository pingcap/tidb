// Copyright 2023 PingCAP, Inc.

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

package kv

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSetCDCWriteSource(t *testing.T) {
	for _, tc := range []struct {
		name                   string
		cdcWriteSource         uint64
		expectedSet            bool
		expectedCdcWriteSource uint64
		expectedError          string
	}{
		{
			name:                   "cdc write source is set",
			cdcWriteSource:         1,
			expectedSet:            true,
			expectedCdcWriteSource: 1,
		},
		{
			name:                   "cdc write source is not set",
			cdcWriteSource:         0,
			expectedSet:            false,
			expectedCdcWriteSource: 0,
		},
		{
			name:           "cdc write source is not valid",
			cdcWriteSource: 16,
			expectedError:  ".*out of TiCDC write source range.*",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var txnOption uint64
			err := SetCDCWriteSource(&txnOption, tc.cdcWriteSource)
			if tc.expectedError != "" {
				require.Regexp(t, tc.expectedError, err.Error())
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.expectedSet, isCDCWriteSourceSet(txnOption))
			require.Equal(t, tc.expectedCdcWriteSource, getCDCWriteSource(txnOption))
		})
	}
}

func TestSetLossyDDLReorgSource(t *testing.T) {
	for _, tc := range []struct {
		name                        string
		currentSource               uint64
		lossyDDLReorgSource         uint64
		expectedSet                 bool
		expectedLossyDDLReorgSource uint64
		expectedError               string
	}{
		{
			name:                        "lossy ddl reorg source is set",
			currentSource:               0,
			lossyDDLReorgSource:         1,
			expectedSet:                 true,
			expectedLossyDDLReorgSource: 1,
		},
		{
			name:                        "lossy ddl reorg source is set",
			currentSource:               12, // SetCDCWriteSource
			lossyDDLReorgSource:         1,
			expectedSet:                 true,
			expectedLossyDDLReorgSource: 1,
		},
		{
			name:                        "lossy ddl reorg source is not set",
			currentSource:               12, // SetCDCWriteSource
			lossyDDLReorgSource:         0,
			expectedSet:                 false,
			expectedLossyDDLReorgSource: 0,
		},
		{
			name:                "lossy ddl reorg source is not valid",
			currentSource:       12, // SetCDCWriteSource
			lossyDDLReorgSource: 256,
			expectedError:       ".*out of lossy DDL reorg source range.*",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := SetLossyDDLReorgSource(&tc.currentSource, tc.lossyDDLReorgSource)
			if tc.expectedError != "" {
				require.Regexp(t, tc.expectedError, err.Error())
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.expectedSet, isLossyDDLReorgSourceSet(tc.currentSource))
			require.Equal(t, tc.expectedLossyDDLReorgSource, getLossyDDLReorgSource(tc.currentSource))
		})
	}
}
