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

package tikvapi

import (
	"crypto/sha256"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNormalizeEndpoints(t *testing.T) {
	endpoints, err := NormalizeEndpoints([]string{
		" HTTP://Worker-0:80/ ",
		"worker-1:19000",
		"https://WORKER-2:443/",
		"HTTP://[2001:DB8::1]:19000/",
		"[2001:db8::2]:19000",
		"http://worker-0:80",
		" ",
	}, true)
	require.NoError(t, err)
	require.Equal(t, []string{
		"http://worker-0:80",
		"http://worker-1:19000",
		"https://worker-2:443",
		"http://[2001:db8::1]:19000",
		"http://[2001:db8::2]:19000",
	}, endpoints)
}

func TestNormalizeEndpointsOptionalEmpty(t *testing.T) {
	endpoints, err := NormalizeEndpoints([]string{"", " "}, false)
	require.NoError(t, err)
	require.Empty(t, endpoints)
}

func TestNormalizeEndpointsRequiredEmpty(t *testing.T) {
	_, err := NormalizeEndpoints(nil, true)
	require.EqualError(t, err, "tikv-api-service-addrs is not configured")

	_, err = NormalizeEndpoints([]string{"", " "}, true)
	require.EqualError(t, err, "tikv-api-service-addrs is not configured")
}

func TestNormalizeEndpointsRejectsInvalidEntries(t *testing.T) {
	tests := []string{
		"ftp://worker-0:19000",
		"http://",
		"http://user@worker-0:19000",
		"http://worker-0:19000/path",
		"http://worker-0:19000?debug=true",
		"http://worker-0:19000?",
		"http://worker-0:19000#fragment",
		"http:worker-0:19000",
		"https:worker-0:19000",
		"grpc:worker-0:19000",
	}

	for _, input := range tests {
		t.Run(input, func(t *testing.T) {
			_, err := NormalizeEndpoints([]string{input}, true)
			require.Error(t, err)
		})
	}
}

func TestSelectRendezvousEndpoint(t *testing.T) {
	endpoints := []string{
		"http://worker-0:19000",
		"http://worker-1:19000",
		"http://worker-2:19000",
	}

	selected, err := SelectRendezvousEndpoint("42-101-202-303", endpoints)
	require.NoError(t, err)
	require.Equal(t, "http://worker-0:19000", selected)

	selectedAgain, err := SelectRendezvousEndpoint("42-101-202-303", endpoints)
	require.NoError(t, err)
	require.Equal(t, selected, selectedAgain)

	reordered := []string{
		"http://worker-2:19000",
		"http://worker-0:19000",
		"http://worker-1:19000",
	}
	selectedReordered, err := SelectRendezvousEndpoint("42-101-202-303", reordered)
	require.NoError(t, err)
	require.Equal(t, selected, selectedReordered)
}

func TestSelectRendezvousEndpointEmpty(t *testing.T) {
	_, err := SelectRendezvousEndpoint("task", nil)
	require.EqualError(t, err, "tikv-api-service-addrs is not configured")
}

func TestSelectRendezvousEndpointTieBreaksLexicographically(t *testing.T) {
	score := func(_, _ string) [sha256.Size]byte {
		return [sha256.Size]byte{1}
	}

	selected := selectRendezvousEndpoint("task", []string{
		"http://worker-b:19000",
		"http://worker-a:19000",
	}, score)
	require.Equal(t, "http://worker-a:19000", selected)
}
