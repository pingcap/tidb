// Copyright 2025 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package schstatus

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestStatusPrint(t *testing.T) {
	status := &Status{}
	for i := range 10 {
		status.TiDBWorker.BusyNodes = append(status.TiDBWorker.BusyNodes, Node{ID: fmt.Sprintf("tidb-%d", i)})
	}
	require.Len(t, status.TiDBWorker.BusyNodes, 10)
	s := &Status{}
	require.NoError(t, json.Unmarshal([]byte(status.String()), s))
	require.Len(t, s.TiDBWorker.BusyNodes, 6)
	require.Contains(t, s.TiDBWorker.BusyNodes[5].ID, "too many nodes, total 10 busy nodes")
	require.Len(t, status.TiDBWorker.BusyNodes, 10)
}
