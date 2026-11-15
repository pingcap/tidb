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

package meta

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSPFreshBulkCleanupRequiresSweepSchedule(t *testing.T) {
	const record = `{"job_id":17,"run_id":"run-j17","storage_uri":"s3://bucket/run-j17","worker_endpoints":["http://worker:19000"],"finished_at_unix_milli":1234`
	for _, suffix := range []string{`}`, `,"next_sweep_at_unix_milli":0}`, `,"next_sweep_at_unix_milli":null}`} {
		_, err := decodeSPFreshBulkCleanup([]byte(record + suffix))
		require.ErrorContains(t, err, "incomplete SPFresh bulk cleanup record")
	}
	got, err := decodeSPFreshBulkCleanup([]byte(record + `,"next_sweep_at_unix_milli":1}`))
	require.NoError(t, err)
	require.EqualValues(t, 1, got.NextSweepAtUnixMilli)
}
