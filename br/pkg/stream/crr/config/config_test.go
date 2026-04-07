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

package config_test

import (
	"testing"
	"time"

	crrconfig "github.com/pingcap/tidb/br/pkg/stream/crr/config"
	"github.com/pingcap/tidb/br/pkg/stream/crr/internal/checkpoint"
	"github.com/pingcap/tidb/br/pkg/stream/crr/service"
	"github.com/spf13/pflag"
	"github.com/stretchr/testify/require"
)

func TestDefaultConfig(t *testing.T) {
	cfg := crrconfig.DefaultConfig()
	require.Equal(t, service.DefaultRetryInterval, cfg.RetryInterval)
	require.Equal(t, checkpoint.DefaultPollInterval, cfg.PollInterval)
	require.Equal(t, checkpoint.DefaultMetaReadConcurrency, cfg.MetaReadConcurrency)
}

func TestParse(t *testing.T) {
	flags := pflag.NewFlagSet("crr", pflag.ContinueOnError)
	crrconfig.DefineFlags(flags)
	require.NoError(t, flags.Parse([]string{
		"--task-name", "task",
		"--state-storage-sub-dir", "state/subdir",
		"--retry-interval", "7s",
		"--calc.poll-interval", "3s",
		"--calc.meta-read-concurrency", "9",
	}))

	cfg := crrconfig.Config{}
	require.NoError(t, cfg.Parse(flags))
	require.Equal(t, "task", cfg.TaskName)
	require.Equal(t, "state/subdir", cfg.StateStorageSubDir)
	require.Equal(t, 7*time.Second, cfg.RetryInterval)
	require.Equal(t, 3*time.Second, cfg.PollInterval)
	require.Equal(t, 9, cfg.MetaReadConcurrency)
}
