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

package config

import (
	"flag"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/pingcap/log"
	"github.com/stretchr/testify/require"
	tikvcfg "github.com/tikv/client-go/v2/config"
	"go.uber.org/zap"
)

func TestReloadGlobalConfig(t *testing.T) {
	t.Cleanup(RestoreFunc())
	oldLevel := log.GetLevel()
	t.Cleanup(func() { log.SetLevel(oldLevel) })
	path := filepath.Join(t.TempDir(), "tidb.toml")
	initial := NewConfig()
	initial.ConfigFile = path
	initial.Port = 4333                    // Simulate a command-line override.
	initial.TxnLocalLatches.Capacity = 123 // Not serialized by CloneConf.
	StoreGlobalConfig(initial)
	contents := `# Managed externally; reload must preserve comments and bytes.
port = 4555
unknown-option = "ignored"
treat-old-version-utf8-as-utf8mb4 = false
compatible-kill-query = true
[log]
level = "warn"
[performance]
max-procs = 2
cross-join = false
pseudo-estimate-ratio = 0.5
stmt-count-limit = 123
max-memory = 999
tcp-keep-alive = false
[tikv-client]
store-limit = 100
[instance]
tidb_slow_log_threshold = 25
tidb_check_mb4_value_in_utf8 = false
`
	require.NoError(t, os.WriteFile(path, []byte(contents), 0400))
	calls := 0
	reload := func(old, next *Config) {
		calls++
		if calls == 1 {
			require.Same(t, initial, old)
		}
		require.Equal(t, uint(2), next.Performance.MaxProcs)
	}
	result, err := ReloadGlobalConfigWithResult(path, reload)
	require.NoError(t, err)
	require.Contains(t, result.Applied, "performance.stmt-count-limit")
	require.Contains(t, result.RestartRequired, "port")
	require.NotContains(t, result.RestartRequired, "host")
	require.Contains(t, result.Unknown, "unknown-option")
	current := GetGlobalConfig()
	require.Equal(t, 1, calls)
	require.Equal(t, uint(4333), current.Port)
	require.Equal(t, path, current.ConfigFile)
	require.Equal(t, initial.TxnLocalLatches, current.TxnLocalLatches)
	require.Equal(t, initial.Performance.MaxMemory, current.Performance.MaxMemory)
	require.Equal(t, initial.Performance.TCPKeepAlive, current.Performance.TCPKeepAlive)
	require.Equal(t, initial.TreatOldVersionUTF8AsUTF8MB4, current.TreatOldVersionUTF8AsUTF8MB4)
	require.True(t, current.CompatibleKillQuery)
	require.False(t, current.Performance.CrossJoin)
	require.Equal(t, 0.5, current.Performance.PseudoEstimateRatio)
	require.Equal(t, uint(123), current.Performance.StmtCountLimit)
	require.Equal(t, int64(100), current.TiKVClient.StoreLimit)
	require.Equal(t, uint64(25), current.Instance.SlowThreshold)
	require.False(t, current.Instance.CheckMb4ValueInUTF8.Load())
	require.Equal(t, zap.WarnLevel, log.GetLevel())
	require.Equal(t, "info", initial.Log.Level)
	require.True(t, initial.Instance.CheckMb4ValueInUTF8.Load())
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, contents, string(data))
	// Runtime consumers may have changed independently of the config snapshot.
	log.SetLevel(zap.ErrorLevel)
	require.NoError(t, ReloadGlobalConfig(path, reload))
	require.Equal(t, 2, calls)
	require.Equal(t, zap.WarnLevel, log.GetLevel())
	// Removing an option from the managed file restores its default.
	require.NoError(t, os.Chmod(path, 0600))
	require.NoError(t, os.WriteFile(path, []byte("port = 4999\n"), 0600))
	require.NoError(t, ReloadGlobalConfig(path, nil))
	require.Equal(t, NewConfig().Performance, GetGlobalConfig().Performance)
	require.Equal(t, uint(4333), GetGlobalConfig().Port)
	require.Equal(t, NewConfig().Log.Level, GetGlobalConfig().Log.Level)
}

func TestReloadGlobalConfigErrors(t *testing.T) {
	t.Cleanup(RestoreFunc())
	path := filepath.Join(t.TempDir(), "tidb.toml")
	initial := NewConfig()
	StoreGlobalConfig(initial)
	oldLevel := log.GetLevel()
	const secret = "reload-secret-must-not-leak"
	for _, tc := range []struct{ name, contents string }{
		{"syntax", "password = \"" + secret},
		{"type", "[performance]\nmax-procs = \"" + secret + "\""},
		{"log level", "[log]\nlevel = \"" + secret + "\""},
		{"max procs", "[performance]\nmax-procs = 2147483648"},
		{"statement limit", "[performance]\nstmt-count-limit = 18446744073709551615"},
		{"nan ratio", "[performance]\npseudo-estimate-ratio = nan"},
		{"infinite ratio", "[performance]\npseudo-estimate-ratio = inf"},
		{"negative ratio", "[performance]\npseudo-estimate-ratio = -1"},
		{"worker credentials", "tikv-worker-url = 'https://user:" + secret + "@host:19000'"},
		{"worker query", "tikv-worker-url = 'https://host:19000?token=" + secret + "'"},
		{"worker scheme", "tikv-worker-url = 'ftp://host:19000'"},
		{"worker port", "tikv-worker-url = 'host:99999'"},
		{"txn worker scheme", "[tikv-client]\ntxn-chunk-writer-addr = 'http://host:19000'"},
		{"txn worker credentials", "[tikv-client]\ntxn-chunk-writer-addr = 'user:" + secret + "@host:19000'"},
		{"negative store limit", "[log]\nlevel = 'warn'\n[tikv-client]\nstore-limit = -1"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.NoError(t, os.WriteFile(path, []byte(tc.contents), 0600))
			err := ReloadGlobalConfig(path, func(_, _ *Config) { t.Fatal("applied invalid config") })
			require.Error(t, err)
			require.NotContains(t, err.Error(), secret)
			require.Same(t, initial, GetGlobalConfig())
			require.Equal(t, oldLevel, log.GetLevel())
		})
	}
	require.ErrorContains(t, ReloadGlobalConfig("", nil), "--config")
	err := ReloadGlobalConfig(filepath.Join(t.TempDir(), secret), nil)
	require.ErrorContains(t, err, "does not exist")
	require.NotContains(t, err.Error(), secret)
	require.ErrorContains(t, ReloadGlobalConfig(t.TempDir(), nil), "cannot read")
}

func TestReloadGlobalConfigConcurrent(t *testing.T) {
	t.Cleanup(RestoreFunc())
	oldLevel := log.GetLevel()
	t.Cleanup(func() { log.SetLevel(oldLevel) })
	StoreGlobalConfig(NewConfig())
	path := filepath.Join(t.TempDir(), "tidb.toml")
	require.NoError(t, os.WriteFile(path, []byte("[performance]\nstmt-count-limit = 123"), 0600))
	var wg sync.WaitGroup
	errs := make(chan error, 8)
	for range cap(errs) {
		wg.Go(func() { errs <- ReloadGlobalConfig(path, nil) })
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	require.Equal(t, uint(123), GetGlobalConfig().Performance.StmtCountLimit)
}

func TestInitializeConfigReloadPath(t *testing.T) {
	t.Cleanup(RestoreFunc())
	oldLevel := log.GetLevel()
	t.Cleanup(func() { log.SetLevel(oldLevel) })
	StoreGlobalConfig(NewConfig())
	path := filepath.Join(t.TempDir(), "tidb.toml")
	require.NoError(t, os.WriteFile(path, []byte("port = 4555"), 0600))
	cwd, err := os.Getwd()
	require.NoError(t, err)
	relative, err := filepath.Rel(cwd, path)
	require.NoError(t, err)
	InitializeConfig(relative, false, false, func(cfg *Config, _ *flag.FlagSet) {
		cfg.Port = 4333
	}, flag.NewFlagSet("reload", flag.ContinueOnError))
	require.Equal(t, path, GetGlobalConfig().ConfigFile)
	require.NoError(t, ReloadGlobalConfig(GetGlobalConfig().ConfigFile, nil))
	require.Equal(t, uint(4333), GetGlobalConfig().Port)
}

func TestReloadGlobalConfigOverridesAndEndpoints(t *testing.T) {
	t.Cleanup(RestoreFunc())
	oldLevel := log.GetLevel()
	t.Cleanup(func() { log.SetLevel(oldLevel) })
	path := filepath.Join(t.TempDir(), "tidb.toml")
	require.NoError(t, os.WriteFile(path, []byte("tikv-worker-url = 'new-worker:19000'\n[tikv-client]\ntxn-chunk-writer-addr = 'txn-worker:19000'\n"), 0600))
	StoreGlobalConfig(NewConfig())
	flags := flag.NewFlagSet("reload", flag.ContinueOnError)
	flags.String("L", "info", "log level")
	require.NoError(t, flags.Parse([]string{"-L", "debug"}))
	InitializeConfig(path, false, false, func(c *Config, f *flag.FlagSet) { c.Log.Level = f.Lookup("L").Value.String() }, flags)
	require.NoError(t, os.WriteFile(path, []byte("tikv-worker-url = 'replacement:19000'\n[tikv-client]\ntxn-chunk-writer-addr = 'replacement-txn:19000'\n"), 0600))
	before := GetGlobalConfig()
	clientBefore := tikvcfg.GetGlobalConfig()
	result, err := ReloadGlobalConfigWithResult(path, nil)
	require.NoError(t, err)
	require.Equal(t, []string{"log.level"}, result.OverriddenByCommandLine)
	require.ElementsMatch(t, []string{"tikv-worker-url", "tikv-client.txn-chunk-writer-addr"}, result.AppliedForNewClients)
	require.Equal(t, "new-worker:19000", before.TiKVWorkerURL)
	require.Equal(t, "txn-worker:19000", clientBefore.TiKVClient.TxnChunkWriterAddr)
	require.Equal(t, "replacement-txn:19000", tikvcfg.GetGlobalConfig().TiKVClient.TxnChunkWriterAddr)
	require.Equal(t, "debug", GetGlobalConfig().Log.Level)
	require.Equal(t, "replacement:19000", GetGlobalConfig().TiKVWorkerURL)
	require.Equal(t, "replacement-txn:19000", GetGlobalConfig().TiKVClient.TxnChunkWriterAddr)
}

func TestReloadGlobalConfigMatchingLogOverride(t *testing.T) {
	t.Cleanup(RestoreFunc())
	oldLevel := log.GetLevel()
	t.Cleanup(func() { log.SetLevel(oldLevel) })
	StoreGlobalConfig(NewConfig())
	path := filepath.Join(t.TempDir(), "tidb.toml")
	require.NoError(t, os.WriteFile(path, []byte("[log]\nlevel = 'debug'"), 0600))
	flags := flag.NewFlagSet("reload", flag.ContinueOnError)
	level := flags.String("L", "info", "log level")
	require.NoError(t, flags.Parse([]string{"-L", "debug"}))
	InitializeConfig(path, false, false, func(c *Config, _ *flag.FlagSet) { c.Log.Level = *level }, flags)
	require.NoError(t, os.WriteFile(path, []byte("[log]\nlevel = 'warn'"), 0600))
	require.NoError(t, ReloadGlobalConfig(path, nil))
	require.Equal(t, "debug", GetGlobalConfig().Log.Level)
	require.Equal(t, zap.DebugLevel, log.GetLevel())
}
