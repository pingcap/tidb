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
	"errors"
	"math"
	"os"
	"sync"

	"github.com/BurntSushi/toml"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

var configReloadMu sync.Mutex

// ReloadGlobalConfig reads the startup TOML file without writing it. Only dynamic
// options are applied; omitted dynamic options return to their defaults. The
// callback must apply runtime changes without failing and must not call this
// function recursively. Failed reads and validation leave the live config intact.
func ReloadGlobalConfig(confFile string, reload ConfReloadFunc) error {
	configReloadMu.Lock()
	defer configReloadMu.Unlock()

	if confFile == "" {
		return errors.New("configuration reload requires a startup configuration file (--config)")
	}
	content, err := os.ReadFile(confFile)
	if err != nil {
		// File paths and TOML parser errors can contain credentials. Do not return
		// the raw errors through the status API or log the configuration contents.
		if errors.Is(err, os.ErrNotExist) {
			return errors.New("startup configuration file does not exist")
		}
		return errors.New("cannot read startup configuration file; check file permissions")
	}
	incoming := NewConfig()
	if _, err := toml.Decode(string(content), incoming); err != nil {
		return errors.New("cannot parse startup configuration file; check TOML syntax and value types")
	}
	current := GetGlobalConfig()
	// Keep startup-only and non-serialized fields, including command-line
	// overrides. MergeConfigItems only replaces allowlisted scalar fields.
	next := *current
	accepted, _ := MergeConfigItems(&next, incoming)
	if next.Performance.MaxProcs > math.MaxInt32 {
		return errors.New("performance.max-procs must be between 0 and 2147483647")
	}
	if next.Performance.StmtCountLimit > math.MaxInt {
		return errors.New("performance.stmt-count-limit exceeds the supported integer range")
	}
	ratio := next.Performance.PseudoEstimateRatio
	if math.IsNaN(ratio) || math.IsInf(ratio, 0) || ratio < 0 {
		return errors.New("performance.pseudo-estimate-ratio must be finite and nonnegative")
	}
	if next.TiKVClient.StoreLimit < 0 {
		return errors.New("tikv-client.store-limit must be nonnegative")
	}
	level := zap.NewAtomicLevel()
	if err := level.UnmarshalText([]byte(next.Log.Level)); err != nil {
		return errors.New("invalid log.level; use debug, info, warn, error, dpanic, panic, or fatal")
	}
	// SQL and other runtime setters do not necessarily update the config snapshot.
	// Reapply supported settings even when the file's values have not changed.
	if err := logutil.SetLevel(next.Log.Level); err != nil {
		return errors.New("cannot apply log.level")
	}
	if reload != nil {
		reload(current, &next)
	}
	if len(accepted) > 0 {
		StoreGlobalConfig(&next)
	}
	return nil
}
