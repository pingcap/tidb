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
	"net/url"
	"os"
	"reflect"
	"slices"
	"strconv"
	"strings"

	"github.com/BurntSushi/toml"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

// ReloadResult reports option names only, never configuration values or secrets.
// AppliedForNewClients takes effect when a new ingest backend or txn chunk writer
// is constructed; existing clients retain their original address.
type ReloadResult struct {
	Applied                 []string `json:"applied"`
	AppliedForNewClients    []string `json:"applied_for_new_clients"`
	RestartRequired         []string `json:"restart_required"`
	Unknown                 []string `json:"unknown"`
	OverriddenByCommandLine []string `json:"overridden_by_command_line"`
}

// ReloadGlobalConfig reloads the startup file without writing it. The callback
// applies runtime changes under the config writer lock; it must not fail or
// recursively update config. Omitted reloadable options reset to defaults.
func ReloadGlobalConfig(confFile string, reload ConfReloadFunc) error {
	_, err := ReloadGlobalConfigWithResult(confFile, reload)
	return err
}

// ReloadGlobalConfigWithResult reloads the file and reports how each option is
// handled. Read or validation failures leave configuration and runtime unchanged.
func ReloadGlobalConfigWithResult(confFile string, reload ConfReloadFunc) (ReloadResult, error) {
	result := ReloadResult{Applied: []string{}, AppliedForNewClients: []string{}, RestartRequired: []string{}, Unknown: []string{}, OverriddenByCommandLine: []string{}}
	if confFile == "" {
		return result, errors.New("configuration reload requires a startup configuration file (--config)")
	}
	// File reads are serialized too, so an older read cannot publish after a newer
	// reload. The same mutex protects SQL and HTTP read-modify-publish operations.
	globalConfigMu.Lock()
	defer globalConfigMu.Unlock()
	content, err := os.ReadFile(confFile)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return result, errors.New("startup configuration file does not exist")
		}
		return result, errors.New("cannot read startup configuration file; check file permissions")
	}
	incoming := NewConfig()
	metadata, err := toml.Decode(string(content), incoming)
	if err != nil {
		// Parser errors and addresses can contain credentials. Return no raw values.
		return result, errors.New("cannot parse startup configuration file; check TOML syntax and value types")
	}
	current := GetGlobalConfig()
	if current.LogLevelOverride != "" {
		incoming.Log.Level = current.LogLevelOverride
		result.OverriddenByCommandLine = append(result.OverriddenByCommandLine, "log.level")
	}
	next := *current
	_, rejected := MergeConfigItems(&next, incoming)
	if err := validateReloadConfig(&next); err != nil {
		return result, err
	}
	for field := range dynamicConfigItems {
		name := strings.Join(configTOMLKey(field), ".")
		switch field {
		case "TiKVWorkerURL", "TiKVClient.TxnChunkWriterAddr":
			result.AppliedForNewClients = append(result.AppliedForNewClients, name)
		case "Log.Level":
			if current.LogLevelOverride == "" {
				result.Applied = append(result.Applied, name)
			}
		default:
			result.Applied = append(result.Applied, name)
		}
	}
	for _, field := range rejected {
		key := configTOMLKey(field)
		// Defaults for omitted startup settings are not requests to change them.
		if len(key) > 0 && metadata.IsDefined(key...) {
			result.RestartRequired = append(result.RestartRequired, strings.Join(key, "."))
		}
	}
	for _, key := range metadata.Undecoded() {
		result.Unknown = append(result.Unknown, key.String())
	}
	for _, names := range [][]string{result.Applied, result.AppliedForNewClients, result.RestartRequired, result.Unknown, result.OverriddenByCommandLine} {
		slices.Sort(names)
	}
	// Reapply runtime values even if the file is unchanged: an explicit SQL SET
	// may have changed them. Cache refreshes must not subsequently undo this reload.
	if err := logutil.SetLevel(next.Log.Level); err != nil {
		return result, errors.New("cannot apply log.level")
	}
	next.StoreLimitReloaded = true
	if reload != nil {
		reload(current, &next)
	}
	storeGlobalConfig(&next)
	return result, nil
}

func validateReloadConfig(next *Config) error {
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
	if !validReloadAddress(next.TiKVWorkerURL, true) {
		return errors.New("invalid tikv-worker-url; use an HTTP(S) address without credentials, query, or fragment")
	}
	if !validReloadAddress(next.TiKVClient.TxnChunkWriterAddr, false) {
		return errors.New("invalid tikv-client.txn-chunk-writer-addr; use a host and optional port without a URL scheme")
	}
	if err := next.TiKVClient.Valid(); err != nil {
		return errors.New("invalid tikv-client configuration; check txn chunk size and concurrency limits")
	}
	level := zap.NewAtomicLevel()
	if err := level.UnmarshalText([]byte(next.Log.Level)); err != nil {
		return errors.New("invalid log.level; use debug, info, warn, error, dpanic, panic, or fatal")
	}
	return nil
}

func validReloadAddress(address string, allowScheme bool) bool {
	if address == "" {
		return true
	}
	if strings.Contains(address, "://") {
		if !allowScheme {
			return false
		}
	} else {
		address = "http://" + address
	}
	u, err := url.Parse(address)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Hostname() == "" || u.User != nil || (!allowScheme && u.Path != "") || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" {
		return false
	}
	if port := u.Port(); port != "" {
		n, err := strconv.Atoi(port)
		if err != nil || n < 1 || n > 65535 {
			return false
		}
	}
	return true
}

// configTOMLKey translates reflection field paths into public option names.
// Non-serialized runtime metadata has no TOML key.
func configTOMLKey(fieldPath string) []string {
	t := reflect.TypeOf(Config{})
	fields := strings.Split(fieldPath, ".")
	key := make([]string, 0, len(fields))
	for _, name := range fields {
		for t.Kind() == reflect.Pointer {
			t = t.Elem()
		}
		field, ok := t.FieldByName(name)
		if !ok {
			return nil
		}
		tag := strings.Split(field.Tag.Get("toml"), ",")[0]
		if tag == "-" {
			return nil
		}
		if tag == "" {
			tag = field.Name
		}
		key = append(key, tag)
		t = field.Type
	}
	return key
}
