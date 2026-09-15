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

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

// preparePluginModule inherits TiDB's replacements, which Go otherwise ignores
// when TiDB is a dependency of the plugin. Keep the plugin's module files intact.
func preparePluginModule(ctx context.Context, pluginDir string) (string, func(), error) {
	cmd := exec.CommandContext(ctx, "go", "list", "-m", "-json", "github.com/pingcap/tidb") // #nosec G204 -- fixed Go command, no shell.
	cmd.Dir = pluginDir
	output, err := cmd.Output()
	if err != nil {
		return "", nil, fmt.Errorf("locate plugin's TiDB module: %w", err)
	}
	var tidb struct{ Dir string }
	if err = json.Unmarshal(output, &tidb); err != nil {
		return "", nil, err
	}
	if tidb.Dir == "" {
		return "", nil, fmt.Errorf("plugin's TiDB module has no source directory")
	}
	cmd = exec.CommandContext(ctx, "go", "mod", "edit", "-json", filepath.Join(tidb.Dir, "go.mod")) // #nosec G204 -- module path is passed as an argument, not shell code.
	cmd.Dir = pluginDir
	output, err = cmd.Output()
	if err != nil {
		return "", nil, err
	}
	type module struct{ Path, Version string }
	var config struct{ Replace []struct{ Old, New module } }
	if err = json.Unmarshal(output, &config); err != nil {
		return "", nil, err
	}
	cmd = exec.CommandContext(ctx, "go", "env", "GOMOD") // #nosec G204 -- fixed Go command, no shell.
	cmd.Dir = pluginDir
	output, err = cmd.Output()
	if err != nil {
		return "", nil, err
	}
	pluginModuleDir := filepath.Dir(strings.TrimSpace(string(output)))
	dir, err := os.MkdirTemp("", "tidb-plugin-module-")
	if err != nil {
		return "", nil, err
	}
	cleanup := func() { _ = os.RemoveAll(dir) }
	modfile := filepath.Join(dir, "go.mod")
	for _, name := range []string{"go.mod", "go.sum"} {
		data, readErr := os.ReadFile(filepath.Join(pluginModuleDir, name))
		if os.IsNotExist(readErr) && name == "go.sum" {
			continue
		}
		if readErr != nil {
			cleanup()
			return "", nil, readErr
		}
		if err = os.WriteFile(filepath.Join(dir, name), data, 0600); err != nil {
			cleanup()
			return "", nil, err
		}
	}
	// Resolve the plugin's existing local replacements before moving its modfile.
	cmd = exec.CommandContext(ctx, "go", "mod", "edit", "-json", modfile) // #nosec G204 -- modfile is created by this function.
	output, err = cmd.Output()
	if err != nil {
		cleanup()
		return "", nil, err
	}
	var pluginConfig struct{ Replace []struct{ Old, New module } }
	if err = json.Unmarshal(output, &pluginConfig); err != nil {
		cleanup()
		return "", nil, err
	}
	args := []string{"mod", "edit", "-modfile=" + modfile}
	add := func(old, replacement module, base string) {
		if old.Version != "" {
			old.Path += "@" + old.Version
		}
		if replacement.Version != "" {
			replacement.Path += "@" + replacement.Version
		} else if !filepath.IsAbs(replacement.Path) {
			replacement.Path = filepath.Join(base, replacement.Path)
		}
		args = append(args, "-replace="+old.Path+"="+replacement.Path)
	}
	for _, replacement := range pluginConfig.Replace {
		add(replacement.Old, replacement.New, pluginModuleDir)
	}
	for _, replacement := range config.Replace {
		add(replacement.Old, replacement.New, tidb.Dir)
	}
	cmd = exec.CommandContext(ctx, "go", args...) // #nosec G204 -- replacement arguments are constructed above; no shell is used.
	cmd.Dir = pluginDir
	if output, err = cmd.CombinedOutput(); err != nil {
		cleanup()
		return "", nil, fmt.Errorf("prepare plugin module: %w: %s", err, output)
	}
	return modfile, cleanup, nil
}
