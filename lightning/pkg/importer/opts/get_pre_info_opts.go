// Copyright 2023 PingCAP, Inc.
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

// Package opts contains all kinds of options definitions that can affect the behavior of restore & get infos.
package opts

// GetPreInfoConfig stores some configs to affect behavior to get pre restore infos.
type GetPreInfoConfig struct {
	IgnoreDBNotExist bool
	ForceReloadCache bool
}

// Clone clones a new independent config object from the original one.
func (c *GetPreInfoConfig) Clone() *GetPreInfoConfig {
	clonedCfg := NewDefaultGetPreInfoConfig()
	if c != nil {
		*clonedCfg = *c
	}
	return clonedCfg
}

// NewDefaultGetPreInfoConfig returns the default get-pre-info config.
func NewDefaultGetPreInfoConfig() *GetPreInfoConfig {
	return &GetPreInfoConfig{
		IgnoreDBNotExist: false,
		ForceReloadCache: false,
	}
}

// GetPreInfoOption defines the type for passing optional arguments for PreInfoGetter methods.
type GetPreInfoOption func(c *GetPreInfoConfig)

// WithIgnoreDBNotExist sets whether to ignore DB not exist error when getting DB schemas.
func WithIgnoreDBNotExist(ignoreDBNotExist bool) GetPreInfoOption {
	return func(c *GetPreInfoConfig) {
		c.IgnoreDBNotExist = ignoreDBNotExist
	}
}

// ForceReloadCache sets whether to reload the cache for some caching results.
func ForceReloadCache(forceReloadCache bool) GetPreInfoOption {
	return func(c *GetPreInfoConfig) {
		c.ForceReloadCache = forceReloadCache
	}
}
