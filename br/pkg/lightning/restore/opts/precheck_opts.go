// Copyright 2022 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

// opts contains all kinds of options definitions that can affect the behavior of restore & get infos.
package opts

import "github.com/pingcap/tidb/br/pkg/lightning/mydump"

type PrecheckItemBuilderConfig struct {
	PreInfoGetterOptions []GetPreInfoOption
	MDLoaderSetupOptions []mydump.MDLoaderSetupOption
}

type PrecheckItemBuilderOption func(c *PrecheckItemBuilderConfig)

func WithPreInfoGetterOptions(opts ...GetPreInfoOption) PrecheckItemBuilderOption {
	return func(c *PrecheckItemBuilderConfig) {
		c.PreInfoGetterOptions = append([]GetPreInfoOption{}, opts...)
	}
}

func WithMDLoaderSetupOptions(opts ...mydump.MDLoaderSetupOption) PrecheckItemBuilderOption {
	return func(c *PrecheckItemBuilderConfig) {
		c.MDLoaderSetupOptions = append([]mydump.MDLoaderSetupOption{}, opts...)
	}
}
