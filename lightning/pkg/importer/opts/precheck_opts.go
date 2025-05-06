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

import (
	"github.com/pingcap/tidb/pkg/lightning/mydump"
)

// PrecheckItemBuilderConfig defines the config used in a precheck builder,
// which affects the behavior for executing precheck items.
type PrecheckItemBuilderConfig struct {
	PreInfoGetterOptions []GetPreInfoOption
	MDLoaderSetupOptions []mydump.MDLoaderSetupOption
}

// PrecheckItemBuilderOption defines the options when constructing a precheck builder,
// which affects the behavior for executing precheck items.
type PrecheckItemBuilderOption func(c *PrecheckItemBuilderConfig)

// WithPreInfoGetterOptions generates a precheck item builder option
// to control the get pre info behaviors.
func WithPreInfoGetterOptions(opts ...GetPreInfoOption) PrecheckItemBuilderOption {
	return func(c *PrecheckItemBuilderConfig) {
		c.PreInfoGetterOptions = append([]GetPreInfoOption{}, opts...)
	}
}

// WithMDLoaderSetupOptions generates a precheck item builder option
// to control the mydumper loader setup behaviors.
func WithMDLoaderSetupOptions(opts ...mydump.MDLoaderSetupOption) PrecheckItemBuilderOption {
	return func(c *PrecheckItemBuilderConfig) {
		c.MDLoaderSetupOptions = append([]mydump.MDLoaderSetupOption{}, opts...)
	}
}
