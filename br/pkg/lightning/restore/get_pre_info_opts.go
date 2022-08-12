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

package restore

type GetPreInfoConfig struct {
	IgnoreDBNotExist bool
}

func NewDefaultGetPreInfoConfig() *GetPreInfoConfig {
	return &GetPreInfoConfig{
		IgnoreDBNotExist: false,
	}
}

type GetPreInfoOption interface {
	Apply(c *GetPreInfoConfig)
}

type ignoreDBNotExistOption struct {
	ignoreDBNotExist bool
}

func (o *ignoreDBNotExistOption) Apply(c *GetPreInfoConfig) {
	c.IgnoreDBNotExist = o.ignoreDBNotExist
}

func WithIgnoreDBNotExist(ignoreDBNotExist bool) GetPreInfoOption {
	return &ignoreDBNotExistOption{
		ignoreDBNotExist: ignoreDBNotExist,
	}
}
