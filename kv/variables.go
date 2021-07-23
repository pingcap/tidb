// Copyright 2017 PingCAP, Inc.
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

package kv

import (
	tikv "github.com/tikv/client-go/v2/kv"
)

// Variables defines the variables used by KV storage. TODO:remove it when br is ready.
type Variables = tikv.Variables

// NewVariables create a new Variables instance with default values. TODO:remove it when br is ready.
func NewVariables(killed *uint32) *Variables {
	return tikv.NewVariables(killed)
}
