// Copyright 2021 PingCAP, Inc.

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

package option

import (
	"github.com/pingcap/tidb/kv"
)

// Setter provides functions to set and delete options.
type Setter interface {
	// SetOption sets an option with a value, when val is nil, uses the default
	// value of this option. Only ReplicaRead is supported for snapshot
	SetOption(opt kv.Option, val interface{})
	// DelOption deletes an option.
	DelOption(opt kv.Option)
}

// Getter provides the function to get the value of option.
type Getter interface {
	GetOption(opt kv.Option) interface{}
}
