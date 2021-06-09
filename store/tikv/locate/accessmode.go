// Copyright 2021 PingCAP, Inc.
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

package locate

import (
	"fmt"
)

// accessMode uses to index stores for different region cache access requirements.
type accessMode int

const (
	// tiKVOnly indicates stores list that use for TiKv access(include both leader request and follower read).
	tiKVOnly accessMode = iota
	// tiFlashOnly indicates stores list that use for TiFlash request.
	tiFlashOnly
	// numAccessMode reserved to keep max access mode value.
	numAccessMode
)

func (a accessMode) String() string {
	switch a {
	case tiKVOnly:
		return "TiKvOnly"
	case tiFlashOnly:
		return "TiFlashOnly"
	default:
		return fmt.Sprintf("%d", a)
	}
}
