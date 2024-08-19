// Copyright 2024 PingCAP, Inc.
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

package cdcutil

import "path"

func (c *CDCNameSet) TESTGetChangefeedNames() []string {
	names := make([]string, 0, len(c.changefeeds))
	for ns, cfs := range c.changefeeds {
		for _, cf := range cfs {
			names = append(names, path.Join(ns, cf))
		}
	}
	return names
}
