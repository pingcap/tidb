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

// TESTGetChangefeedNames returns the changefeed names from CDCNameSet for testing.
func (s *CDCNameSet) TESTGetChangefeedNames() []string {
	if s.changefeeds == nil {
		return []string{}
	}
	result := make([]string, 0, len(s.changefeeds))
	for clusterNamespace, changefeedIDs := range s.changefeeds {
		for _, changefeedID := range changefeedIDs {
			result = append(result, clusterNamespace+"/"+changefeedID)
		}
	}
	return result
}
