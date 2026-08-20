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

package variable

// GetMaskingPolicyExprCache returns the cached object and the schema version it is built on.
func (s *SessionVars) GetMaskingPolicyExprCache() (any, int64) {
	return s.maskingPolicyExprCache, s.maskingPolicyExprCacheSchemaVersion
}

// SetMaskingPolicyExprCache stores the cache object along with its schema version.
func (s *SessionVars) SetMaskingPolicyExprCache(cache any, schemaVersion int64) {
	s.maskingPolicyExprCache = cache
	s.maskingPolicyExprCacheSchemaVersion = schemaVersion
}
