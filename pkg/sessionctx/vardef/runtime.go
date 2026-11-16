// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package vardef

import "sync/atomic"

// disableLegacyECDSASessionToken rejects legacy raw-input ECDSA signatures, which
// leave token timestamps unauthenticated because ECDSA truncates the JSON input.
// Keep this false until the cluster upgrade gate is durably completed.
var disableLegacyECDSASessionToken atomic.Bool

// IsLegacyECDSASessionTokenDisabled reports whether legacy raw-input ECDSA
// session-token signatures are rejected.
func IsLegacyECDSASessionTokenDisabled() bool {
	return disableLegacyECDSASessionToken.Load()
}

// SetLegacyECDSASessionTokenDisabled controls whether legacy raw-input ECDSA
// session-token signatures are rejected.
func SetLegacyECDSASessionTokenDisabled(disabled bool) {
	disableLegacyECDSASessionToken.Store(disabled)
}
