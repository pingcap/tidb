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

//! Go `const.go`.

/// Go `SASLAuthMethodSCRAMSHA1`: the authentication mechanism SCRAM-SHA-1.
pub const SASL_AUTH_METHOD_SCRAM_SHA1: &str = "SCRAM-SHA-1";

/// Go `SASLAuthMethodSCRAMSHA256`: the authentication mechanism SCRAM-SHA-256.
pub const SASL_AUTH_METHOD_SCRAM_SHA256: &str = "SCRAM-SHA-256";

/// Go `SASLAuthMethodGSSAPI`: the authentication mechanism GSSAPI.
pub const SASL_AUTH_METHOD_GSSAPI: &str = "GSSAPI";
