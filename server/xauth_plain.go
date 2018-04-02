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

package server

type saslPlainAuth struct{}

func (spa *saslPlainAuth) handleStart(mechanism *string, data []byte, initialResponses []byte) *response {
	return nil
}

func (spa *saslPlainAuth) handleContinue(data []byte) *response {
	return nil
}

// Config contains configuration options.
type Config struct {
	Addr       string `json:"addr" toml:"addr"`
	Socket     string `json:"socket" toml:"socket"`
	SkipAuth   bool   `json:"skip-auth" toml:"skip-auth"`
	TokenLimit uint   `json:"token-limit" toml:"token-limit"`
}
