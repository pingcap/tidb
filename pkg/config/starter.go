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

package config

// Starter holds runtime state for starter deployment mode, populated from
// keyspace meta during startup. Fields are only consulted when
// deploymode.IsStarter() is true.
type Starter struct {
	// IsBranch is true when this cluster is a branch of another cluster.
	IsBranch bool `toml:"is-branch" json:"is-branch"`
	// IsBranchBootstrapped: "True" once branch users have been amended,
	// empty when unknown (fall back to local mysql.tidb marker).
	IsBranchBootstrapped string `toml:"is-branch-bootstrapped" json:"is-branch-bootstrapped"`
	// IsBootstrappedForRestore: "True" once a restored cluster has been
	// amended; empty when not from restore.
	IsBootstrappedForRestore string `toml:"is-bootstrapped-for-restore" json:"is-bootstrapped-for-restore"`
}
