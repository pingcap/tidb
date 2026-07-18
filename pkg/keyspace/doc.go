// Copyright 2025 PingCAP, Inc.
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

// Package keyspace provides utilities for keyspace for nextgen TiDB.
//
// Keyspace are used to isolate data and operations, allowing for multi-tenancy
// in next generation TiDB. Each keyspace represents a logical cluster on top of
// the underlying physical cluster.
//
// There are two types of keyspace: user keyspace and reserved internal keyspace,
// currently, only SYSTEM keyspace is reserved for internal use.
//
// SYSTEM keyspace is reserved for system-level services and data, currently,
// only the DXF service uses this keyspace. As user keyspace depends on SYSTEM
// keyspace, we need to make sure SYSTEM keyspace exist before user keyspace
// start serving any user traffic. So for the deployment of nextgen cluster,
// we need to:
//   - Deploy PD/TiKV and other components, wait them to be ready to serve TiDB access.
//   - Deploy SYSTEM keyspace, wait it fully bootstrapped.
//   - Deploy other user keyspace, they can be deployed concurrently.
//
// During upgrade, we also need to follow above order, i.e. We need to upgrade
// the SYSTEM keyspace first, then user keyspace.
//
// Note: serverless also use keyspace, and have the special NULL and DEFAULT
// keyspace, while nextgen hasn't.
package keyspace
