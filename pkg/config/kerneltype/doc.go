// Copyright 2025 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package kerneltype provides the kernel type of TiDB.
//
// We have 2 types of kernel: Classic and NextGen.
//
// TiDB Classic Kernel refers to the original architecture used during the early
// development stages of TiDB. It utilizes a share-nothing architecture, primarily
// implemented through TiKV, TiDB's distributed transactional key-value storage
// component. In this setup, each TiKV instance independently manages its own
// local storage and computing resources, eliminating dependencies on shared resources.
//
// This architecture provides advantages in terms of horizontal scalability, fault
// tolerance, and simplified management. Each node independently handles data,
// allowing for easy addition or removal of nodes to adapt to workload changes.
// However, unlike the next-generation (cloud native) kernel, it does not leverage
// shared storage solutions like S3 and requires managing local storage directly
// on each node.
//
// The TiDB Next-gen (Cloud Native) Kernel is a new architecture specifically
// designed for cloud-native infrastructure. It adopts a shared-storage architecture
// for the data plane, typically using object storage solutions like Amazon S3 as
// the single source of truth for data storage.
package kerneltype
