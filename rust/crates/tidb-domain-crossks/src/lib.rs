// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Go `pkg/domain/crossks`: ownership of target-keyspace runtimes.
//!
//! The manager owns its session managers; handles explicitly release holder IDs.
//! Resource construction and shutdown live in the session manager implementation,
//! allowing the same manager to exercise Go's internal tests with counted resources.

/// Go `cross_ks.go`: handle ownership and eviction.
pub mod cross_ks;
pub use cross_ks::{Manager, SessionManager};
