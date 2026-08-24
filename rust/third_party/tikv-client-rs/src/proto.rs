// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

#![allow(clippy::large_enum_variant)]
#![allow(clippy::enum_variant_names)]

pub use protos::*;

// Rust 1.93's clippy::all flags many protobuf-generated wire types as dead code.
// Prior toolchain (1.84.1) did not fail on these generated definitions.
#[allow(clippy::doc_lazy_continuation)]
#[allow(dead_code)]
mod protos {
    include!("generated/mod.rs");
}
