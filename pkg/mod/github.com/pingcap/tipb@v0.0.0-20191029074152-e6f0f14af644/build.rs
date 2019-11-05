// Copyright 2019 PingCAP, Inc.

use protobuf_build::Builder;

fn main() {
    Builder::new()
        .search_dir_for_protos("proto")
        .package_name("tipb")
        .generate()
}
