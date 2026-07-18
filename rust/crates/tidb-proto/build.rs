#![allow(missing_docs)]

fn main() {
    println!("cargo:rerun-if-changed=proto/resourcetag.proto");
    println!("cargo:rerun-if-changed=proto/select.proto");
    println!("cargo:rerun-if-changed=proto/errorpb.proto");
    println!("cargo:rerun-if-changed=proto/kvrpcpb.proto");
    println!("cargo:rerun-if-changed=proto/coprocessor.proto");

    prost_build::Config::new()
        .compile_protos(
            &[
                "proto/resourcetag.proto",
                "proto/select.proto",
                "proto/errorpb.proto",
                "proto/kvrpcpb.proto",
                "proto/coprocessor.proto",
            ],
            &["proto"],
        )
        .expect("compile checked-in tipb resource-tag and select response protos");
}
