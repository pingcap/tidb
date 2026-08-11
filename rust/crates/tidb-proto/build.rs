#![allow(missing_docs)]

fn main() {
    println!("cargo:rerun-if-changed=proto/resourcetag.proto");
    println!("cargo:rerun-if-changed=proto/select.proto");
    println!("cargo:rerun-if-changed=proto/errorpb.proto");
    println!("cargo:rerun-if-changed=proto/kvrpcpb.proto");
    println!("cargo:rerun-if-changed=proto/coprocessor.proto");
    println!("cargo:rerun-if-changed=proto/tikvpb.proto");
    println!("cargo:rerun-if-changed=proto/metapb.proto");
    println!("cargo:rerun-if-changed=proto/pdpb.proto");
    println!("cargo:rerun-if-changed=proto/mpp.proto");
    println!("cargo:rerun-if-changed=proto/mvccpb.proto");
    println!("cargo:rerun-if-changed=proto/etcdserverpb.proto");
    println!("cargo:rerun-if-changed=proto/brpb.proto");
    println!("cargo:rerun-if-changed=proto/encryptionpb.proto");

    tonic_prost_build::configure()
        .build_client(true)
        .build_server(true)
        .boxed(".encryptionpb.MasterKey.backend.kms")
        .compile_protos(
            &[
                "proto/resourcetag.proto",
                "proto/select.proto",
                "proto/errorpb.proto",
                "proto/kvrpcpb.proto",
                "proto/coprocessor.proto",
                "proto/tikvpb.proto",
                "proto/metapb.proto",
                "proto/pdpb.proto",
                "proto/mpp.proto",
                "proto/mvccpb.proto",
                "proto/etcdserverpb.proto",
                "proto/brpb.proto",
                "proto/encryptionpb.proto",
            ],
            &["proto"],
        )
        .expect("compile checked-in dependency-closed TiDB protocol inputs");
}
