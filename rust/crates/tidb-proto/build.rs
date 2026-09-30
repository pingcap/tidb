#![allow(missing_docs)]

use prost::Message;
use prost_types::{field_descriptor_proto::Type, FileDescriptorSet, OneofDescriptorProto};
use std::path::{Path, PathBuf};

const NATIVE: &str = "../../third_party/tikv-client-rs/proto";

fn sources(directory: &Path, result: &mut Vec<PathBuf>) {
    for entry in std::fs::read_dir(directory).expect("read upstream protocol inputs") {
        let path = entry.expect("read protocol input path").path();
        if path.is_dir() {
            if path.file_name().unwrap() != "include" {
                sources(&path, result);
            }
        } else if path
            .extension()
            .is_some_and(|extension| extension == "proto")
        {
            result.push(path);
        }
    }
}

fn native_service(package: &str) -> FileDescriptorSet {
    let data = std::fs::read(
        "../../third_party/tikv-client-rs/kvproto/src/generated/file_descriptor_set.bin",
    )
    .expect("read native protocol descriptor");
    let mut descriptors = FileDescriptorSet::decode(data.as_slice())
        .expect("decode complete native protocol descriptor");
    descriptors
        .file
        .retain(|file| file.package.as_deref() == Some(package));
    descriptors
}

fn main() {
    println!("cargo:rerun-if-changed=proto/tipb");
    println!("cargo:rerun-if-changed=proto/etcd");
    println!("cargo:rerun-if-changed={NATIVE}");
    println!("cargo:rerun-if-changed=../../third_party/tikv-client-rs/kvproto/src/generated");
    let output = PathBuf::from(std::env::var_os("OUT_DIR").expect("Cargo output directory"));
    let mut inputs = Vec::new();
    sources(Path::new("proto/tipb"), &mut inputs);
    sources(Path::new("proto/etcd"), &mut inputs);
    inputs.sort();
    tonic_prost_build::configure()
        .build_client(true)
        .build_server(true)
        // Like Go's embedded Unimplemented server, a fixture overrides only
        // its supported RPCs; new upstream methods keep the standard status.
        .generate_default_stubs(true)
        .bytes(".tipb.Chunk.rows_data")
        .file_descriptor_set_path(output.join("upstream_descriptor.bin"))
        .compile_protos(
            &inputs,
            &[
                "proto/tipb",
                "proto/tipb/include",
                // raft_internal.proto imports rpc.proto by its basename. Prefer
                // this include before the module root so protoc assigns one name.
                "proto/etcd/etcd/api/etcdserverpb",
                "proto/etcd",
                NATIVE,
                "../../third_party/tikv-client-rs/proto/include",
            ]
            .map(PathBuf::from),
        )
        .expect("compile complete TiPB and etcd inputs");

    // All message and enum identities belong to the native client. This
    // generated test-server adapter adds default stubs, without maintaining
    // another request schema or a hand-written list of missing RPC methods.
    let pd_output = output.join("pd-test-server");
    std::fs::create_dir_all(&pd_output).expect("create test server output");
    tonic_prost_build::configure()
        .build_client(false)
        .build_server(true)
        .generate_default_stubs(true)
        .extern_path(".pdpb", "::tikv_client_kvproto::pdpb")
        .out_dir(pd_output)
        .compile_fds(native_service("pdpb"))
        .expect("generate complete PD test server");

    let mut tikv = native_service("tikvpb");
    for file in &mut tikv.file {
        for message in &mut file.message_type {
            // The transport multiplexes already encoded RPC bodies. Derive
            // every oneof arm from upstream so new tags cannot be omitted.
            if matches!(
                message.name.as_deref(),
                Some("BatchCommandsRequest" | "BatchCommandsResponse")
            ) {
                for nested in &mut message.nested_type {
                    for field in &mut nested.field {
                        if field.oneof_index.is_some() {
                            assert_eq!(field.r#type, Some(Type::Message as i32));
                            field.r#type = Some(Type::Bytes as i32);
                            field.type_name = None;
                        }
                    }
                }
            }
            if message.name.as_deref() == Some("BatchCommandsResponse") {
                let field = message
                    .field
                    .iter_mut()
                    .find(|field| field.name.as_deref() == Some("health_feedback"))
                    .expect("upstream batch health feedback");
                assert_eq!(field.r#type, Some(Type::Message as i32));
                field.r#type = Some(Type::Bytes as i32);
                field.type_name = None;
                // A message has presence even when empty. Preserve that with
                // proto3 optional bytes, including the synthetic oneof.
                field.proto3_optional = Some(true);
                field.oneof_index = Some(message.oneof_decl.len() as i32);
                message.oneof_decl.push(OneofDescriptorProto {
                    name: Some("_health_feedback".to_owned()),
                    ..Default::default()
                });
            }
        }
    }
    std::fs::write(
        output.join("tikv_transport_descriptor.bin"),
        tikv.encode_to_vec(),
    )
    .expect("write transport contract descriptor");
    let mut builder = tonic_prost_build::configure()
        .build_client(true)
        .build_server(true)
        .generate_default_stubs(true)
        .bytes(".tikvpb.BatchCommandsRequest.Request")
        .bytes(".tikvpb.BatchCommandsResponse.Response");
    // Resolve every imported protocol through the same native type owner.
    // Include all native modules, not a selected set of today's RPC inputs.
    let modules =
        std::fs::read_to_string("../../third_party/tikv-client-rs/kvproto/src/generated/mod.rs")
            .expect("read native generated module index");
    for line in modules.lines() {
        if let Some(package) = line
            .strip_prefix("pub mod ")
            .and_then(|line| line.strip_suffix(" {"))
        {
            if package != "tikvpb" {
                builder = builder.extern_path(
                    format!(".{package}"),
                    format!("::tikv_client_kvproto::{package}"),
                );
            }
        }
    }
    builder
        .compile_fds(tikv)
        .expect("generate complete TiKV transport view");
}
