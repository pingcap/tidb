#![allow(missing_docs)]

use std::path::{Path, PathBuf};

fn tipb_sources(directory: &Path, sources: &mut Vec<PathBuf>) {
    for entry in std::fs::read_dir(directory).expect("read upstream TiPB inputs") {
        let path = entry.expect("read TiPB input path").path();
        if path.is_dir() {
            // Imported protobuf/compiler options are inputs, not TiPB packages.
            if path.file_name().unwrap() != "include" {
                tipb_sources(&path, sources);
            }
        } else if path
            .extension()
            .is_some_and(|extension| extension == "proto")
        {
            sources.push(path);
        }
    }
}

fn main() {
    println!("cargo:rerun-if-changed=proto/tipb");
    println!("cargo:rerun-if-changed=proto/coprocessor.proto");
    println!("cargo:rerun-if-changed=proto/tikvpb.proto");
    println!("cargo:rerun-if-changed=proto/pdpb.proto");
    println!("cargo:rerun-if-changed=proto/mpp.proto");
    println!("cargo:rerun-if-changed=proto/mvccpb.proto");
    println!("cargo:rerun-if-changed=proto/etcdserverpb.proto");
    println!("cargo:rerun-if-changed=proto/brpb.proto");

    let mut sources = Vec::new();
    tipb_sources(Path::new("proto/tipb"), &mut sources);
    sources.sort();
    sources.extend(
        [
            "proto/coprocessor.proto",
            "proto/tikvpb.proto",
            "proto/pdpb.proto",
            "proto/mpp.proto",
            "proto/mvccpb.proto",
            "proto/etcdserverpb.proto",
            "proto/brpb.proto",
        ]
        .map(PathBuf::from),
    );

    println!("cargo:rerun-if-changed=../../third_party/tikv-client-rs/proto");
    tonic_prost_build::configure()
        // Use the complete client protocol packages, including imported message
        // identities. A local projection silently discards fields on decoding.
        .extern_path(".kvrpcpb", "::tikv_client_kvproto::kvrpcpb")
        .extern_path(".errorpb", "::tikv_client_kvproto::errorpb")
        .extern_path(".metapb", "::tikv_client_kvproto::metapb")
        .extern_path(".encryptionpb", "::tikv_client_kvproto::encryptionpb")
        .build_client(true)
        .build_server(true)
        // A coprocessor chunk's rows are sliced out of the response buffer
        // the way Go's chunk decoder points columns at the gRPC message
        // (`decodeColumn`: `col.data = buffer[:numDataBytes]`), so the
        // payload is shared rather than copied on decode.
        .bytes(".tipb.Chunk.rows_data")
        // The coprocessor response body is likewise sliced out of the gRPC
        // message rather than copied out of it, so the chunk decoder above
        // shares the bytes the transport received.
        .bytes(".coprocessor.Response.data")
        // A BatchCommands envelope carries each command's encoded body as
        // opaque bytes; a response body is sliced out of the stream frame
        // (the coprocessor response above then slices its data out of it)
        // and a request body is handed over without a copy.
        .bytes(".tikvpb.BatchCommandsRequest.Request")
        .bytes(".tikvpb.BatchCommandsResponse.Response")
        .compile_protos(
            &sources,
            &[
                "proto/tipb",
                "proto/tipb/include",
                "proto",
                "../../third_party/tikv-client-rs/proto",
                "../../third_party/tikv-client-rs/proto/include",
            ]
            .map(PathBuf::from),
        )
        .expect("compile checked-in TiDB protocol inputs");
}
