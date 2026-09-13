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
    println!("cargo:rerun-if-changed=proto/explain.proto");

    tonic_prost_build::configure()
        .build_client(true)
        .build_server(true)
        .boxed(".encryptionpb.MasterKey.backend.kms")
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
                "proto/explain.proto",
            ],
            &["proto"],
        )
        .expect("compile checked-in dependency-closed TiDB protocol inputs");
}
