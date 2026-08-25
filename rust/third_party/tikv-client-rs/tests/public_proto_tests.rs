// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

//! Downstream-crate checks for the public generated protocol boundary.

use prost::Message;
#[cfg(feature = "internal-tests")]
use tikv_client::proto::coprocessor;
use tikv_client::proto::{kvrpcpb, metapb};

#[test]
fn downstream_crates_can_name_and_use_generated_protocol_types() {
    let context = kvrpcpb::Context {
        region_id: 42,
        peer: Some(metapb::Peer {
            id: 7,
            store_id: 9,
            ..Default::default()
        }),
        ..Default::default()
    };

    let encoded = context.encode_to_vec();
    let decoded = kvrpcpb::Context::decode(encoded.as_slice()).unwrap();
    assert_eq!(decoded.region_id, 42);
    assert_eq!(decoded.peer.unwrap().store_id, 9);
}

#[cfg(feature = "internal-tests")]
struct DownstreamCoprocessorHandler;

#[cfg(feature = "internal-tests")]
impl tikv_client::mock::mocktikv::CoprocessorHandler for DownstreamCoprocessorHandler {
    fn handle(
        &self,
        _context: &kvrpcpb::Context,
        _session: &tikv_client::mock::mocktikv::Session,
        _request: &coprocessor::Request,
    ) -> coprocessor::Response {
        coprocessor::Response::default()
    }
}

#[cfg(feature = "internal-tests")]
#[test]
fn downstream_crates_can_implement_coprocessor_handler() {
    fn assert_handler<T: tikv_client::mock::mocktikv::CoprocessorHandler>() {}
    assert_handler::<DownstreamCoprocessorHandler>();
}
