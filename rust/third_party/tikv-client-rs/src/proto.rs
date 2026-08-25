// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

#![allow(clippy::large_enum_variant)]
#![allow(clippy::enum_variant_names)]

pub use protos::*;

// Rust 1.93's clippy::all flags many protobuf-generated wire types as dead code.
// Prior toolchain (1.84.1) did not fail on these generated definitions.
#[allow(clippy::doc_lazy_continuation)]
#[allow(clippy::all)]
#[allow(dead_code)]
#[allow(rustdoc::bare_urls)]
mod protos {
    include!("generated/mod.rs");
}

#[cfg(test)]
mod tests {
    use prost::Message;

    use super::{
        apipb, autoid, cdcpb, db9_coprocessor, externalworkloadpb, keyspace_encryptionpb,
        resource_manager, tsopb,
    };

    fn identity() -> apipb::KeyspaceIdentity {
        apipb::KeyspaceIdentity {
            namespace_id: 1,
            keyspace_id: 2,
        }
    }

    #[test]
    fn pinned_generated_keyspace_and_cdc_fields_preserve_wire_tags() {
        let auto_id = autoid::AutoIdRequest {
            keyspace: Some(autoid::auto_id_request::Keyspace::KeyspaceIdentity(
                identity(),
            )),
            ..Default::default()
        };
        assert_eq!(
            auto_id.encode_to_vec(),
            [0x42, 0x04, 0x08, 0x01, 0x10, 0x02]
        );
        assert_eq!(
            autoid::AutoIdRequest {
                keyspace: Some(autoid::auto_id_request::Keyspace::KeyspaceId(42)),
                ..Default::default()
            }
            .encode_to_vec(),
            [0x38, 42]
        );
        assert_eq!(
            autoid::RebaseRequest {
                keyspace: Some(autoid::rebase_request::Keyspace::KeyspaceIdentity(
                    identity(),
                )),
                ..Default::default()
            }
            .encode_to_vec(),
            [0x3a, 0x04, 0x08, 0x01, 0x10, 0x02]
        );

        assert_eq!(
            tsopb::RequestHeader {
                keyspace: Some(tsopb::request_header::Keyspace::KeyspaceIdentity(
                    identity(),
                )),
                ..Default::default()
            }
            .encode_to_vec(),
            [0x32, 0x04, 0x08, 0x01, 0x10, 0x02]
        );
        assert_eq!(
            tsopb::ResponseHeader {
                keyspace: Some(tsopb::response_header::Keyspace::KeyspaceIdentity(
                    identity(),
                )),
                ..Default::default()
            }
            .encode_to_vec(),
            [0x2a, 0x04, 0x08, 0x01, 0x10, 0x02]
        );
        assert_eq!(
            tsopb::FindGroupByKeyspaceIdRequest {
                keyspace: Some(
                    tsopb::find_group_by_keyspace_id_request::Keyspace::KeyspaceIdentity(
                        identity(),
                    ),
                ),
                ..Default::default()
            }
            .encode_to_vec(),
            [0x22, 0x04, 0x08, 0x01, 0x10, 0x02]
        );

        assert_eq!(
            resource_manager::KeyspaceIdValue {
                keyspace: Some(
                    resource_manager::keyspace_id_value::Keyspace::KeyspaceIdentity(identity()),
                ),
            }
            .encode_to_vec(),
            [0x12, 0x04, 0x08, 0x01, 0x10, 0x02]
        );
        assert_eq!(
            cdcpb::ChangeDataRequest {
                scan_priority: cdcpb::ScanPriority::High as i32,
                ..Default::default()
            }
            .encode_to_vec(),
            [0x70, 0x02]
        );

        let descriptors = crate::logutil::protobuf_descriptors();
        for (message, field, number) in [
            ("autoid.AutoIDRequest", "keyspace_identity", 8),
            ("autoid.RebaseRequest", "keyspace_identity", 7),
            ("tsopb.RequestHeader", "keyspace_identity", 6),
            ("tsopb.ResponseHeader", "keyspace_identity", 5),
            ("tsopb.FindGroupByKeyspaceIDRequest", "keyspace_identity", 4),
            ("resource_manager.KeyspaceIDValue", "keyspace_identity", 2),
            ("cdcpb.ChangeDataRequest", "scan_priority", 14),
        ] {
            assert_eq!(
                descriptors
                    .get_message_by_name(message)
                    .unwrap()
                    .get_field_by_name(field)
                    .unwrap()
                    .number(),
                number
            );
        }
    }

    #[test]
    fn pinned_generated_root_includes_every_added_kvproto_family() {
        assert_eq!(
            db9_coprocessor::Db9LimitSpec { limit: 42 }.encode_to_vec(),
            [0x08, 42]
        );
        assert_eq!(
            externalworkloadpb::RequestHeader {
                keyspace: Some(
                    externalworkloadpb::request_header::Keyspace::KeyspaceIdentity(identity()),
                ),
                ..Default::default()
            }
            .encode_to_vec(),
            [0x22, 0x04, 0x08, 0x01, 0x10, 0x02]
        );
        assert_eq!(
            keyspace_encryptionpb::EncryptionMeta {
                keyspace: Some(
                    keyspace_encryptionpb::encryption_meta::Keyspace::KeyspaceIdentity(identity()),
                ),
                ..Default::default()
            }
            .encode_to_vec(),
            [0x32, 0x04, 0x08, 0x01, 0x10, 0x02]
        );

        let descriptors = crate::logutil::protobuf_descriptors();
        assert!(descriptors
            .get_message_by_name("db9_coprocessor.Db9DagRequest")
            .is_some());
        assert!(descriptors
            .get_service_by_name("externalworkloadpb.ExternalWorkloadController")
            .is_some());
        assert!(descriptors.get_service_by_name("routerpb.Router").is_some());
    }
}
