// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

//! Shared generated TiKV, PD, and ecosystem protocol types.
//!
//! `tikv-client` re-exports this crate as `tikv_client::proto`, so direct
//! consumers and the client always use one Rust type identity.

#![allow(clippy::all)]
#![allow(clippy::doc_lazy_continuation)]
#![allow(clippy::enum_variant_names)]
#![allow(clippy::large_enum_variant)]
#![allow(dead_code)]
#![allow(rustdoc::bare_urls)]

include!("generated/mod.rs");

/// Pinned descriptor set generated from the same inputs as the Rust modules.
pub const FILE_DESCRIPTOR_SET: &[u8] = include_bytes!("generated/file_descriptor_set.bin");

#[cfg(test)]
mod tests {
    use prost::Message;
    use prost_types::{DescriptorProto, FileDescriptorSet, ServiceDescriptorProto};

    use super::{
        apipb, autoid, cdcpb, db9_coprocessor, externalworkloadpb, keyspace_encryptionpb,
        resource_manager, tsopb, FILE_DESCRIPTOR_SET,
    };

    fn identity() -> apipb::KeyspaceIdentity {
        apipb::KeyspaceIdentity {
            namespace_id: 1,
            keyspace_id: 2,
        }
    }

    fn descriptor_set() -> FileDescriptorSet {
        FileDescriptorSet::decode(FILE_DESCRIPTOR_SET).unwrap()
    }

    fn message<'a>(descriptors: &'a FileDescriptorSet, full_name: &str) -> &'a DescriptorProto {
        let (package, name) = full_name.rsplit_once('.').unwrap();
        descriptors
            .file
            .iter()
            .filter(|file| file.package.as_deref() == Some(package))
            .flat_map(|file| &file.message_type)
            .find(|message| message.name.as_deref() == Some(name))
            .unwrap()
    }

    fn service<'a>(
        descriptors: &'a FileDescriptorSet,
        full_name: &str,
    ) -> &'a ServiceDescriptorProto {
        let (package, name) = full_name.rsplit_once('.').unwrap();
        descriptors
            .file
            .iter()
            .filter(|file| file.package.as_deref() == Some(package))
            .flat_map(|file| &file.service)
            .find(|service| service.name.as_deref() == Some(name))
            .unwrap()
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

        let descriptors = descriptor_set();
        for (message_name, field_name, number) in [
            ("autoid.AutoIDRequest", "keyspace_identity", 8),
            ("autoid.RebaseRequest", "keyspace_identity", 7),
            ("tsopb.RequestHeader", "keyspace_identity", 6),
            ("tsopb.ResponseHeader", "keyspace_identity", 5),
            ("tsopb.FindGroupByKeyspaceIDRequest", "keyspace_identity", 4),
            ("resource_manager.KeyspaceIDValue", "keyspace_identity", 2),
            ("cdcpb.ChangeDataRequest", "scan_priority", 14),
        ] {
            assert_eq!(
                message(&descriptors, message_name)
                    .field
                    .iter()
                    .find(|field| field.name.as_deref() == Some(field_name))
                    .unwrap()
                    .number,
                Some(number)
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

        let descriptors = descriptor_set();
        message(&descriptors, "db9_coprocessor.Db9DagRequest");
        service(
            &descriptors,
            "externalworkloadpb.ExternalWorkloadController",
        );
        service(&descriptors, "routerpb.Router");
    }
}
