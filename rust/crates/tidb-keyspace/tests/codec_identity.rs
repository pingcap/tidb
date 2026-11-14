//! Go `zzprobe/o15_keyspace/TestCodecV1Identity`, using `pkg/keyspace.CodecV1`.

/// Go `TestCodecV1Identity`: API V1 uses NullspaceID, not the real keyspace 0.
#[test]
fn test_codec_v1_identity() {
    use tidb_keyspace::{CodecV1, KeyspaceCodec};
    assert!(CodecV1.is_api_v1());
    assert_eq!(CodecV1.keyspace_id(), u32::MAX);
    assert_eq!(tidb_keyspace::make_keyspace_etcd_namespace(&CodecV1), "");
    assert_eq!(
        tidb_keyspace::make_keyspace_etcd_namespace_slash(&CodecV1),
        ""
    );
}
