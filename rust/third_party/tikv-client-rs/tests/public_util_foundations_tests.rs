// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

use tikv_client::{codec, intest, israce, redact, Priority};

#[test]
fn downstream_crate_can_use_priority_and_codec_surfaces() {
    let future_priority = Priority::from_i32(99);
    assert_eq!(future_priority.to_pb(), 99);

    let mut encoded = b"prefix".to_vec();
    codec::encode_bytes(&mut encoded, b"key");
    encoded.extend_from_slice(b"tail");

    let mut decoded = Vec::new();
    let leftover = codec::decode_bytes(&encoded[b"prefix".len()..], &mut decoded).unwrap();
    assert_eq!(decoded, b"key");
    assert_eq!(leftover, b"tail");
}

#[test]
#[serial_test::serial]
fn downstream_crate_can_observe_build_variants_and_redaction_modes() {
    let initial_in_test = intest::in_test();
    assert_eq!(initial_in_test, cfg!(feature = "internal-tests"));
    intest::set_in_test(!initial_in_test);
    assert_eq!(intest::in_test(), !initial_in_test);
    intest::set_in_test(initial_in_test);

    assert_eq!(israce::RACE_ENABLED, cfg!(feature = "race-tests"));

    redact::set_redact_log_mode("OFF");
    assert_eq!(redact::key(&[0xab, 0xcd]), "ABCD");
    redact::set_redact_log_mode("MARKER");
    assert_eq!(redact::key(b"secret"), "?");
    let arbitrary_bytes = [0xff, 0];
    assert_eq!(redact::string(&arbitrary_bytes), arbitrary_bytes);
    redact::set_redact_log_mode("");
}
