// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

use super::*;
use serde_json::{json, Value};

fn oracle() -> Value {
    serde_json::from_str(include_str!("../../doc/pd-errors-oracle/errors.json")).unwrap()
}
fn cause(message: &str) -> SharedError {
    Arc::new(crate::Error::StringError(message.to_owned()))
}

#[test]
fn all_definitions_codes_templates_wrapping_and_cause_generation_match_go() {
    let expected = oracle();
    let rows = expected["definitions"].as_array().unwrap();
    assert_eq!(rows.len(), DEFINITIONS.len());
    let source = cause("underlying");
    for (definition, row) in DEFINITIONS.iter().zip(rows) {
        let error = definition.error();
        assert_eq!(row["name"], definition.name);
        assert_eq!(row["code"], definition.code.unwrap_or(""));
        assert_eq!(row["message"], definition.message);
        assert_eq!(row["display"], error.to_string());
        assert!(error.is_prototype(*definition));
        assert!(error.source().is_none());
        assert!(definition.wrap(None).is_none());
        if definition.code.is_some() {
            let wrapped = definition.wrap(Some(source.clone())).unwrap();
            assert_eq!(row["wrapped"], wrapped.to_string());
            assert!(!wrapped.is_prototype(*definition));
            assert!(std::ptr::eq(
                wrapped.source().unwrap(),
                source.as_ref() as &(dyn StdError + 'static)
            ));
            let generated = wrapped.gen_with_stack_by_cause();
            assert_eq!(row["by_cause"], generated.to_string());
            assert_eq!(generated.code(), definition.code);
            assert!(generated.backtrace().is_some());
            assert!(std::ptr::eq(
                generated.source().unwrap(),
                source.as_ref() as &(dyn StdError + 'static)
            ));
        }
    }
}

#[test]
fn typed_template_arguments_match_every_formatted_source_definition() {
    let errors = [
        client_create_tso_stream("input"),
        client_get_tso("input"),
        client_get_min_tso("input"),
        client_get_leader("input"),
        client_update_member("input"),
        client_get_multi_response("input"),
        security_config("input"),
        client_list_resource_group("input"),
        client_resource_group_config_unavailable("input"),
        scheduler_config_unavailable("input"),
        client_resource_group_throttled("25ms", 1.125, -2.5),
        client_resource_group_throttled("25ms", f64::INFINITY, f64::NEG_INFINITY),
        client_resource_group_throttled("25ms", f64::NAN, -0.0),
        client_put_resource_group_mismatch_keyspace_id(0, u32::MAX),
    ];
    assert_eq!(
        json!(errors.iter().map(ToString::to_string).collect::<Vec<_>>()),
        oracle()["formatted"]
    );
    assert_eq!(
        DEFINITIONS
            .iter()
            .filter(|definition| definition.message.contains('%'))
            .count(),
        12
    );
    for error in errors {
        assert!(!error.is_prototype(error.definition()));
    }
}

#[test]
fn direct_identity_and_substring_classifiers_match_go() {
    let stream = ERR_CLIENT_TSO_STREAM_CLOSED.error();
    let mut actual = json!({
        "nil_leader":is_leader_change(None),
        "direct_stream":is_leader_change(Some(&stream)),
        "stack_stream":is_leader_change(Some(&stream.clone().with_stack())),
        "generated_stream":is_leader_change(Some(&stream.fast_gen_without_args())),
        "wrapped_stream":is_leader_change(Some(&stream.wrap(Some(cause("underlying"))).unwrap())),
        "fake_stream":is_leader_change(Some(&crate::Error::StringError(stream.to_string()))),
        "wrapped_not_leader":is_leader_change(Some(&ERR_GRPC_DIAL.wrap(Some(cause("server is not leader now"))).unwrap())),
        "nil_callee":is_callee_mismatch(None),
        "callee":is_callee_mismatch(Some(&crate::Error::StringError("server mismatch callee id".to_owned()))),
        "wrong_case":is_callee_mismatch(Some(&crate::Error::StringError("Mismatch callee id".to_owned()))),
    });
    for text in [
        NO_LEADER_ERR,
        NOT_LEADER_ERR,
        NOT_SERVED_ERR,
        NOT_PRIMARY_ERR,
        RETRY_TIMEOUT_ERR,
        MISMATCH_CALLEE_ID_ERR,
    ] {
        actual[format!("leader_{text}")] = json!(is_leader_change(Some(
            &crate::Error::StringError(format!("before {text} after"))
        )));
    }
    assert_eq!(actual, oracle()["classifiers"]);
    let envelope = crate::Error::from(stream);
    assert!(
        is_leader_change(Some(&envelope)),
        "native enum envelope preserves source identity"
    );
    let external_wrapper = ERR_GRPC_DIAL.wrap(Some(Arc::new(envelope))).unwrap();
    assert!(
        !is_leader_change(Some(&external_wrapper)),
        "Go does not unwrap for its direct sentinel comparison"
    );
}

#[test]
fn all_grpc_network_codes_match_go() {
    let actual = (0..=16)
        .map(|code| is_network_error(tonic::Code::from_i32(code)))
        .collect::<Vec<_>>();
    assert_eq!(json!(actual), oracle()["network_codes"]);
    assert!(!is_network_error(tonic::Code::ResourceExhausted));
}

#[test]
fn structured_logging_preserves_nil_first_cause_and_concrete_error_identity() {
    let proto = ERR_CLIENT_GET_TSO.error();
    let source = cause("underlying");
    let generated = client_get_tso("original");
    let static_error = ERR_CLOSING.error();
    let fields = [
        ("nil", zap_error(None, &[Some(source.clone())])),
        ("prototype", zap_error(Some(&proto), &[])),
        (
            "wrapped",
            zap_error(
                Some(&proto),
                &[Some(source.clone()), Some(cause("ignored"))],
            ),
        ),
        ("nil_cause", zap_error(Some(&proto), &[None])),
        (
            "foreign",
            zap_error(Some(source.as_ref()), &[Some(cause("ignored"))]),
        ),
        (
            "generated",
            zap_error(Some(&generated), &[Some(source.clone())]),
        ),
        (
            "static",
            zap_error(Some(&static_error), &[Some(source.clone())]),
        ),
    ];
    let expected = oracle();
    for (name, field) in fields {
        assert_eq!(
            json!(field.as_ref().map(ToString::to_string)),
            expected["logging"][name]
        );
        if let Some(field) = field {
            assert_eq!(field.key(), "error");
        }
    }
    let field = zap_error(Some(&generated), &[Some(source.clone())]).unwrap();
    assert!(std::ptr::eq(
        field.error().unwrap(),
        &generated as &(dyn StdError + 'static)
    ));
    let field = zap_error(Some(source.as_ref()), &[None]).unwrap();
    assert!(std::ptr::eq(
        field.error().unwrap(),
        source.as_ref() as &(dyn StdError + 'static)
    ));
    let field = zap_error(Some(&proto), &[Some(source.clone())]).unwrap();
    assert!(std::ptr::eq(
        field.error().unwrap().source().unwrap(),
        source.as_ref() as &(dyn StdError + 'static)
    ));
    assert!(
        proto.source().is_none(),
        "logging must not mutate the prototype"
    );
}

#[test]
fn resource_group_diagnostic_precedence_never_loses_the_original_cause() {
    let source = cause("underlying");
    let cases = [
        ClientGetResourceGroupError::default(),
        ClientGetResourceGroupError {
            resource_group_name: "group".into(),
            ..Default::default()
        },
        ClientGetResourceGroupError {
            resource_group_name: "group".into(),
            error: Some(source.clone()),
            ..Default::default()
        },
        ClientGetResourceGroupError {
            resource_group_name: "group".into(),
            cause: "explicit".into(),
            error: Some(source.clone()),
        },
        ClientGetResourceGroupError {
            resource_group_name: "group".into(),
            error: Some(cause("")),
            ..Default::default()
        },
    ];
    assert_eq!(
        json!(cases.iter().map(ToString::to_string).collect::<Vec<_>>()),
        oracle()["resource_group"]
    );
    for error in &cases[2..4] {
        assert!(std::ptr::eq(
            error.source().unwrap(),
            source.as_ref() as &(dyn StdError + 'static)
        ));
    }
    let status = tonic::Status::unavailable("original RPC");
    let resource = ClientGetResourceGroupError {
        resource_group_name: "group".into(),
        cause: "explicit".into(),
        error: Some(Arc::new(status)),
    };
    assert_eq!(
        resource
            .source()
            .unwrap()
            .downcast_ref::<tonic::Status>()
            .unwrap()
            .code(),
        tonic::Code::Unavailable
    );
}

#[test]
fn native_error_envelope_preserves_coded_owner_and_nested_rpc_cause() {
    let status: SharedError = Arc::new(tonic::Status::unavailable("original RPC"));
    let owner = ERR_GRPC_DIAL.wrap(Some(status.clone())).unwrap();
    let envelope = crate::Error::from(owner);
    let pd = envelope
        .source()
        .unwrap()
        .downcast_ref::<PdError>()
        .unwrap();
    assert_eq!(pd.code(), ERR_GRPC_DIAL.code);
    assert!(std::ptr::eq(
        pd.source().unwrap(),
        status.as_ref() as &(dyn StdError + 'static)
    ));
    assert_eq!(
        pd.source()
            .unwrap()
            .downcast_ref::<tonic::Status>()
            .unwrap()
            .code(),
        tonic::Code::Unavailable
    );
}

#[test]
fn resource_group_envelope_keeps_context_error_downcastable() {
    let resource = ClientGetResourceGroupError {
        resource_group_name: "group".into(),
        cause: "explicit".into(),
        error: Some(Arc::new(crate::Error::ContextCanceled)),
    };
    let envelope = crate::Error::from(resource);
    let owner = envelope
        .source()
        .unwrap()
        .downcast_ref::<ClientGetResourceGroupError>()
        .unwrap();
    assert!(matches!(
        owner.source().unwrap().downcast_ref::<crate::Error>(),
        Some(crate::Error::ContextCanceled)
    ));
    assert_eq!(
        envelope.to_string(),
        "get resource group group failed, explicit"
    );
}
