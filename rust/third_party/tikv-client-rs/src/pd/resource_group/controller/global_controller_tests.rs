// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

//! Go `global_controller_test.go`. Go's `TestMain` goroutine-leak check has
//! no Rust counterpart; started controllers are stopped explicitly.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use futures::FutureExt;
use serial_test::serial;
use tokio::sync::mpsc;

use super::config::{default_config, default_ru_config, BaseConfig, Config, RUVersionPolicy};
use super::context::ContextError;
use super::global_controller::{
    new_resource_group_controller, with_degraded_ru_settings, ResourceControlCreateOption,
    ResourceGroupRuntimeState, ResourceGroupsController, DEFAULT_RESOURCE_GROUP_NAME,
};
use super::group_controller::{new_group_cost_controller, new_request_source_metrics_state};
use super::model::AccessLocationType;
use super::test_support::{
    closed_channel, eventually, new_mock_resource_group_provider, ru_group,
    MockResourceGroupProvider,
};
use super::testutil::{new_test_request_info, new_test_response_info};
use crate::pd::errs::ClientGetResourceGroupError;
use crate::pd::metastorage::WatchResponse;
use crate::proto::meta_storagepb;
use crate::proto::resource_manager::{
    Consumption, GrantedRuTokenBucket, GroupRequestUnitSettings, TokenBucket, TokenBucketResponse,
    TokenBucketsRequest, TokenLimitSettings,
};
use crate::request::NULL_KEYSPACE_ID;

async fn new_controller(
    provider: &Arc<MockResourceGroupProvider>,
    keyspace_id: u32,
    opts: Vec<ResourceControlCreateOption>,
) -> Arc<ResourceGroupsController> {
    new_resource_group_controller(1, provider.clone(), None, keyspace_id, opts)
        .await
        .unwrap()
}

fn wrap_get_resource_group_err(
    name: &str,
    error: Arc<dyn std::error::Error + Send + Sync>,
) -> crate::Error {
    crate::Error::PdResourceGroup(ClientGetResourceGroupError {
        resource_group_name: name.to_owned(),
        cause: error.to_string(),
        error: Some(error),
    })
}

fn grpc_error(status: tonic::Status) -> Arc<dyn std::error::Error + Send + Sync> {
    // grpc-go's error text, which the Go tests record as the cause.
    struct GrpcError(tonic::Status);
    impl std::fmt::Debug for GrpcError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            std::fmt::Debug::fmt(&self.0, f)
        }
    }
    impl std::fmt::Display for GrpcError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str(&crate::pd::cluster::grpc_error_text(&self.0))
        }
    }
    impl std::error::Error for GrpcError {
        fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
            Some(&self.0)
        }
    }
    Arc::new(GrpcError(status))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
#[allow(non_snake_case)]
async fn source_go_resource_group_controller_global_controller_test_TestControllerWithTwoGroupRequestConcurrency(
) {
    let _scenario = fail::FailScenario::setup();
    fail::cfg(
        "triggerPeriodicReport",
        &format!("return({DEFAULT_RESOURCE_GROUP_NAME})"),
    )
    .unwrap();
    fail::cfg("triggerLowRUReport", "return(test-group)").unwrap();

    let mock_provider = new_mock_resource_group_provider();
    let controller = new_controller(&mock_provider, NULL_KEYSPACE_ID, vec![]).await;
    controller.start();

    let default_resource_group = ru_group(DEFAULT_RESOURCE_GROUP_NAME, 1000000, 0);
    let test_resource_group = ru_group("test-group", 1000000, 0);
    mock_provider.on_get_resource_group(
        DEFAULT_RESOURCE_GROUP_NAME,
        Some(default_resource_group.clone()),
    );
    mock_provider.on_get_resource_group("test-group", Some(test_resource_group.clone()));

    let c1 = controller
        .try_get_resource_group_controller(DEFAULT_RESOURCE_GROUP_NAME, false)
        .await
        .unwrap();
    assert_eq!(*c1.get_meta(), default_resource_group);

    let c2 = controller
        .try_get_resource_group_controller("test-group", false)
        .await
        .unwrap();
    assert_eq!(*c2.get_meta(), test_resource_group);

    // Report RU consumption.
    let mut total_consumption = c2.mu.lock().unwrap().consumption.clone();
    let delta = Consumption {
        r_r_u: 1.0,
        w_r_u: 2.0,
        read_bytes: 10.0,
        write_bytes: 20.0,
        total_cpu_time_ms: 30.0,
        sql_layer_cpu_time_ms: 40.0,
        kv_read_rpc_count: 50.0,
        kv_write_rpc_count: 60.0,
        read_cross_az_traffic_bytes: 100,
        write_cross_az_traffic_bytes: 200,
        ..Consumption::default()
    };
    controller.report_consumption("test-group", &delta);
    super::model::add(Some(&mut total_consumption), Some(&delta));
    assert_eq!(c2.mu.lock().unwrap().consumption, total_consumption);

    controller.report_ruv2_consumption("test-group", 3.0, 4.0, 5.0);
    total_consumption.tikv_r_u_v2 += 3.0;
    total_consumption.tidb_r_u_v2 += 4.0;
    total_consumption.tiflash_r_u_v2 += 5.0;
    assert_eq!(c2.mu.lock().unwrap().consumption, total_consumption);

    // Reports for an unknown group are ignored.
    controller.report_consumption("unknown-name", &delta);
    controller.report_ruv2_consumption("unknown-name", 1.0, 1.0, 1.0);

    let (rec_test_group_acquire_token_request, mut received) = mpsc::channel::<bool>(1);
    mock_provider.on_acquire_token_buckets(Arc::new(move |request: TokenBucketsRequest| {
        let rec = rec_test_group_acquire_token_request.clone();
        async move {
            let mut responses = Vec::new();
            for req in &request.requests {
                if req.resource_group_name == DEFAULT_RESOURCE_GROUP_NAME {
                    // The default group's request is never answered, so the
                    // controller always has a request in flight.
                    tokio::time::sleep(Duration::from_secs(100)).await;
                }
                responses.push(TokenBucketResponse {
                    resource_group_name: req.resource_group_name.clone(),
                    granted_r_u_tokens: vec![GrantedRuTokenBucket {
                        granted_tokens: Some(TokenBucket {
                            tokens: 100000.0,
                            ..TokenBucket::default()
                        }),
                        ..GrantedRuTokenBucket::default()
                    }],
                    ..TokenBucketResponse::default()
                });
            }
            // The test-group request arrived.
            if request.requests.len() == 1
                && request.requests[0].resource_group_name == "test-group"
            {
                let _ = rec.send(true).await;
            }
            // Go's mock returns the nil response captured at registration.
            drop(responses);
            Ok::<_, crate::Error>(Vec::<TokenBucketResponse>::new())
        }
        .boxed()
    }));
    // The default group requests tokens by its periodic report.
    tokio::time::sleep(Duration::from_secs(2)).await;
    c2.request_unit_tokens.limiter.notify();
    let res = tokio::time::timeout(Duration::from_secs(5), received.recv())
        .await
        .expect("timeout");
    assert_eq!(res, Some(true));
    controller.stop().await.unwrap();
}

#[tokio::test]
#[serial]
#[allow(non_snake_case)]
async fn source_go_resource_group_controller_global_controller_test_TestTryGetController() {
    let mock_provider = new_mock_resource_group_provider();
    let controller = new_controller(&mock_provider, NULL_KEYSPACE_ID, vec![]).await;
    controller.start();

    let default_resource_group = ru_group(DEFAULT_RESOURCE_GROUP_NAME, 1000000, 0);
    let test_resource_group = ru_group("test-group", 1000000, 0);
    mock_provider.on_get_resource_group(
        DEFAULT_RESOURCE_GROUP_NAME,
        Some(default_resource_group.clone()),
    );
    mock_provider.on_get_resource_group("test-group", Some(test_resource_group.clone()));
    mock_provider.on_get_resource_group("test-group-non-existent", None);

    let gc = controller
        .try_get_resource_group_controller("test-group-non-existent", false)
        .await;
    assert!(gc.is_err());
    let gc = controller
        .try_get_resource_group_controller(DEFAULT_RESOURCE_GROUP_NAME, false)
        .await
        .unwrap();
    assert_eq!(*gc.get_meta(), default_resource_group);
    let gc = controller
        .try_get_resource_group_controller("test-group", false)
        .await
        .unwrap();
    assert_eq!(*gc.get_meta(), test_resource_group);
    let request_info = new_test_request_info(true, 1, 1, AccessLocationType::AccessCrossZone);
    let response_info = new_test_response_info(1, Duration::from_millis(1), true);
    controller
        .on_request_wait("test-group", &request_info)
        .await
        .unwrap();
    let consumption = controller
        .on_response("test-group", &request_info, &response_info)
        .unwrap();
    assert_ne!(consumption, Consumption::default());
    // Mark the tombstone manually to test the fallback.
    let gc = controller
        .try_get_resource_group_controller("test-group", false)
        .await;
    assert!(gc.is_ok());
    controller
        .tombstone_group_cost_controller("test-group")
        .await;
    let gc = controller
        .try_get_resource_group_controller("test-group", false)
        .await;
    assert!(gc.is_err());
    let gc = controller
        .try_get_resource_group_controller("test-group", true)
        .await
        .unwrap();
    assert_eq!(*gc.get_meta(), default_resource_group);
    controller
        .on_request_wait("test-group", &request_info)
        .await
        .unwrap();
    let consumption = controller
        .on_response("test-group", &request_info, &response_info)
        .unwrap();
    assert_ne!(consumption, Consumption::default());
    // The default group is protected.
    let gc = controller
        .try_get_resource_group_controller(DEFAULT_RESOURCE_GROUP_NAME, false)
        .await
        .unwrap();
    assert_eq!(*gc.get_meta(), default_resource_group);
    controller
        .tombstone_group_cost_controller(DEFAULT_RESOURCE_GROUP_NAME)
        .await;
    let gc = controller
        .try_get_resource_group_controller(DEFAULT_RESOURCE_GROUP_NAME, false)
        .await
        .unwrap();
    assert_eq!(*gc.get_meta(), default_resource_group);
    let gc = controller
        .try_get_resource_group_controller(DEFAULT_RESOURCE_GROUP_NAME, true)
        .await
        .unwrap();
    assert_eq!(*gc.get_meta(), default_resource_group);
    controller
        .on_request_wait(DEFAULT_RESOURCE_GROUP_NAME, &request_info)
        .await
        .unwrap();
    let consumption = controller
        .on_response(DEFAULT_RESOURCE_GROUP_NAME, &request_info, &response_info)
        .unwrap();
    assert_ne!(consumption, Consumption::default());
    controller.stop().await.unwrap();
}

fn degraded_ru_settings() -> GroupRequestUnitSettings {
    GroupRequestUnitSettings {
        r_u: Some(TokenBucket {
            settings: Some(TokenLimitSettings {
                fill_rate: 50,
                burst_limit: 100,
                ..TokenLimitSettings::default()
            }),
            ..TokenBucket::default()
        }),
    }
}

#[tokio::test]
#[serial]
#[allow(non_snake_case)]
async fn source_go_resource_group_controller_global_controller_test_TestGetResourceGroup_transient_rm_unavailability_uses_degraded_and_recovers(
) {
    let mock_provider = new_mock_resource_group_provider();
    let controller = new_controller(
        &mock_provider,
        NULL_KEYSPACE_ID,
        vec![with_degraded_ru_settings(Some(degraded_ru_settings()))],
    )
    .await;
    let degraded_resource_group = ru_group("test-group", 50, 100);

    let real_group = ru_group("test-group", 1000000, 0);
    mock_provider.on_get_resource_group_once("test-group", || {
        Err(wrap_get_resource_group_err(
            "test-group",
            grpc_error(tonic::Status::unavailable("resource manager unavailable")),
        ))
    });
    {
        let real_group = real_group.clone();
        mock_provider
            .on_get_resource_group_once("test-group", move || Ok(Some(real_group.clone())));
    }

    let gc = controller
        .try_get_resource_group_controller("test-group", false)
        .await
        .unwrap();
    assert_eq!(*gc.get_meta(), degraded_resource_group);

    let gc = controller
        .try_get_resource_group_controller("test-group", false)
        .await
        .unwrap();
    assert_eq!(*gc.get_meta(), real_group);
    assert_eq!(mock_provider.get_resource_group_calls(), 2);
}

#[tokio::test]
#[serial]
#[allow(non_snake_case)]
async fn source_go_resource_group_controller_global_controller_test_TestGetResourceGroup_resource_group_not_found_returns_original_error(
) {
    let mock_provider = new_mock_resource_group_provider();
    let controller = new_controller(
        &mock_provider,
        NULL_KEYSPACE_ID,
        vec![with_degraded_ru_settings(Some(degraded_ru_settings()))],
    )
    .await;

    let not_found_err = || {
        wrap_get_resource_group_err(
            "test-group",
            grpc_error(tonic::Status::unknown(
                "[PD:resourcemanager:ErrGroupNotExists]the test-group resource group does not exist",
            )),
        )
    };
    let expected = not_found_err().to_string();
    mock_provider.on_get_resource_group_once("test-group", move || Err(not_found_err()));

    let error = controller
        .try_get_resource_group_controller("test-group", false)
        .await
        .err()
        .unwrap();
    assert!(matches!(error, crate::Error::PdResourceGroup(_)));
    assert_eq!(error.to_string(), expected);
    assert_eq!(mock_provider.get_resource_group_calls(), 1);
}

#[tokio::test]
#[serial]
#[allow(non_snake_case)]
async fn source_go_resource_group_controller_global_controller_test_TestGetResourceGroup_caller_cancellation_returns_context_canceled(
) {
    let mock_provider = new_mock_resource_group_provider();
    let controller = new_controller(
        &mock_provider,
        NULL_KEYSPACE_ID,
        vec![with_degraded_ru_settings(Some(degraded_ru_settings()))],
    )
    .await;

    mock_provider.on_get_resource_group_once("test-group", || {
        Err(wrap_get_resource_group_err(
            "test-group",
            Arc::new(ContextError::Canceled),
        ))
    });

    let error = controller
        .try_get_resource_group_controller("test-group", false)
        .await
        .err()
        .unwrap();
    assert!(matches!(error, crate::Error::ContextCanceled));
    assert_eq!(mock_provider.get_resource_group_calls(), 1);
}

#[tokio::test]
#[serial]
#[allow(non_snake_case)]
async fn source_go_resource_group_controller_global_controller_test_TestGetResourceGroup_caller_deadline_returns_context_deadline_exceeded(
) {
    let mock_provider = new_mock_resource_group_provider();
    let controller = new_controller(
        &mock_provider,
        NULL_KEYSPACE_ID,
        vec![with_degraded_ru_settings(Some(degraded_ru_settings()))],
    )
    .await;

    mock_provider.on_get_resource_group_once("test-group", || {
        Err(wrap_get_resource_group_err(
            "test-group",
            Arc::new(ContextError::DeadlineExceeded),
        ))
    });

    let error = controller
        .try_get_resource_group_controller("test-group", false)
        .await
        .err()
        .unwrap();
    assert!(matches!(
        &error,
        crate::Error::GrpcAPI(status)
            if status.code() == tonic::Code::DeadlineExceeded
                && status.message() == "context deadline exceeded"
    ));
    assert_eq!(mock_provider.get_resource_group_calls(), 1);
}

#[tokio::test]
#[serial]
#[allow(non_snake_case)]
async fn source_go_resource_group_controller_global_controller_test_TestGetResourceGroup_server_grpc_status_without_degraded_settings_preserves_original_error(
) {
    for (name, code) in [
        ("canceled", tonic::Code::Cancelled),
        ("deadline-exceeded", tonic::Code::DeadlineExceeded),
    ] {
        let mock_provider = new_mock_resource_group_provider();
        let controller = new_controller(&mock_provider, NULL_KEYSPACE_ID, vec![]).await;

        let wrapped_err = move || {
            wrap_get_resource_group_err(
                "test-group",
                grpc_error(tonic::Status::new(
                    code,
                    "resource manager returned an error",
                )),
            )
        };
        let expected = wrapped_err().to_string();
        mock_provider.on_get_resource_group_once("test-group", move || Err(wrapped_err()));

        let error = controller
            .try_get_resource_group_controller("test-group", false)
            .await
            .err()
            .unwrap();
        assert!(matches!(error, crate::Error::PdResourceGroup(_)), "{name}");
        assert_eq!(error.to_string(), expected, "{name}");
        assert_eq!(super::global_controller::grpc_code(&error), code, "{name}");
        assert_eq!(mock_provider.get_resource_group_calls(), 1, "{name}");
    }
}

#[tokio::test]
#[serial]
#[allow(non_snake_case)]
async fn source_go_resource_group_controller_global_controller_test_TestGetResourceGroup_grpc_deadline_exceeded_uses_degraded_group(
) {
    let mock_provider = new_mock_resource_group_provider();
    let controller = new_controller(
        &mock_provider,
        NULL_KEYSPACE_ID,
        vec![with_degraded_ru_settings(Some(degraded_ru_settings()))],
    )
    .await;

    mock_provider.on_get_resource_group_once("test-group", || {
        Err(wrap_get_resource_group_err(
            "test-group",
            grpc_error(tonic::Status::deadline_exceeded("rpc deadline exceeded")),
        ))
    });

    let gc = controller
        .try_get_resource_group_controller("test-group", false)
        .await
        .unwrap();
    assert_eq!(*gc.get_meta(), ru_group("test-group", 50, 100));
    assert_eq!(mock_provider.get_resource_group_calls(), 1);
}

#[tokio::test]
#[serial]
#[allow(non_snake_case)]
async fn source_go_resource_group_controller_global_controller_test_TestGetResourceGroup_generic_non_retryable_error_returns_original_error(
) {
    let mock_provider = new_mock_resource_group_provider();
    let controller = new_controller(
        &mock_provider,
        NULL_KEYSPACE_ID,
        vec![with_degraded_ru_settings(Some(degraded_ru_settings()))],
    )
    .await;

    let permission_err = || {
        wrap_get_resource_group_err(
            "test-group",
            grpc_error(tonic::Status::permission_denied("permission denied")),
        )
    };
    let expected = permission_err().to_string();
    mock_provider.on_get_resource_group_once("test-group", move || Err(permission_err()));

    let error = controller
        .try_get_resource_group_controller("test-group", false)
        .await
        .err()
        .unwrap();
    assert_eq!(error.to_string(), expected);
    assert_eq!(mock_provider.get_resource_group_calls(), 1);
}

#[tokio::test]
#[serial]
#[allow(non_snake_case)]
async fn source_go_resource_group_controller_global_controller_test_TestGetResourceGroup_without_degraded_settings_propagates_transient_error(
) {
    let mock_provider = new_mock_resource_group_provider();
    let controller = new_controller(&mock_provider, NULL_KEYSPACE_ID, vec![]).await;

    let unavailable_err = || {
        wrap_get_resource_group_err(
            "test-group",
            grpc_error(tonic::Status::unavailable("resource manager unavailable")),
        )
    };
    let expected = unavailable_err().to_string();
    mock_provider.on_get_resource_group_once("test-group", move || Err(unavailable_err()));

    let error = controller
        .try_get_resource_group_controller("test-group", false)
        .await
        .err()
        .unwrap();
    assert_eq!(error.to_string(), expected);
    assert_eq!(mock_provider.get_resource_group_calls(), 1);
}

#[test]
#[serial]
#[allow(non_snake_case)]
fn source_go_resource_group_controller_global_controller_test_TestGetResourceGroupRuntimeState() {
    struct Case {
        name: &'static str,
        response_burst_limit: i64,
        send_response: bool,
        resource_group_name: &'static str,
        expect_ok: bool,
        expect_state: ResourceGroupRuntimeState,
    }
    let test_cases = [
        Case {
            name: "unknown before first token response",
            response_burst_limit: 0,
            send_response: false,
            resource_group_name: "test-group",
            expect_ok: false,
            expect_state: ResourceGroupRuntimeState::default(),
        },
        Case {
            name: "unlimited response remains burstable",
            response_burst_limit: -1,
            send_response: true,
            resource_group_name: "test-group",
            expect_ok: true,
            expect_state: ResourceGroupRuntimeState::default(),
        },
        Case {
            name: "override limited burst from token response",
            response_burst_limit: 100,
            send_response: true,
            resource_group_name: "test-group",
            expect_ok: true,
            expect_state: ResourceGroupRuntimeState {
                has_limited_burst: true,
            },
        },
        Case {
            name: "unknown resource group",
            response_burst_limit: 100,
            send_response: true,
            resource_group_name: "unknown",
            expect_ok: false,
            expect_state: ResourceGroupRuntimeState::default(),
        },
    ];

    for tc in test_cases {
        let group = ru_group("test-group", 1000, -1);
        let gc = new_group_cost_controller(
            Arc::new(group.clone()),
            Arc::new(default_ru_config()),
            closed_channel(),
            closed_channel(),
            Some(new_request_source_metrics_state(&group.name)),
        )
        .unwrap();

        let controller = ResourceGroupsController::build(
            0,
            new_mock_resource_group_provider(),
            Default::default(),
            0,
            vec![],
        );
        controller.store_group_controller(&group.name, gc.clone());

        if tc.send_response {
            controller.handle_token_bucket_response(vec![TokenBucketResponse {
                resource_group_name: group.name.clone(),
                granted_r_u_tokens: vec![GrantedRuTokenBucket {
                    granted_tokens: Some(TokenBucket {
                        settings: Some(TokenLimitSettings {
                            fill_rate: 1000,
                            burst_limit: tc.response_burst_limit,
                            ..TokenLimitSettings::default()
                        }),
                        tokens: 1000.0,
                    }),
                    ..GrantedRuTokenBucket::default()
                }],
                ..TokenBucketResponse::default()
            }]);
            // Mirror the main loop, which refreshes the derived state (such
            // as burstable) after handling token bucket responses.
            gc.update_run_state();
            gc.update_avg_request_resource_per_sec();
        }

        let state = controller.get_resource_group_runtime_state(tc.resource_group_name);
        assert_eq!(tc.expect_ok, state.is_some(), "{}", tc.name);
        assert_eq!(tc.expect_state, state.unwrap_or_default(), "{}", tc.name);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
#[allow(non_snake_case)]
async fn source_go_resource_group_controller_global_controller_test_TestTokenBucketsRequestWithKeyspaceID(
) {
    async fn check_keyspace(keyspace_id: u32) {
        let mock_provider = new_mock_resource_group_provider();
        let controller = new_controller(&mock_provider, keyspace_id, vec![]).await;
        controller.start();

        let test_resource_group = ru_group("test-group", 1000000, 0);
        mock_provider.on_get_resource_group("test-group", Some(test_resource_group));

        let gc = controller
            .try_get_resource_group_controller("test-group", false)
            .await
            .unwrap();

        let (request_received, mut received) = mpsc::channel::<Result<(), String>>(1);
        mock_provider.on_acquire_token_buckets(Arc::new(move |request: TokenBucketsRequest| {
            let request_received = request_received.clone();
            async move {
                let check = (|| {
                    if request.requests.len() != 1 {
                        return Err(format!(
                            "expected 1 request, got {}",
                            request.requests.len()
                        ));
                    }
                    let keyspace = request.requests[0]
                        .keyspace_id
                        .as_ref()
                        .ok_or("missing keyspace id")?;
                    match keyspace.keyspace {
                        Some(
                            crate::proto::resource_manager::keyspace_id_value::Keyspace::Value(
                                value,
                            ),
                        ) if value == keyspace_id => Ok(()),
                        ref other => Err(format!("unexpected keyspace {other:?}")),
                    }
                })();
                let _ = request_received.try_send(check);
                Ok::<_, crate::Error>(Vec::<TokenBucketResponse>::new())
            }
            .boxed()
        }));

        // A low-token report makes the controller collect token requests.
        gc.request_unit_tokens.limiter.notify();

        let result = tokio::time::timeout(Duration::from_secs(5), received.recv())
            .await
            .expect("timed out waiting for AcquireTokenBuckets to be called")
            .unwrap();
        result.unwrap();
        controller.stop().await.unwrap();
    }
    check_keyspace(NULL_KEYSPACE_ID).await;
    check_keyspace(1).await;
}

#[tokio::test]
#[serial]
#[allow(non_snake_case)]
async fn source_go_resource_group_controller_global_controller_test_TestGetRUVersionDefault() {
    let mock_provider = new_mock_resource_group_provider();
    let gc = new_controller(&mock_provider, 1, vec![]).await;

    // v1 when no policy is set.
    assert_eq!(gc.get_ru_version(), 1);
}

#[tokio::test]
#[serial]
#[allow(non_snake_case)]
async fn source_go_resource_group_controller_global_controller_test_TestGetRUVersionAfterSet() {
    use std::sync::atomic::Ordering;
    let mock_provider = new_mock_resource_group_provider();
    let gc = new_controller(&mock_provider, 1, vec![]).await;

    // An RU version update through the atomic.
    gc.ru_version.store(3, Ordering::SeqCst);
    assert_eq!(gc.get_ru_version(), 3);

    // Zero answers the default 1.
    gc.ru_version.store(0, Ordering::SeqCst);
    assert_eq!(gc.get_ru_version(), 1);

    // Negative answers the default too.
    gc.ru_version.store(-1, Ordering::SeqCst);
    assert_eq!(gc.get_ru_version(), 1);
}

fn policy(default: i32, overrides: &[(u32, i32)]) -> Option<RUVersionPolicy> {
    Some(RUVersionPolicy {
        default,
        overrides: overrides.iter().copied().collect::<HashMap<_, _>>(),
    })
}

#[tokio::test]
#[serial]
#[allow(non_snake_case)]
async fn source_go_resource_group_controller_global_controller_test_TestRUVersionFromControllerConfig(
) {
    let mock_provider = new_mock_resource_group_provider();
    let gc = new_controller(&mock_provider, 42, vec![]).await;

    // A policy with an override for keyspace 42.
    let mut config = default_config();
    config.base_config.ru_version_policy = policy(1, &[(42, 3)]);
    gc.update_ru_version_from_config(&config);
    assert_eq!(gc.get_ru_version(), 3);
}

#[tokio::test]
#[serial]
#[allow(non_snake_case)]
async fn source_go_resource_group_controller_global_controller_test_TestRUVersionOverrideFromControllerConfig(
) {
    let mock_provider = new_mock_resource_group_provider();
    let gc = new_controller(&mock_provider, 42, vec![]).await;

    // The override takes precedence over the default.
    let mut config = default_config();
    config.base_config.ru_version_policy = policy(5, &[(42, 3), (100, 7)]);
    gc.update_ru_version_from_config(&config);
    assert_eq!(gc.get_ru_version(), 3);
}

#[tokio::test]
#[serial]
#[allow(non_snake_case)]
async fn source_go_resource_group_controller_global_controller_test_TestRUVersionDefaultFallback() {
    let mock_provider = new_mock_resource_group_provider();
    let gc = new_controller(&mock_provider, 42, vec![]).await;

    let mut config = default_config();
    config.base_config.ru_version_policy = policy(5, &[(100, 7)]);
    gc.update_ru_version_from_config(&config);
    // Without an override for keyspace 42 the default applies.
    assert_eq!(gc.get_ru_version(), 5);
}

#[tokio::test]
#[serial]
#[allow(non_snake_case)]
async fn source_go_resource_group_controller_global_controller_test_TestRUVersionNilPolicy() {
    use std::sync::atomic::Ordering;
    let mock_provider = new_mock_resource_group_provider();
    let gc = new_controller(&mock_provider, 42, vec![]).await;

    // A non-default version first.
    gc.ru_version.store(5, Ordering::SeqCst);
    assert_eq!(gc.get_ru_version(), 5);

    // A nil policy resets to 0, answered as the default 1.
    let mut config = default_config();
    config.base_config.ru_version_policy = None;
    gc.update_ru_version_from_config(&config);
    assert_eq!(gc.get_ru_version(), 1);
}

#[tokio::test]
#[serial]
#[allow(non_snake_case)]
async fn source_go_resource_group_controller_global_controller_test_TestRUVersionFromInitialControllerConfig(
) {
    // The provider answers a controller config with an RU version policy.
    let config_with_policy = Config {
        base_config: BaseConfig {
            ru_version_policy: policy(1, &[(42, 3)]),
            ..BaseConfig::default()
        },
        ..Config::default()
    };
    let config_bytes = config_with_policy.marshal_json().into_bytes();

    let mock_provider = new_mock_resource_group_provider();
    mock_provider.set_get_response(meta_storagepb::GetResponse {
        header: Some(meta_storagepb::ResponseHeader {
            revision: 1,
            ..meta_storagepb::ResponseHeader::default()
        }),
        kvs: vec![meta_storagepb::KeyValue {
            value: config_bytes,
            ..meta_storagepb::KeyValue::default()
        }],
        ..meta_storagepb::GetResponse::default()
    });

    let gc = new_controller(&mock_provider, 42, vec![]).await;
    // The initial load sets the RU version from the policy.
    assert_eq!(gc.get_ru_version(), 3);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
#[allow(non_snake_case)]
async fn source_go_resource_group_controller_global_controller_test_TestRUVersionWatchViaControllerConfig(
) {
    let mock_provider = new_mock_resource_group_provider();
    mock_provider.set_get_response(meta_storagepb::GetResponse {
        header: Some(meta_storagepb::ResponseHeader {
            revision: 1,
            ..meta_storagepb::ResponseHeader::default()
        }),
        ..meta_storagepb::GetResponse::default()
    });
    let (watch_config_chan, watch_config_rx) = mpsc::channel::<WatchResponse>(1);
    mock_provider.on_watch_config(watch_config_rx);

    let controller = new_controller(&mock_provider, 42, vec![]).await;
    controller.start();

    let put_event = |value: Vec<u8>| WatchResponse {
        events: vec![meta_storagepb::Event {
            r#type: meta_storagepb::event::EventType::Put as i32,
            kv: Some(meta_storagepb::KeyValue {
                value,
                ..meta_storagepb::KeyValue::default()
            }),
            ..meta_storagepb::Event::default()
        }],
        ..WatchResponse::default()
    };

    // Case 1: a policy with an override for keyspace 42.
    let config_with_policy = Config {
        base_config: BaseConfig {
            ru_version_policy: policy(1, &[(42, 3)]),
            ..BaseConfig::default()
        },
        ..Config::default()
    };
    watch_config_chan
        .send(put_event(config_with_policy.marshal_json().into_bytes()))
        .await
        .unwrap();
    assert!(
        eventually(
            || controller.get_ru_version() == 3,
            Duration::from_secs(20),
            Duration::from_millis(100)
        )
        .await
    );

    // Case 2: a config without a policy resets to the default.
    let config_no_policy = Config::default();
    watch_config_chan
        .send(put_event(config_no_policy.marshal_json().into_bytes()))
        .await
        .unwrap();
    assert!(
        eventually(
            || controller.get_ru_version() == 1,
            Duration::from_secs(20),
            Duration::from_millis(100)
        )
        .await
    );
    controller.stop().await.unwrap();
}

#[tokio::test]
#[serial]
async fn interceptor_settles_through_go_on_response() {
    // The client-rust interceptor reaches the same group accounting.
    use crate::resource_control::ResourceGroupController as _;
    let mock_provider = new_mock_resource_group_provider();
    let controller = new_controller(&mock_provider, NULL_KEYSPACE_ID, vec![]).await;
    mock_provider.on_get_resource_group("test-group", Some(ru_group("test-group", 1000000, 0)));
    let request = crate::resource_control::RequestInfo::new(Some(64), 1, 1, false);
    let admitted = crate::resource_control::ResourceGroupController::on_request_wait(
        controller.as_ref(),
        "test-group",
        request,
    )
    .await
    .unwrap();
    assert!(admitted.consumption.w_r_u > 0.0);
    assert!(admitted.penalty.is_some());
    let settled = crate::resource_control::ResourceGroupController::on_response_wait(
        controller.as_ref(),
        "test-group",
        request,
        crate::resource_control::ResponseInfo::default(),
    )
    .unwrap();
    assert_eq!(settled.wait_duration, Duration::ZERO);
    let settled = controller
        .on_response_wait_async(
            "test-group",
            request,
            crate::resource_control::ResponseInfo::default(),
        )
        .await
        .unwrap();
    assert_eq!(settled.wait_duration, Duration::ZERO);
}
