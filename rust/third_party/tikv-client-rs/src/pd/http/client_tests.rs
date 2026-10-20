// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::atomic::AtomicUsize;

use super::super::api::HOT_HISTORY;
use super::super::testutil::{
    new_client_with_mock_service_discovery, new_http_client_with_request_checker,
};
use super::super::types::HistoryHotRegionsRequest;
use super::*;

// Go's tests dial `http://127.0.0.1`, port 80, expecting nothing to listen. A
// port just released by a listener refuses connections the same way without
// depending on the host's port 80.
fn refused_url() -> String {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    format!("http://127.0.0.1:{port}")
}

fn is_prototype(error: &Error, definition: errs::Definition) -> bool {
    matches!(error, Error::Pd(error) if error.is_prototype(definition))
}

// Go `TestPDAllowFollowerHandleHeader`.
#[tokio::test]
async fn test_pd_allow_follower_handle_header() {
    let checked = Arc::new(AtomicUsize::new(0));
    let counter = checked.clone();
    let http_client = new_http_client_with_request_checker(move |req| {
        let path = url::Url::parse(&req.url).unwrap().path().to_owned();
        let expected_val = if path == HOT_HISTORY { "true" } else { "" };
        let val = req
            .headers
            .get(PD_ALLOW_FOLLOWER_HANDLE_KEY)
            .map(|value| value.to_str().unwrap().to_owned())
            .unwrap_or_default();
        assert_eq!(
            expected_val, val,
            "PD allow follower handler header check failed"
        );
        counter.fetch_add(1, Ordering::SeqCst);
        Ok(())
    });
    let c = new_client_with_mock_service_discovery(
        "test-header",
        &["http://127.0.0.1".to_owned()],
        vec![with_http_client(http_client)],
    )
    .unwrap();
    c.get_regions().await.unwrap();
    c.get_history_hot_regions(&HistoryHotRegionsRequest::default())
        .await
        .unwrap();
    assert_eq!(2, checked.load(Ordering::SeqCst));
    c.close().await;
}

// Go `TestWithCallerID`.
#[tokio::test]
async fn test_with_caller_id() {
    let checked = Arc::new(AtomicUsize::new(0));
    let expected_val = Arc::new(Mutex::new(DEFAULT_CALLER_ID.to_owned()));
    let counter = checked.clone();
    let expected = expected_val.clone();
    let http_client = new_http_client_with_request_checker(move |req| {
        let val = req
            .headers
            .get(X_CALLER_ID_KEY)
            .map(|value| value.to_str().unwrap().to_owned())
            .unwrap_or_default();
        // Exclude the request sent by the inner client.
        let expected = expected.lock().unwrap().clone();
        if !val.contains(DEFAULT_INNER_CALLER_ID) {
            assert_eq!(expected, val, "Caller ID header check failed");
        }
        counter.fetch_add(1, Ordering::SeqCst);
        Ok(())
    });
    let c = new_client_with_mock_service_discovery(
        "test-caller-id",
        &["http://127.0.0.1".to_owned()],
        vec![with_http_client(http_client)],
    )
    .unwrap();
    c.get_regions().await.unwrap();
    *expected_val.lock().unwrap() = "test".to_owned();
    let caller_id = expected_val.lock().unwrap().clone();
    c.with_caller_id(caller_id).get_regions().await.unwrap();
    assert_eq!(2, checked.load(Ordering::SeqCst));
    c.close().await;
}

// Go `TestWithBackoffer`. Go bounds the infinite retry with a context deadline;
// the Rust caller bounds the request future with `tokio::time::timeout`.
#[tokio::test]
async fn test_with_backoffer() {
    let c = new_client_with_mock_service_discovery("test-with-backoffer", &[refused_url()], vec![])
        .unwrap();

    let base = 100 * 1_000_000;
    let max = 500 * 1_000_000;
    let total = 1_000_000_000;
    let bo = Backoffer::new(base, max, total);
    // Test the time cost of the backoff.
    let start = Instant::now();
    let result = c.with_backoffer(bo).get_pd_version().await;
    let elapsed = start.elapsed();
    assert!(
        elapsed >= Duration::from_millis(750) && elapsed <= Duration::from_millis(1250),
        "{elapsed:?}"
    );
    assert!(result.is_err());
    // Test if the infinite retry works.
    let bo = Backoffer::new(base, max, 0);
    let start = Instant::now();
    let result = tokio::time::timeout(
        Duration::from_secs(3),
        c.with_backoffer(bo).get_pd_version(),
    )
    .await;
    let elapsed = start.elapsed();
    assert!(
        elapsed >= Duration::from_millis(2750) && elapsed <= Duration::from_millis(3250),
        "{elapsed:?}"
    );
    assert!(result.is_err(), "the deadline ends the infinite retry");
    c.close().await;
}

// Go `TestWithTargetURL`.
#[tokio::test]
async fn test_with_target_url() {
    let urls = vec![refused_url(), refused_url(), refused_url()];
    let c = new_client_with_mock_service_discovery("test-with-target-url", &urls, vec![]).unwrap();

    let absent = loop {
        let url = refused_url();
        if !urls.contains(&url) {
            break url;
        }
    };
    let error = c.with_target_url(absent).get_status().await.unwrap_err();
    assert!(
        is_prototype(&error, errs::ERR_CLIENT_NO_TARGET_MEMBER),
        "{error}"
    );
    let error = c
        .with_target_url(urls[1].clone())
        .get_status()
        .await
        .unwrap_err();
    assert!(
        error
            .to_string()
            .to_lowercase()
            .contains("connection refused"),
        "{error}"
    );
    c.close().await;
}

// The retry loop's member order and stop conditions: the leader first, then
// the followers; a 404 ends the pass; an empty membership has no member.
#[tokio::test]
async fn test_request_with_retry_member_order() {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let seen_by_checker = seen.clone();
    let http_client = new_http_client_with_request_checker(move |req| {
        let url = url::Url::parse(&req.url).unwrap();
        seen_by_checker
            .lock()
            .unwrap()
            .push(url.host_str().unwrap().to_owned());
        Err(Error::StringError("refused".to_owned()))
    });
    let c = new_client_with_mock_service_discovery(
        "test-member-order",
        &["http://a".to_owned(), "http://b".to_owned()],
        vec![with_http_client(http_client)],
    )
    .unwrap();
    assert!(c.get_regions().await.is_err());
    assert_eq!(*seen.lock().unwrap(), vec!["a".to_owned(), "b".to_owned()]);

    let empty = new_client_with_mock_service_discovery("test-no-member", &[], vec![]).unwrap();
    let error = empty.get_regions().await.unwrap_err();
    assert!(
        is_prototype(&error, errs::ERR_CLIENT_NO_AVAILABLE_MEMBER),
        "{error}"
    );
    assert!(no_need_retry(404) && no_need_retry(403) && no_need_retry(400));
    assert!(!no_need_retry(500));
    assert!(no_need_retry_for_request_name(
        UPDATE_KEYSPACE_CONFIG_NAME,
        409
    ));
    assert!(!no_need_retry_for_request_name("GetRegions", 409));
}

// Go `json.Unmarshal` leaves a field's zero value for a `null` member; PD
// answers `{"history_hot_region": null}` when it has no history.
#[test]
fn a_null_member_decodes_to_the_zero_value() {
    use super::super::types::HistoryHotRegions;

    let body = br#"{"history_hot_region": null}"#;
    let decoded: HistoryHotRegions = decode_first_json_value(body).unwrap().unwrap();
    assert!(decoded.history_hot_region.is_empty());

    let mut target = JsonTarget::<HistoryHotRegions> { value: None };
    target.unmarshal(body).unwrap();
    assert_eq!(target.value, Some(HistoryHotRegions::default()));

    let mut target = JsonTarget::<HistoryHotRegions> { value: None };
    target.unmarshal(b"null").unwrap();
    assert_eq!(target.value, None);
}
