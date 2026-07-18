// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Source-derived response-channel ordering, error, and close lifecycle tests.

use tidb_distsql::{
    unsupported_raw_tipb_response, unsupported_tikv_response_channel, ResponseChannel,
    ResponseChannelError, ResponseChannelEvent, ResponseChannelState, ResponseChannelUnsupported,
    Warning, WarningClass, WarningLevel,
};

#[test]
fn go_fetch_response_preserves_result_warning_and_close_order() {
    // Structural port of `pkg/distsql/select_result.go:369-486` and
    // `pkg/distsql/select_result_test.go:288 TestSelectResultIter`: a fetched
    // response owns its result and warnings, then reaches an explicit end.
    let mut channel = ResponseChannel::new();
    channel.push_result(7).unwrap();
    channel
        .push_warning(Warning {
            level: WarningLevel::Warning,
            class: WarningClass::Statement,
            code: None,
            message: "source warning".to_owned(),
        })
        .unwrap();
    channel.finish().unwrap();

    assert_eq!(channel.state(), ResponseChannelState::Closed);
    assert_eq!(channel.next_event(), Some(ResponseChannelEvent::Result(7)));
    assert_eq!(
        channel.next_event(),
        Some(ResponseChannelEvent::Warning(Warning {
            level: WarningLevel::Warning,
            class: WarningClass::Statement,
            code: None,
            message: "source warning".to_owned(),
        }))
    );
    assert_eq!(channel.next_event(), Some(ResponseChannelEvent::Closed));
    assert_eq!(channel.next_event(), None);
}

#[test]
fn go_fetch_response_reports_terminal_error_after_owned_events() {
    // Structural port of `pkg/distsql/select_result.go:400-418`: a source
    // error terminates the response fetch without inventing a decoded row.
    let mut channel = ResponseChannel::from_events([ResponseChannelEvent::Result("row")]);
    channel.fail("response failed").unwrap();

    assert_eq!(
        channel.next_event(),
        Some(ResponseChannelEvent::Result("row"))
    );
    assert_eq!(
        channel.next_event(),
        Some(ResponseChannelEvent::Error("response failed".to_owned()))
    );
    assert_eq!(channel.next_event(), Some(ResponseChannelEvent::Closed));
    assert_eq!(channel.next_event(), None);
    assert_eq!(channel.state(), ResponseChannelState::Closed);
    assert_eq!(
        channel.push_result("late").unwrap_err(),
        ResponseChannelError::InvalidState {
            state: ResponseChannelState::Closed,
            operation: "append result",
        }
    );
}

#[test]
fn explicit_close_is_idempotent_and_drops_pending_events() {
    let mut channel = ResponseChannel::from_events([
        ResponseChannelEvent::Result(1),
        ResponseChannelEvent::Warning(Warning {
            level: WarningLevel::Note,
            class: WarningClass::Statement,
            code: None,
            message: "note".to_owned(),
        }),
    ]);
    channel.close();
    channel.close();

    assert!(channel.is_closed());
    assert_eq!(channel.state(), ResponseChannelState::Closed);
    assert_eq!(channel.next_event(), None);
}

#[test]
fn raw_response_and_transport_boundaries_are_explicit() {
    assert_eq!(
        unsupported_raw_tipb_response(),
        ResponseChannelError::Unsupported(ResponseChannelUnsupported::RawTipbResponse)
    );
    assert_eq!(
        unsupported_tikv_response_channel(),
        ResponseChannelError::Unsupported(ResponseChannelUnsupported::TiKvResponseChannel)
    );
    assert!(unsupported_raw_tipb_response()
        .to_string()
        .contains("raw tipb response"));
    assert!(unsupported_tikv_response_channel()
        .to_string()
        .contains("TiKV response channel"));
}
