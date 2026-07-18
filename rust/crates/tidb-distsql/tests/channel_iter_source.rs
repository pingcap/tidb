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

#![allow(missing_docs)]

// Keep this source-shaped leaf isolated until the crate root assigns its
// public module/re-export.  A later integration change can re-export the same
// types without changing the state machine or these source anchors.
#[path = "../src/channel_iter.rs"]
mod channel_iter;

use channel_iter::{ChannelIter, ChannelIterError, ChannelIterUnsupported, ChannelRow};

#[test]
fn go_new_sel_resp_channel_iter_validates_channel_layout() {
    // Structural port of `pkg/distsql/select_result_test.go:77
    // TestNewSelRespChannelIter`: two intermediate channels plus the final
    // channel means three valid channel indexes.
    let iter = ChannelIter::try_new(2, 3, [vec![10, 11], Vec::<i32>::new()]).unwrap();
    assert_eq!(iter.channel(), 2);
    assert_eq!(
        ChannelIter::<i32>::try_new(3, 3, Vec::<Vec<i32>>::new()).unwrap_err(),
        ChannelIterError::InvalidChannel {
            channel: 3,
            available_channels: 3,
        }
    );
}

#[test]
fn go_sel_resp_channel_iter_read_skips_empty_chunks_and_preserves_rows() {
    // Structural port of `pkg/distsql/select_result_test.go:183
    // TestSelRespChannelIterRead`: empty response chunks do not produce a
    // sentinel row, and rows keep their channel index.
    let mut iter = ChannelIter::new(
        1,
        [Vec::<i32>::new(), vec![1, 2], Vec::<i32>::new(), vec![3]],
    );

    assert_eq!(iter.next_row().unwrap(), Some(ChannelRow::new(1, 1)));
    assert_eq!(iter.next_row().unwrap(), Some(ChannelRow::new(1, 2)));
    assert_eq!(iter.next_row().unwrap(), Some(ChannelRow::new(1, 3)));
    assert_eq!(iter.next_row().unwrap(), None);
    assert_eq!(iter.next_row().unwrap(), None);
    assert!(iter.is_drained());
}

#[test]
fn channel_iter_propagates_owned_input_errors_without_losing_following_rows() {
    let mut iter = ChannelIter::from_results(
        0,
        [vec![
            Ok(7),
            Err(ChannelIterError::source("decode failed")),
            Ok(8),
        ]],
    );

    assert_eq!(iter.next_row().unwrap(), Some(ChannelRow::new(0, 7)));
    assert_eq!(
        iter.next_row().unwrap_err(),
        ChannelIterError::Source("decode failed".to_owned())
    );
    assert_eq!(iter.next_row().unwrap(), Some(ChannelRow::new(0, 8)));
    assert_eq!(iter.next_row().unwrap(), None);
}

#[test]
fn channel_iter_close_is_idempotent_and_drops_owned_rows() {
    let mut iter = ChannelIter::from_rows(4, [1, 2, 3]);
    assert!(!iter.is_drained());
    iter.close();
    iter.close();
    assert!(iter.is_drained());
    assert_eq!(iter.next_row().unwrap(), None);
}

#[test]
fn unsupported_response_boundaries_are_explicit() {
    assert_eq!(
        ChannelIterError::unsupported_raw_tipb_response(),
        ChannelIterError::Unsupported(ChannelIterUnsupported::RawTipbResponse)
    );
    assert_eq!(
        ChannelIterError::unsupported_chunk_decoding(),
        ChannelIterError::Unsupported(ChannelIterUnsupported::ChunkDecoding)
    );
    assert_eq!(
        ChannelIterError::unsupported_tikv_response_channel(),
        ChannelIterError::Unsupported(ChannelIterUnsupported::TiKvResponseChannel)
    );
    assert!(ChannelIterError::unsupported_raw_tipb_response()
        .to_string()
        .contains("raw tipb response"));
    assert!(ChannelIterError::unsupported_chunk_decoding()
        .to_string()
        .contains("chunk decoding"));
    assert!(ChannelIterError::unsupported_tikv_response_channel()
        .to_string()
        .contains("TiKV response channel"));
}

#[test]
fn channel_row_map_retains_channel_index() {
    assert_eq!(
        ChannelRow::new(9, "owned").map(str::len),
        ChannelRow::new(9, 5)
    );
}
