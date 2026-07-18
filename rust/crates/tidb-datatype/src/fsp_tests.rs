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

use super::*;

/// Complete translation of `pkg/types/fsp_test.go::TestCheckFsp`.
#[test]
fn check_fsp_executes_every_original_assertion() {
    assert_eq!(check_fsp(UNSPECIFIED_FSP), Ok(DEFAULT_FSP));

    let error = check_fsp(-2019).unwrap_err();
    assert_eq!(error, FspError::InvalidFsp(-2019));
    assert_eq!(error.to_string(), "Invalid fsp -2019");

    let below_u32 = MIN_FSP - 4_294_967_296;
    let error = check_fsp(below_u32).unwrap_err();
    assert_eq!(error, FspError::InvalidFsp(below_u32));
    assert_eq!(error.to_string(), format!("Invalid fsp {below_u32}"));

    assert_eq!(check_fsp(-1), Ok(DEFAULT_FSP));
    assert_eq!(check_fsp(MAX_FSP + 1), Ok(MAX_FSP));
    assert_eq!(check_fsp(MAX_FSP + 2019), Ok(MAX_FSP));
    assert_eq!(check_fsp(MAX_FSP + 4_294_967_296), Ok(MAX_FSP));
    assert_eq!(check_fsp((MAX_FSP + MIN_FSP) / 2), Ok(3));
    assert_eq!(check_fsp(5), Ok(5));
}

/// Complete translation of `pkg/types/fsp_test.go::TestParseFrac`.
#[test]
fn parse_frac_executes_every_original_assertion() {
    assert_eq!(parse_frac(b"", 5), Ok((0, false)));

    let error = parse_frac(b"999", -56).unwrap_err();
    assert!(error.to_string().starts_with("Invalid fsp "));

    let error = parse_frac(b"NotNum", MAX_FSP).unwrap_err();
    assert!(error.to_string().starts_with("strconv.ParseInt:"));

    assert_eq!(parse_frac(b"1235", 6), Ok((123_500, false)));
    assert_eq!(parse_frac(b"123456", 4), Ok((123_500, false)));
    assert_eq!(parse_frac(b"1234567", 6), Ok((123_457, false)));
    assert_eq!(parse_frac(b"1234567", 4), Ok((123_500, false)));
    assert_eq!(parse_frac(b"1236", 3), Ok((124_000, false)));
    assert_eq!(parse_frac(b"0312", 2), Ok((30_000, false)));
    assert_eq!(parse_frac(b"999", 2), Ok((0, true)));
}

#[test]
fn parse_frac_keeps_go_byte_slicing_and_empty_input_ordering() {
    assert_eq!(parse_frac(b"", i64::MIN), Ok((0, false)));
    assert!(matches!(
        parse_frac(&[0xff], MAX_FSP),
        Err(FspError::ParseInt { .. })
    ));
}

/// Complete translation of `pkg/types/fsp_test.go::TestAlignFrac`.
#[test]
fn align_frac_executes_every_original_assertion() {
    assert_eq!(align_frac(b"100", 6), b"100000");
    assert_eq!(align_frac(b"10000000000", 6), b"10000000000");
    assert_eq!(align_frac(b"-100", 6), b"-100000");
    assert_eq!(align_frac(b"-10000000000", 6), b"-10000000000");
}
