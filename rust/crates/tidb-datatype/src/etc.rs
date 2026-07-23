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

//! Remaining source helpers from `pkg/types/etc.go`.

use std::io;

/// Source `EOFAsNil`: normalize the stream-end sentinel while preserving every
/// other I/O error.
pub fn eof_as_nil(error: io::Error) -> io::Result<()> {
    if error.kind() == io::ErrorKind::UnexpectedEof {
        Ok(())
    } else {
        Err(error)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Exact source `TestEOFAsNil`.
    #[test]
    fn test_eof_as_nil() {
        assert!(eof_as_nil(io::Error::from(io::ErrorKind::UnexpectedEof)).is_ok());
        assert_eq!(
            eof_as_nil(io::Error::other("test"))
                .unwrap_err()
                .to_string(),
            "test"
        );
    }
}
