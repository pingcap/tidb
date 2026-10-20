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

//! Go `system_info.go`.
//!
//! Go's unexported `getProcessList` is not ported: nothing in the package
//! calls it, so it contributes no behavior to `ServerInfo`.

use std::path::Path;

use tidb_proto::diagnosticspb as pb;

/// Go `tryProcFs`: every readable file under `/proc/sys/`, keyed by its
/// dotted relative path, in `filepath.Walk` order.
fn try_proc_fs() -> Vec<pb::ServerInfoItem> {
    const DIR: &str = "/proc/sys/";
    let mut pairs = Vec::new();
    if walk(Path::new(DIR), DIR, &mut pairs).is_err() {
        return Vec::new();
    }
    vec![pb::ServerInfoItem {
        tp: "system".to_owned(),
        name: "sysctl".to_owned(),
        pairs,
    }]
}

/// Go `filepath.Walk` with `tryProcFs`'s visitor: a directory that cannot be
/// read aborts the walk; a file that cannot be read is skipped. Walk does not
/// follow symbolic links into directories.
fn walk(path: &Path, dir: &str, pairs: &mut Vec<pb::ServerInfoPair>) -> std::io::Result<()> {
    let info = std::fs::symlink_metadata(path)?;
    let text = path.to_string_lossy();
    // The visitor reads every visited path; directories fail and are skipped.
    if let Ok(content) = std::fs::read(path) {
        pairs.push(pb::ServerInfoPair {
            key: text.strip_prefix(dir).unwrap_or(&text).replace('/', "."),
            value: String::from_utf8_lossy(&content).trim().to_owned(),
        });
    }
    if !info.is_dir() {
        return Ok(());
    }
    let mut names = std::fs::read_dir(path)?
        .map(|entry| entry.map(|entry| entry.file_name()))
        .collect::<std::io::Result<Vec<_>>>()?;
    names.sort();
    for name in names {
        match walk(&path.join(&name), dir, pairs) {
            Ok(()) => {}
            // Walk hands a child's Lstat failure to the visitor, which
            // returns it; a vanished entry therefore aborts too.
            Err(error) => return Err(error),
        }
    }
    Ok(())
}

/// Go `getSystemInfo`: `/proc/sys` when it exists, else `sysctl -a`.
pub(crate) fn get_system_info() -> Vec<pb::ServerInfoItem> {
    let huge_page = get_transparent_hugepage_enabled();
    let mut results = try_proc_fs();
    if !results.is_empty() {
        results.extend(huge_page);
        return results;
    }
    // fallback to command line
    let Ok(out) = std::process::Command::new("sysctl").arg("-a").output() else {
        return Vec::new();
    };
    if !out.status.success() {
        return Vec::new();
    }
    // Go reads the combined output.
    let mut combined = out.stdout;
    combined.extend_from_slice(&out.stderr);
    let mut pairs = Vec::with_capacity(2048);
    for line in read_lines(&combined) {
        let line = String::from_utf8_lossy(line);
        let kv: Vec<&str> = line.split(':').collect();
        if kv.len() >= 2 {
            pairs.push(pb::ServerInfoPair {
                key: kv[0].to_owned(),
                value: kv[1].trim().to_owned(),
            });
        }
    }
    let mut results = vec![pb::ServerInfoItem {
        tp: "system".to_owned(),
        name: "sysctl".to_owned(),
        pairs,
    }];
    results.extend(huge_page);
    results
}

/// Go `bufio.Reader.ReadLine` over the default 4096-byte buffer, whose
/// `isPrefix` the caller ignores: a longer line arrives as several lines.
fn read_lines(data: &[u8]) -> Vec<&[u8]> {
    const BUF: usize = 4096;
    let mut lines = Vec::new();
    let mut rest = data;
    while !rest.is_empty() {
        let window = &rest[..rest.len().min(BUF)];
        if let Some(end) = window.iter().position(|&b| b == b'\n') {
            let mut line = &rest[..end];
            if line.last() == Some(&b'\r') {
                line = &line[..line.len() - 1];
            }
            lines.push(line);
            rest = &rest[end + 1..];
        } else if window.len() == BUF {
            // A full buffer without a newline; ReadLine keeps a final '\r'
            // back for the next call in case "\r\n" straddles the buffer.
            let take = if window[BUF - 1] == b'\r' {
                BUF - 1
            } else {
                BUF
            };
            lines.push(&rest[..take]);
            rest = &rest[take..];
        } else {
            // The final line without a newline.
            lines.push(rest);
            rest = &[];
        }
    }
    lines
}

/// Go `getTransparentHugepageEnabled`.
fn get_transparent_hugepage_enabled() -> Vec<pb::ServerInfoItem> {
    let Ok(content) = std::fs::read("/sys/kernel/mm/transparent_hugepage/enabled") else {
        return Vec::new();
    };
    vec![pb::ServerInfoItem {
        tp: "system".to_owned(),
        name: "kernel".to_owned(),
        pairs: vec![pb::ServerInfoPair {
            key: "transparent_hugepage_enabled".to_owned(),
            value: String::from_utf8_lossy(&content).trim().to_owned(),
        }],
    }]
}

#[cfg(test)]
mod tests {
    use super::read_lines;

    #[test]
    fn read_line_splits_long_lines_at_the_buffer() {
        let long = vec![b'a'; 5000];
        let mut data = long.clone();
        data.extend_from_slice(b"\r\nb\nc");
        let lines = read_lines(&data);
        assert_eq!(lines.len(), 4);
        assert_eq!(lines[0].len(), 4096);
        assert_eq!(lines[1].len(), 5000 - 4096);
        assert_eq!(lines[2], b"b");
        assert_eq!(lines[3], b"c");
    }
}
