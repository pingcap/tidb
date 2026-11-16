// Copyright 2013-2015 CoreOS, Inc.
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

//! `github.com/coreos/go-semver/semver` v0.3.1 (the version Go TiDB
//! master's `go.mod` pins): `semver.go` and `sort.go`. The YAML and flag
//! adapters are not ported; JSON is [`Version::marshal_json`] and
//! [`Version::unmarshal_json`].

use std::cmp::Ordering;
use std::fmt;

/// Go `Version`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Version {
    /// Go `Major`.
    pub major: i64,
    /// Go `Minor`.
    pub minor: i64,
    /// Go `Patch`.
    pub patch: i64,
    /// Go `PreRelease`.
    pub pre_release: String,
    /// Go `Metadata`.
    pub metadata: String,
}

/// Go `splitOff`: the part after the first `delim`, leaving the part
/// before it in `input`.
fn split_off(input: &mut String, delim: char) -> String {
    match input.split_once(delim) {
        Some((head, tail)) => {
            let tail = tail.to_owned();
            *input = head.to_owned();
            tail
        }
        None => String::new(),
    }
}

/// Go `New`: panics on an invalid version.
#[must_use]
pub fn new(version: &str) -> Version {
    must(new_version(version))
}

/// Go `NewVersion`.
///
/// # Errors
/// Go's `Set` errors, as their text.
pub fn new_version(version: &str) -> Result<Version, String> {
    let mut v = Version::default();
    v.set(version)?;
    Ok(v)
}

/// Go `Must`.
#[must_use]
pub fn must(v: Result<Version, String>) -> Version {
    match v {
        Ok(v) => v,
        Err(err) => panic!("{err}"),
    }
}

impl Version {
    /// Go `Set`.
    ///
    /// # Errors
    /// Go's error texts: not dotted-tri, an invalid pre-release or metadata
    /// identifier, or `strconv.ParseInt`'s error for a field.
    pub fn set(&mut self, version: &str) -> Result<(), String> {
        let mut version = version.to_owned();
        let metadata = split_off(&mut version, '+');
        let pre_release = split_off(&mut version, '-');
        let dot_parts: Vec<&str> = version.splitn(3, '.').collect();
        if dot_parts.len() != 3 {
            return Err(format!("{version} is not in dotted-tri format"));
        }
        validate_identifier(&pre_release).map_err(|e| format!("failed to validate pre-release: {e}"))?;
        validate_identifier(&metadata).map_err(|e| format!("failed to validate metadata: {e}"))?;
        let mut parsed = [0_i64; 3];
        for (i, part) in dot_parts.iter().enumerate() {
            match tidb_go_strconv::parse_int(part, 10, 64) {
                Ok(val) => parsed[i] = val,
                Err((_, err)) => return Err(err.to_string()),
            }
        }
        self.metadata = metadata;
        self.pre_release = pre_release;
        self.major = parsed[0];
        self.minor = parsed[1];
        self.patch = parsed[2];
        Ok(())
    }

    /// Go `MarshalJSON`.
    #[must_use]
    pub fn marshal_json(&self) -> String {
        format!("\"{self}\"")
    }

    /// Go `UnmarshalJSON`: an empty input or `""` leaves the version as is.
    ///
    /// # Errors
    /// `invalid semver string`, or [`Version::set`]'s errors.
    pub fn unmarshal_json(&mut self, data: &[u8]) -> Result<(), String> {
        let l = data.len();
        if l == 0 || data == b"\"\"" {
            return Ok(());
        }
        if l < 2 || data[0] != b'"' || data[l - 1] != b'"' {
            return Err("invalid semver string".to_owned());
        }
        self.set(&String::from_utf8_lossy(&data[1..l - 1]))
    }

    /// Go `Compare`: -1, 0 or 1.
    #[must_use]
    pub fn compare(&self, b: &Version) -> i32 {
        let cmp = recursive_compare(&self.slice(), &b.slice());
        if cmp != 0 {
            return cmp;
        }
        pre_release_compare(self, b)
    }

    /// Go `Equal`.
    #[must_use]
    pub fn equal(&self, b: &Version) -> bool {
        self.compare(b) == 0
    }

    /// Go `LessThan`.
    #[must_use]
    pub fn less_than(&self, b: &Version) -> bool {
        self.compare(b) < 0
    }

    /// Go `Slice`.
    #[must_use]
    pub fn slice(&self) -> [i64; 3] {
        [self.major, self.minor, self.patch]
    }

    /// Go `BumpMajor`.
    pub fn bump_major(&mut self) {
        self.major += 1;
        self.minor = 0;
        self.patch = 0;
        self.pre_release.clear();
        self.metadata.clear();
    }

    /// Go `BumpMinor`.
    pub fn bump_minor(&mut self) {
        self.minor += 1;
        self.patch = 0;
        self.pre_release.clear();
        self.metadata.clear();
    }

    /// Go `BumpPatch`.
    pub fn bump_patch(&mut self) {
        self.patch += 1;
        self.pre_release.clear();
        self.metadata.clear();
    }
}

impl fmt::Display for Version {
    /// Go `String`.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{}.{}", self.major, self.minor, self.patch)?;
        if !self.pre_release.is_empty() {
            write!(f, "-{}", self.pre_release)?;
        }
        if !self.metadata.is_empty() {
            write!(f, "+{}", self.metadata)?;
        }
        Ok(())
    }
}

/// Go `preReleaseCompare`.
fn pre_release_compare(a: &Version, b: &Version) -> i32 {
    let (a, b) = (&a.pre_release, &b.pre_release);
    if a.is_empty() && !b.is_empty() {
        return 1;
    } else if b.is_empty() && !a.is_empty() {
        return -1;
    }
    let a: Vec<&str> = a.split('.').collect();
    let b: Vec<&str> = b.split('.').collect();
    recursive_pre_release_compare(&a, &b)
}

/// Go `recursiveCompare`.
fn recursive_compare(a: &[i64], b: &[i64]) -> i32 {
    for (x, y) in a.iter().zip(b) {
        match x.cmp(y) {
            Ordering::Greater => return 1,
            Ordering::Less => return -1,
            Ordering::Equal => {}
        }
    }
    0
}

/// Go `strconv.Atoi` on a 64-bit platform.
fn atoi(s: &str) -> Option<i64> {
    tidb_go_strconv::parse_int(s, 10, 64).ok()
}

/// Go `recursivePreReleaseCompare`.
fn recursive_pre_release_compare(a: &[&str], b: &[&str]) -> i32 {
    match (a.first(), b.first()) {
        (None, None) => 0,
        (None, Some(_)) => -1,
        (Some(_), None) => 1,
        (Some(x), Some(y)) => {
            let (xi, yi) = (atoi(x), atoi(y));
            match (xi, yi) {
                (Some(_), None) => return -1,
                (None, Some(_)) => return 1,
                (Some(xi), Some(yi)) => match xi.cmp(&yi) {
                    Ordering::Greater => return 1,
                    Ordering::Less => return -1,
                    Ordering::Equal => {}
                },
                (None, None) => {}
            }
            match x.cmp(y) {
                Ordering::Greater => 1,
                Ordering::Less => -1,
                Ordering::Equal => recursive_pre_release_compare(&a[1..], &b[1..]),
            }
        }
    }
}

/// Go `validateIdentifier`: `^[0-9A-Za-z-]+(\.[0-9A-Za-z-]+)*$` or empty.
fn validate_identifier(id: &str) -> Result<(), String> {
    let valid = id.is_empty()
        || id.split('.').all(|part| !part.is_empty() && part.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-'));
    if valid {
        Ok(())
    } else {
        Err(format!("{id} is not a valid semver identifier"))
    }
}

/// Go `Sort`. Go's `sort.Sort` is not stable; equal versions compare equal
/// in every field but metadata, which only a caller printing them sees.
pub fn sort(versions: &mut [Version]) {
    versions.sort_by(|a, b| a.compare(b).cmp(&0));
}

#[cfg(test)]
mod tests;
