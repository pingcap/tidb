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

//! Go `semver_test.go` (v0.3.1). `TestYAML` and `TestFlag` test adapters
//! that are not ported.

use super::*;

/// Go `fixtures`: greater, lesser.
const FIXTURES: &[(&str, &str)] = &[
    ("0.0.0", "0.0.0-foo"),
    ("0.0.1", "0.0.0"),
    ("1.0.0", "0.9.9"),
    ("0.10.0", "0.9.0"),
    ("0.99.0", "0.10.0"),
    ("2.0.0", "1.2.3"),
    ("0.0.0", "0.0.0-foo"),
    ("0.0.1", "0.0.0"),
    ("1.0.0", "0.9.9"),
    ("0.10.0", "0.9.0"),
    ("0.99.0", "0.10.0"),
    ("2.0.0", "1.2.3"),
    ("0.0.0", "0.0.0-foo"),
    ("0.0.1", "0.0.0"),
    ("1.0.0", "0.9.9"),
    ("0.10.0", "0.9.0"),
    ("0.99.0", "0.10.0"),
    ("2.0.0", "1.2.3"),
    ("1.2.3", "1.2.3-asdf"),
    ("1.2.3", "1.2.3-4"),
    ("1.2.3", "1.2.3-4-foo"),
    ("1.2.3-5-foo", "1.2.3-5"),
    ("1.2.3-5", "1.2.3-4"),
    ("1.2.3-5-foo", "1.2.3-5-Foo"),
    ("3.0.0", "2.7.2+asdf"),
    ("3.0.0+foobar", "2.7.2"),
    ("1.2.3-a.10", "1.2.3-a.5"),
    ("1.2.3-a.b", "1.2.3-a.5"),
    ("1.2.3-a.b", "1.2.3-a"),
    ("1.2.3-a.b.c.10.d.5", "1.2.3-a.b.c.5.d.100"),
    ("1.0.0", "1.0.0-rc.1"),
    ("1.0.0-rc.2", "1.0.0-rc.1"),
    ("1.0.0-rc.1", "1.0.0-beta.11"),
    ("1.0.0-beta.11", "1.0.0-beta.2"),
    ("1.0.0-beta.2", "1.0.0-beta"),
    ("1.0.0-beta", "1.0.0-alpha.beta"),
    ("1.0.0-alpha.beta", "1.0.0-alpha.1"),
    ("1.0.0-alpha.1", "1.0.0-alpha"),
    ("1.2.3-rc.1-1-1hash", "1.2.3-rc.2"),
];

/// Go `TestCompare`.
#[test]
fn test_compare() {
    for (g, l) in FIXTURES {
        let gt = new_version(g).unwrap();
        let lt = new_version(l).unwrap();
        assert!(!gt.less_than(&lt), "{gt} should not be less than {lt}");
        assert!(!gt.equal(&lt), "{gt} should not be equal to {lt}");
        assert!(gt.compare(&lt) > 0, "{gt} should be greater than {lt}");
        assert!(lt.less_than(&gt), "{lt} should be less than {gt}");
        assert!(lt.equal(&lt), "{lt} should be equal to {lt}");
        assert!(lt.compare(&gt) <= 0, "{lt} should not be greater than {gt}");
    }
}

/// Go `TestString`.
#[test]
fn test_string() {
    for (g, l) in FIXTURES {
        assert_eq!(new_version(g).unwrap().to_string(), *g);
        assert_eq!(new_version(l).unwrap().to_string(), *l);
    }
}

/// Go `TestSort`, over every rotation instead of a random shuffle.
#[test]
fn test_sort() {
    let sorted = ["1.0.0", "1.0.2", "1.2.0", "3.1.1"];
    for shift in 0..sorted.len() {
        let mut versions: Vec<Version> =
            sorted.iter().cycle().skip(shift).take(sorted.len()).map(|v| new_version(v).unwrap()).collect();
        sort(&mut versions);
        let got: Vec<String> = versions.iter().map(ToString::to_string).collect();
        assert_eq!(got, sorted);
    }
}

/// Go `TestBumpMajor`.
#[test]
fn test_bump_major() {
    let mut v = new_version("1.0.0").unwrap();
    v.bump_major();
    assert_eq!(v.major, 2);
    let mut v = new_version("1.5.2").unwrap();
    v.bump_major();
    assert_eq!((v.minor, v.patch), (0, 0));
    let mut v = new_version("1.0.0+build.1-alpha.1").unwrap();
    v.bump_major();
    assert_eq!((v.pre_release.as_str(), v.metadata.as_str()), ("", ""));
}

/// Go `TestBumpMinor`.
#[test]
fn test_bump_minor() {
    let mut v = new_version("1.0.0").unwrap();
    v.bump_minor();
    assert_eq!((v.major, v.minor), (1, 1));
    let mut v = new_version("1.0.0+build.1-alpha.1").unwrap();
    v.bump_minor();
    assert_eq!((v.pre_release.as_str(), v.metadata.as_str()), ("", ""));
}

/// Go `TestBumpPatch`.
#[test]
fn test_bump_patch() {
    let mut v = new_version("1.0.0").unwrap();
    v.bump_patch();
    assert_eq!((v.major, v.minor, v.patch), (1, 0, 1));
    let mut v = new_version("1.0.0+build.1-alpha.1").unwrap();
    v.bump_patch();
    assert_eq!((v.pre_release.as_str(), v.metadata.as_str()), ("", ""));
}

/// Go `TestMust`.
#[test]
fn test_must() {
    assert_eq!(must(new_version("1.0.0")), Version { major: 1, ..Version::default() });
    let panic = std::panic::catch_unwind(|| must(new_version("version number"))).unwrap_err();
    assert_eq!(panic.downcast_ref::<String>().unwrap(), "version number is not in dotted-tri format");
}

/// Go `TestJSON`: marshalling the versions gives the fixture strings, and
/// unmarshalling them gives the versions back.
#[test]
fn test_json() {
    for (g, l) in FIXTURES {
        for s in [g, l] {
            let v = new_version(s).unwrap();
            let json = v.marshal_json();
            assert_eq!(json, serde_json::to_string(s).unwrap());
            let mut back = Version::default();
            back.unmarshal_json(json.as_bytes()).unwrap();
            assert_eq!(back, v);
        }
    }
    let mut v = new("1.2.3");
    v.unmarshal_json(b"\"\"").unwrap();
    assert_eq!(v, new("1.2.3"));
    assert_eq!(v.unmarshal_json(b"1.2.3").unwrap_err(), "invalid semver string");
}

/// Go `TestBadInput`, with Go's error texts.
#[test]
fn test_bad_input() {
    for (bad, err) in [
        ("1.2", "1.2 is not in dotted-tri format"),
        ("1.2.3x", "strconv.ParseInt: parsing \"3x\": invalid syntax"),
        ("0x1.3.4", "strconv.ParseInt: parsing \"0x1\": invalid syntax"),
        ("-1.2.3", " is not in dotted-tri format"),
        ("1.2.3.4", "strconv.ParseInt: parsing \"3.4\": invalid syntax"),
        ("0.88.0-11_e4e5dcabb", "failed to validate pre-release: 11_e4e5dcabb is not a valid semver identifier"),
        ("0.88.0+11_e4e5dcabb", "failed to validate metadata: 11_e4e5dcabb is not a valid semver identifier"),
    ] {
        assert_eq!(new_version(bad).unwrap_err(), err, "{bad}");
    }
}

/// Go `ExampleVersion_LessThan`.
#[test]
fn example_version_less_than() {
    let (a, b) = (new("1.2.3"), new("3.2.1"));
    assert_eq!(format!("{a} < {b} == {}", a.less_than(&b)), "1.2.3 < 3.2.1 == true");
}
