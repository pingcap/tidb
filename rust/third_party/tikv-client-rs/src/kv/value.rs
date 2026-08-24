// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

const _PROPTEST_VALUE_MAX: usize = 1024 * 16; // 16 KB

/// The value part of a key/value pair. An alias for `Vec<u8>`.
///
/// In TiKV, a value is an ordered sequence of bytes. This has an advantage over choosing `String`
/// as valid `UTF-8` is not required. This means that the user is permitted to store any data they wish,
/// as long as it can be represented by bytes. (Which is to say, pretty much anything!)
///
/// Since `Value` is just an alias for `Vec<u8>`, conversions to and from it are easy.
///
/// Many functions which accept a `Value` accept an `Into<Value>`.
pub type Value = Vec<u8>;
