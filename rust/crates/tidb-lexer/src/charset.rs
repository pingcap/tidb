// Generated from pkg/parser/charset/charset.go (the `charsets` map keys).
// DO NOT EDIT BY HAND.
//
// A leading-underscore identifier whose remainder names one of these charsets
// is a charset introducer (`_utf8mb4'x'`), which the Go scanner turns into an
// `underscoreCS` token whose literal is the canonical charset name.
#![allow(clippy::all)]

/// Recognized charset names (lowercase), sorted for binary search.
pub static CHARSET_NAMES: &[&str] = &[
    "armscii8", "ascii", "big5", "binary", "cp1250", "cp1251", "cp1256", "cp1257", "cp850",
    "cp852", "cp866", "cp932", "dec8", "eucjpms", "euckr", "gb18030", "gb2312", "gbk", "geostd8",
    "greek", "hebrew", "hp8", "keybcs2", "koi8r", "koi8u", "latin1", "latin2", "latin5", "latin7",
    "macce", "macroman", "sjis", "swe7", "tis620", "ucs2", "ujis", "utf16", "utf16le", "utf32",
    "utf8", "utf8mb3", "utf8mb4",
];

/// Returns the canonical charset name if `name` (case-insensitive) is a
/// recognized charset. `utf8mb3` is normalized to `utf8`, matching
/// charset.GetCharsetInfo.
pub fn canonical_charset(name: &str) -> Option<&'static str> {
    let lower = name.to_ascii_lowercase();
    let idx = CHARSET_NAMES.binary_search(&lower.as_str()).ok()?;
    let canon = CHARSET_NAMES[idx];
    Some(if canon == "utf8mb3" { "utf8" } else { canon })
}
