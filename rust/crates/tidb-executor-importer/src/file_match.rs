//! Go `filepath.Match`, used by `LoadDataController.InitDataFiles`.

const BAD_PATTERN: &str = "syntax error in pattern";

/// Go `filepath.Match` on Unix (storage object names also use `/`).
pub(super) fn filepath_match(mut pattern: &[u8], mut name: &[u8]) -> Result<bool, &'static str> {
    'pattern: while !pattern.is_empty() {
        let (star, chunk, rest) = scan_chunk(pattern);
        pattern = rest;
        if star && chunk.is_empty() {
            return Ok(!name.contains(&b'/'));
        }
        let (tail, ok) = match_chunk(chunk, name)?;
        if ok && (tail.is_empty() || !pattern.is_empty()) {
            name = tail;
            continue;
        }
        if star {
            for i in 0..name.len() {
                if name[i] == b'/' {
                    break;
                }
                let (tail, ok) = match_chunk(chunk, &name[i + 1..])?;
                if ok && (!pattern.is_empty() || tail.is_empty()) {
                    name = tail;
                    continue 'pattern;
                }
            }
        }
        return Ok(false);
    }
    Ok(name.is_empty())
}

/// Go `filepath.scanChunk`.
fn scan_chunk(mut pattern: &[u8]) -> (bool, &[u8], &[u8]) {
    let mut star = false;
    while pattern.first() == Some(&b'*') {
        pattern = &pattern[1..];
        star = true;
    }
    let mut in_range = false;
    let mut i = 0;
    while i < pattern.len() {
        match pattern[i] {
            b'\\' if i + 1 < pattern.len() => i += 1,
            b'[' => in_range = true,
            b']' => in_range = false,
            b'*' if !in_range => return (star, &pattern[..i], &pattern[i..]),
            _ => {}
        }
        i += 1;
    }
    (star, pattern, &[])
}

/// Go `filepath.matchChunk`.
fn match_chunk<'a>(mut chunk: &[u8], mut s: &'a [u8]) -> Result<(&'a [u8], bool), &'static str> {
    let mut failed = false;
    while !chunk.is_empty() {
        failed |= s.is_empty();
        match chunk[0] {
            b'[' => {
                let mut r = '\0';
                if !failed {
                    let (c, n) = decode_rune(s);
                    r = c;
                    s = &s[n..];
                }
                chunk = &chunk[1..];
                let negated = chunk.first() == Some(&b'^');
                if negated {
                    chunk = &chunk[1..];
                }
                let mut matched = false;
                let mut nrange = 0;
                loop {
                    if chunk.first() == Some(&b']') && nrange > 0 {
                        chunk = &chunk[1..];
                        break;
                    }
                    let (lo, rest) = get_esc(chunk)?;
                    chunk = rest;
                    let mut hi = lo;
                    if chunk[0] == b'-' {
                        let (h, rest) = get_esc(&chunk[1..])?;
                        hi = h;
                        chunk = rest;
                    }
                    matched |= lo <= r && r <= hi;
                    nrange += 1;
                }
                failed |= matched == negated;
            }
            b'?' => {
                if !failed {
                    failed = s[0] == b'/';
                    let (_, n) = decode_rune(s);
                    s = &s[n..];
                }
                chunk = &chunk[1..];
            }
            _ => {
                if chunk[0] == b'\\' {
                    chunk = &chunk[1..];
                    if chunk.is_empty() {
                        return Err(BAD_PATTERN);
                    }
                }
                if !failed {
                    failed = chunk[0] != s[0];
                    s = &s[1..];
                }
                chunk = &chunk[1..];
            }
        }
    }
    Ok(if failed { (&[], false) } else { (s, true) })
}

/// Go `filepath.getEsc`.
fn get_esc(mut chunk: &[u8]) -> Result<(char, &[u8]), &'static str> {
    if chunk.is_empty() || matches!(chunk[0], b'-' | b']') {
        return Err(BAD_PATTERN);
    }
    if chunk[0] == b'\\' {
        chunk = &chunk[1..];
        if chunk.is_empty() {
            return Err(BAD_PATTERN);
        }
    }
    let (r, n) = decode_rune(chunk);
    if r == '\u{fffd}' && n == 1 {
        return Err(BAD_PATTERN);
    }
    chunk = &chunk[n..];
    if chunk.is_empty() {
        return Err(BAD_PATTERN);
    }
    Ok((r, chunk))
}

/// Go `utf8.DecodeRuneInString`; invalid bytes consume one byte.
fn decode_rune(bytes: &[u8]) -> (char, usize) {
    let valid = match std::str::from_utf8(bytes) {
        Ok(s) => s,
        Err(e) => std::str::from_utf8(&bytes[..e.valid_up_to()]).unwrap(),
    };
    valid
        .chars()
        .next()
        .map(|c| (c, c.len_utf8()))
        .unwrap_or(('\u{fffd}', 1))
}
