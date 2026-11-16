//! Go `path/filepath` and `os` error text, as local storage reports them.

use std::io;

/// Go `filepath.Clean` for slash-separated paths.
#[must_use]
pub fn clean(path: &str) -> String {
    if path.is_empty() {
        return ".".to_owned();
    }
    let rooted = path.starts_with('/');
    let mut parts: Vec<&str> = Vec::new();
    for part in path.split('/') {
        match part {
            "" | "." => {}
            ".." => {
                if parts.last().is_some_and(|p| *p != "..") {
                    parts.pop();
                } else if !rooted {
                    parts.push("..");
                }
            }
            other => parts.push(other),
        }
    }
    let joined = parts.join("/");
    match (rooted, joined.is_empty()) {
        (true, _) => format!("/{joined}"),
        (false, true) => ".".to_owned(),
        (false, false) => joined,
    }
}

/// Go `filepath.Join`.
#[must_use]
pub fn join(parts: &[&str]) -> String {
    let non_empty: Vec<&str> = parts.iter().copied().filter(|p| !p.is_empty()).collect();
    if non_empty.is_empty() {
        return String::new();
    }
    clean(&non_empty.join("/"))
}

/// Go `filepath.Rel` for a `target` under `base` (both cleaned); `None` when
/// it is not under it.
#[must_use]
pub fn rel(base: &str, target: &str) -> Option<String> {
    let base = clean(base);
    let target = clean(target);
    if base == target {
        return Some(".".to_owned());
    }
    if base == "." {
        return Some(target);
    }
    let prefix = if base.ends_with('/') { base.clone() } else { format!("{base}/") };
    target.strip_prefix(&prefix).map(str::to_owned)
}

/// Go `filepath.Dir`.
#[must_use]
pub fn dir(path: &str) -> String {
    match path.rfind('/') {
        Some(0) => "/".to_owned(),
        Some(at) => clean(&path[..at]),
        None => ".".to_owned(),
    }
}

/// Go `filepath.Base`.
#[must_use]
pub fn base(path: &str) -> String {
    let trimmed = path.trim_end_matches('/');
    if trimmed.is_empty() {
        return if path.is_empty() { ".".to_owned() } else { "/".to_owned() };
    }
    trimmed.rsplit('/').next().unwrap_or(trimmed).to_owned()
}

/// Go's `syscall.Errno.Error()` text for an OS error.
#[must_use]
pub fn errno_text(error: &io::Error) -> String {
    match error.kind() {
        io::ErrorKind::NotFound => "no such file or directory".to_owned(),
        io::ErrorKind::PermissionDenied => "permission denied".to_owned(),
        io::ErrorKind::AlreadyExists => "file exists".to_owned(),
        io::ErrorKind::IsADirectory => "is a directory".to_owned(),
        io::ErrorKind::NotADirectory => "not a directory".to_owned(),
        _ => {
            let text = error.to_string();
            let text = text.split(" (os error").next().unwrap_or(&text);
            let mut chars = text.chars();
            match chars.next() {
                Some(first) => first.to_lowercase().chain(chars).collect(),
                None => String::new(),
            }
        }
    }
}

/// Go `*fs.PathError.Error()`: `op path: errno`.
pub fn path_error(op: &str, path: &str, error: &io::Error) -> crate::Error {
    let kind = if error.kind() == io::ErrorKind::NotFound {
        crate::ErrorKind::NotFound
    } else {
        crate::ErrorKind::Other
    };
    crate::Error::new(kind, format!("{op} {path}: {}", errno_text(error)))
}

/// Go `*os.LinkError.Error()`: `op old new: errno`.
pub fn link_error(op: &str, old: &str, new: &str, error: &io::Error) -> crate::Error {
    crate::Error::other(format!("{op} {old} {new}: {}", errno_text(error)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_clean_join() {
        assert_eq!(clean("a//b/./c/.."), "a/b");
        assert_eq!(clean("/../a"), "/a");
        assert_eq!(clean("../a/.."), "..");
        assert_eq!(join(&["/base", "x/y"]), "/base/x/y");
        assert_eq!(join(&["/base", ""]), "/base");
        assert_eq!(rel("/base", "/base/x/y").as_deref(), Some("x/y"));
        assert_eq!(rel("/base", "/base").as_deref(), Some("."));
        assert_eq!(dir("/a/b"), "/a");
        assert_eq!(base("/a/b/"), "b");
    }
}
