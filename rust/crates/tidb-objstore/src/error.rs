//! The errors storage operations return: Go's `error.Error()` text, with
//! the not-found and canceled causes kept so callers can tell them apart
//! (Go `os.IsNotExist`, `context.Canceled`).

use std::fmt;

/// What kind of failure an [`Error`] is.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ErrorKind {
    /// The object or file does not exist.
    NotFound,
    /// The caller's context was canceled or timed out.
    Canceled,
    /// Anything else.
    Other,
}

/// A storage error.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Error {
    kind: ErrorKind,
    message: String,
}

/// Storage results.
pub type Result<T> = std::result::Result<T, Error>;

impl Error {
    /// An error of `kind` with Go's message.
    pub fn new(kind: ErrorKind, message: impl Into<String>) -> Self {
        Self { kind, message: message.into() }
    }

    /// Go `errors.New` / `errors.Errorf`.
    pub fn other(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::Other, message)
    }

    /// The failure's kind.
    #[must_use]
    pub fn kind(&self) -> ErrorKind {
        self.kind
    }

    /// Whether the object does not exist.
    #[must_use]
    pub fn is_not_found(&self) -> bool {
        self.kind == ErrorKind::NotFound
    }

    /// Go `errors.Annotate(err, msg)`: `msg: err`, keeping the kind.
    #[must_use]
    pub fn annotate(self, msg: impl fmt::Display) -> Self {
        Self { kind: self.kind, message: format!("{msg}: {}", self.message) }
    }

    /// The error's text.
    #[must_use]
    pub fn message(&self) -> &str {
        &self.message
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for Error {}

impl From<std::io::Error> for Error {
    fn from(error: std::io::Error) -> Self {
        let kind = match error.kind() {
            std::io::ErrorKind::NotFound => ErrorKind::NotFound,
            _ => ErrorKind::Other,
        };
        Self::new(kind, error.to_string())
    }
}

impl From<Error> for std::io::Error {
    fn from(error: Error) -> Self {
        let kind = match error.kind {
            ErrorKind::NotFound => std::io::ErrorKind::NotFound,
            ErrorKind::Canceled => std::io::ErrorKind::Interrupted,
            ErrorKind::Other => std::io::ErrorKind::Other,
        };
        std::io::Error::new(kind, error.message)
    }
}

/// Go `ctx.Err()` as a storage error.
pub(crate) fn check_ctx(ctx: &tidb_gocontext::Context) -> Result<()> {
    match ctx.err() {
        Some(error) => Err(Error::new(ErrorKind::Canceled, error.to_string())),
        None => Ok(()),
    }
}

/// `br/pkg/errors` definitions storage errors are annotated with.
pub mod berrors {
    /// Go `ErrStorageInvalidConfig`.
    pub const ERR_STORAGE_INVALID_CONFIG: &str =
        "[BR:ExternalStorage:ErrStorageInvalidConfig]invalid external storage config";
    /// Go `ErrStorageInvalidPermission`.
    pub const ERR_STORAGE_INVALID_PERMISSION: &str =
        "[BR:ExternalStorage:ErrStorageInvalidPermission]external storage permission";
    /// Go `ErrStorageUnknown`.
    pub const ERR_STORAGE_UNKNOWN: &str =
        "[BR:ExternalStorage:ErrStorageUnknown]unknown external storage error";
    /// Go `ErrUnsupportedOperation`.
    pub const ERR_UNSUPPORTED_OPERATION: &str =
        "[BR:Common:ErrUnsupportedOperation]the operation is not supported";
    /// Go `ErrInvalidArgument`.
    pub const ERR_INVALID_ARGUMENT: &str = "[BR:Common:ErrInvalidArgument]invalid argument";

    /// Go `errors.Annotate(class, msg)`.
    pub fn annotate(class: &str, msg: impl std::fmt::Display) -> super::Error {
        super::Error::other(format!("{msg}: {class}"))
    }
}
