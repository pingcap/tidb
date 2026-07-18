//! TiKV region identity, cache, and single-region request routing authority.
//!
//! Campaign 09 fills this module from client-go's locate package. Concrete PD
//! networking remains behind an injected loader until its own pinned external
//! source universe is governed.
