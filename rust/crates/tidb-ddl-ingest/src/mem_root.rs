// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Go `pkg/ddl/ingest/mem_root.go`.
use std::collections::HashMap;
use std::sync::RwLock;

#[derive(Default)]
struct State {
    max_limit: i64,
    curr_usage: i64,
    struct_size: HashMap<String, i64>,
}

/// Go `memRootImpl`. Checking and consuming are deliberately separate, as in Go.
#[derive(Default)]
pub struct MemRootImpl {
    state: RwLock<State>,
}
impl MemRootImpl {
    /// Go `NewMemRootImpl`.
    pub fn new(max_quota: i64) -> Self {
        Self {
            state: RwLock::new(State {
                max_limit: max_quota,
                ..State::default()
            }),
        }
    }
    /// Go `memRootImpl.SetMaxMemoryQuota`.
    pub fn set_max_memory_quota(&self, quota: i64) {
        self.state.write().unwrap().max_limit = quota;
    }
    /// Go `memRootImpl.MaxMemoryQuota`.
    pub fn max_memory_quota(&self) -> i64 {
        self.state.read().unwrap().max_limit
    }
    /// Go `memRootImpl.CurrentUsage`.
    pub fn current_usage(&self) -> i64 {
        self.state.read().unwrap().curr_usage
    }
    /// Go `memRootImpl.CurrentUsageWithTag`.
    pub fn current_usage_with_tag(&self, tag: &str) -> i64 {
        self.state
            .read()
            .unwrap()
            .struct_size
            .get(tag)
            .copied()
            .unwrap_or_default()
    }
    /// Go `memRootImpl.Consume`.
    pub fn consume(&self, size: i64) {
        self.state.write().unwrap().curr_usage += size;
    }
    /// Go `memRootImpl.Release`.
    pub fn release(&self, size: i64) {
        self.state.write().unwrap().curr_usage -= size;
    }
    /// Go `memRootImpl.CheckConsume`.
    pub fn check_consume(&self, size: i64) -> bool {
        let s = self.state.read().unwrap();
        s.curr_usage + size <= s.max_limit
    }
    /// Go `memRootImpl.ConsumeWithTag`.
    pub fn consume_with_tag(&self, tag: &str, size: i64) {
        let mut s = self.state.write().unwrap();
        s.curr_usage += size;
        *s.struct_size.entry(tag.to_owned()).or_default() += size;
    }
    /// Go `memRootImpl.ReleaseWithTag`.
    pub fn release_with_tag(&self, tag: &str) {
        let mut s = self.state.write().unwrap();
        s.curr_usage -= s.struct_size.remove(tag).unwrap_or_default();
    }
    /// Go `memRootImpl.RefreshConsumption`, currently a no-op in Go.
    pub fn refresh_consumption(&self) {}
}

#[cfg(test)]
mod tests {
    use super::*;
    /// Go `pkg/ddl/ingest/mem_root_test.go:TestMemoryRoot`.
    #[test]
    fn test_memory_root() {
        let m = MemRootImpl::new(1024);
        assert_eq!(m.max_memory_quota(), 1024);
        assert_eq!(m.current_usage(), 0);
        assert!(m.check_consume(1023));
        assert!(m.check_consume(1024));
        assert!(!m.check_consume(1025));
        m.consume(512);
        assert_eq!(m.current_usage(), 512);
        assert!(m.check_consume(512));
        assert!(!m.check_consume(513));
        assert_eq!(m.max_memory_quota(), 1024);
        m.release(10);
        assert_eq!(m.current_usage(), 502);
        assert_eq!(m.max_memory_quota(), 1024);
        m.set_max_memory_quota(512);
        assert!(!m.check_consume(20));
        m.release(502);
        assert_eq!(m.current_usage(), 0);
        m.set_max_memory_quota(1024);
        m.consume_with_tag("a", 512);
        m.consume_with_tag("b", 512);
        assert_eq!(m.current_usage(), 1024);
        assert!(!m.check_consume(1));
        m.release_with_tag("a");
        assert_eq!(m.current_usage(), 512);
        m.release_with_tag("a");
        assert_eq!(m.current_usage(), 512);
        assert!(m.check_consume(10));
        m.consume(10);
        assert_eq!(m.current_usage(), 522);
    }
}
