// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::atomic::{AtomicUsize, Ordering};

use tokio::sync::Notify;

use crate::async_util::Cancellation;

/// Cancellation-aware fixed-capacity token limiter.
pub struct RateLimit {
    capacity: usize,
    in_use: AtomicUsize,
    available: Notify,
}

impl RateLimit {
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity,
            in_use: AtomicUsize::new(0),
            available: Notify::new(),
        }
    }

    /// Acquire one token. Returns true when cancellation wins.
    pub async fn get_token(&self, done: &Cancellation) -> bool {
        loop {
            let notified = self.available.notified();
            let mut current = self.in_use.load(Ordering::Acquire);
            while current < self.capacity {
                match self.in_use.compare_exchange_weak(
                    current,
                    current + 1,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                ) {
                    Ok(_) => return false,
                    Err(observed) => current = observed,
                }
            }
            tokio::select! {
                _ = done.cancelled() => return true,
                _ = notified => {}
            }
        }
    }

    /// Return one token.
    ///
    /// # Panics
    /// Panics when no token is held, matching client-go's redundant-put guard.
    pub fn put_token(&self) {
        let mut current = self.in_use.load(Ordering::Acquire);
        loop {
            assert!(current != 0, "put a redundant token");
            match self.in_use.compare_exchange_weak(
                current,
                current - 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    self.available.notify_one();
                    return;
                }
                Err(observed) => current = observed,
            }
        }
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;

    #[tokio::test]
    async fn source_token_capacity_cancellation_and_redundant_put() {
        let limiter = Arc::new(RateLimit::new(1));
        assert_eq!(limiter.capacity(), 1);
        assert!(std::panic::catch_unwind(|| limiter.put_token()).is_err());
        let done = Cancellation::default();
        assert!(!limiter.get_token(&done).await);

        let waiter = {
            let limiter = limiter.clone();
            let done = done.clone();
            tokio::spawn(async move { limiter.get_token(&done).await })
        };
        tokio::task::yield_now().await;
        limiter.put_token();
        assert!(!waiter.await.unwrap());
        limiter.put_token();

        assert!(!limiter.get_token(&done).await);
        done.cancel();
        assert!(limiter.get_token(&done).await);
        limiter.put_token();
        assert!(std::panic::catch_unwind(|| limiter.put_token()).is_err());
    }
}
