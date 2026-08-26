//! Priority admission primitive used by the internal-client batch loop.
//!
//! This is a native, ownership-based mapping of client-go
//! `internal/client/priority_queue.go`, used by `BatchCommandsBuilder` for
//! source-compatible priority selection and cancellation cleanup.

/// An entry accepted by [`PriorityQueue`].
pub(crate) trait PriorityItem {
    /// Higher values are selected first, matching client-go's heap ordering.
    fn priority(&self) -> u64;

    /// Whether the caller has cancelled this entry before batch selection.
    fn is_cancelled(&self) -> bool;
}

/// A max-heap with client-go-compatible priority and cancellation operations.
#[derive(Debug)]
pub(crate) struct PriorityQueue<T> {
    items: Vec<T>,
}

impl<T: PriorityItem> PriorityQueue<T> {
    pub(crate) fn new() -> Self {
        Self { items: Vec::new() }
    }

    pub(crate) fn len(&self) -> usize {
        self.items.len()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    pub(crate) fn push(&mut self, item: T) {
        self.items.push(item);
        self.sift_up(self.items.len() - 1);
    }

    /// Removes and returns up to `count` highest-priority entries.
    ///
    /// Like client-go `Take`, zero takes nothing and a count at least the
    /// queue length transfers all queued entries. Rust ownership eliminates
    /// the Go backing-array reference-retention concern after that transfer.
    pub(crate) fn take(&mut self, count: usize) -> Vec<T> {
        if count == 0 {
            return Vec::new();
        }
        if count >= self.len() {
            return std::mem::take(&mut self.items);
        }
        (0..count).filter_map(|_| self.pop()).collect()
    }

    pub(crate) fn highest_priority(&self) -> u64 {
        self.items.first().map_or(0, PriorityItem::priority)
    }

    /// Returns every queued entry in heap order, which is intentionally not
    /// full priority order just as client-go's `all` helper is not.
    pub(crate) fn all(&self) -> &[T] {
        &self.items
    }

    /// Removes entries cancelled before selection and restores heap order.
    pub(crate) fn clean(&mut self) {
        self.items.retain(|item| !item.is_cancelled());
        if self.items.len() > 1 {
            for index in (0..self.items.len() / 2).rev() {
                self.sift_down(index);
            }
        }
    }

    pub(crate) fn reset(&mut self) {
        self.items.clear();
    }

    fn pop(&mut self) -> Option<T> {
        let item = self.items.swap_remove(0);
        if !self.items.is_empty() {
            self.sift_down(0);
        }
        Some(item)
    }

    fn sift_up(&mut self, mut child: usize) {
        while child > 0 {
            let parent = (child - 1) / 2;
            if self.items[parent].priority() >= self.items[child].priority() {
                break;
            }
            self.items.swap(parent, child);
            child = parent;
        }
    }

    fn sift_down(&mut self, mut parent: usize) {
        loop {
            let left = parent * 2 + 1;
            if left >= self.items.len() {
                return;
            }
            let right = left + 1;
            let largest = if right < self.items.len()
                && self.items[right].priority() > self.items[left].priority()
            {
                right
            } else {
                left
            };
            if self.items[parent].priority() >= self.items[largest].priority() {
                return;
            }
            self.items.swap(parent, largest);
            parent = largest;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug)]
    struct Item {
        priority: u64,
        cancelled: bool,
    }

    impl PriorityItem for Item {
        fn priority(&self) -> u64 {
            self.priority
        }

        fn is_cancelled(&self) -> bool {
            self.cancelled
        }
    }

    #[test]
    fn source_priority_take_and_cancelled_cleanup_contract() {
        let mut queue = PriorityQueue::new();
        for priority in 1..=5 {
            queue.push(Item {
                priority,
                cancelled: false,
            });
        }
        assert_eq!(queue.len(), 5);
        assert_eq!(queue.highest_priority(), 5);
        assert!(queue.all().iter().any(|item| item.priority == 5));

        assert_eq!(queue.take(0).len(), 0);
        assert_eq!(queue.take(1)[0].priority, 5);
        assert_eq!(
            queue
                .take(2)
                .iter()
                .map(|item| item.priority)
                .collect::<Vec<_>>(),
            [4, 3]
        );
        assert_eq!(queue.highest_priority(), 2);
        assert_eq!(
            queue
                .take(5)
                .iter()
                .map(|item| item.priority)
                .collect::<Vec<_>>(),
            [2, 1]
        );
        assert!(queue.is_empty());

        queue.push(Item {
            priority: 1,
            cancelled: true,
        });
        queue.push(Item {
            priority: 2,
            cancelled: false,
        });
        queue.clean();
        assert_eq!(queue.len(), 1);
        assert_eq!(queue.highest_priority(), 2);
        queue.reset();
        assert!(queue.is_empty());
    }
}
