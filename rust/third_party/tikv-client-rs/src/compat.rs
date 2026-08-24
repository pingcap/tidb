// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

//! This module contains utility types and functions for making the transition
//! from futures 0.1 to 1.0 easier.

use std::pin::Pin;

use futures::prelude::*;
use futures::ready;
use futures::task::Context;
use futures::task::Poll;

/// A future implementing a tail-recursive loop.
///
/// Created by the `loop_fn` function.
#[derive(Debug)]
#[must_use = "futures do nothing unless polled"]
pub struct LoopFn<A, F> {
    future: A,
    func: F,
    errored: bool,
}

pub fn stream_fn<S, T, A, F, E>(initial_state: S, mut func: F) -> LoopFn<A, F>
where
    F: FnMut(S) -> A,
    A: Future<Output = Result<Option<(S, T)>, E>>,
{
    LoopFn {
        future: func(initial_state),
        func,
        errored: false,
    }
}

impl<S, T, A, F, E> Stream for LoopFn<A, F>
where
    F: FnMut(S) -> A,
    A: Future<Output = Result<Option<(S, T)>, E>>,
{
    type Item = Result<T, E>;
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        if self.errored {
            return Poll::Ready(None);
        }
        unsafe {
            let this = Pin::get_unchecked_mut(self);
            match ready!(Pin::new_unchecked(&mut this.future).poll(cx)) {
                Err(e) => {
                    this.errored = true;
                    Poll::Ready(Some(Err(e)))
                }
                Ok(None) => Poll::Ready(None),
                Ok(Some((s, t))) => {
                    this.future = (this.func)(s);
                    Poll::Ready(Some(Ok(t)))
                }
            }
        }
    }
}
