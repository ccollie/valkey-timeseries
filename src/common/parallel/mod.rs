use crate::common::parallel::items::{IntoItems, Items};
/// Copyright (c) 2023-present, The SWC authors. All rights reserved
/// Licensed under the Apache License, Version 2.0 (the "License");
use std::{cell::RefCell, mem::transmute};

pub mod items;
pub mod merge;

#[derive(Default)]
pub struct MaybeScope<'a>(ScopeLike<'a>);

enum ScopeLike<'a> {
    Scope(Scope<'a>),
    Global(Option<chili::Scope<'a>>),
}

impl Default for ScopeLike<'_> {
    fn default() -> Self {
        ScopeLike::Global(None)
    }
}

impl<'a> From<Scope<'a>> for MaybeScope<'a> {
    fn from(value: Scope<'a>) -> Self {
        MaybeScope(ScopeLike::Scope(value))
    }
}

impl<'a> MaybeScope<'a> {
    #[allow(clippy::redundant_closure)]
    pub fn with<F, R>(&mut self, f: F) -> R
    where
        F: FnOnce(Scope<'a>) -> R,
    {
        let scope: &mut chili::Scope = match &mut self.0 {
            ScopeLike::Scope(scope) => unsafe {
                // Safety: chili Scope will be alive until the end of the function, because its
                // contract of 'a lifetime in the type.
                transmute::<&mut chili::Scope, &mut chili::Scope>(&mut scope.0)
            },
            ScopeLike::Global(global_scope) => {
                // Initialize the global scope lazily, and only once.
                let scope = global_scope.get_or_insert_with(|| chili::Scope::global());

                unsafe {
                    // Safety: Global scope is not dropped until the end of the program, and no one
                    // can access this **instance** of the global scope in the same time.
                    transmute::<&mut chili::Scope, &mut chili::Scope>(scope)
                }
            }
        };

        let scope = Scope(scope);

        f(scope)
    }
}

pub struct Scope<'a>(&'a mut chili::Scope<'a>);

#[inline]
pub fn join<A, B, RA, RB>(oper_a: A, oper_b: B) -> (RA, RB)
where
    A: Send + FnOnce() -> RA,
    B: Send + FnOnce() -> RB,
    RA: Send,
    RB: Send,
{
    thread_local! {
        static SCOPE: RefCell<Option<MaybeScope<'static>>> = Default::default();
    }

    struct RemoveScopeGuard;

    impl Drop for RemoveScopeGuard {
        fn drop(&mut self) {
            SCOPE.set(None);
        }
    }

    let mut scope = SCOPE.take().unwrap_or_default();

    let (ra, rb) = join_maybe_scoped(
        &mut scope,
        |scope| {
            let scope = unsafe {
                // Safety: inner scope cannot outlive the outer scope
                transmute::<Scope, Scope>(scope)
            };
            let _guard = RemoveScopeGuard;
            SCOPE.set(Some(MaybeScope(ScopeLike::Scope(scope))));

            oper_a()
        },
        |scope| {
            let scope = unsafe {
                // Safety: inner scope cannot outlive the outer scope
                transmute::<Scope, Scope>(scope)
            };
            let _guard = RemoveScopeGuard;
            SCOPE.set(Some(MaybeScope(ScopeLike::Scope(scope))));

            oper_b()
        },
    );

    // In case of panic, we does not restore the scope so it will be None.
    SCOPE.set(Some(scope));

    (ra, rb)
}

#[inline]
pub fn join_maybe_scoped<'a, A, B, RA, RB>(
    scope: &mut MaybeScope<'a>,
    oper_a: A,
    oper_b: B,
) -> (RA, RB)
where
    A: Send + FnOnce(Scope<'a>) -> RA,
    B: Send + FnOnce(Scope<'a>) -> RB,
    RA: Send,
    RB: Send,
{
    scope.with(|scope| join_scoped(scope, oper_a, oper_b))
}

#[inline]
pub fn join_scoped<'a, A, B, RA, RB>(scope: Scope<'a>, oper_a: A, oper_b: B) -> (RA, RB)
where
    A: Send + FnOnce(Scope<'a>) -> RA,
    B: Send + FnOnce(Scope<'a>) -> RB,
    RA: Send,
    RB: Send,
{
    let (ra, rb) = scope.0.join(
        |scope| {
            let scope = Scope(unsafe {
                // Safety: This can be dangerous if the user do transmute on the scope, but it's
                // not our fault if the user uses transmute.
                transmute::<&mut chili::Scope, &mut chili::Scope>(scope)
            });

            oper_a(scope)
        },
        |scope| {
            let scope = Scope(unsafe {
                // Safety: This can be dangerous if the user do transmute on the scope, but it's
                // not our fault if the user uses transmute.
                transmute::<&mut chili::Scope, &mut chili::Scope>(scope)
            });

            oper_b(scope)
        },
    );

    (ra, rb)
}

pub trait Parallel: Send + Sync {
    /// Used to create visitor.
    fn create(&self) -> Self;

    /// This can be called in anytime.
    fn merge(&mut self, other: Self);
}

pub trait ParallelExt {
    /// Invoke `op` in parallel if the concurrent feature is enabled and `nodes.len()` is bigger than
    /// a threshold.
    fn maybe_par<I, F>(&mut self, threshold: usize, nodes: I, op: F)
    where
        I: IntoItems,
        F: Send + Sync + Fn(&mut Self, I::Elem),
    {
        self.maybe_par_idx(threshold, nodes, |v, _, n| op(v, n))
    }

    /// Invoke `op` in parallel, if compiled with the concurrent feature enabled and `nodes.len()` is
    /// larger than a threshold.
    ///
    fn maybe_par_idx<I, F>(&mut self, threshold: usize, nodes: I, op: F)
    where
        I: IntoItems,
        F: Send + Sync + Fn(&mut Self, usize, I::Elem),
    {
        self.maybe_par_idx_raw(threshold, nodes.into_items(), &op)
    }

    /// If you don't have a special reason, use [`Parallel::maybe_par`] or
    /// [`Parallel::maybe_par_idx`] instead.
    fn maybe_par_idx_raw<I, F>(&mut self, threshold: usize, nodes: I, op: &F)
    where
        I: Items,
        F: Send + Sync + Fn(&mut Self, usize, I::Elem);
}

// #[cfg(feature = "concurrent")]
impl<T> ParallelExt for T
where
    T: Parallel,
{
    fn maybe_par_idx_raw<I, F>(&mut self, threshold: usize, nodes: I, op: &F)
    where
        I: Items,
        F: Send + Sync + Fn(&mut Self, usize, I::Elem),
    {
        if nodes.len() >= threshold {
            let len = nodes.len();
            if len == 0 {
                return;
            }

            if len == 1 {
                op(self, 0, nodes.into_iter().next().unwrap());
                return;
            }

            let (na, nb) = nodes.split_at(len / 2);
            let mut vb = Parallel::create(&*self);
            let (_, vb) = join(
                || self.maybe_par_idx_raw(threshold, na, op),
                || {
                    vb.maybe_par_idx_raw(threshold, nb, op);
                    vb
                },
            );

            Parallel::merge(self, vb);

            return;
        }

        for (idx, n) in nodes.into_iter().enumerate() {
            op(self, idx, n);
        }
    }
}

// #[cfg(not(feature = "concurrent"))]
// impl<T> Parallel for T
// where
//     T: Parallel,
// {
//     fn maybe_par_idx_raw<I, F>(&mut self, _threshold: usize, nodes: I, op: &F)
//     where
//         I: Items,
//         F: Send + Sync + Fn(&mut Self, usize, I::Elem),
//     {
//         for (idx, n) in nodes.into_iter().enumerate() {
//             op(self, idx, n);
//         }
//     }
// }

pub fn par_try_for_each_mut<T: Send, F, E>(slice: &mut [T], f: F) -> Result<(), E>
where
    F: Fn(&mut T) -> Result<(), E> + Send + Sync,
    E: Send,
{
    fn for_each_internal<E, T, F>(slice: &mut [T], f: &F) -> Result<(), E>
    where
        F: Fn(&mut T) -> Result<(), E> + Send + Sync,
        E: Send,
        T: Send,
    {
        match slice {
            [] => Ok(()),
            [first] => f(first),
            [first, second] => {
                let (l, r) = join(|| f(first), || f(second));
                l?;
                r?;
                Ok(())
            }
            _ => {
                let mid = slice.len() / 2;
                let (left, right) = slice.split_at_mut(mid);
                let (l, r) = join(
                    || for_each_internal(left, f),
                    || for_each_internal(right, f),
                );
                l?;
                r?;
                Ok(())
            }
        }
    }

    for_each_internal(slice, &f)
}
