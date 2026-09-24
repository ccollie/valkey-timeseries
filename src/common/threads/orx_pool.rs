use core::num::NonZeroUsize;
use orx_parallel::{
    IntoParIter, IterIntoParIter, Par, ParCollection, ParCollectionMut, Parallelizable, Runner,
    ThreadPool,
};

/// orx-parallel adapter over the module's global rayon pool (built in
/// [`init_thread_pool`](super::init_thread_pool)).
///
/// orx-parallel's own `rayon-core` feature only covers a `rayon_core::ThreadPool` handle, and
/// rayon never hands out a handle to the global registry, so this delegates to the free
/// functions instead. Without it every `.par()` call spins up std threads via orx's
/// `StdDefaultPool`, ignoring `ts-num-threads` entirely. Enter parallel iteration through
/// the `*_rayon` methods below rather than the bare orx entry points.
///
/// Like rayon's own free functions, it runs on the *calling worker's* pool when called from
/// inside one, and on the global pool otherwise. PromQL evaluations rely on that: they run
/// installed on their own pool (`query_workers::run_evaluation`), so the `*_rayon` calls
/// they reach never park global workers that a module-lock holder may be waiting on.
#[derive(Clone, Copy, Debug, Default)]
pub struct GlobalRayonPool;

impl ThreadPool for GlobalRayonPool {
    type ScopeRef<'s, 'env, 'scope>
        = &'s rayon_core::Scope<'scope>
    where
        'scope: 's,
        'env: 'scope + 's;

    fn scope<'env, 'scope, F>(&'env self, f: F)
    where
        'env: 'scope,
        for<'s> F: FnOnce(&'s rayon_core::Scope<'scope>) + Send,
    {
        rayon_core::scope(f)
    }

    fn max_num_threads(&self) -> NonZeroUsize {
        NonZeroUsize::new(rayon_core::current_num_threads().max(1)).expect(">0")
    }
}

/// orx-parallel adapter over one specific `rayon_core::ThreadPool`, for a
/// computation that must not depend on the global pool — the PromQL selector
/// executor's materialization, which runs while global workers may be parked
/// waiting for it. Attach with `.with_pool(RayonPool(&pool))`.
#[derive(Clone, Copy)]
pub struct RayonPool(pub &'static rayon_core::ThreadPool);

impl ThreadPool for RayonPool {
    type ScopeRef<'s, 'env, 'scope>
        = &'s rayon_core::Scope<'scope>
    where
        'scope: 's,
        'env: 'scope + 's;

    fn scope<'env, 'scope, F>(&'env self, f: F)
    where
        'env: 'scope,
        for<'s> F: FnOnce(&'s rayon_core::Scope<'scope>) + Send,
    {
        self.0.scope(f)
    }

    fn max_num_threads(&self) -> NonZeroUsize {
        NonZeroUsize::new(self.0.current_num_threads().max(1)).expect(">0")
    }
}

/// orx 3's `.with_pool(pool)`, which 4.0 replaced with `.runner(..)`: runs the computation on
/// `pool` with fixed chunk sizing — the executor 3.x attached there (4.0's default runner
/// chunks adaptively).
pub trait ParWithPool: Par {
    fn with_pool<P: ThreadPool + Sync>(self, pool: P) -> impl Par<Item = Self::Item> {
        self.runner(Runner::fixed_with_pool(pool))
    }
}

impl<T: Par> ParWithPool for T {}

/// `.par()` on the global rayon pool, for borrowed sources such as `&[T]` and ranges.
///
/// orx splits `.par()` across two disjoint blanket traits ([`Parallelizable`] for types that
/// are themselves concurrent-iterable, [`ParCollection`] for owning collections
/// like `Vec<T>`), so `par_rayon` is split the same way; both resolve at the call site
/// exactly where orx's own `.par()` does.
pub trait ParRayon: Parallelizable {
    fn par_rayon(&self) -> impl Par<Item = Self::Item> {
        self.par().with_pool(GlobalRayonPool)
    }
}

impl<T: Parallelizable> ParRayon for T {}

/// `.par()` on the global rayon pool, for owning collections such as `Vec<T>`.
pub trait ParCollectionRayon: ParCollection {
    fn par_rayon(&self) -> impl Par<Item = &Self::Item> {
        self.par().with_pool(GlobalRayonPool)
    }
}

impl<T: ParCollection> ParCollectionRayon for T {}

/// `.par_mut()` on the global rayon pool.
pub trait ParMutRayon: ParCollectionMut {
    fn par_mut_rayon(&mut self) -> impl Par<Item = &mut Self::Item> {
        self.par_mut().with_pool(GlobalRayonPool)
    }
}

impl<T: ParCollectionMut> ParMutRayon for T {}

/// `.into_par()` on the global rayon pool.
pub trait IntoParRayon: IntoParIter {
    fn into_par_rayon(self) -> impl Par<Item = Self::Item>
    where
        Self: Sized,
    {
        self.into_par().with_pool(GlobalRayonPool)
    }
}

impl<T: IntoParIter> IntoParRayon for T {}

/// `.iter_into_par()` on the global rayon pool.
pub trait IterIntoParRayon: IterIntoParIter {
    fn iter_into_par_rayon(self) -> impl Par<Item = Self::Item>
    where
        Self: Sized,
        Self::Item: Send,
    {
        self.iter_into_par().with_pool(GlobalRayonPool)
    }
}

impl<T: IterIntoParIter> IterIntoParRayon for T {}

#[cfg(test)]
mod tests {
    use super::{GlobalRayonPool, ParCollectionRayon};
    use orx_parallel::{Par, ThreadPool};

    #[test]
    fn runs_on_rayon_workers() {
        let pool = rayon_core::ThreadPoolBuilder::new()
            .num_threads(3)
            .thread_name(|i| format!("orx-pool-test-{i}"))
            .build()
            .unwrap();
        let inputs: Vec<u64> = (0..10_000).collect();
        // Entered from inside a worker so the scope lands on this pool rather than the global one.
        let (sum, names) = pool.install(|| {
            let names: Vec<String> = inputs
                .par_rayon()
                .map(|_| std::thread::current().name().unwrap_or("").to_string())
                .collect();
            (inputs.par_rayon().sum(), names)
        });
        assert_eq!(sum, (0..10_000u64).sum::<u64>());
        assert!(
            names.iter().all(|n| n.starts_with("orx-pool-test-")),
            "{names:?}"
        );
    }

    #[test]
    fn max_threads_tracks_rayon() {
        assert_eq!(
            GlobalRayonPool.max_num_threads().get(),
            rayon_core::current_num_threads().max(1)
        );
    }
}
