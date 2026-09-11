use core::num::NonZeroUsize;
use orx_parallel::{
    DefaultExecutor, IntoParIter, IterIntoParIter, ParIter, ParThreadPool, Parallelizable,
    ParallelizableCollection, ParallelizableCollectionMut, RunnerWithPool,
};

/// orx-parallel adapter over the module's global rayon pool (built in
/// [`init_thread_pool`](super::init_thread_pool)).
///
/// orx-parallel's own `rayon-core` feature only covers an owned `rayon_core::ThreadPool`, and
/// rayon never hands out a handle to the global registry, so this delegates to the free
/// functions instead. Without it every `.par()` call spins up std threads via orx's
/// `StdDefaultPool`, ignoring `ts-num-threads` entirely. Enter parallel iteration through
/// the `*_rayon` methods below rather than the bare orx entry points.
#[derive(Clone, Copy, Debug, Default)]
pub struct GlobalRayonPool;

impl ParThreadPool for GlobalRayonPool {
    type ScopeRef<'s, 'env, 'scope>
        = &'s rayon_core::Scope<'scope>
    where
        'scope: 's,
        'env: 'scope + 's;

    fn run_in_scope<'s, 'env, 'scope, W>(s: &Self::ScopeRef<'s, 'env, 'scope>, work: W)
    where
        'scope: 's,
        'env: 'scope + 's,
        W: Fn() + Send + 'scope + 'env,
    {
        s.spawn(move |_| work());
    }

    fn scoped_computation<'env, 'scope, F>(&'env mut self, f: F)
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

/// The orx runner every `*_rayon` entry point produces: the global rayon pool with orx's
/// default chunk-sizing executor.
pub type RayonRunner = RunnerWithPool<GlobalRayonPool, DefaultExecutor>;

/// `.par()` on the global rayon pool, for borrowed sources such as `&[T]` and ranges.
///
/// orx splits `.par()` across two disjoint blanket traits ([`Parallelizable`] for types that
/// are themselves concurrent-iterable, [`ParallelizableCollection`] for owning collections
/// like `Vec<T>`), so `par_rayon` is split the same way; both resolve at the call site
/// exactly where orx's own `.par()` does.
pub trait ParRayon: Parallelizable {
    fn par_rayon(&self) -> impl ParIter<RayonRunner, Item = Self::Item> {
        self.par().with_pool(GlobalRayonPool)
    }
}

impl<T: Parallelizable> ParRayon for T {}

/// `.par()` on the global rayon pool, for owning collections such as `Vec<T>`.
pub trait ParCollectionRayon: ParallelizableCollection {
    fn par_rayon(&self) -> impl ParIter<RayonRunner, Item = &Self::Item> {
        self.par().with_pool(GlobalRayonPool)
    }
}

impl<T: ParallelizableCollection> ParCollectionRayon for T {}

/// `.par_mut()` on the global rayon pool.
pub trait ParMutRayon: ParallelizableCollectionMut {
    fn par_mut_rayon(&mut self) -> impl ParIter<RayonRunner, Item = &mut Self::Item> {
        self.par_mut().with_pool(GlobalRayonPool)
    }
}

impl<T: ParallelizableCollectionMut> ParMutRayon for T {}

/// `.into_par()` on the global rayon pool.
pub trait IntoParRayon: IntoParIter {
    fn into_par_rayon(self) -> impl ParIter<RayonRunner, Item = Self::Item>
    where
        Self: Sized,
    {
        self.into_par().with_pool(GlobalRayonPool)
    }
}

impl<T: IntoParIter> IntoParRayon for T {}

/// `.iter_into_par()` on the global rayon pool.
pub trait IterIntoParRayon: IterIntoParIter {
    fn iter_into_par_rayon(self) -> impl ParIter<RayonRunner, Item = Self::Item>
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
    use orx_parallel::{ParIter, ParThreadPool};

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
