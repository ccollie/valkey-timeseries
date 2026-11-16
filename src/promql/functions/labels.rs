use crate::labels::build_with_repeat_fallback;
use crate::labels::regex_cache::{REGEX_CACHE_MAX_PATTERN_LEN, evict_oldest};
use crate::promql::exec::utils::ensure_unique_labelsets;
use crate::promql::functions::types::{PromQLArg, PromQLFunction};
use crate::promql::functions::utils::{
    expect_exact_arg_count, expect_instant_vector, expect_label_name, expect_min_arg_count,
    expect_string,
};
use crate::promql::{EvalContext, EvalResult, EvalSample, EvaluationError, ExprResult};
use ahash::AHashMap;
use promql_parser::label::METRIC_NAME;
use regex::Regex;
use std::cell::RefCell;
use std::time::Instant;

struct CachedRegex {
    regex: Regex,
    created: Instant,
}

thread_local! {
    /// Compiled `label_replace` patterns, keyed by the anchored source.
    ///
    /// The regex argument reaches the function as a string evaluated afresh at
    /// every step, so a range query would otherwise compile the same pattern
    /// once per step — and compiling costs ~200x a match.
    ///
    /// The cache is per thread, not shared, on purpose. Range steps are
    /// evaluated in parallel, and a `Regex` shared across workers serialises
    /// them twice on every match: its cache pool has a lock-free path for one
    /// owner thread only, and every `captures` call clones the shared
    /// `GroupInfo` `Arc`. A per-thread compile (a clone would still share the
    /// `Arc`) measured 5x faster than any shared map at 8 workers x 100
    /// series; the one-time compile per worker is 20-35 µs.
    ///
    /// Patterns come from user queries, so the map is bounded: once it holds
    /// [`REGEX_CACHE_CAPACITY`] entries, the oldest tenth make room for the
    /// next pattern rather than the map growing with every distinct query, and
    /// a pattern longer than [`REGEX_CACHE_MAX_PATTERN_LEN`] is never stored.
    ///
    /// Each entry is bounded too: patterns compile under the module's regex
    /// limits (a 64 KiB program and a 16 KiB lazy-DFA cache), so a full cache
    /// holds roughly 5 MiB per thread. With the regex crate's defaults (10 MiB
    /// and 2 MiB) it could hold about 770 MiB, and one hostile pattern took
    /// ~100 ms to build or refuse.
    static REGEX_CACHE: RefCell<AHashMap<String, CachedRegex>> = RefCell::new(AHashMap::new());
}

const REGEX_CACHE_CAPACITY: usize = 64;

/// How many entries a full cache gives up at once: a tenth, rounded up so a
/// full cache always frees at least one slot.
const REGEX_CACHE_EVICT_COUNT: usize = REGEX_CACHE_CAPACITY.div_ceil(10);

/// Runs `f` against the compiled, fully anchored form of `regex_src`,
/// compiling it on this thread's first sight of the pattern. A pattern longer
/// than [`REGEX_CACHE_MAX_PATTERN_LEN`] is compiled for this call only, so
/// the keys of a full cache stay bounded however long a query may be.
fn with_anchored_regex<T>(regex_src: &str, f: impl FnOnce(&Regex) -> T) -> EvalResult<T> {
    let anchored = format!("^(?s:{regex_src})$");
    // Also accepts Go's literal braces (`a{b}`), as Prometheus does and as
    // selector matchers already did.
    let compile = |pattern: &str| {
        build_with_repeat_fallback(pattern).map_err(|err| {
            EvaluationError::ArgumentError(format!(
                "invalid regular expression in label_replace(): {err}"
            ))
        })
    };
    if anchored.len() > REGEX_CACHE_MAX_PATTERN_LEN {
        return Ok(f(&compile(&anchored)?));
    }
    REGEX_CACHE.with(|cache| {
        let mut cache = cache.borrow_mut();
        if !cache.contains_key(&anchored) {
            let regex = compile(&anchored)?;
            if cache.len() >= REGEX_CACHE_CAPACITY {
                evict_oldest(&mut cache, REGEX_CACHE_EVICT_COUNT, |entry| entry.created);
            }
            cache.insert(
                anchored.clone(),
                CachedRegex {
                    regex,
                    created: Instant::now(),
                },
            );
        }
        Ok(f(&cache[&anchored].regex))
    })
}

/// The longest value `label_replace` or `label_join` may write into a label.
///
/// Prometheus has no such limit. Without one, each nesting level of these
/// functions can double a value (`label_join(v, "a", "", "a", "a")`) or
/// multiply it (a replacement of `$1` repeated), so a sub-KiB query over
/// `vector(1)` asked for terabytes: 26 levels of `label_join` took the server
/// to 1 GB in 0.4 s, and an allocation that fails aborts the process, which no
/// panic guard can catch. A value is refused while it is being built, before
/// the memory is spent.
pub(in crate::promql) const MAX_GENERATED_LABEL_VALUE_BYTES: usize = 16 * 1024;

/// The largest label set, names and values together, a `label_replace` or
/// `label_join` write may grow a series to. The per-value cap alone still let
/// a deep query add hundreds of maximal labels to every series.
///
/// Only growth is refused: a stored series whose labels are already larger
/// can still be rewritten so long as the write does not make them larger.
pub(in crate::promql) const MAX_GENERATED_LABEL_SET_BYTES: usize = 64 * 1024;

fn label_value_too_long(function: &str, dst_label: &str) -> EvaluationError {
    EvaluationError::execution(format!(
        "{function}(): the value of label \"{dst_label}\" would be longer than \
         {MAX_GENERATED_LABEL_VALUE_BYTES} bytes"
    ))
}

#[derive(Copy, Clone)]
pub(in crate::promql) struct LabelReplaceFunction;

/// Write `value` into `sample`'s `dst_label`, or remove the label when `value`
/// is empty. A sample whose `__name__` is written is named again, so it no
/// longer owes the name drop a function before it left pending.
///
/// Refused when the write would grow the sample's labels past
/// [`MAX_GENERATED_LABEL_SET_BYTES`]; `value` itself was bounded by its builder.
fn write_label(
    sample: &mut EvalSample,
    function: &str,
    dst_label: &str,
    value: String,
) -> EvalResult<()> {
    if value.is_empty() {
        sample.labels.remove(dst_label);
    } else {
        let current: usize = sample
            .labels
            .iter()
            .map(|l| l.name.len() + l.value.len())
            .sum();
        let replaced = sample
            .labels
            .get(dst_label)
            .map_or(0, |old| dst_label.len() + old.len());
        let grown = current - replaced + dst_label.len() + value.len();
        if grown > current && grown > MAX_GENERATED_LABEL_SET_BYTES {
            return Err(EvaluationError::execution(format!(
                "{function}(): writing label \"{dst_label}\" would make a series' labels \
                 longer than {MAX_GENERATED_LABEL_SET_BYTES} bytes"
            )));
        }
        sample.labels.set(dst_label, value);
    }
    if dst_label == METRIC_NAME {
        sample.drop_name = false;
    }
    Ok(())
}

/// [`regex::Captures::expand`], refused once the result would pass
/// [`MAX_GENERATED_LABEL_VALUE_BYTES`]: each capture is checked before it is
/// appended, so at most the literal text of `replacement` lands past the limit.
fn expand_bounded(
    regex: &Regex,
    captures: &regex::Captures<'_>,
    replacement: &str,
    dst_label: &str,
) -> EvalResult<String> {
    let mut expanded = String::new();
    let mut too_long = false;
    regex_automata::util::interpolate::string(
        replacement,
        |index, dst| {
            let Some(group) = captures.get(index) else {
                return;
            };
            if too_long || dst.len() + group.len() > MAX_GENERATED_LABEL_VALUE_BYTES {
                too_long = true;
                return;
            }
            dst.push_str(group.as_str());
        },
        |name| regex.capture_names().position(|n| n == Some(name)),
        &mut expanded,
    );
    if too_long || expanded.len() > MAX_GENERATED_LABEL_VALUE_BYTES {
        return Err(label_value_too_long("label_replace", dst_label));
    }
    Ok(expanded)
}

fn apply_label_replace_to_samples(
    mut samples: Vec<EvalSample>,
    dst_label: &str,
    replacement: &str,
    src_label: &str,
    regex_src: &str,
) -> EvalResult<ExprResult> {
    expect_label_name(dst_label)?;

    with_anchored_regex(regex_src, |regex| {
        for sample in &mut samples {
            let src_value = sample.labels.get(src_label).unwrap_or_default();

            if let Some(captures) = regex.captures(src_value) {
                let replaced = expand_bounded(regex, &captures, replacement, dst_label)?;
                write_label(sample, "label_replace", dst_label, replaced)?;
            }
        }
        EvalResult::Ok(())
    })??;

    Ok(ExprResult::InstantVector(samples))
}

impl PromQLFunction for LabelReplaceFunction {
    fn apply_args(&self, args: Vec<PromQLArg>, _ctx: &EvalContext) -> EvalResult<ExprResult> {
        expect_exact_arg_count("label_replace", 5, args.len())?;

        let mut args_iter = args.into_iter();
        let samples = expect_instant_vector(
            args_iter.next().expect("validated args.len() == 5"),
            "label_replace",
        )?;

        let dst_label = expect_string(
            args_iter.next().expect("validated args.len() == 5"),
            "label_replace",
            "destination_label",
        )?;
        let replacement = expect_string(
            args_iter.next().expect("validated args.len() == 5"),
            "label_replace",
            "replacement",
        )?;
        let src_label = expect_string(
            args_iter.next().expect("validated args.len() == 5"),
            "label_replace",
            "source_label",
        )?;
        let regex_src = expect_string(
            args_iter.next().expect("validated args.len() == 5"),
            "label_replace",
            "regex",
        )?;

        let result = apply_label_replace_to_samples(
            samples,
            &dst_label,
            &replacement,
            &src_label,
            &regex_src,
        )?;
        let ExprResult::InstantVector(iv) = &result else {
            // should never occur!
            panic!("Invalid return value in LabelReplaceFunction");
        };
        ensure_unique_labelsets(iv)?;
        Ok(result)
    }
}

impl Default for LabelReplaceFunction {
    fn default() -> Self {
        LabelReplaceFunction
    }
}

/// `label_join(v instant-vector, dst_label string, separator string, src_label_1 string, src_label_2 string, ...)`
#[derive(Copy, Clone)]
pub(in crate::promql) struct LabelJoinFunction;

impl PromQLFunction for LabelJoinFunction {
    fn apply_args(&self, args: Vec<PromQLArg>, _ctx: &EvalContext) -> EvalResult<ExprResult> {
        let actual_args = args.len();
        expect_min_arg_count("label_join", 3, actual_args)?;

        let mut args_iter = args.into_iter();
        let mut samples = expect_instant_vector(
            args_iter
                .next()
                .expect("validated args.len() == ctx.raw_args.len() >= 3"),
            "label_join",
        )?;
        let dst_arg = args_iter
            .next()
            .expect("validated evaluated_args.len() == 4");
        let separator_arg = args_iter
            .next()
            .expect("validated evaluated_args.len() == 3");

        let dst_label = expect_string(dst_arg, "label_join", "destination_label")?;
        let separator = expect_string(separator_arg, "label_join", "separator")?;
        let src_labels = args_iter
            .map(|arg| expect_string(arg, "label_join", "source_label"))
            .collect::<EvalResult<Vec<_>>>()?;

        expect_label_name(&dst_label)?;

        for sample in &mut samples {
            let mut joined = String::new();
            for (index, src_label) in src_labels.iter().enumerate() {
                let value = sample.labels.get(src_label).unwrap_or_default();
                let separator = if index > 0 { separator.as_str() } else { "" };
                // Checked before growing: a nested join doubles the value per level.
                if joined.len() + separator.len() + value.len() > MAX_GENERATED_LABEL_VALUE_BYTES {
                    return Err(label_value_too_long("label_join", &dst_label));
                }
                joined.push_str(separator);
                joined.push_str(value);
            }

            write_label(sample, "label_join", &dst_label, joined)?;
        }

        ensure_unique_labelsets(&samples)?;

        Ok(ExprResult::InstantVector(samples))
    }
}

impl Default for LabelJoinFunction {
    fn default() -> Self {
        LabelJoinFunction
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cached_patterns() -> Vec<String> {
        REGEX_CACHE.with(|cache| {
            let cache = cache.borrow();
            let mut entries: Vec<(Instant, &String)> = cache
                .iter()
                .map(|(pattern, entry)| (entry.created, pattern))
                .collect();
            entries.sort();
            entries.into_iter().map(|(_, p)| p.clone()).collect()
        })
    }

    #[test]
    fn should_evict_oldest_tenth_when_full() {
        // Thread-local state: this test's thread starts with an empty cache.
        REGEX_CACHE.with(|cache| cache.borrow_mut().clear());

        for i in 0..REGEX_CACHE_CAPACITY {
            with_anchored_regex(&format!("p{i}"), |_| ()).unwrap();
        }
        assert_eq!(cached_patterns().len(), REGEX_CACHE_CAPACITY);

        with_anchored_regex("overflow", |_| ()).unwrap();

        let remaining = cached_patterns();
        assert_eq!(
            remaining.len(),
            REGEX_CACHE_CAPACITY - REGEX_CACHE_EVICT_COUNT + 1
        );
        // The oldest tenth is gone, everything younger survived in order,
        // and the newcomer is the youngest entry.
        for i in 0..REGEX_CACHE_EVICT_COUNT {
            assert!(!remaining.contains(&format!("^(?s:p{i})$")));
        }
        let survivors: Vec<String> = (REGEX_CACHE_EVICT_COUNT..REGEX_CACHE_CAPACITY)
            .map(|i| format!("^(?s:p{i})$"))
            .chain(std::iter::once("^(?s:overflow)$".to_string()))
            .collect();
        assert_eq!(remaining, survivors);
    }

    #[test]
    fn should_not_cache_patterns_over_the_length_limit() {
        REGEX_CACHE.with(|cache| cache.borrow_mut().clear());
        // Whitespace in verbose mode: a long source with a tiny program.
        let long = format!("(?x)a{}", " ".repeat(REGEX_CACHE_MAX_PATTERN_LEN));
        assert!(with_anchored_regex(&long, |regex| regex.is_match("a")).unwrap());
        assert!(cached_patterns().is_empty());
        assert!(with_anchored_regex("(", |_| ()).is_err());
    }

    fn sample(labels: &[(&str, &str)]) -> EvalSample {
        EvalSample {
            timestamp_ms: 1000,
            value: 1.0,
            labels: crate::promql::exec::types::EvalLabels::from_pairs(labels),
            drop_name: false,
        }
    }

    fn call(
        function: impl PromQLFunction,
        input: EvalSample,
        args: &[&str],
    ) -> EvalResult<Vec<EvalSample>> {
        let mut all = vec![PromQLArg::InstantVector(vec![input])];
        all.extend(args.iter().map(|&arg| PromQLArg::from(arg)));
        match function.apply_args(all, &EvalContext::at(1000))? {
            ExprResult::InstantVector(samples) => Ok(samples),
            other => panic!("expected an instant vector, got {other:?}"),
        }
    }

    fn label_join(input: EvalSample, args: &[&str]) -> EvalResult<Vec<EvalSample>> {
        call(LabelJoinFunction, input, args)
    }

    fn label_replace(input: EvalSample, args: &[&str]) -> EvalResult<Vec<EvalSample>> {
        call(LabelReplaceFunction, input, args)
    }

    fn assert_refused(result: EvalResult<Vec<EvalSample>>, what: &str) {
        let err = result.expect_err("an oversized label must be refused");
        assert!(err.to_string().contains(what), "unexpected error: {err}");
    }

    #[test]
    fn label_join_refuses_a_value_past_the_limit() {
        let half = "x".repeat(MAX_GENERATED_LABEL_VALUE_BYTES / 2);
        let at_limit = label_join(sample(&[("a", &half)]), &["b", "", "a", "a"]).unwrap();
        assert_eq!(
            at_limit[0].labels.get("b").map(str::len),
            Some(MAX_GENERATED_LABEL_VALUE_BYTES)
        );

        // One separator byte more than fits.
        assert_refused(
            label_join(sample(&[("a", &half)]), &["b", "-", "a", "a"]),
            "label_join(): the value of label \"b\" would be longer than",
        );
    }

    #[test]
    fn label_replace_refuses_a_value_past_the_limit() {
        let half = "x".repeat(MAX_GENERATED_LABEL_VALUE_BYTES / 2);
        let at_limit = label_replace(sample(&[("a", &half)]), &["b", "$1$1", "a", "(.*)"]).unwrap();
        assert_eq!(
            at_limit[0].labels.get("b").map(str::len),
            Some(MAX_GENERATED_LABEL_VALUE_BYTES)
        );

        // Exactly at it, then past it through a capture and through trailing literal text.
        label_replace(sample(&[("a", &half)]), &["b", "${1}$1", "a", "(.*)"]).unwrap();
        for replacement in ["$1$1$1", "$1${1}-"] {
            assert_refused(
                label_replace(sample(&[("a", &half)]), &["b", replacement, "a", "(.*)"]),
                "label_replace(): the value of label \"b\" would be longer than",
            );
        }
    }

    /// The bounded expansion must keep `Captures::expand`'s syntax exactly.
    #[test]
    fn label_replace_expands_like_the_regex_crate() {
        let pattern = "(?P<name>[a-z]+)-(?P<num>[0-9]+)\\.(.*)";
        let haystack = "host-42.example";
        let regex = Regex::new(&format!("^(?s:{pattern})$")).unwrap();
        let captures = regex.captures(haystack).unwrap();
        for replacement in [
            "$1:$3",
            "${name}_$num",
            "$name$num",
            "$name_$num",
            "$$1",
            "$9-x",
            "${missing}",
            "${3}x$",
            "plain",
        ] {
            let mut reference = String::new();
            captures.expand(replacement, &mut reference);
            let result = label_replace(
                sample(&[("src", haystack)]),
                &["dst", replacement, "src", pattern],
            )
            .unwrap();
            let got = result[0].labels.get("dst").unwrap_or_default();
            assert_eq!(got, reference, "expansion of {replacement:?}");
        }
    }

    fn label_bytes(sample: &EvalSample) -> usize {
        sample
            .labels
            .iter()
            .map(|l| l.name.len() + l.value.len())
            .sum()
    }

    #[test]
    fn label_writes_may_not_grow_a_label_set_past_the_limit() {
        let value = "v".repeat(MAX_GENERATED_LABEL_VALUE_BYTES - 1);
        let mut input = sample(&[("src", &value)]);
        // Copies of `src` under new names until the next one would not fit.
        for n in 0.. {
            let dst = format!("l{n}");
            match label_replace(input.clone(), &[&dst, "$1", "src", "(.*)"]) {
                Ok(mut out) => input = out.pop().unwrap(),
                Err(err) => {
                    assert!(
                        err.to_string().contains(&format!(
                            "label_replace(): writing label \"{dst}\" would make a series' \
                             labels longer than"
                        )),
                        "unexpected error: {err}"
                    );
                    assert!(
                        label_bytes(&input) + dst.len() + value.len()
                            > MAX_GENERATED_LABEL_SET_BYTES
                    );
                    break;
                }
            }
        }
        assert!(label_bytes(&input) <= MAX_GENERATED_LABEL_SET_BYTES);
    }

    /// A stored series' labels may already be larger than the limit: such a set can be
    /// rewritten in place or shrunk, only not grown.
    #[test]
    fn an_oversized_stored_label_set_may_be_rewritten_but_not_grown() {
        let value = "v".repeat(MAX_GENERATED_LABEL_VALUE_BYTES);
        let pairs: Vec<(String, String)> =
            (0..5).map(|n| (format!("l{n}"), value.clone())).collect();
        let pairs: Vec<(&str, &str)> = pairs
            .iter()
            .map(|(n, v)| (n.as_str(), v.as_str()))
            .collect();
        let stored = sample(&pairs);
        assert!(label_bytes(&stored) > MAX_GENERATED_LABEL_SET_BYTES);

        let shrunk = label_replace(stored.clone(), &["l0", "short", "l1", ".*"]).unwrap();
        assert_eq!(shrunk[0].labels.get("l0"), Some("short"));
        let same = label_join(stored.clone(), &["l0", "", "l1"]).unwrap();
        assert_eq!(same[0].labels.get("l0"), Some(value.as_str()));

        assert_refused(
            label_replace(stored, &["extra", "x", "l1", ".*"]),
            "would make a series' labels longer than",
        );
    }

    /// The attack the limits exist for: no data, under 1 KiB of query text.
    #[test]
    fn nested_label_join_is_refused_before_it_grows() {
        let mut input = sample(&[("a", "xxxxxxxx")]);
        let started = Instant::now();
        let err = loop {
            match label_join(input.clone(), &["a", "", "a", "a"]) {
                Ok(mut out) => input = out.pop().unwrap(),
                Err(err) => break err,
            }
        };
        assert!(err.to_string().contains("label_join()"), "{err}");
        assert!(input.labels.get("a").unwrap().len() <= MAX_GENERATED_LABEL_VALUE_BYTES);
        assert!(started.elapsed() < std::time::Duration::from_secs(1));
    }

    #[test]
    fn should_reuse_cached_regex_for_same_pattern() {
        REGEX_CACHE.with(|cache| cache.borrow_mut().clear());
        let created = |pattern: &str| {
            with_anchored_regex(pattern, |_| ()).unwrap();
            REGEX_CACHE.with(|c| c.borrow()[&format!("^(?s:{pattern})$")].created)
        };
        let first = created("same");
        let second = created("same");
        assert_eq!(first, second, "a cache hit must not recompile");
        assert_eq!(cached_patterns().len(), 1);
    }
}
