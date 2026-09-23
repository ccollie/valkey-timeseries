use crate::analysis::forecasting::features::{
    FeatureCategory, compute_features_map, parse_feature,
};
use crate::commands::analysis_runner::{
    AnalysisTimeout, parse_timeout, run_analysis_in_background,
};
use crate::commands::parse_timestamp_range;
use crate::common::replies::{reply_with_double, reply_with_map, reply_with_null, reply_with_str};
use crate::series::get_timeseries;
use anofox_forecast::features::Feature;
use valkey_module::{
    AclPermissions, Context, NextArg, ValkeyError, ValkeyResult, ValkeyString, ValkeyValue,
};

acl_categories!(TS_FEATURES, "ts.features", "read timeseries");
/// ```text
/// TS.FEATURES key startTimestamp endTimestamp
///     [CATEGORY <basic|distribution|autocorrelation|trend>,..]
///     [FEATURE feature1,feature2,feature3..]
///     [TIMEOUT milliseconds]
/// ```
///
/// `TS.FEATURES` computes a set of statistical features on a time series.
///
/// Categories:
/// - `basic`: mean, median, variance, variance_sample, minimum, maximum, length
/// - `distribution`: skewness, kurtosis, quantiles (0.25, 0.5, 0.75)
/// - `autocorrelation`: autocorrelation at lags 1, 2, 3
/// - `trend`: linear trend intercept, slope, p-value, r-squared
///
/// Features are specified by name (case-insensitive). Parameterized features
/// use the format `name:value`:
/// - `quantile:<q>` — q is a float between 0.0 and 1.0
/// - `autocorrelation:<lag>` — lag is a positive integer
/// - `partial_autocorrelation:<lag>` or `pacf:<lag>` — lag is a positive integer
///
/// The final feature list is the union of features from CATEGORY and FEATURE,
/// with duplicates removed.
///
/// Returns a map of `{feature_name: value}`.
#[valkey_module_macros::command({
    name: "ts.features",
    flags: [ReadOnly, DenyOOM],
    summary: "Compute statistical features for a time series.",
    complexity: "O(N*F) where N is the number of samples in the range and F is the number of requested features.",
    since: "1.0.0",
    arity: -4,
    key_spec: [{
        flags: [ReadOnly, Access],
        begin_search: Index({ index: 1 }),
        find_keys: Range({ last_key: 0, steps: 1, limit: 0 })
    }]
})]
pub fn ts_features_cmd(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    if args.len() < 4 {
        return Err(ValkeyError::WrongArity);
    }

    let mut args = args.into_iter().skip(1).peekable();

    let key = args.next_arg()?;
    let date_range = parse_timestamp_range(&mut args)?;

    // Parse optional CATEGORY and FEATURE arguments
    let mut categories: Vec<FeatureCategory> = Vec::new();
    let mut features: Vec<Feature> = Vec::new();
    let mut timeout = AnalysisTimeout::default();

    while args.peek().is_some() {
        let arg = args.peek().unwrap();
        let arg_str = arg
            .try_as_str()
            .map_err(|_| ValkeyError::Str("TSDB: invalid argument"))?;

        match arg_str.to_uppercase().as_str() {
            "CATEGORY" => {
                args.next(); // consume CATEGORY
                let cat_str = args.next_str()?;
                categories = parse_categories(cat_str)?;
            }
            "FEATURE" => {
                args.next(); // consume FEATURE
                let feat_str = args.next_str()?;
                features = parse_features(feat_str)?;
            }
            "TIMEOUT" => {
                args.next(); // consume TIMEOUT
                timeout.set(parse_timeout(&mut args)?);
            }
            other => {
                return Err(ValkeyError::String(format!(
                    "TSDB: unrecognized argument '{}'",
                    other
                )));
            }
        }
    }

    // If no categories or features specified, return error
    if categories.is_empty() && features.is_empty() {
        return Err(ValkeyError::Str(
            "TSDB: at least one of CATEGORY or FEATURE must be specified",
        ));
    }

    // Collect features from categories
    for cat in &categories {
        features.extend_from_slice(cat.features());
    }

    // Deduplicate by canonical name (keeping first occurrence)
    let mut seen = std::collections::HashSet::new();
    let unique_features: Vec<Feature> = features
        .into_iter()
        .filter(|f| seen.insert(f.name()))
        .collect();

    if unique_features.is_empty() {
        return Err(ValkeyError::Str("TSDB: no features to compute"));
    }

    // Get the time series and extract sample values
    let series = get_timeseries(ctx, &key, Some(AclPermissions::ACCESS))?;

    let (start, end) = date_range.get_series_range(&series, None, false);
    let samples = series.get_range(start, end);

    if samples.is_empty() {
        return Err(ValkeyError::Str(
            "TSDB: no samples in the specified time range",
        ));
    }

    let values: Vec<f64> = samples.iter().map(|s| s.value).collect();

    run_analysis_in_background(
        ctx,
        timeout,
        move || Ok(compute_features_map(&values, &unique_features)),
        |actx, result_map| {
            let reply_ctx = actx.reply_ctx();
            reply_with_map(&reply_ctx, result_map.len());
            for (name, value) in &result_map {
                reply_with_str(&reply_ctx, name);
                if value.is_nan() {
                    reply_with_null(&reply_ctx);
                } else {
                    reply_with_double(&reply_ctx, *value);
                }
            }
            Ok(ValkeyValue::NoReply)
        },
    )
}

/// Parse a comma-separated list of category names, rejecting duplicates.
fn parse_categories(input: &str) -> Result<Vec<FeatureCategory>, ValkeyError> {
    let mut categories = Vec::new();
    let mut seen = std::collections::HashSet::new();

    for part in input.split(',') {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }
        let category = FeatureCategory::try_from(part)
            .map_err(|e| ValkeyError::String(format!("TSDB: {e}")))?;

        if !seen.insert(category) {
            return Err(ValkeyError::String(format!(
                "TSDB: duplicate category '{}'",
                category.as_str()
            )));
        }

        categories.push(category);
    }

    if categories.is_empty() {
        return Err(ValkeyError::Str("TSDB: empty category list"));
    }

    Ok(categories)
}

/// Parse a comma-separated list of feature names.
fn parse_features(input: &str) -> Result<Vec<Feature>, ValkeyError> {
    let mut features = Vec::new();

    for part in input.split(',') {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }
        let feature = parse_feature(part).map_err(|e| ValkeyError::String(format!("TSDB: {e}")))?;
        features.push(feature);
    }

    if features.is_empty() {
        return Err(ValkeyError::Str("TSDB: empty feature list"));
    }

    Ok(features)
}
