use crate::commands::arg_parse::{
    parse_command_arg_token, parse_label_list, CommandArgIterator, CommandArgToken,
};
use crate::commands::arg_types::{MGetOptions, MatchFilterOptions};
use crate::commands::range_utils::get_series_labels;
use crate::common::Sample;
use crate::error_consts;
use crate::labels::{parse_series_selector, Label};
use crate::series::index::with_matched_series;
use valkey_module::{
    AclPermissions, Context, NextArg, ValkeyError, ValkeyResult, ValkeyString, ValkeyValue,
};

/// TS.MGET selector
///   [WITHLABELS]
///   [SELECTED_LABELS label...]
pub fn mget(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    let mut args = args.into_iter().skip(1).peekable();

    let options = parse_mget_options(&mut args)?;

    // NOTE: we currently don't support cross-cluster mget
    let mget_results = handle_mget(ctx, options)?;

    let result = mget_results
        .series
        .into_iter()
        .map(|s| s.into())
        .collect();

    Ok(ValkeyValue::Array(result))
}

pub fn parse_mget_options(args: &mut CommandArgIterator) -> ValkeyResult<MGetOptions> {
    const CMD_TOKENS: &[CommandArgToken] = &[CommandArgToken::WithLabels];

    let filter = parse_series_selector(args.next_str()?)?;
    let mut options = MGetOptions {
        with_labels: false,
        filter,
        selected_labels: Default::default(),
    };

    while let Some(arg) = args.next() {
        let token = parse_command_arg_token(arg.as_slice()).unwrap_or_default();
        match token {
            CommandArgToken::WithLabels => {
                options.with_labels = true;
            }
            CommandArgToken::SelectedLabels => {
                options.selected_labels = parse_label_list(args, CMD_TOKENS)?;
            }
            _ => {}
        }
    }

    if options.filter.is_empty() {
        return Err(ValkeyError::Str(error_consts::MISSING_FILTER));
    }

    Ok(options)
}

pub struct MGetSeriesData {
    series_key: ValkeyString,
    labels: Vec<Option<Label>>,
    sample: Option<Sample>,
}

impl From<MGetSeriesData> for ValkeyValue {
    fn from(series: MGetSeriesData) -> Self {
        let labels: Vec<_> = series.labels
            .into_iter()
            .map(|label| match label {
                Some(label) => label.into(),
                None => ValkeyValue::Null,
            })
            .collect();

        let sample_value: ValkeyValue = if let Some(sample) = series.sample {
            sample.into()
        } else {
            ValkeyValue::Array(vec![])
        };
        let series = vec![
            ValkeyValue::from(series.series_key),
            ValkeyValue::Array(labels),
            sample_value,
        ];
        ValkeyValue::Array(series)
    }
}

pub struct MGetResult {
    with_labels: bool,
    selected_labels: Vec<String>,
    series: Vec<MGetSeriesData>,
}

pub fn handle_mget(ctx: &Context, options: MGetOptions) -> ValkeyResult<MGetResult>{

    let mut state = MGetResult {
        with_labels: options.with_labels,
        selected_labels: options.selected_labels,
        series: Vec::new(),
    };

    // NOTE: we currently don't support cross-cluster mget
    let opts: MatchFilterOptions = options.filter.into();
    with_matched_series(
        ctx,
        &mut state,
        &opts,
        Some(AclPermissions::ACCESS),
        |acc, series, key| {
            let sample = series.last_sample;
            let labels = get_series_labels(series, acc.with_labels, &acc.selected_labels)
                .into_iter()
                .map(|label| label.map(|x| Label::new(x.name, x.value)))
                .collect();

            acc.series.push(MGetSeriesData {
                sample,
                labels,
                series_key: key,
            });
        },
    )?;

    Ok(state)
}