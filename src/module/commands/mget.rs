use crate::arg_types::MatchFilterOptions;
use crate::common::Sample;
use crate::error_consts;
use crate::labels::parse_series_selector;
use crate::module::arg_parse::{
    parse_command_arg_token, parse_label_list, CommandArgIterator, CommandArgToken,
};
use crate::module::commands::range_utils::get_series_labels;
use crate::module::result::sample_to_value;
use crate::series::index::with_matched_series;
use valkey_module::{Context, NextArg, ValkeyError, ValkeyResult, ValkeyString, ValkeyValue};

struct MGetOptions {
    with_labels: bool,
    filter: MatchFilterOptions,
    selected_labels: Vec<String>,
}

/// TS.MGET selector
///   [WITHLABELS]
///   [SELECTED_LABELS label...]
pub fn mget(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    let mut args = args.into_iter().skip(1).peekable();

    let options = parse_mget_options(&mut args)?;

    struct SeriesData {
        series_key: ValkeyString,
        labels: Vec<ValkeyValue>,
        sample: Option<Sample>,
    }

    struct State {
        with_labels: bool,
        selected_labels: Vec<String>,
        series: Vec<SeriesData>,
    }

    let mut state = State {
        with_labels: options.with_labels,
        selected_labels: options.selected_labels,
        series: Vec::new(),
    };

    with_matched_series(ctx, &mut state, &options.filter, |acc, series, key| {
        let sample = series.last_sample;
        let labels = get_series_labels(series, acc.with_labels, &acc.selected_labels);
        let labels: Vec<_> = labels
            .into_iter()
            .map(|label| {
                ValkeyValue::Array(vec![
                    ValkeyValue::from(label.name),
                    ValkeyValue::from(label.value),
                ])
            })
            .collect();
        acc.series.push(SeriesData {
            sample,
            labels,
            series_key: key,
        });
    })?;

    let result = state
        .series
        .into_iter()
        .map(|s| {
            let sample_value = if let Some(sample) = s.sample {
                sample_to_value(sample)
            } else {
                ValkeyValue::Array(vec![])
            };
            let series = vec![
                ValkeyValue::from(s.series_key),
                ValkeyValue::Array(s.labels),
                sample_value,
            ];
            ValkeyValue::Array(series)
        })
        .collect();

    Ok(ValkeyValue::Array(result))
}

fn parse_mget_options(args: &mut CommandArgIterator) -> ValkeyResult<MGetOptions> {
    const CMD_TOKENS: &[CommandArgToken] = &[CommandArgToken::WithLabels];

    let filter = parse_series_selector(args.next_str()?)?;
    let mut options = MGetOptions {
        with_labels: false,
        filter: filter.into(),
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
