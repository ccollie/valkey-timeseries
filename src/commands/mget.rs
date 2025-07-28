use crate::commands::arg_parse::CommandArgToken;
use crate::commands::{parse_command_arg_token, parse_label_list, parse_series_selector_list};
use crate::error_consts;
use crate::fanout::cluster::is_clustered;
use crate::fanout::{perform_remote_mget_request, MultiGetResponse};
use crate::labels::Label;
use crate::series::get_latest_compaction_sample;
use crate::series::index::with_matched_series;
use crate::series::range_utils::get_series_labels;
use crate::series::request_types::{MGetRequest, MGetSeriesData, MatchFilterOptions};
use blart::AsBytes;
use valkey_module::{
    AclPermissions, BlockedClient, Context, NextArg, ThreadSafeContext, ValkeyError, ValkeyResult,
    ValkeyString, ValkeyValue,
};

/// TS.MGET
///   [LATEST]
///   [WITHLABELS | SELECTED_LABELS label...]
///   [FILTER filterExpr...]
pub fn mget(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    if args.len() < 2 {
        return Err(ValkeyError::WrongArity);
    }

    let options = parse_mget_options(args)?;

    if is_clustered(ctx) {
        perform_remote_mget_request(ctx, options, on_mget_request_done)?;
        return Ok(ValkeyValue::NoReply);
    }

    let mget_results = process_mget_request(ctx, options)?;

    let result = mget_results.into_iter().map(|s| s.into()).collect();

    Ok(ValkeyValue::Array(result))
}

/// Parsing commands with variadic args gets wonky. For example, if we have something like:
///
/// `TS.MGET SELECTED_LABELS label1 label2 FILTER a=b x=y`
///
/// It's not clear if FILTER a=b and x=y are to be parsed as part of SELECTED_LABELS.
/// To avoid this, we need to check backwards for variadic command arguments and remove them
/// before handling the rest of the arguments.
pub fn parse_mget_options(args: Vec<ValkeyString>) -> ValkeyResult<MGetRequest> {
    let supported_tokens = &[
        CommandArgToken::SelectedLabels,
        CommandArgToken::Filter,
        CommandArgToken::Latest,
        CommandArgToken::WithLabels,
    ];

    let mut options = MGetRequest::default();

    let Some(filter_index) = args
        .iter()
        .rposition(|arg| arg.as_bytes().eq_ignore_ascii_case(b"FILTER"))
    else {
        return Err(ValkeyError::WrongArity);
    };

    let mut args = args;
    let filter_args = args.split_off(filter_index);

    if filter_args.len() < 2 {
        return Err(ValkeyError::WrongArity);
    }

    let mut args = args.into_iter().skip(1).peekable(); // Skip the command name

    while let Some(arg) = args.next() {
        let token = parse_command_arg_token(arg.as_bytes())
            .ok_or(ValkeyError::Str(error_consts::INVALID_ARGUMENT))?;

        match token {
            CommandArgToken::SelectedLabels => {
                options.selected_labels = parse_label_list(&mut args, supported_tokens)?;
                if options.selected_labels.is_empty() {
                    return Err(ValkeyError::Str(
                        "TSDB: SELECT_LABELS should have at least 1 parameter",
                    ));
                }
            }
            CommandArgToken::WithLabels => {
                options.with_labels = true;
            }
            CommandArgToken::Latest => {
                options.latest = true;
            }
            CommandArgToken::Filter => {
                return Err(ValkeyError::Str("TSDB: FILTER must be the last argument"));
            }
            _ => return Err(ValkeyError::Str(error_consts::INVALID_ARGUMENT)),
        }
    }

    if options.with_labels && !options.selected_labels.is_empty() {
        return Err(ValkeyError::Str(
            error_consts::WITH_LABELS_AND_SELECTED_LABELS_SPECIFIED,
        ));
    }

    let mut args = filter_args.into_iter().skip(1).peekable();
    options.filters = parse_series_selector_list(&mut args, &[])?;

    args.done()?;

    if options.filters.is_empty() {
        return Err(ValkeyError::Str(error_consts::MISSING_FILTER));
    }

    if !options.selected_labels.is_empty() && options.with_labels {
        return Err(ValkeyError::Str(
            error_consts::WITH_LABELS_AND_SELECTED_LABELS_SPECIFIED,
        ));
    }

    Ok(options)
}

pub fn process_mget_request(
    ctx: &Context,
    options: MGetRequest,
) -> ValkeyResult<Vec<MGetSeriesData>> {
    let with_labels = options.with_labels;
    let selected_labels = &options.selected_labels;
    let mut series = vec![];

    let opts: MatchFilterOptions = options.filters.into();
    with_matched_series(
        ctx,
        &mut series,
        &opts,
        Some(AclPermissions::ACCESS),
        move |acc, series, key| {
            let sample = if options.latest {
                if let Some(value) = get_latest_compaction_sample(ctx, series) {
                    Some(value)
                } else {
                    series.last_sample
                }
            } else {
                series.last_sample
            };
            let labels = get_series_labels(series, with_labels, selected_labels)
                .into_iter()
                .map(|label| label.map(|x| Label::new(x.name, x.value)))
                .collect();

            acc.push(MGetSeriesData {
                sample,
                labels,
                series_key: key,
            });
        },
    )?;

    Ok(series)
}

fn on_mget_request_done(
    ctx: &ThreadSafeContext<BlockedClient>,
    _req: MGetRequest,
    res: Vec<MultiGetResponse>,
) {
    let count = res.iter().map(|s| s.series.len()).sum::<usize>();
    let mut arr = Vec::with_capacity(count);

    for s in res.into_iter() {
        for series in s.series.into_iter().filter(|s| s.value.is_some()) {
            arr.push(series.into());
        }
    }

    ctx.reply(Ok(ValkeyValue::Array(arr)));
}
