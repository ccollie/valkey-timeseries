use crate::commands::arg_parse::{find_last_token_instance, parse_label_list, CommandArgToken};
use crate::commands::parse_series_selector_list;
use crate::error_consts;
use crate::fanout::cluster::is_clustered;
use crate::fanout::{perform_remote_mget_request, MultiGetResponse};
use crate::labels::Label;
use crate::series::get_latest_compaction_sample;
use crate::series::index::with_matched_series;
use crate::series::range_utils::get_series_labels;
use crate::series::request_types::{MGetRequest, MGetSeriesData, MatchFilterOptions};
use valkey_module::{
    AclPermissions, BlockedClient, Context, ThreadSafeContext, ValkeyError, ValkeyResult,
    ValkeyString, ValkeyValue,
};

/// TS.MGET
///   [WITHLABELS | SELECTED_LABELS label...]
///   [FILTER filterExpr...]
///   [LATEST]
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
    // Look for all variadic tokens in the command, processing from right to left
    // until no more tokens are found
    let supported_tokens = &[
        CommandArgToken::SelectedLabels,
        CommandArgToken::Filter,
        CommandArgToken::Latest,
        CommandArgToken::WithLabels,
    ];

    let mut options = MGetRequest::default();
    let mut args = args;

    while let Some((token, token_position)) = find_last_token_instance(&args, supported_tokens) {
        // Split off the token and its arguments from the main args
        let token_with_args = args.split_off(token_position);

        let mut arg_iterator = token_with_args.into_iter().skip(1).peekable();

        match token {
            CommandArgToken::SelectedLabels => {
                options.selected_labels = parse_label_list(&mut arg_iterator, &[])?;
            }
            CommandArgToken::Filter => {
                options.filters = parse_series_selector_list(&mut arg_iterator, &[])?;
            }
            CommandArgToken::WithLabels => {
                options.with_labels = true;
            }
            CommandArgToken::Latest => {
                options.latest = true;
            }
            _ => unreachable!("Token should be one of the supported tokens"),
        }
    }

    if options.filters.is_empty() {
        return Err(ValkeyError::Str(error_consts::MISSING_FILTER));
    }

    if !options.selected_labels.is_empty() && options.with_labels {
        return Err(ValkeyError::Str(
            error_consts::WITH_LABELS_AND_SELECTED_LABELS_SPECIFIED,
        ));
    }

    if args.len() > 1 {
        let arg_debug = args[1..]
            .iter()
            .map(|x| x.to_string())
            .collect::<Vec<_>>()
            .join(", ");
        return Err(ValkeyError::String(arg_debug));
        // return Err(ValkeyError::Str(error_consts::INVALID_ARGUMENT));
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
                get_latest_compaction_sample(ctx, series)
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
