use crate::commands::arg_parse::{
    parse_command_arg_token, parse_label_list, CommandArgIterator, CommandArgToken,
};
use crate::commands::range_utils::get_series_labels;
use crate::error_consts;
use crate::fanout::cluster::is_clustered;
use crate::fanout::{perform_remote_mget_request, MultiGetResponse};
use crate::labels::{parse_series_selector, Label};
use crate::series::index::with_matched_series;
use crate::series::request_types::{MGetRequest, MGetSeriesData, MatchFilterOptions};
use valkey_module::{
    AclPermissions, BlockedClient, Context, NextArg, ThreadSafeContext, ValkeyError, ValkeyResult,
    ValkeyString, ValkeyValue,
};

/// TS.MGET selector
///   [WITHLABELS]
///   [SELECTED_LABELS label...]
pub fn mget(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    let mut args = args.into_iter().skip(1).peekable();

    let options = parse_mget_options(&mut args)?;

    if is_clustered(ctx) {
        perform_remote_mget_request(ctx, options, on_mget_request_done)?;
        return Ok(ValkeyValue::NoReply);
    }

    let mget_results = process_mget_request(ctx, options)?;

    let result = mget_results.into_iter().map(|s| s.into()).collect();

    Ok(ValkeyValue::Array(result))
}

pub fn parse_mget_options(args: &mut CommandArgIterator) -> ValkeyResult<MGetRequest> {
    const CMD_TOKENS: &[CommandArgToken] = &[CommandArgToken::WithLabels];

    let filter = parse_series_selector(args.next_str()?)?;
    let mut options = MGetRequest {
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

pub fn process_mget_request(
    ctx: &Context,
    options: MGetRequest,
) -> ValkeyResult<Vec<MGetSeriesData>> {
    let with_labels = options.with_labels;
    let selected_labels = &options.selected_labels;
    let mut series = vec![];

    let opts: MatchFilterOptions = options.filter.into();
    with_matched_series(
        ctx,
        &mut series,
        &opts,
        Some(AclPermissions::ACCESS),
        move |acc, series, key| {
            let sample = series.last_sample;
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
