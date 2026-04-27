use super::ts_label_search_fanout_command::LabelSearchFanoutCommand;
use crate::commands::fanout::LabelSearchType;
use crate::commands::label_search_utils::{parse_label_name_search_args, process_label_search_request};
use crate::fanout::{is_clustered, FanoutClientCommand};
use crate::series::index::SEARCH_RESULT_DEFAULT_LIMIT;
use valkey_module::ValkeyError::WrongArity;
use valkey_module::{Context, ValkeyError, ValkeyResult, ValkeyString, ValkeyValue};


/// TS.LABELNAMESEARCH
/// [SEARCH term [term...]]
/// [FUZZ_THRESHOLD 0.0..1.0]
/// [FUZZ_ALG jarowinkler|subsequence]
/// [IGNORE_CASE true|false]
/// [INCLUDE_SCORE true|false]
/// [SORT_BY alpha|score]
/// [SORT_DIR asc|dsc]
/// [FILTER_BY_RANGE [NOT] fromTimestamp toTimestamp]
/// [LIMIT limit]
/// [FILTER seriesMatcher...]
pub fn ts_labelnamesearch_cmd(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    if args.len() < 2 {
        return Err(WrongArity);
    }

    let mut parsed = parse_label_name_search_args(args, LabelSearchType::Name)?;

    // Search APIs default to a bounded response to keep metadata discovery interactive.
    if parsed.metadata.limit.is_none() {
        parsed.metadata.limit = Some(SEARCH_RESULT_DEFAULT_LIMIT);
    }

    if is_clustered(ctx) {
        if parsed.metadata.matchers.is_empty() {
            return Err(ValkeyError::Str(
                "TS.LABELNAMESEARCH in cluster mode requires at least one matcher",
            ));
        }
        let operation = LabelSearchFanoutCommand::new(parsed)?;
        return operation.exec(ctx);
    }

    let results = process_label_search_request(ctx, &parsed)?;
    let reply = if parsed.include_score {
        results
            .into_iter()
            .map(|r| {
                ValkeyValue::Array(vec![
                    ValkeyValue::BulkString(r.value),
                    ValkeyValue::BulkString(r.score.to_string()),
                ])
            })
            .collect::<Vec<_>>()
    } else {
        results
            .into_iter()
            .map(|r| ValkeyValue::BulkString(r.value))
            .collect::<Vec<_>>()
    };

    Ok(ValkeyValue::Array(reply))
}