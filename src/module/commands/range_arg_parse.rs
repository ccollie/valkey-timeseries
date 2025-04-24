use crate::arg_types::RangeOptions;
use crate::labels::parse_series_selector;
use crate::module::arg_parse::{
    parse_aggregation_options, parse_command_arg_token, parse_count_arg, parse_grouping_params,
    parse_label_list, parse_timestamp_filter, parse_timestamp_range, parse_value_filter,
    CommandArgIterator, CommandArgToken,
};
use valkey_module::{NextArg, ValkeyResult};

pub fn parse_range_options(args: &mut CommandArgIterator) -> ValkeyResult<RangeOptions> {
    const RANGE_OPTION_ARGS: [CommandArgToken; 10] = [
        CommandArgToken::Aggregation,
        CommandArgToken::Count,
        CommandArgToken::BucketTimestamp,
        CommandArgToken::Filter,
        CommandArgToken::FilterByTs,
        CommandArgToken::FilterByValue,
        CommandArgToken::GroupBy,
        CommandArgToken::Reduce,
        CommandArgToken::SelectedLabels,
        CommandArgToken::WithLabels,
    ];

    let date_range = parse_timestamp_range(args)?;

    let mut options = RangeOptions {
        date_range,
        ..Default::default()
    };

    while let Some(arg) = args.next() {
        let token = parse_command_arg_token(arg.as_slice()).unwrap_or_default();
        match token {
            CommandArgToken::Aggregation => {
                options.aggregation = Some(parse_aggregation_options(args)?);
            }
            CommandArgToken::Count => {
                options.count = Some(parse_count_arg(args)?);
            }
            CommandArgToken::Filter => {
                let filter = args.next_str()?;
                options.series_selector = parse_series_selector(filter)?;
            }
            CommandArgToken::FilterByValue => {
                options.value_filter = Some(parse_value_filter(args)?);
            }
            CommandArgToken::FilterByTs => {
                options.timestamp_filter = Some(parse_timestamp_filter(args, &RANGE_OPTION_ARGS)?);
            }
            CommandArgToken::GroupBy => {
                options.grouping = Some(parse_grouping_params(args)?);
            }
            CommandArgToken::SelectedLabels => {
                options.selected_labels = parse_label_list(args, &RANGE_OPTION_ARGS)?;
            }
            CommandArgToken::WithLabels => {
                options.with_labels = true;
            }
            _ => {}
        }
    }

    // if options.series_selector.is_empty() {
    //     return Err(ValkeyError::Str("TSDB: no FILTER given"));
    // }

    Ok(options)
}
