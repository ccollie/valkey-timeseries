mod add;
mod add_bulk;
mod alter_series;
mod card;
mod card_fanout_command;
pub mod command_args;
mod create_rule;
mod create_series;
mod del;
mod delete_rule;
mod fanout;
mod get;
mod incr_decr_by;
mod info;
mod join;
mod label_names;
mod label_names_fanout_command;
mod label_stats;
mod label_stats_fanout_command;
mod label_values;
mod label_values_fanout_command;
mod madd;
mod mdel;
mod mdel_fanout_command;
mod mget;
mod mget_fanout_command;
mod mrange;
mod mrange_fanout_command;
mod query_index;
mod query_index_fanout_command;
mod range;
mod utils;

pub use command_args::*;
pub use create_rule::*;
pub use delete_rule::*;

pub use add::*;
pub use add_bulk::*;
pub use alter_series::*;
pub use card::*;
pub use create_series::*;
pub use del::*;
pub use get::*;
pub use incr_decr_by::*;
pub use info::*;
pub use join::*;
pub use label_names::*;
pub use label_stats::*;
pub use label_values::*;
pub use madd::*;
pub use mdel::*;
pub use mget::*;
pub use mrange::*;
pub use query_index::*;
pub use range::*;
use valkey_module::ValkeyResult;

use crate::fanout::register_fanout_operation;
use card_fanout_command::CardFanoutCommand;
use label_names_fanout_command::LabelNamesFanoutCommand;
use label_stats_fanout_command::LabelStatsFanoutCommand;
use label_values_fanout_command::LabelValuesFanoutCommand;
use mdel_fanout_command::MDelFanoutCommand;
use mget_fanout_command::MGetFanoutCommand;
use mrange_fanout_command::MRangeFanoutCommand;
use query_index_fanout_command::QueryIndexFanoutCommand;

pub(crate) fn register_fanout_operations() -> ValkeyResult<()> {
    register_fanout_operation::<LabelStatsFanoutCommand>()?;
    register_fanout_operation::<CardFanoutCommand>()?;
    register_fanout_operation::<LabelNamesFanoutCommand>()?;
    register_fanout_operation::<LabelValuesFanoutCommand>()?;
    register_fanout_operation::<MDelFanoutCommand>()?;
    register_fanout_operation::<MGetFanoutCommand>()?;
    register_fanout_operation::<MRangeFanoutCommand>()?;
    register_fanout_operation::<QueryIndexFanoutCommand>()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::commands::register_fanout_operations;

    #[test]
    fn test_register_fanout_operations() {
        let result = register_fanout_operations();
        assert!(result.is_ok());
    }
}
