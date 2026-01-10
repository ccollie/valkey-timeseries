pub mod command_parser;
mod fanout;
mod ts_add;
mod ts_addbulk;
mod ts_alter;
mod ts_card;
mod ts_card_fanout_command;
mod ts_create;
mod ts_createrule;
mod ts_debug;
mod ts_debug_configs;
mod ts_del;
mod ts_deleterule;
mod ts_get;
mod ts_incr_decr_by;
mod ts_info;
mod ts_join;
mod ts_labelnames;
mod ts_labelnames_fanout_command;
mod ts_labelstats;
mod ts_labelstats_fanout_command;
mod ts_labelvalues;
mod ts_labelvalues_fanout_command;
mod ts_madd;
mod ts_mdel;
mod ts_mdel_fanout_command;
mod ts_mget;
mod ts_mget_fanout_command;
mod ts_mrange;
mod ts_mrange_fanout_command;
mod ts_queryindex;
mod ts_queryindex_fanout_command;
mod ts_range;
mod utils;

pub use command_parser::*;
pub use ts_createrule::*;
pub use ts_deleterule::*;
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
pub use outliers::*;
pub use query_index::*;
pub use range::*;
pub use ts_debug::*;
pub use ts_del::*;
pub use ts_get::*;
pub use ts_incr_decr_by::*;
pub use ts_info::*;
pub use ts_join::*;
pub use ts_labelnames::*;
pub use ts_labelstats::*;
pub use ts_labelvalues::*;
pub use ts_madd::*;
pub use ts_mdel::*;
pub use ts_mget::*;
pub use ts_mrange::*;
pub use ts_queryindex::*;
pub use ts_range::*;
use valkey_module::ValkeyResult;

use crate::fanout::register_fanout_operation;
use ts_card_fanout_command::CardFanoutCommand;
use ts_labelnames_fanout_command::LabelNamesFanoutCommand;
use ts_labelstats_fanout_command::LabelStatsFanoutCommand;
use ts_labelvalues_fanout_command::LabelValuesFanoutCommand;
use ts_mdel_fanout_command::MDelFanoutCommand;
use ts_mget_fanout_command::MGetFanoutCommand;
use ts_mrange_fanout_command::MRangeFanoutCommand;
use ts_queryindex_fanout_command::QueryIndexFanoutCommand;

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
