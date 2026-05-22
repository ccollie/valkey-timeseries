pub mod command_parser;
mod fanout;
mod label_search_utils;
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
mod ts_label_search_fanout_command;
mod ts_labelnames;
mod ts_labelstats;
mod ts_labelstats_fanout_command;
mod ts_labelvalues;
mod ts_madd;
mod ts_mdel;
mod ts_mdel_fanout_command;
mod ts_metricnames;
mod ts_mget;
mod ts_mget_fanout_command;
mod ts_mrange;
mod ts_mrange_fanout_command;
mod ts_outliers;
mod ts_queryindex;
mod ts_queryindex_fanout_command;
mod ts_range;
mod utils;

pub use command_parser::*;
pub use ts_add::*;
pub use ts_addbulk::*;
pub use ts_alter::*;
pub use ts_card::*;
pub use ts_create::*;
pub use ts_createrule::*;
pub use ts_debug::*;
pub use ts_del::*;
pub use ts_deleterule::*;
pub use ts_get::*;
pub use ts_incr_decr_by::*;
pub use ts_info::*;
pub use ts_join::*;
pub use ts_labelnames::*;
pub use ts_labelstats::*;
pub use ts_labelvalues::*;
pub use ts_madd::*;
pub use ts_mdel::*;
pub use ts_metricnames::*;
pub use ts_mget::*;
pub use ts_mrange::*;
pub use ts_outliers::*;
pub use ts_queryindex::*;
pub use ts_range::*;
use valkey_module::ValkeyResult;

use crate::fanout::register_fanout_operation;
use ts_card_fanout_command::CardFanoutCommand;
use ts_label_search_fanout_command::LabelSearchFanoutCommand;
use ts_labelstats_fanout_command::LabelStatsFanoutCommand;
use ts_mdel_fanout_command::MDelFanoutCommand;
use ts_mget_fanout_command::MGetFanoutCommand;
use ts_mrange_fanout_command::MRangeFanoutCommand;
use ts_queryindex_fanout_command::QueryIndexFanoutCommand;

pub(crate) fn register_fanout_operations() -> ValkeyResult<()> {
    register_fanout_operation::<LabelStatsFanoutCommand>()?;
    register_fanout_operation::<CardFanoutCommand>()?;
    register_fanout_operation::<LabelSearchFanoutCommand>()?;
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
