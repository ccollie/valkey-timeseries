use valkey_module::{Context, ValkeyResult, ValkeyString, VALKEY_OK};
use valkey_module_macros::command;

/// Stub function for easier testing of arbitrary code
#[command(
    {
        name: "TEST",
        flags: [ReadOnly],
        arity: 0,
        key_spec: [
            {
                notes: "test command that define all the arguments at even position as keys",
                flags: [ReadOnly, Access],
                begin_search: Keyword({ keyword : "foo", startfrom : 1 }),
                find_keys: Range({ last_key :- 1, steps : 0, limit : 0 }),
            }
        ]
    }
)]
pub fn test(_ctx: &Context, _args: Vec<ValkeyString>) -> ValkeyResult {
    VALKEY_OK
}
