use crate::common::threads::spawn;
use crate::error_consts;
use crate::series::acl::check_key_permissions;
use crate::series::{IngestedSamples, bulk_insert_samples, get_timeseries_mut};
use valkey_module::{
    AclPermissions, Context, NotifyEvent, ThreadSafeContext, ValkeyError, ValkeyResult,
    ValkeyString, ValkeyValue,
};

/// TS.MADDBULK key payload [key payload ...]
///
pub fn madd_bulk(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    let arg_count = args.len() - 1;

    if arg_count < 2 || !arg_count.is_multiple_of(2) {
        return Err(ValkeyError::WrongArity);
    }

    let data = parse_args(ctx, args)?;
    handle_update(ctx, data)
}

fn handle_update(ctx: &Context, args: Vec<IngestedSamples>) -> ValkeyResult {
    let blocked_client = ctx.block_client();
    let thread_ctx = ThreadSafeContext::with_blocked_client(blocked_client);

    spawn(move || {
        let mut res: Vec<(usize, usize)> = Vec::new();
        for ingested in args {
            let ctx = thread_ctx.lock();
            let one_result = process_one_series(&ctx, ingested);
            drop(ctx);

            match one_result {
                Ok(counts) => {
                    res.push(counts);
                }
                Err(e) => {
                    thread_ctx.reply(Err(e));
                    return;
                }
            }
        }
        // construct reply
        let result: Vec<ValkeyValue> = res
            .into_iter()
            .map(|(success, total)| {
                ValkeyValue::Array(vec![
                    ValkeyValue::Integer(success as i64),
                    ValkeyValue::Integer(total as i64),
                ])
            })
            .collect::<Vec<_>>();

        thread_ctx.reply(Ok(ValkeyValue::Array(result)));

        // Replicate after successful processing
        let ctx = thread_ctx.lock();
        ctx.replicate_verbatim();
    });

    // We will reply later, from the thread
    Ok(ValkeyValue::NoReply)
}

fn process_one_series(ctx: &Context, ingested: IngestedSamples) -> ValkeyResult<(usize, usize)> {
    let key = ctx.create_string(ingested.key.as_bytes());
    let mut series = get_timeseries_mut(ctx, &key, true, Some(AclPermissions::UPDATE))?
        .expect(error_consts::KEY_NOT_FOUND);
    let policy = series.sample_duplicates.policy;
    let items = bulk_insert_samples(ctx, &mut series, &ingested.samples, policy);
    let success_count = items.iter().filter(|res| res.is_ok()).count();
    Ok((success_count, items.len()))
}

fn parse_args(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult<Vec<IngestedSamples>> {
    let mut results: Vec<IngestedSamples> = Vec::with_capacity(args.len() / 2);
    let mut index: usize = 1; // start after command name

    while index < args.len() {
        let key = &args[index];
        check_key_permissions(ctx, key, &AclPermissions::UPDATE)?;
        let mut buf = {
            let data = args[index + 1].as_slice();
            data.to_vec()
        };
        let mut sample_data = IngestedSamples::from_json_lines(&mut buf)?;
        sample_data.key = key.to_string_lossy();
        results.push(sample_data);
        index += 2;
    }

    Ok(results)
}
