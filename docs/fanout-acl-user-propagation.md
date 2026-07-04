# Implementation Plan: Propagating User Credentials Through Fanout Operations

**Status:** Draft / proposed
**Scope:** Cluster-mode fanout RPC (`src/fanout/`), ACL helpers (`src/series/acl.rs`, `src/common/context.rs`)
**Affected commands:** `TS.MRANGE`, `TS.MREVRANGE`, `TS.MGET`, `TS.MDEL`, `TS.CARD`, `TS.QUERYINDEX`, `TS.LABELNAMES`, `TS.LABELVALUES`, `TS.LABELSTATS`, `TS.METRICNAMES` — every command registered through `register_fanout_operation` / `FanoutClientCommand`.

---

## 1. Problem Statement

Time series commands enforce ACLs per key (`AclPermissions::ACCESS` / `UPDATE` / `DELETE`) and
per "all keys" for metadata queries. In **standalone mode** this works: the command executes on
the client's context, `Context::get_current_user()` returns the calling user, and the helpers in
`src/series/acl.rs` filter or reject accordingly.

In **cluster mode** the checks are silently bypassed on every node except (sometimes) the
requester:

1. **Remote shard execution.** A fanout request arrives via the cluster bus in
   `on_request_received` (`src/fanout/cluster_rpc.rs`). The handler is executed through
   `spawn_with_context`, which locks the global detached context `MODULE_CONTEXT`
   (`src/common/threads/mod.rs`). A detached context has **no client and no user**:
   `ctx.get_client_id()` returns `0`.

2. **The ACL gate fails open.** Every ACL helper begins with:

   ```rust
   // src/series/acl.rs
   if !is_real_user_client(ctx) {
       return true; // or Ok(())
   }
   ```

   and `is_real_user_client` (`src/common/context.rs:30`) returns `false` when
   `client_id == 0`. The intent was to skip checks for AOF-loading / replication / internal
   contexts — but the detached context used by fanout handlers matches the same condition.
   **Result: a user restricted to `~ts:foo*` can read, count, and delete any series that lives
   on another shard.**

3. **The local node has the same hole.** When a fanout targets more than one node,
   the *local* portion is also offloaded to a pool thread running on `MODULE_CONTEXT`
   (`spawn_local_request`, `src/fanout/fanout_command.rs:310`). So even the requester node
   skips ACL checks whenever the cluster has ≥ 2 target nodes. Only the single-target
   shortcut (`fanout_command.rs:123`) runs on the real client context today.

The fix has two independent halves, both required:

- **(A) Identity propagation** — carry the requesting username inside the fanout message and
  attach it to the execution context on the target node (and on local offload threads).
- **(B) Enforcement gating** — teach the ACL helpers that a context carrying a propagated
  fanout user must be enforced, even though it has no real client attached.

Doing (A) without (B) changes nothing (checks still short-circuit on `is_real_user_client`).
Doing (B) without (A) would break internal operations or enforce the wrong user.

---

## 2. Existing Building Blocks

No new FFI is required. `valkey-module` 0.1.11 already exposes everything needed:

| API | What it does |
|---|---|
| `Context::get_current_user() -> ValkeyString` | `RedisModule_GetCurrentUserName` — the username attached to the context (client user, or context user if one was set). |
| `Context::authenticate_user(&ValkeyString) -> Result<ContextUserScope, ValkeyError>` | `RedisModule_GetModuleUserFromUserName` + `RedisModule_SetContextUser`. Fails if the user does not exist or is disabled. Returns an RAII guard. |
| `ContextUserScope` (Drop) | Calls `SetContextUser(NULL)` and frees the module user. Lifetime-bound to the `Context`, so it cannot outlive the GIL lock scope — this is what makes using the *shared* `MODULE_CONTEXT` safe. |
| `Context::acl_check_key_permission(user, key, perms)` | `RedisModule_ACLCheckKeyPermissions` for an explicit user name. |

Key property: after `authenticate_user`, `ctx.get_current_user()` returns the attached user, so
the existing helpers in `acl.rs` (which resolve the user via `get_current_user()`) work
**unchanged** once the enforcement gate (part B) is fixed. No per-command changes are needed —
`process_mget_request`, the index `querier`, `multi_del`, etc. all pick up the correct user
automatically.

Minimum supported Valkey is 8.2 (8.0 support was removed), and `RM_SetContextUser` /
`RM_GetModuleUserFromUserName` are available there.

---

## 3. Design Overview

```
 Requester node                                Target shard
 ─────────────────                             ─────────────────
 TS.MRANGE (client ctx, user = "alice")
   │
   ├─ capture: get_fanout_user(ctx)
   │      = Some("alice") iff is_real_user_client(ctx)
   │
   ├─ FanoutMessageHeader v2 { db, handler,     on_request_received
   │      user: "alice", ... }  ──cluster bus──►  parse header (user = "alice")
   │                                              │
   ├─ local offload (pool thread):                spawn_with_context(MODULE_CONTEXT)
   │    authenticate_user("alice")                 │
   │    + FanoutAclScope guard                     ├─ scope = ctx.authenticate_user("alice")?
   │    OP::get_local_response(ctx, req)           │    (fail ⇒ error response, fail-closed)
   │                                               ├─ _guard = FanoutAclScope::enter()
   │                                               ├─ handler(ctx, req, &mut out)
   │                                               │    └─ acl.rs helpers now ENFORCE:
   │                                               │       is_acl_enforced(ctx) == true
   │                                               └─ drop(guard, scope)  // before GIL unlock
   ◄──── response (already ACL-filtered) ────────┘
   aggregate + reply on blocked client (original client ctx)
```

Policy decisions baked into the design (rationale in §7):

- **Empty/absent user ⇒ no enforcement.** Requests originating from internal contexts
  (no real user client) carry no user and behave exactly as today. This preserves internal
  operations and replication-driven paths.
- **Unknown/disabled user on the shard ⇒ fail closed.** The shard replies with a permission
  error for that request; the fanout surfaces a per-shard error. ACL definitions are per-node
  and must be provisioned consistently across the cluster by the operator (same expectation
  Valkey itself has); we do not attempt to ship ACL rules in the message.
- **The `default` user is propagated like any other** — real clients authenticated as
  `default` are ACL-checked today in standalone mode, so shards must check `default` too.

---

## 4. Wire Format Change (`src/fanout/fanout_message.rs`)

Bump `FANOUT_MESSAGE_VERSION` from `1` to `2` and add a length-prefixed `user` field after
`handler`:

| Field | Encoding | v1 | v2 |
|---|---|---|---|
| marker | `u32` LE = `0xBADCAB` | ✓ | ✓ |
| version | `u16` LE | `1` | `2` |
| request_id | uvarint | ✓ | ✓ |
| db | signed varint | ✓ | ✓ |
| handler | len-prefixed bytes | ✓ | ✓ |
| **user** | **len-prefixed bytes** | — | **✓ (empty = no user)** |
| reserved | `u16` LE | ✓ | ✓ |
| payload | opaque | ✓ | ✓ |

Struct changes:

```rust
pub(super) struct FanoutMessageHeader {
    pub version: u16,
    pub request_id: u64,
    pub db: i32,
    pub handler: String,
    /// ACL username of the requesting client. `None`/empty means the request
    /// did not originate from a real user client and no enforcement applies.
    pub user: Option<String>,          // NEW
    pub reserved: u16,
}
```

`FanoutMessage` gains the same field. `serialize_request_message` gains a
`user: Option<&str>` parameter (serialized as an empty string when `None`).

Deserialization rules:

- `version == 2`: read `user` between `handler` and `reserved`; map empty string to `None`.
- `version == 1`: do **not** read a user field; `user = None`. (Legacy peer during a rolling
  upgrade — executes without enforcement, i.e., today's behavior.)
- `version > FANOUT_MESSAGE_VERSION`: reject with a serialization error. This check does not
  exist today and should be added so future bumps degrade cleanly.
- Defense-in-depth: cap the accepted user length (e.g. 1 KiB) even though `try_read_string`
  is already bounded by the buffer.

**Response and error messages** reuse the same header (`send_message_internal`). The user
field is always written empty on responses — identity only needs to travel
requester → shard. Cost: 1 extra byte per response.

### Rolling-upgrade behavior (mixed-version cluster)

- **v1 requester → v2 shard:** header parses as v1, `user = None`, shard executes without
  enforcement — identical to current behavior. Rolling upgrades keep working.
- **v2 requester → v1 shard:** the v1 node consumes the first two bytes of the user field as
  `reserved` and treats the rest as payload; request deserialization fails on the shard and an
  error response is returned. The command completes with a per-shard error rather than hanging
  or crashing. This is acceptable for the upgrade window and matches the general expectation
  that fanout RPC peers run the same module version; release notes must call it out.

There is deliberately **no** attempt at version negotiation in this change; if it becomes a
recurring need, a capabilities exchange can be layered on later using the `reserved` field.

---

## 5. Enforcement Gate (`src/common/context.rs`, `src/series/acl.rs`)

Attaching a context user does not flip `is_real_user_client` (the detached context still has
`client_id == 0`), and there is no module API to ask "was `SetContextUser` called?". We cannot
key off `get_current_user() == "default"` either, because a real client legitimately running as
a (possibly restricted) `default` user is indistinguishable from a bare detached context.

Add an explicit, RAII-scoped thread-local marker:

```rust
// src/common/context.rs
use std::cell::Cell;

thread_local! {
    static FANOUT_ACL_ACTIVE: Cell<bool> = const { Cell::new(false) };
}

/// RAII marker: while alive, ACL helpers must enforce permissions on this
/// thread even though the context has no real client attached.
pub struct FanoutAclScope(());

impl FanoutAclScope {
    pub fn enter() -> Self {
        FANOUT_ACL_ACTIVE.with(|f| f.set(true));
        FanoutAclScope(())
    }
}

impl Drop for FanoutAclScope {
    fn drop(&mut self) {
        FANOUT_ACL_ACTIVE.with(|f| f.set(false));
    }
}

pub fn fanout_acl_scope_active() -> bool {
    FANOUT_ACL_ACTIVE.with(|f| f.get())
}

/// Replaces `is_real_user_client` as the gate for ACL enforcement decisions.
pub fn is_acl_enforced(ctx: &Context) -> bool {
    is_real_user_client(ctx) || fanout_acl_scope_active()
}
```

Then switch the gate at every enforcement site — `is_real_user_client` remains for its other
semantic uses, but ACL decisions must use `is_acl_enforced`:

| Site | Change |
|---|---|
| `src/series/acl.rs:21` (`has_key_permissions`) | `is_real_user_client` → `is_acl_enforced` |
| `src/series/acl.rs:34` (`has_all_keys_permissions`) | same |
| `src/series/acl.rs:54` (`check_key_permissions`) | same |
| `src/series/multi_del.rs:144` (`fetch_series_batch`) | same |
| `src/series/index/timeseries_index.rs:247` | same |

Audit for any other `is_real_user_client` call sites added in the interim before merging.

**Why a thread-local is sound here:** `Context` is not `Send`, so every ACL helper invocation
happens on the thread that owns the context — which is exactly the thread where the guard was
entered (the pool thread holding the `MODULE_CONTEXT` GIL). The guard is created after the
user scope and dropped before it, and both drop before the GIL is released.
**Caveat to document in code:** if a handler ever migrates ctx-dependent ACL checks onto other
rayon threads (e.g. via `spawn_scoped`/`join` with a `Sync` detached context), the marker will
not follow. Add a comment on `FanoutAclScope` stating this invariant.

---

## 6. Code Changes by File

### 6.1 `src/fanout/cluster_rpc.rs` — requester capture + shard attach

**Send path** (`send_cluster_request`): capture the user exactly once, on the client's thread,
before serialization:

```rust
/// Username to propagate with a fanout request: Some(name) only when the
/// request originates from a real user client (mirrors standalone ACL gating).
fn get_fanout_user(ctx: &Context) -> Option<String> {
    if is_real_user_client(ctx) {
        Some(ctx.get_current_user().to_string())
    } else {
        None
    }
}
```

Thread `user: Option<&str>` through `serialize_request_message`. `invoke_rpc` needs no
signature change — capture happens inside `send_cluster_request`, which already has the real
client context (fanout commands are rejected in MULTI/Lua by `validate_cluster_exec`, and the
command handler runs on the client context before the client is blocked).

**Receive path** (`on_request_received` → `process_request_message`): the header now carries
`user`. Attach it around the handler invocation, on the pool thread that holds the GIL:

```rust
fn process_request_message(
    ctx: &Context,
    header: FanoutMessageHeader,
    handler: RequestHandlerCallback,
    request_buf: &[u8],
    sender_id: NodeId,
) {
    let request_id = header.request_id;
    let db = header.db;
    let _ = set_current_db(ctx, db);

    // Impersonate the requesting user for the duration of the handler.
    // Order matters: user_scope and acl_guard must drop before this function
    // returns (i.e., before the MODULE_CONTEXT GIL is released).
    let (_user_scope, _acl_guard) = match &header.user {
        Some(name) if !name.is_empty() => {
            let user = ctx.create_string(name.as_str());
            match ctx.authenticate_user(&user) {
                Ok(scope) => (Some(scope), Some(FanoutAclScope::enter())),
                Err(_) => {
                    // Fail closed: unknown/disabled user on this node.
                    let err = FanoutError::permission(format!(
                        "ACL user '{name}' does not exist or is disabled on node {}",
                        *CURRENT_NODE_ID
                    ));
                    send_error_response(ctx, request_id, db, sender_id.raw_ptr(), err);
                    return;
                }
            }
        }
        _ => (None, None),
    };

    let mut dest = Vec::with_capacity(1024);
    let res = handler(ctx, request_buf, &mut dest);
    // ... existing response/error send logic unchanged ...
}
```

Notes:

- `on_request_received` already constructs the `FanoutMessageHeader` that is moved into the
  spawned closure — add `user: message.user.take()` there.
- Add a `FanoutError` constructor/kind for permission failures if one does not exist
  (`fanout_error.rs`), so the requester can distinguish NOPERM-style failures from transport
  errors and reply with a proper `NOPERM`/permission message to the client.
- `send_response_message` / `send_error_response` pass `user: None`.

### 6.2 `src/fanout/fanout_command.rs` — local offload path

`exec_command` runs on the real client context. Capture the user there and hand it to the
spawned local job:

```rust
pub fn exec_command<OP: FanoutCommand, F>(ctx: &Context, command: OP, ...) -> FanoutResult {
    ...
    let local_node = targets.iter().find(|x| x.is_local());
    let fanout_user = get_fanout_user(ctx);   // NEW — capture before any spawn

    if let Some(local) = local_node {
        if outstanding > 1 {
            ...
            spawn_local_request(local_state, req_local, *local, fanout_user);
        } else {
            // Synchronous path: executes on the real client context.
            // ACL checks already work here; no user attachment needed.
            state.handle_local_request(ctx, req, local);
            return Ok(());
        }
    }
    ...
}

fn spawn_local_request<OP, F>(..., user: Option<String>) {
    spawn(move || {
        let result = {
            let ctx = MODULE_CONTEXT.lock();
            match attach_fanout_user(&ctx, user.as_deref()) {
                Ok((_scope, _guard)) => OP::get_local_response(&ctx, req),
                Err(e) => Err(e),   // fail closed, surfaces as per-shard error
            }
            // _scope/_guard drop here, before the GIL guard
        };
        ...
    });
}
```

Factor the attach-or-fail-closed logic into one helper (used by both 6.1 and 6.2), e.g.
`attach_fanout_user(ctx, user: Option<&str>) -> ValkeyResult<(Option<ContextUserScope>, Option<FanoutAclScope>)>`
in `src/fanout/utils.rs`, so the two paths cannot drift.

`get_fanout_user` should live in `src/common/context.rs` next to `is_real_user_client`, since
both `cluster_rpc.rs` and `fanout_command.rs` need it.

### 6.3 `src/fanout/fanout_message.rs`

As specified in §4: header field, version bump, version validation, serializer/deserializer
branches, and updated unit tests (see §8).

### 6.4 Unchanged on purpose

- `registry.rs` — the `RequestHandlerCallback` signature (`fn(&Context, &[u8], &mut Vec<u8>)`)
  stays as is; impersonation happens around the call, not inside it.
- Every `FanoutCommand` / `FanoutClientCommand` implementation — `get_local_response`
  implementations inherit correct behavior via `ctx.get_current_user()`.
- `blocked_client.rs` — the final `reply` runs on the unblocked client's own context, which
  carries the original user naturally.
- Requester-side `on_response` aggregation — operates on data the shards already filtered;
  it must remain free of ctx-based ACL-sensitive reads (add a doc comment on the trait method
  stating this).

---

## 7. Security Considerations

- **Trust model.** The username travels in a cluster-bus module message. Only cluster members
  can send these, and the cluster bus is already the trust boundary for keyspace content
  (enable cluster-bus TLS for confidentiality). A malicious cluster member could always read
  data directly; propagating a username grants no new capability. We are *narrowing*
  privileges on the shard (from implicit superuser to the requester's user), never widening.
- **Fail-closed on unknown users** prevents privilege escalation via a user that exists on the
  requester but was never provisioned (or was disabled) on the shard.
- **No privilege leakage across requests.** `ContextUserScope::drop` clears the context user
  (`SetContextUser(NULL)`) and the `FanoutAclScope` clears the thread-local. Both are
  lifetime-bound inside the GIL critical section, so the shared `MODULE_CONTEXT` can never be
  observed by another job with a stale user — including during panic unwinding, since both are
  RAII guards.
- **ACL mutations mid-flight.** The user is resolved on the shard at execution time
  (`GetModuleUserFromUserName`), so an `ACL SETUSER`/`DELUSER` between dispatch and execution
  is honored — same semantics as a client command racing an ACL change.
- **Input hardening.** Cap the deserialized username length; treat any malformed header as a
  serialization error (existing behavior).
- **Operator requirement (must be documented in user-facing docs):** ACL users must be defined
  consistently on every node of the cluster (Valkey does not replicate ACLs). Inconsistent
  ACLs now produce per-shard permission errors instead of silently returning unauthorized
  data — that is the intended behavior change.

---

## 8. Testing Plan

### Rust unit tests

`fanout_message.rs`:
- v2 round-trip with a user, with `None`, with an empty string (⇒ `None`).
- Hand-crafted v1 buffer (version=1, no user field) parses with `user == None` and correct
  payload offset.
- Buffer with `version = 3` is rejected.
- Oversized username is rejected.
- Existing header tests updated for the new field/signature.

`common/context.rs`:
- `FanoutAclScope` set/clear semantics, including nested-drop ordering and the
  thread-locality of the flag.

### Python integration tests (new `tests/test_ts_acls_cme.py`, based on `ValkeyTimeSeriesClusterTestCase`)

Setup: N-shard cluster; provision the same ACL users on **every** node (helper that iterates
all primaries/replicas), e.g.
`ACL SETUSER alice on >pw ~ts:{slot1}* +@read +@timeseries`.

| Test | Expectation |
|---|---|
| `TS.MRANGE`/`TS.MGET` as `alice`, filter matching series in permitted and forbidden slots | Only series matching `~ts:{slot1}*` returned; forbidden shard data filtered *on the shard* |
| `TS.CARD` as `alice` with a filter | Count includes only permitted keys across all shards |
| Bare `TS.CARD` / `TS.QUERYINDEX` / `TS.LABELNAMES` as a user without `~*` ACCESS | Command errors (metadata permission), error text propagated from shard |
| `TS.MDEL` as a user without DELETE on remote keys | Remote keys not deleted; appropriate error/count |
| User exists on requester but deleted on one shard | Command returns a permission error (fail-closed), does not hang, in-flight bookkeeping cleaned up |
| Same commands as `default`/admin user | Unrestricted results, unchanged from today |
| ≥ 2 target nodes so the local shard uses the offload path | Local shard's data is also filtered (regression test for `spawn_local_request`) |
| Existing `test_ts_acls.py` (standalone) | Unchanged, still green |

Also re-run the full `*_cme.py` suite to confirm no regression for unauthenticated/default
setups (all those tests run as `default` with full permissions, and must be unaffected).

### Manual/targeted checks

- Rolling-upgrade smoke: old-version node + new-version node; verify v1→v2 requests execute
  (unenforced) and v2→v1 requests produce a clean per-shard error, no crash or hang.
- Timeout path: shard that fails user attach must still send the error response so the
  requester's `InFlightRequest` completes without waiting for the timer.

---

## 9. Implementation Order

Each step compiles and passes tests independently:

1. **Enforcement plumbing (no behavior change):** add `FanoutAclScope`, `is_acl_enforced`,
   `get_fanout_user` to `common/context.rs`; switch the five gate call sites (§5). Since no
   scope is ever entered yet, behavior is identical.
2. **Wire format v2:** `fanout_message.rs` header field, version bump + validation,
   serializer/deserializer, unit tests. Senders still pass `None`.
3. **Requester capture:** `send_cluster_request` captures and serializes the user;
   `fanout_command.rs` threads the user into `spawn_local_request` via `attach_fanout_user`.
4. **Shard-side attach:** `process_request_message` impersonation + fail-closed error path;
   add the permission `FanoutError` kind.
5. **Integration tests** (`test_ts_acls_cme.py`) and user-facing docs: note the
   consistent-ACL-provisioning requirement and the rolling-upgrade caveat in
   `docs/overview.md` / release notes.

## 10. Open Questions / Future Work

- **Strict mode for legacy peers.** Should a config (e.g. `ts-fanout-acl-strict`) reject
  v1 (userless) requests once a cluster is fully upgraded? Recommended default: off for one
  release to keep rolling upgrades painless; revisit after.
- **Version negotiation.** If more header evolution is expected, consider a capability
  handshake piggybacked on cluster-map refresh rather than per-message versioning alone.
- **Replica reads.** Fanout may target replicas (`FanoutTargetMode::Random`); ACL user lookup
  and key-permission checks work identically there, but test coverage should include a
  replica-targeted read once replica targeting is deterministic in tests.
