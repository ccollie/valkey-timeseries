use super::fanout_operation::FanoutOperation;
use super::serialization::{Deserialized, Serializable, Serialized};
use crate::fanout::{FanoutError, FanoutResult};
use ahash::RandomState;
use std::sync::{Arc, LazyLock};
use valkey_module::{Context, ValkeyError, ValkeyResult};

/// Type-erased function pointer for executing a fanout operation.
/// This allows us to store different fanout operations with different
/// Request/Response types in the same registry.
pub(super) type RequestHandlerCallback =
    Arc<dyn Fn(&Context, &[u8], &mut Vec<u8>) -> FanoutResult + Send + Sync>;

/// A registry for fanout operations that allows type-erased storage and retrieval
/// of [`FanoutOperation`] implementations.
pub struct FanoutOperationRegistry {
    operations: papaya::HashMap<&'static str, RequestHandlerCallback, RandomState>,
}

impl FanoutOperationRegistry {
    /// Create a new empty registry.
    pub fn new() -> Self {
        Self {
            operations: papaya::HashMap::with_hasher(RandomState::new()),
        }
    }

    /// Register a fanout operation by name.
    ///
    /// # Type Parameters
    /// - `OP`: The operation type implementing FanoutOperation
    pub fn register<OP>(&self) -> ValkeyResult<()>
    where
        OP: FanoutOperation + 'static,
        OP::Request: Serializable + Send + 'static,
        OP::Response: Serializable + Send + 'static,
    {
        let name = OP::name();

        let request_handler = Arc::new(
            |ctx: &Context, req_buf: &[u8], dest: &mut Vec<u8>| -> FanoutResult {
                match OP::Request::deserialize(req_buf) {
                    Ok(request) => {
                        let response = OP::get_local_response(ctx, request)?;
                        response.serialize(dest);
                    }
                    Err(e) => {
                        let msg =
                            format!("Failed to deserialize {} fanout request: {e}", OP::name());
                        return Err(FanoutError::serialization(&msg));
                    }
                }
                Ok(())
            },
        );

        let map = self.operations.pin();
        if map.contains_key(name) {
            return Err(ValkeyError::String(format!(
                "Operation '{name}' is already registered"
            )));
        }

        map.insert(name, request_handler);

        Ok(())
    }

    /// Execute a registered fanout operation by name.
    ///
    /// # Arguments
    /// - `ctx`: The Valkey context
    /// - `name`: The name of the operation to execute
    /// - `payload`: Serialized request data
    /// - `dest`: destination buffer. This will be sent back to requester
    fn execute(
        &self,
        ctx: &Context,
        name: &str,
        payload: &[u8],
        dest: &mut Vec<u8>,
    ) -> FanoutResult<()> {
        let executor = self.get_operation_by_name(name, true).unwrap();
        executor(ctx, payload, dest)
    }

    fn get_operation_by_name(
        &self,
        name: &str,
        must_exist: bool,
    ) -> Option<RequestHandlerCallback> {
        let map = self.operations.pin();
        match map.get(name) {
            Some(op) => Some(op.clone()),
            None => {
                if must_exist {
                    panic!("Fanout Operation '{name}' not found in registry");
                } else {
                    None
                }
            }
        }
    }

    /// Check if an operation is registered.
    pub fn contains(&self, name: &str) -> bool {
        self.operations.pin().contains_key(name)
    }

    /// Get the list of all registered operation names.
    pub fn list_operations(&self) -> Vec<&'static str> {
        let mut ops = Vec::new();
        let map = self.operations.pin();
        for key in map.keys() {
            ops.push(*key);
        }
        ops.sort();
        ops
    }
}

static FANOUT_REGISTRY: LazyLock<FanoutOperationRegistry> =
    LazyLock::new(FanoutOperationRegistry::new);

// Register a fanout operation.
///
/// # Type Parameters
/// - `OP`: The operation type implementing FanoutOperation
pub fn register_fanout_operation<OP>() -> ValkeyResult<()>
where
    OP: FanoutOperation + 'static,
    OP::Request: Serializable + Send + 'static,
    OP::Response: Serializable + Send + 'static,
{
    FANOUT_REGISTRY.register::<OP>()
}

/// Execute a registered fanout operation by name.
///
/// # Arguments
/// - `ctx`: The Valkey context
/// - `name`: The name of the operation to execute
/// - `payload`: Serialized request data
/// - `dest`: destination buffer. This will be sent back to requester
pub fn handle_fanout_request(
    ctx: &Context,
    name: &str,
    payload: &[u8],
    dest: &mut Vec<u8>,
) -> FanoutResult {
    FANOUT_REGISTRY.execute(ctx, name, payload, dest)
}

pub(super) fn get_fanout_request_handler(name: &str) -> Option<RequestHandlerCallback> {
    FANOUT_REGISTRY.get_operation_by_name(name, false)
}

pub(crate) fn get_registered_fanout_operations() -> Vec<&'static str> {
    FANOUT_REGISTRY.list_operations()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_registry_basic_operations() {
        let registry = FanoutOperationRegistry::new();

        // Initially empty
        assert_eq!(registry.list_operations().len(), 0);
        assert!(!registry.contains("test_op"));
    }
}
