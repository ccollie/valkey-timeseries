#[cfg(test)]
mod tests {
    use redis::ToRedisArgs;
    use crate::tests::integration::utils::{start_redis_server_with_module, validate_key_exists, with_valkey_connection};

    fn exec_create_internal<K: ToRedisArgs>(conn: &mut redis::Connection, key: K, args: &[&str]) -> bool {
        let b: bool = redis::cmd("CREATE")
            .arg(key)
            .arg(args)
            .query(conn).unwrap();
        b
    }

    fn get_port() -> u16 {
        6379
        // randomize
    }
    
    #[test]
    fn test_create() {
        let port = get_port();
        // let guard = start_redis_server_with_module(port, &[]).unwrap();
        with_valkey_connection(port, |conn| {
            assert!(exec_create_internal(conn, "key",&[]));
            validate_key_exists(conn, "key", true);
            
            Ok(())
        }).unwrap();
    }
}