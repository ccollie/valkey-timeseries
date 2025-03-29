use anyhow::{Context, Result};
use std::{env, fs};
use std::path::PathBuf;
use std::process::Command;
use std::time::Duration;

pub const MODULE_NAME: &str = "valkey_timeseries";
const CONNECTION_TIMEOUT: Duration = Duration::from_secs(5);

/// Ensure child process is killed both on normal exit and when panicking due to a failed test.
pub struct ChildGuard {
    pub name: &'static str,
    pub child: std::process::Child,
}

impl Drop for ChildGuard {
    fn drop(&mut self) {
        if let Err(e) = self.child.kill() {
            println!("Could not kill {}: {}", self.name, e);
        }
        if let Err(e) = self.child.wait() {
            println!("Could not wait for {}: {}", self.name, e);
        }
    }
}

fn create_valkey_client(port: u16) -> redis::RedisResult<redis::Client> {
    let addr = format!("redis://127.0.0.1:{}", port);
    let client = redis::Client::open(addr)?;
    Ok(client)
}

pub(crate) fn with_valkey_connection<F, R>(port: u16, f: F) -> redis::RedisResult<R>
where
    F: FnOnce(&mut redis::Connection) -> redis::RedisResult<R>,
{
    let client = create_valkey_client(port)?;
    let mut conn = client.get_connection_with_timeout(CONNECTION_TIMEOUT)?;
    f(&mut conn)
}

fn get_valkey_binary_version() -> String {
    let key = "VALKEY_VERSION";
    env::var(key).unwrap_or_else(|_e| "unstable".to_string())
}

fn get_root_path() -> PathBuf {
    let current_dir = env::current_dir().unwrap();
    // let module_path: PathBuf = [current_dir, "../../..".into()].iter().collect();
    fs::canonicalize(&current_dir).unwrap()
}

pub fn get_server_binary_path() -> PathBuf {
    let version = get_valkey_binary_version();
    let mut module_path = get_root_path();
    module_path.push(
        PathBuf::from(format!("tests/.build/binaries/{version}/valkey-server"))
    );

    fs::canonicalize(&module_path).unwrap()
}

fn get_profile() -> &'static str {
    if cfg!(not(debug_assertions)) {
        "release"
    } else {
        "debug"
    }
}

pub fn get_module_path() -> PathBuf {
    let profile = get_profile();
    let extension = if cfg!(target_os = "macos") {
        "dylib"
    } else if cfg!(target_os = "windows") {
        "dll"
    } else {
        "so"
    };
    let mut module_path: PathBuf = get_root_path();
    module_path.push(format!("target/{profile}/lib{MODULE_NAME}.{extension}"));

    fs::canonicalize(&module_path).unwrap()
}

pub fn start_redis_server_with_module(port: u16, opts: &[&str]) -> Result<ChildGuard> {
    let module_path= get_module_path();
    let binary_path = get_server_binary_path();

    let module_path = format!("{}", module_path.display());
    assert!(fs::metadata(&module_path)
        .with_context(|| format!("Loading valkey module: {}", module_path.as_str()))?
        .is_file());
    println!("Loading valkey module: {}", module_path.as_str());

    let port_str = port.to_string();
    let mut args = vec![
        "--port",
        &port_str,
        "--loadmodule",
        module_path.as_str(),
    ];
    args.extend_from_slice(opts);

    let valkey_server = Command::new(binary_path)
        .args(&args)
        .spawn()
        .map(|c| ChildGuard {
            name: "valkey-server",
            child: c,
        })
        .with_context(|| format!("Error in raising valkey-server => {}", module_path.as_str()))?;

    Ok(valkey_server)
}

pub fn stop_valkey_server(child: &mut ChildGuard) {
    child.child.kill().expect("Ohh no!");
}

pub(super) fn key_exists(conn: &mut redis::Connection, key: &str) -> bool {
    let exists: bool = redis::cmd("EXISTS").arg(key).query(conn).unwrap();
    exists
}

pub(super) fn validate_key_exists(conn: &mut redis::Connection, key: &str, should_exist: bool) {
    if should_exist {
        assert!(key_exists(conn, key), "Key {} should exist", key);
    } else {
        assert!(!key_exists(conn, key), "Key {} should not exist", key);
    }
}

pub(super) fn key_count(conn: &mut redis::Connection, pattern: &str) -> usize {
    let count: usize = redis::cmd("DBSIZE").arg(pattern).query(conn).unwrap();
    count
}

pub(super) fn key_type(conn: &mut redis::Connection, key: &str) -> String {
    let key_type: String = redis::cmd("TYPE").arg(key).query(conn).unwrap();
    key_type
}