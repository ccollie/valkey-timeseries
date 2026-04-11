extern crate prost_build;

use std::fs;
use std::path::Path;

fn main() {
    let mut config = prost_build::Config::new();
    config
        .compile_protos(
            &[
                "src/commands/fanout.request.proto",
                "src/commands/fanout.response.proto",
                "src/promql/types.proto",
            ],
            &["src/"],
        )
        .unwrap();

    // Ensure a placeholder file for promql test generation exists so
    // `include!(concat!(env!("OUT_DIR"), "/promql_tests_generated.rs"))`
    // does not fail when no external generator has produced the file.
    if let Ok(out_dir) = std::env::var("OUT_DIR") {
        let dest_path = Path::new(&out_dir).join("promql_tests_generated.rs");
        // Only write if the file does not already exist to avoid stomping real generated content.
        if !dest_path.exists() {
            let _ = std::fs::write(
                &dest_path,
                "// Auto-generated placeholder for promql tests\n",
            );
        }

        let testdata_dir = Path::new("src/promql/promqltest/testdata");
        let mut code = String::new();

        if testdata_dir.exists() {
            let mut entries: Vec<_> = fs::read_dir(testdata_dir)
                .expect("failed to read testdata directory")
                .filter_map(|e| e.ok())
                .filter(|e| {
                    e.path()
                        .extension()
                        .and_then(|s| s.to_str())
                        .map(|s| s == "test")
                        .unwrap_or(false)
                })
                .collect();

            entries.sort_by_key(|e| e.file_name());

            code.push_str("use crate::promql::promqltest::runner::run_test;\n\n");
            for entry in entries {
                let path = entry.path();
                let stem = path
                    .file_stem()
                    .and_then(|s| s.to_str())
                    .expect("invalid test filename");

                let fn_name = stem.replace('-', "_");

                code.push_str(&format!(
                    r#"
#[test]
fn should_pass_{fn_name}() {{
    run_test("{stem}", include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/src/promql/promqltest/testdata/{stem}.test")))
        .unwrap();
}}
"#,
                ));
            }
        }

        fs::write(&dest_path, code).expect("failed to write generated test file");
    }
}
