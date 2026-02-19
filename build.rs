extern crate prost_build;

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
        let dest_path = std::path::Path::new(&out_dir).join("promql_tests_generated.rs");
        // Only write if the file does not already exist to avoid stomping real generated content.
        if !dest_path.exists() {
            let _ = std::fs::write(
                &dest_path,
                "// Auto-generated placeholder for promql tests\n",
            );
        }
    }
}
