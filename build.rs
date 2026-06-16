extern crate prost_build;

fn main() {
    // Tell Cargo about our custom cfg so it doesn't warn on unexpected cfg.
    println!("cargo::rustc-check-cfg=cfg(use_system_alloc)");

    // Use the system allocator for debug builds (including tests and doctests)
    // so the Valkey allocator isn't required outside a Valkey server.
    let profile = std::env::var("PROFILE").unwrap_or_default();
    if profile == "debug" {
        println!("cargo:rustc-cfg=use_system_alloc");
    }

    let mut config = prost_build::Config::new();
    config
        .compile_protos(
            &[
                "src/commands/fanout.request.proto",
                "src/commands/fanout.response.proto",
            ],
            &["src/"],
        )
        .unwrap();
}
