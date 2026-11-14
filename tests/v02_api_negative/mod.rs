use std::fs;
use std::path::PathBuf;
use std::process::Command;
use std::sync::atomic::{AtomicUsize, Ordering};

static NEXT_CASE: AtomicUsize = AtomicUsize::new(0);

#[test]
fn public_api_rejects_the_removed_or_forbidden_capabilities() {
    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let fixtures = manifest.join("tests/v02_api_negative");
    let deps = manifest.join("target/debug/deps");
    let library = fs::read_dir(&deps)
        .unwrap()
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .find(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| {
                    name.starts_with("libqubit_operation-") && name.ends_with(".rlib")
                })
        })
        .expect("cargo test built qubit_operation rlib");
    let rustc = std::env::var_os("RUSTC").unwrap_or_else(|| "rustc".into());

    for entry in fs::read_dir(fixtures).unwrap().filter_map(Result::ok) {
        let source = entry.path();
        if source.file_name().and_then(|name| name.to_str()) == Some("mod.rs")
            || source.extension().and_then(|ext| ext.to_str()) != Some("rs")
        {
            continue;
        }
        let contents = fs::read_to_string(&source).unwrap();
        let expected = contents
            .lines()
            .find_map(|line| line.strip_prefix("// EXPECT: "))
            .unwrap_or_else(|| panic!("{} has no exact expected diagnostic", source.display()));
        let serial = NEXT_CASE.fetch_add(1, Ordering::Relaxed);
        let output = std::env::temp_dir().join(format!(
            "qubit_operation_negative_{}_{}.rlib",
            std::process::id(),
            serial
        ));
        let result = Command::new(&rustc)
            .arg("--edition=2024")
            .arg("--crate-type=lib")
            .arg("--crate-name=public_api_negative")
            .arg("--extern")
            .arg(format!("qubit_operation={}", library.display()))
            .arg("-L")
            .arg(format!("dependency={}", deps.display()))
            .arg(&source)
            .arg("-o")
            .arg(&output)
            .output()
            .expect("run rustc for API contract fixture");
        let stderr = String::from_utf8_lossy(&result.stderr);
        assert!(
            !result.status.success(),
            "{} unexpectedly compiled",
            source.display()
        );
        assert!(
            stderr.contains(expected),
            "{} failed for an unrelated reason; expected diagnostic fragment {expected:?}, got:\n{stderr}",
            source.display()
        );
        let _ = fs::remove_file(output);
    }
}
