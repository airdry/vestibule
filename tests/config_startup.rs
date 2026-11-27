//! Startup contract tests for the binary: configuration is resolved from
//! `--config` or `VESTIBULE_CONFIG`, validated on startup, and
//! failures exit with a clear error message.

mod support;

use std::io::Read;
use std::process::{Command, Stdio};

/// Path to the built binary under test.
const BINARY: &str = env!("CARGO_BIN_EXE_vestibule");

struct BinaryOutput {
    status_code: Option<i32>,
    stderr: String,
}

fn run_binary(args: &[&str], env: Option<(&str, &str)>) -> BinaryOutput {
    let mut command = Command::new(BINARY);
    command.args(args).stderr(Stdio::piped());
    if let Some((key, value)) = env {
        command.env(key, value);
    }
    let mut child = command.spawn().expect("spawn binary");
    let mut stderr = String::new();
    if let Some(mut pipe) = child.stderr.take() {
        pipe.read_to_string(&mut stderr)
            .expect("read binary stderr");
    }
    let status = child.wait().expect("wait for binary");
    BinaryOutput {
        status_code: status.code(),
        stderr,
    }
}

fn write_temp_config(content: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("vestibule-test-{}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("create temp dir");
    let path = dir.join(format!("config-{}.yaml", support::ephemeral_port()));
    std::fs::write(&path, content).expect("write temp config");
    path
}

/// The Admin Plane port from a YAML produced by `support::config_yaml`.
fn admin_port_of(yaml: &str) -> u16 {
    yaml.lines()
        .find_map(|line| {
            line.trim_start().starts_with("admin_addr:").then(|| {
                line.split('"')
                    .nth(1)
                    .expect("socket addr is quoted")
                    .rsplit(':')
                    .next()
                    .expect("addr has a port")
                    .parse()
                    .expect("valid port")
            })
        })
        .expect("admin_addr is configured")
}

/// Starts the binary with the given extra args/environment, waits for the
/// Admin Plane, checks that `/healthz` answers `200`, then kills it.
fn assert_binary_serves_probes(
    yaml: &str,
    spawn: impl FnOnce(&std::path::Path) -> std::process::Child,
) {
    let config_path = write_temp_config(yaml);
    let mut child = spawn(&config_path);

    let admin_port = admin_port_of(yaml);
    support::wait_until_listening(
        format!("127.0.0.1:{admin_port}")
            .parse()
            .expect("valid addr"),
    );

    let response = support::http_client()
        .get(format!("http://127.0.0.1:{admin_port}/healthz"))
        .send()
        .expect("GET /healthz from the started binary");
    assert_eq!(response.status().as_u16(), 200);

    child.kill().expect("kill binary");
    child.wait().expect("reap binary");
}

fn minimal_yaml() -> String {
    support::config_yaml(
        "http://127.0.0.1:1",
        support::ephemeral_port(),
        support::ephemeral_port(),
    )
}

#[test]
fn exits_with_error_when_no_config_given() {
    let output = run_binary(&[], None);

    assert_ne!(output.status_code, Some(0));
    assert!(
        output.stderr.contains("no configuration file given"),
        "stderr was: {}",
        output.stderr
    );
    assert!(
        output.stderr.contains("VESTIBULE_CONFIG"),
        "stderr was: {}",
        output.stderr
    );
}

#[test]
fn exits_with_error_when_config_file_missing() {
    let output = run_binary(&["--config", "/nonexistent/config.yaml"], None);

    assert_ne!(output.status_code, Some(0));
    assert!(
        output.stderr.contains("failed to read config file"),
        "stderr was: {}",
        output.stderr
    );
}

#[test]
fn exits_with_error_on_invalid_yaml() {
    let config_path = write_temp_config("server: [broken");
    let output = run_binary(&["--config", config_path.to_str().unwrap()], None);

    assert_ne!(output.status_code, Some(0));
    assert!(
        output.stderr.contains("invalid config"),
        "stderr was: {}",
        output.stderr
    );
}

#[test]
fn exits_with_error_when_server_section_missing() {
    let config_path = write_temp_config("upstream:\n  url: \"http://127.0.0.1:1\"\n");
    let output = run_binary(&["--config", config_path.to_str().unwrap()], None);

    assert_ne!(output.status_code, Some(0));
    assert!(
        output.stderr.contains("missing required section 'server'"),
        "stderr was: {}",
        output.stderr
    );
}

#[test]
fn exits_with_error_when_upstream_section_missing() {
    let config_path = write_temp_config("server:\n  data_plane_addr: \"127.0.0.1:18080\"\n");
    let output = run_binary(&["--config", config_path.to_str().unwrap()], None);

    assert_ne!(output.status_code, Some(0));
    assert!(
        output
            .stderr
            .contains("missing required section 'upstream'"),
        "stderr was: {}",
        output.stderr
    );
}

#[test]
fn exits_with_error_on_unsupported_upstream_scheme() {
    let yaml = support::config_yaml(
        "ftp://127.0.0.1:21",
        support::ephemeral_port(),
        support::ephemeral_port(),
    );
    let config_path = write_temp_config(&yaml);
    let output = run_binary(&["--config", config_path.to_str().unwrap()], None);

    assert_ne!(output.status_code, Some(0));
    assert!(
        output.stderr.contains("unsupported scheme 'ftp'"),
        "stderr was: {}",
        output.stderr
    );
}

#[test]
fn starts_and_serves_probes_with_valid_config() {
    let yaml = minimal_yaml();
    assert_binary_serves_probes(&yaml, |config_path| {
        Command::new(BINARY)
            .arg("--config")
            .arg(config_path)
            .spawn()
            .expect("spawn binary")
    });
}

#[test]
fn accepts_config_path_from_environment_variable() {
    let yaml = minimal_yaml();
    assert_binary_serves_probes(&yaml, |config_path| {
        Command::new(BINARY)
            .env("VESTIBULE_CONFIG", config_path)
            .spawn()
            .expect("spawn binary")
    });
}
