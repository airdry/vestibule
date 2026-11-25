//! Binary entry point: resolve the configuration path, load and validate the
//! YAML configuration, then run the service.

use std::convert::Infallible;
use std::path::PathBuf;
use std::process::ExitCode;

use sentry_ratelimiter::app;
use sentry_ratelimiter::config::load_config;

/// Environment variable pointing to the YAML configuration file.
const CONFIG_ENV_VAR: &str = "SENTRY_RATELIMITER_CONFIG";

const USAGE: &str = "usage: sentry-ratelimiter [--config <path>] (or set \
                      SENTRY_RATELIMITER_CONFIG=<path>)";

fn main() -> ExitCode {
    match run() {
        Ok(never) => match never {},
        Err(error) => {
            eprintln!("error: {error:#}");
            ExitCode::FAILURE
        }
    }
}

fn run() -> anyhow::Result<Infallible> {
    let config_path = resolve_config_path(std::env::args_os().skip(1))?;
    let config = load_config(&config_path)?;
    app::run(config, config_path).map_err(anyhow::Error::from)
}

/// Resolves the configuration path from `--config <path>` (or `-c <path>`),
/// falling back to `SENTRY_RATELIMITER_CONFIG`.
fn resolve_config_path<I>(args: I) -> anyhow::Result<PathBuf>
where
    I: Iterator<Item = std::ffi::OsString>,
{
    let mut args = args;

    if let Some(arg) = args.next() {
        let arg_lossy = arg.to_string_lossy();
        let path = match arg_lossy.as_ref() {
            value if value == "--config" || value == "-c" => args
                .next()
                .ok_or_else(|| anyhow::anyhow!("missing value after '{value}'\n{USAGE}"))?,
            value if value.starts_with("--config=") => {
                std::ffi::OsString::from(value.trim_start_matches("--config="))
            }
            _ => {
                return Err(anyhow::anyhow!(
                    "unexpected argument '{arg_lossy}'\n{USAGE}"
                ));
            }
        };
        return Ok(PathBuf::from(path));
    }

    let from_env = std::env::var_os(CONFIG_ENV_VAR).ok_or_else(|| {
        anyhow::anyhow!(
            "no configuration file given: pass --config <path> or set {CONFIG_ENV_VAR}\n\
             {USAGE}"
        )
    })?;
    Ok(PathBuf::from(from_env))
}
