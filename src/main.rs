#![feature(hash_drain_filter)]

mod api;
mod runtime;

use std::str::FromStr;

use api::debug_async_api::MODULE_DEBUG_TARGET;
use clap::Parser;
use runtime::{AppConfig, UninitializedAppContext};

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    #[clap(short, long, value_parser)]
    app_config_path: String,
    #[clap(short, long, default_value = "warn")]
    debug_level: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    let debug_level = log::Level::from_str(&args.debug_level)?.to_level_filter();
    fern::Dispatch::new()
        .format(|out, message, record| {
            out.finish(format_args!(
                "{}[{}][{}] {}",
                chrono::Local::now().format("[%Y-%m-%d][%H:%M:%S]"),
                record.target(),
                record.level(),
                message,
            ))
        })
        .level(log::LevelFilter::Warn)
        .level_for(MODULE_DEBUG_TARGET, debug_level)
        .chain(std::io::stdout())
        .apply()?;

    let app_config = AppConfig::from_app_config_file(args.app_config_path)?;

    let unitialized_app_context = UninitializedAppContext::new(&app_config)?;
    let mut initialized_app_context = unitialized_app_context.async_initialize_modules()?;
    initialized_app_context.start().await
}
