use std::str::FromStr;

use clap::Parser;
use wasmtime_poc_core::api::debug_async_api::MODULE_DEBUG_TARGET;
use wasmtime_poc_core::runtime::APP_ASYNC_DEBUG_TARGET;
use wasmtime_poc_core::runtime::{AppConfig, UninitializedAppContext};

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    #[clap(short, long, value_parser)]
    config_path: String,
    #[clap(short, long, default_value = "warn")]
    app_debug: String,
    #[clap(short, long, default_value = "warn")]
    module_debug: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    console_subscriber::init();

    let app_debug_level = log::Level::from_str(&args.app_debug)?.to_level_filter();
    let module_debug_level = log::Level::from_str(&args.module_debug)?.to_level_filter();
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
        .level_for(MODULE_DEBUG_TARGET, module_debug_level)
        .level_for(APP_ASYNC_DEBUG_TARGET, app_debug_level)
        .chain(std::io::stdout())
        .apply()?;

    let app_config = AppConfig::from_app_config_file(args.config_path)?;

    let unitialized_app_context = UninitializedAppContext::new(&app_config)?;
    let mut initialized_app_context = unitialized_app_context.async_initialize_modules()?;
    initialized_app_context.start().await
}
