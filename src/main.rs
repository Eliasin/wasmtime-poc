#![feature(hash_drain_filter)]

mod app;
mod module;
mod mqtt_api;

use app::{AppConfig, UninitializedAppContext};
use clap::Parser;

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    #[clap(short, long, value_parser)]
    app_config_path: String,
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    let app_config = AppConfig::from_app_config_file(args.app_config_path)?;

    let unitialized_app_context = UninitializedAppContext::new(&app_config)?;
    let mut initialized_app_context = unitialized_app_context.initialize_modules()?;
    initialized_app_context.run_all_modules()?;

    for (module_name, result) in initialized_app_context.join_modules() {
        match result {
            Ok(_) => println!("module '{}' finished successfully", module_name),
            Err(e) => println!("module '{}' exited with error: {}", module_name, e),
        }
    }

    Ok(())
}
