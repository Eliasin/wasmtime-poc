#![feature(hash_drain_filter)]

mod api;
mod runtime;

use clap::Parser;
use runtime::{AppConfig, UninitializedAppContext};

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    #[clap(short, long, value_parser)]
    app_config_path: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    let app_config = AppConfig::from_app_config_file(args.app_config_path)?;

    let unitialized_app_context = UninitializedAppContext::new(&app_config)?;
    let mut initialized_app_context = unitialized_app_context.initialize_modules()?;
    initialized_app_context.run_all_modules()?;

    loop {
        let cleaned_up = initialized_app_context.cleanup_finished_modules().await?;

        if cleaned_up.len() > 0 {
            println!("{:?}", cleaned_up);
        }
    }
}
