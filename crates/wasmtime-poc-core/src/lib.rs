#![feature(hash_drain_filter)]
#![feature(entry_insert)]
#![feature(async_fn_in_trait)]

mod api;
mod runtime;

pub use api::debug_async_api::MODULE_DEBUG_TARGET;
pub use runtime::{AppConfig, UninitializedAppContext, APP_ASYNC_DEBUG_TARGET};
