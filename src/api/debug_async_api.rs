use crate::runtime::AsyncWasmModuleStore;

wit_bindgen_host_wasmtime_rust::generate!({
    path: "./wit-bindgen/apis.wit",
    async: true,
});

pub use debug::add_to_linker;

pub const MODULE_DEBUG_TARGET: &str = "module-debug";

#[wit_bindgen_host_wasmtime_rust::async_trait]
impl debug::Debug for AsyncWasmModuleStore {
    async fn trace(&mut self, msg: String) -> anyhow::Result<()> {
        log::trace!(target: MODULE_DEBUG_TARGET, "{}", msg);

        Ok(())
    }

    async fn debug(&mut self, msg: String) -> anyhow::Result<()> {
        log::debug!(target: MODULE_DEBUG_TARGET, "{}", msg);

        Ok(())
    }

    async fn info(&mut self, msg: String) -> anyhow::Result<()> {
        log::info!(target: MODULE_DEBUG_TARGET, "{}", msg);

        Ok(())
    }

    async fn warn(&mut self, msg: String) -> anyhow::Result<()> {
        log::warn!(target: MODULE_DEBUG_TARGET, "{}", msg);

        Ok(())
    }

    async fn error(&mut self, msg: String) -> anyhow::Result<()> {
        log::error!(target: MODULE_DEBUG_TARGET, "{}", msg);

        Ok(())
    }
}
