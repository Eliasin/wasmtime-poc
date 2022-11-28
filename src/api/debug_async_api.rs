use crate::runtime::WasmModuleStore;

wit_bindgen_host_wasmtime_rust::generate!({
    path: "./wit-bindgen/apis.wit",
    async: true,
});

pub use debug::add_to_linker;

#[wit_bindgen_host_wasmtime_rust::async_trait]
impl debug::Debug for WasmModuleStore {
    async fn trace(&mut self, msg: String) -> anyhow::Result<()> {
        log::trace!("{}", msg);

        Ok(())
    }

    async fn debug(&mut self, msg: String) -> anyhow::Result<()> {
        log::debug!("{}", msg);

        Ok(())
    }

    async fn info(&mut self, msg: String) -> anyhow::Result<()> {
        log::info!("{}", msg);

        Ok(())
    }

    async fn warn(&mut self, msg: String) -> anyhow::Result<()> {
        log::warn!("{}", msg);

        Ok(())
    }

    async fn error(&mut self, msg: String) -> anyhow::Result<()> {
        log::error!("{}", msg);

        Ok(())
    }
}
