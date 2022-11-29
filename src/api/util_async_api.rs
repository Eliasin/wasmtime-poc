use crate::runtime::AsyncWasmModuleStore;

wit_bindgen_host_wasmtime_rust::generate!({
    path: "./wit-bindgen/apis.wit",
    async: true,
});

pub use util::add_to_linker;

#[wit_bindgen_host_wasmtime_rust::async_trait]
impl util::Util for AsyncWasmModuleStore {
    async fn sleep(&mut self, millis: u64) -> anyhow::Result<()> {
        tokio::time::sleep(std::time::Duration::from_millis(millis)).await;
        Ok(())
    }
}
