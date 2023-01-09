use crate::runtime::AsyncWasmModuleStore;

wasmtime::component::bindgen!({
    path: "../../wit-bindgen/apis.wit",
    async: true,
});

pub use util::add_to_linker;

#[async_trait::async_trait]
impl util::Util for AsyncWasmModuleStore {
    async fn sleep(&mut self, millis: u64) -> anyhow::Result<()> {
        tokio::time::sleep(std::time::Duration::from_millis(millis)).await;
        Ok(())
    }
}
