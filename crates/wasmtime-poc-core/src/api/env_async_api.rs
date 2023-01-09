use crate::runtime::AsyncWasmModuleStore;

wasmtime::component::bindgen!({
    path: "../../wit-bindgen/apis.wit",
    async: true,
});

pub use env::add_to_linker;

#[async_trait::async_trait]
impl env::Env for AsyncWasmModuleStore {
    async fn get_val(&mut self, var_name: String) -> anyhow::Result<Option<String>> {
        Ok(self.env.get(&var_name).cloned())
    }
}
