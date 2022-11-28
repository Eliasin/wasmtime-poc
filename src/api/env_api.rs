use crate::runtime::WasmModuleStore;

wit_bindgen_host_wasmtime_rust::generate!({
    path: "./wit-bindgen/apis.wit",
    async: true,
});

pub use env::add_to_linker;

#[wit_bindgen_host_wasmtime_rust::async_trait]
impl env::Env for WasmModuleStore {
    async fn get_val(&mut self, var_name: String) -> anyhow::Result<Option<String>> {
        Ok(self.env.get(&var_name).map(|s| s.clone()))
    }
}
