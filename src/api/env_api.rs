use wit_bindgen_host_wasmtime_rust::export;

export!("./wit-bindgen/env.wit");

pub use env::add_to_linker;

use crate::runtime::WasmModuleStore;

impl env::Env for WasmModuleStore {
    fn get_val(&mut self, var_name: &str) -> Option<String> {
        self.env.get(var_name).map(|s| s.clone())
    }
}
