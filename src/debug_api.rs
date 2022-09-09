use wit_bindgen_host_wasmtime_rust::export;

export!("./wit-bindgen/debug.wit");

pub use debug::add_to_linker;

use crate::module::WasmModuleStore;

impl debug::Debug for WasmModuleStore {
    fn sout(&mut self, msg: &str) {
        println!("{}", msg);
    }

    fn serr(&mut self, msg: &str) {
        eprintln!("{}", msg)
    }
}
