use wit_bindgen_host_wasmtime_rust::export;

export!("./wit-bindgen/debug.wit");

pub use debug::add_to_linker;

use crate::runtime::WasmModuleStore;

impl debug::Debug for WasmModuleStore {
    fn trace(&mut self, msg: &str) {
        log::trace!("{}", msg);
    }

    fn debug(&mut self, msg: &str) {
        log::debug!("{}", msg);
    }

    fn info(&mut self, msg: &str) {
        log::info!("{}", msg);
    }

    fn warn(&mut self, msg: &str) {
        log::warn!("{}", msg)
    }

    fn error(&mut self, msg: &str) {
        log::error!("{}", msg)
    }
}
