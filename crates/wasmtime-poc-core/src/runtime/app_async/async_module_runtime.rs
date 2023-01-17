use wasmtime::Store;

use super::AsyncWasmModuleStore;

pub struct AsyncModuleRuntime {
    pub(super) store: Store<AsyncWasmModuleStore>,
    pub(super) module_instance_id: ModuleInstanceId,
    pub(super) module_name: String,
}

pub type ModuleInstanceId = u64;
