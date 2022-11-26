use std::{collections::HashMap, sync::Arc};

use wasmtime::{Engine, Linker, Module};

use super::{AppConfig, ModuleRuntimeConfig};

#[derive(Debug)]
pub enum RuntimeEvent {
    RuntimeTaskStop,
}

pub(super) struct UninitializedModule<C> {
    pub(super) bytes: Box<[u8]>,
    pub(super) runtime_config: C,
}

#[derive(Clone)]
pub(super) struct InitializedModule<T, C> {
    pub(super) module: Module,
    pub(super) linker: Linker<T>,
    pub(super) engine: Arc<Engine>,
    pub(super) runtime_config: C,
}

pub struct UninitializedAppContext {
    pub(super) modules: HashMap<String, UninitializedModule<ModuleRuntimeConfig>>,
}

impl UninitializedAppContext {
    pub fn new(config: &AppConfig) -> anyhow::Result<UninitializedAppContext> {
        let modules: Result<HashMap<String, UninitializedModule<ModuleRuntimeConfig>>, _> =
            config
                .modules
                .iter()
                .map(
                    |(module_name, module_config)| -> std::io::Result<(String, UninitializedModule<ModuleRuntimeConfig>)> {
                        Ok((
                            module_name.clone(),
                            UninitializedModule::<ModuleRuntimeConfig> {
                                bytes: std::fs::read(&module_config.wasm_module_path)?
                                    .into_boxed_slice(),
                                runtime_config: module_config.runtime.clone(),
                            },
                        ))
                    },
                )
                .collect();

        Ok(UninitializedAppContext { modules: modules? })
    }
}
