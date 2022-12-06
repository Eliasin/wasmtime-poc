use anyhow::{anyhow, bail, Context};
use std::collections::HashMap;

use wasmtime::component::{Component, Linker};

use super::{AppConfig, ModuleLocator, ModuleRuntimeConfig};

#[derive(Debug)]
pub enum RuntimeEvent {
    RuntimeTaskStop,
}

pub(super) struct UninitializedModule<C> {
    pub(super) bytes: Box<[u8]>,
    pub(super) runtime_config: C,
}

pub(super) struct InitializedModule<T, C: Clone> {
    pub(super) module: Component,
    pub(super) linker: Linker<T>,
    pub(super) runtime_config: C,
}

pub struct UninitializedAppContext {
    pub(super) modules: HashMap<String, UninitializedModule<ModuleRuntimeConfig>>,
    pub(super) app_config: AppConfig,
}

pub type ModuleRepository = HashMap<String, Vec<u8>>;

impl UninitializedAppContext {
    pub fn new(config: &AppConfig) -> anyhow::Result<UninitializedAppContext> {
        let modules: Result<HashMap<String, UninitializedModule<ModuleRuntimeConfig>>, _> =
            config
                .modules
                .iter()
                .map(
                    |module_config| -> anyhow::Result<(String, UninitializedModule<ModuleRuntimeConfig>)> {
                        let module_bytes = match &module_config.module_locator {
                            ModuleLocator::TestingModule { module_id: _, test_module_repository_id: _ } => bail!("Apps with test modules must be initialized with a `new_with_repository`"),
                            ModuleLocator::ModuleFile { path } => {
                                std::fs::read(path).with_context(|| format!("Failed to find module file at {}", path.display()))?
                            },

                        };

                        Ok((
                            module_config.name.clone(),
                            UninitializedModule::<ModuleRuntimeConfig> {
                                bytes: module_bytes
                                    .into_boxed_slice(),
                                runtime_config: module_config.runtime.clone(),
                            },
                        ))
                    },
                )
                .collect();

        Ok(UninitializedAppContext {
            modules: modules?,
            app_config: config.clone(),
        })
    }

    #[allow(dead_code)]
    pub fn new_with_repository(
        config: &AppConfig,
        module_repositories: HashMap<String, ModuleRepository>,
    ) -> anyhow::Result<UninitializedAppContext> {
        let modules: Result<HashMap<String, UninitializedModule<ModuleRuntimeConfig>>, _> =
            config
                .modules
                .iter()
                .map(
                    |module_config| -> anyhow::Result<(String, UninitializedModule<ModuleRuntimeConfig>)> {
                        let module_bytes = match &module_config.module_locator {
                            ModuleLocator::TestingModule { module_id, test_module_repository_id } => {
                                let repository = module_repositories.get(test_module_repository_id).ok_or(anyhow!("'{}' is not a valid test module repository id", test_module_repository_id))?;

                                repository.get(module_id).ok_or(anyhow!("'{}' is not a valid module id within repository '{}'", module_id, test_module_repository_id))?.clone()
                            },
                            ModuleLocator::ModuleFile { path } => {
                                std::fs::read(path).with_context(|| format!("Failed to find module file at {}", path.display()))?
                            },

                        };

                        Ok((
                            module_config.name.clone(),
                            UninitializedModule::<ModuleRuntimeConfig> {
                                bytes: module_bytes
                                    .into_boxed_slice(),
                                runtime_config: module_config.runtime.clone(),
                            },
                        ))
                    },
                )
                .collect();

        Ok(UninitializedAppContext {
            modules: modules?,
            app_config: config.clone(),
        })
    }
}
