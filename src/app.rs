use std::{collections::HashMap, path::Path, sync::Arc};

use serde::Deserialize;
use wasmtime::{Engine, Linker, Module, Store};

use crate::{
    module::{ModuleConfig, ModuleRuntimeConfig, WasmModuleStore},
    mqtt_api,
};

#[derive(Deserialize)]
pub struct AppConfig {
    pub modules: HashMap<String, ModuleConfig>,
}

pub struct UninitializedModule<C> {
    bytes: Box<[u8]>,
    runtime_config: C,
}

#[derive(Clone)]
pub struct InitializedModule<T, C> {
    pub module: Module,
    pub linker: Linker<T>,
    pub engine: Arc<Engine>,
    pub runtime_config: C,
}

pub struct UninitializedAppContext {
    modules: HashMap<String, UninitializedModule<ModuleRuntimeConfig>>,
}

pub struct InitializedAppContext {
    modules: HashMap<String, InitializedModule<WasmModuleStore, ModuleRuntimeConfig>>,
    module_threads: HashMap<String, std::thread::JoinHandle<anyhow::Result<()>>>,
}

impl AppConfig {
    pub fn from_app_config_file(path: impl AsRef<Path>) -> anyhow::Result<AppConfig> {
        let config_file_contents = std::fs::read_to_string(path)?;

        Ok(toml::from_str(&config_file_contents)?)
    }
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

    pub fn initialize_modules(self) -> anyhow::Result<InitializedAppContext> {
        let engine = Arc::new(Engine::default());

        let initialized_modules: Result<
            HashMap<String, InitializedModule<WasmModuleStore, ModuleRuntimeConfig>>,
            _,
        > = self
            .modules
            .into_iter()
            .map(
                |(module_name, module)| -> anyhow::Result<(
                    String,
                    InitializedModule<WasmModuleStore, ModuleRuntimeConfig>,
                )> {
                    let mut linker = Linker::<WasmModuleStore>::new(&engine);

                    let compiled_module = Module::from_binary(&engine, &module.bytes)?;

                    mqtt_api::add_to_linker(&mut linker, |s| &mut s.mqtt)?;

                    Ok((
                        module_name,
                        InitializedModule::<WasmModuleStore, ModuleRuntimeConfig> {
                            module: compiled_module,
                            linker,
                            engine: engine.clone(),
                            runtime_config: module.runtime_config,
                        },
                    ))
                },
            )
            .collect();

        Ok(InitializedAppContext {
            modules: initialized_modules?,
            module_threads: HashMap::new(),
        })
    }
}

impl InitializedAppContext {
    pub fn executing_modules(&self) -> usize {
        self.module_threads.len()
    }

    pub fn cleanup_finished_modules(&mut self) -> Vec<(String, anyhow::Result<()>)> {
        self.module_threads
            .drain_filter(|_module_name, module_thread_handle| module_thread_handle.is_finished())
            .flat_map(
                |(module_name, module_join_handle)| -> std::thread::Result<(String, anyhow::Result<()>)> {
                    Ok((module_name, module_join_handle.join()?))
                },
            )
            .collect()
    }

    pub fn join_modules(&mut self) -> Vec<(String, anyhow::Result<()>)> {
        let mut module_threads = HashMap::new();
        std::mem::swap(&mut self.module_threads, &mut module_threads);

        module_threads
            .into_iter()
            .map(|(module_name, module_join_handle)| {
                (module_name, module_join_handle.join().unwrap())
            })
            .collect()
    }

    pub fn run_all_modules(&mut self) -> anyhow::Result<()> {
        let modules_not_running: Vec<(
            &String,
            &InitializedModule<WasmModuleStore, ModuleRuntimeConfig>,
        )> = self
            .modules
            .iter()
            .filter(|(module_name, _)| !self.module_threads.contains_key(*module_name))
            .collect();

        for (module_name, module) in modules_not_running {
            let module = module.clone();
            self.module_threads.insert(
                module_name.clone(),
                std::thread::spawn(move || -> anyhow::Result<()> {
                    let mut store = Store::new(
                        &module.engine,
                        WasmModuleStore::new(&module.runtime_config)?,
                    );
                    let instance = module.linker.instantiate(&mut store, &module.module)?;
                    let wasm_entrypoint =
                        instance.get_typed_func::<(), (), _>(&mut store, "start")?;

                    wasm_entrypoint.call(&mut store, ())?;

                    Ok(())
                }),
            );
        }

        Ok(())
    }
}
