use anyhow::bail;
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use rand::{rngs::OsRng, RngCore};
use std::{collections::HashMap, sync::Arc, time::Duration};
use tokio::{sync::mpsc, task::JoinError};
use wasmtime::{
    component::{Component, Linker},
    Config, Engine, Store,
};

pub use async_module_runtime::*;
mod async_module_runtime;
pub use mqtt_runtime::*;
mod mqtt_runtime;

pub const APP_ASYNC_DEBUG_TARGET: &str = "wasmtime_poc_core::runtime::app_async";

use crate::api::{
    debug_async_api, env_async_api,
    fio_async_api::{self, FileIOState},
    mqtt_async_api::{self, MqttConnection},
    spawn_async_api::{self, SpawnState},
    util_async_api,
};

use super::{
    AsyncWasmModuleStore, InitializedModule, ModuleRuntimeConfig, SharedMqttRuntimeConfig,
    SpawnRequest, UninitializedAppContext, UninitializedModule,
};

wasmtime::component::bindgen!({
    path: "../../wit-bindgen/apis.wit",
    async: true,
});

pub struct InitializedAsyncAppContext {
    modules: HashMap<String, InitializedModule<AsyncWasmModuleStore, ModuleRuntimeConfig>>,
    engine: Arc<Engine>,
    shared_mqtt_runtime_configs: Vec<SharedMqttRuntimeConfig>,
    mqtt_runtimes: MqttRuntimes,
}

impl UninitializedAppContext {
    fn initialize_module(
        engine: &Engine,
        module_name: String,
        module: UninitializedModule<ModuleRuntimeConfig>,
    ) -> anyhow::Result<(
        String,
        InitializedModule<AsyncWasmModuleStore, ModuleRuntimeConfig>,
    )> {
        let mut linker = Linker::<AsyncWasmModuleStore>::new(engine);

        let compiled_module = Component::from_binary(engine, &module.bytes)?;

        mqtt_async_api::add_to_linker(&mut linker, |s| &mut s.mqtt_connection)?;
        debug_async_api::add_to_linker(&mut linker, |s| s)?;
        env_async_api::add_to_linker(&mut linker, |s| s)?;
        util_async_api::add_to_linker(&mut linker, |s| s)?;
        fio_async_api::add_to_linker(&mut linker, |s| &mut s.fio)?;
        spawn_async_api::add_to_linker(&mut linker, |s| &mut s.spawn)?;

        Ok((
            module_name,
            InitializedModule::<AsyncWasmModuleStore, ModuleRuntimeConfig> {
                module: compiled_module,
                linker,
                runtime_config: module.runtime_config,
            },
        ))
    }

    pub fn async_initialize_modules(self) -> anyhow::Result<InitializedAsyncAppContext> {
        let mut engine_config = Config::new();
        engine_config
            .async_support(true)
            .epoch_interruption(true)
            .wasm_component_model(true);

        let engine = Arc::new(Engine::new(&engine_config)?);

        let initialized_modules: Result<
            HashMap<String, InitializedModule<AsyncWasmModuleStore, ModuleRuntimeConfig>>,
            _,
        > = self
            .modules
            .into_iter()
            .map(|(module_name, module)| {
                UninitializedAppContext::initialize_module(&engine, module_name, module)
            })
            .collect();

        Ok(InitializedAsyncAppContext {
            modules: initialized_modules?,
            engine,
            mqtt_runtimes: Default::default(),
            shared_mqtt_runtime_configs: self.app_config.shared_mqtt_runtimes,
        })
    }
}

impl InitializedAsyncAppContext {
    async fn cleanup_finished_module(
        &mut self,
        async_module_runtime: AsyncModuleRuntime,
    ) -> anyhow::Result<()> {
        log::debug!(
            "Starting cleanup for module {} instance id {}",
            async_module_runtime.module_name,
            async_module_runtime.module_instance_id
        );

        let AsyncModuleRuntime {
            store,
            module_instance_id,
            module_name,
        } = async_module_runtime;

        let AsyncWasmModuleStore {
            mqtt_connection,
            fio: _,
            env: _,
            spawn: _,
        } = store.into_data();

        if let Some(mqtt_connection) = mqtt_connection {
            log::debug!(
                "Module {module_name} instance {module_instance_id} has mqtt connection that requires cleanup",
            );

            if let Err(e) = self
                .mqtt_runtimes
                .cleanup_module(mqtt_connection, module_instance_id)
                .await
            {
                log::error!(
                    "Module {module_name} instance {module_instance_id} experienced error in mqtt cleanup: {e}",
                );
            } else {
                log::debug!(
                    "Module {module_name} instance {module_instance_id} finished mqtt cleanup",
                );
            }
        }

        log::debug!(
            "Module {} instance {} finished cleanup, dropping store",
            module_name,
            module_instance_id
        );

        log::debug!(
            "Module {} instance {} finished cleanup and store sucessfully droppped",
            module_name,
            module_instance_id
        );
        Ok(())
    }

    async fn handle_module_join(
        &mut self,
        async_module_runtime: anyhow::Result<AsyncModuleRuntime, JoinError>,
    ) {
        match async_module_runtime {
            Ok(async_module_runtime) => {
                if let Err(e) = self.cleanup_finished_module(async_module_runtime).await {
                    log::error!("Error in async module cleanup: {e}")
                }
            }
            Err(e) => log::error!("Error detected in async module execution, cleanup not run: {e}"),
        }
    }

    async fn create_mqtt_runtime_for_module(
        module_name: &str,
        module_instance_id: ModuleInstanceId,
        module_template: &InitializedModule<AsyncWasmModuleStore, ModuleRuntimeConfig>,
        mqtt_runtimes: &mut MqttRuntimes,
    ) -> anyhow::Result<Option<MqttConnection>> {
        if let Some(mqtt_config) = &module_template.runtime_config.mqtt {
            match mqtt_runtimes
                .create_runtime_for_module(module_instance_id, mqtt_config)
                .await
            {
                Ok(connection) => {
                    log::debug!(
                        "Created mqtt runtime for module {} instance {}",
                        module_name,
                        module_instance_id
                    );
                    Ok(Some(connection))
                }
                Err(e) => {
                    bail!(
                        "Error starting mqtt runtime for module '{}': {}",
                        module_name,
                        e
                    )
                }
            }
        } else {
            Ok(None)
        }
    }

    async fn start_module(
        engine: &Engine,
        module_name: &str,
        module_template: &InitializedModule<AsyncWasmModuleStore, ModuleRuntimeConfig>,
        mqtt_runtimes: &mut MqttRuntimes,
        spawn_request_sender: mpsc::Sender<SpawnRequest>,
        arg: Option<Vec<u8>>,
    ) -> anyhow::Result<tokio::task::JoinHandle<AsyncModuleRuntime>> {
        let module_instance_id = OsRng.next_u64();
        let module_name_string = module_name.to_string();

        log::debug!(
            "Starting module {} instance id {}",
            module_name,
            module_instance_id
        );

        let mqtt_connection = Self::create_mqtt_runtime_for_module(
            module_name,
            module_instance_id,
            module_template,
            mqtt_runtimes,
        )
        .await?;
        let fio = module_template
            .runtime_config
            .fio
            .as_ref()
            .map(FileIOState::from_config);

        let env = module_template.runtime_config.env.clone();

        let spawn =
            SpawnState::from_config(&module_template.runtime_config.spawn, spawn_request_sender);

        let mut store = Store::new(
            engine,
            AsyncWasmModuleStore {
                mqtt_connection,
                fio,
                env,
                spawn,
            },
        );

        store.epoch_deadline_async_yield_and_update(10);
        let (exports, _) =
            Apis::instantiate_async(&mut store, &module_template.module, &module_template.linker)
                .await?;

        Ok(tokio::spawn(async move {
            if let Err(err) = exports.call_start(&mut store, arg.as_deref()).await {
                log::warn!("Trap or error occurred in WASM module task, {:?}", err);
            };

            AsyncModuleRuntime {
                store,
                module_instance_id,
                module_name: module_name_string,
            }
        }))
    }

    pub async fn start(mut self) -> anyhow::Result<()> {
        self.mqtt_runtimes
            .start_shared_runtimes_from_configs(&self.shared_mqtt_runtime_configs)
            .await;

        let startup_modules = self
            .modules
            .iter_mut()
            .filter(|(_, module_data)| module_data.runtime_config.on_startup);

        let thread_engine_arc = self.engine.clone();
        std::thread::spawn(move || loop {
            std::thread::sleep(Duration::from_millis(50));

            thread_engine_arc.increment_epoch();
        });

        let mut executing_modules: FuturesUnordered<tokio::task::JoinHandle<AsyncModuleRuntime>> =
            FuturesUnordered::new();

        let (spawn_request_sender, mut spawn_request_receiver) = mpsc::channel(32);

        for (module_name, module) in startup_modules {
            executing_modules.push(
                Self::start_module(
                    &self.engine,
                    module_name,
                    module,
                    &mut self.mqtt_runtimes,
                    spawn_request_sender.clone(),
                    None,
                )
                .await?,
            );
        }

        loop {
            tokio::select! {
                async_module_runtime = executing_modules.next() => {
                    if let Some(async_module_runtime) = async_module_runtime {
                        self.handle_module_join(async_module_runtime).await;
                    } else {
                        break;
                    }
                },
                spawn_request = spawn_request_receiver.recv() => {
                    let Some(SpawnRequest { module_name, arg }) = spawn_request else {
                       log::warn!("Spawn request channel closed");
                        continue;
                    };

                    let Some((module_name, module)) =
                        self
                        .modules
                        .iter_mut()
                        .find(|(name, _)| **name == module_name) else {
                       log::warn!("Spawn request for unknown module {module_name}");
                        continue;
                    };

                    executing_modules.push(
                        Self::start_module(
                            &self.engine,
                            module_name,
                            module,
                            &mut self.mqtt_runtimes,
                            spawn_request_sender.clone(),
                            arg
                        )
                        .await?
                    );
                }
            }
        }

        self.mqtt_runtimes.cleanup().await;
        Ok(())
    }
}
