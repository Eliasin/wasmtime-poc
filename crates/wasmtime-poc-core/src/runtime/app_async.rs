use anyhow::bail;
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use rand::{rngs::OsRng, RngCore};
use std::{collections::HashMap, sync::Arc, time::Duration};
use tokio::task::JoinError;
use wasmtime::{
    component::{Component, Linker},
    Config, Engine, Store,
};

pub use async_module_runtime::*;
mod async_module_runtime;
pub use mqtt_runtime::*;
mod mqtt_runtime;

pub const APP_ASYNC_DEBUG_TARGET: &str = "wasmtime_poc_core::runtime::app_async";

use crate::api::{debug_async_api, env_async_api, fio_async_api, mqtt_async_api, util_async_api};

use super::{
    initialize_fio_for_module, AsyncWasmModuleStore, InitializedModule, ModuleRuntimeConfig,
    SharedMqttRuntimeConfig, UninitializedAppContext, UninitializedModule,
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

    async fn start_module(
        engine: &Engine,
        module_name: &str,
        module_template: &InitializedModule<AsyncWasmModuleStore, ModuleRuntimeConfig>,
        mqtt_runtimes: &mut MqttRuntimes,
    ) -> anyhow::Result<tokio::task::JoinHandle<AsyncModuleRuntime>> {
        let module_instance_id = OsRng.next_u64();
        let module_name_string = module_name.to_string();

        let mut mqtt_connection = None;

        log::debug!(
            "Starting module {} instance id {}",
            module_name,
            module_instance_id
        );

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
                    mqtt_connection = Some(connection);
                }
                Err(e) => {
                    bail!(
                        "Error starting mqtt runtime for module '{}': {}",
                        module_name,
                        e
                    )
                }
            }
        }

        let mut fio = None;
        if let Some(fio_runtime) = initialize_fio_for_module(&module_template.runtime_config) {
            match fio_runtime {
                Ok(fio_runtime) => {
                    fio = Some(fio_runtime.fio);
                }
                Err(e) => log::error!(
                    "Error starting File IO runtime for module '{}': {}",
                    module_name,
                    e
                ),
            }
        }

        let env = module_template.runtime_config.env.clone();

        let mut store = Store::new(
            engine,
            AsyncWasmModuleStore {
                mqtt_connection,
                fio,
                env,
            },
        );

        store.epoch_deadline_async_yield_and_update(10);
        let (exports, _) =
            Apis::instantiate_async(&mut store, &module_template.module, &module_template.linker)
                .await?;

        Ok(tokio::spawn(async move {
            if let Err(err) = exports.start(&mut store).await {
                log::warn!("Trap occurred in WASM module task, {:?}", err);
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
            std::thread::sleep(Duration::from_millis(5));

            thread_engine_arc.increment_epoch();
        });

        let mut executing_modules: FuturesUnordered<tokio::task::JoinHandle<AsyncModuleRuntime>> =
            FuturesUnordered::new();

        for (module_name, module_data) in startup_modules {
            executing_modules.push(
                InitializedAsyncAppContext::start_module(
                    &self.engine,
                    module_name,
                    module_data,
                    &mut self.mqtt_runtimes,
                )
                .await?,
            );
        }

        while let Some(async_module_runtime) = executing_modules.next().await {
            self.handle_module_join(async_module_runtime).await;
        }

        self.mqtt_runtimes.cleanup().await;
        Ok(())
    }
}
