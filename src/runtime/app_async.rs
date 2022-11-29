use std::{collections::HashMap, sync::Arc, time::Duration};

use futures::{stream::FuturesUnordered, StreamExt};
use tokio::sync::mpsc;
use wasmtime::{
    component::{Component, Linker},
    Config, Engine, Store,
};

use crate::api::{debug_async_api, env_async_api, mqtt_async_api, util_async_api};

wit_bindgen_host_wasmtime_rust::generate!({
    path: "./wit-bindgen/apis.wit",
    async: true,
});

use super::{
    async_mqtt_event_loop_task, initialize_async_mqtt_for_module, initialize_fio_for_module,
    AsyncMqttRuntime, AsyncWasmModuleStore, InitializedModule, ModuleRuntimeConfig, RuntimeEvent,
    UninitializedAppContext,
};

struct AsyncMqttEventLoopTask {
    runtime_event_sender: tokio::sync::mpsc::Sender<RuntimeEvent>,
    task_handle: tokio::task::JoinHandle<anyhow::Result<()>>,
}

struct AsyncModuleRuntime {
    store: Store<AsyncWasmModuleStore>,
    module_mqtt_event_loop_task_info: Option<AsyncMqttEventLoopTask>,
}

pub struct InitializedAsyncAppContext {
    modules: HashMap<String, InitializedModule<AsyncWasmModuleStore, ModuleRuntimeConfig>>,
    engine: Arc<Engine>,
}

impl UninitializedAppContext {
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
            .map(
                |(module_name, module)| -> anyhow::Result<(
                    String,
                    InitializedModule<AsyncWasmModuleStore, ModuleRuntimeConfig>,
                )> {
                    let mut linker = Linker::<AsyncWasmModuleStore>::new(&engine);

                    let compiled_module = Component::from_binary(&engine, &module.bytes)?;

                    mqtt_async_api::add_to_linker(&mut linker, |s| &mut s.mqtt_connection)?;
                    debug_async_api::add_to_linker(&mut linker, |s| s)?;
                    env_async_api::add_to_linker(&mut linker, |s| s)?;
                    util_async_api::add_to_linker(&mut linker, |s| s)?;

                    Ok((
                        module_name,
                        InitializedModule::<AsyncWasmModuleStore, ModuleRuntimeConfig> {
                            module: compiled_module,
                            linker,
                            runtime_config: module.runtime_config,
                        },
                    ))
                },
            )
            .collect();

        Ok(InitializedAsyncAppContext {
            modules: initialized_modules?,
            engine: engine.clone(),
        })
    }
}

fn create_async_mqtt_event_loop_task(
    event_loop: rumqttc::EventLoop,
    event_channel_sender: mpsc::Sender<rumqttc::Event>,
) -> AsyncMqttEventLoopTask {
    let (mqtt_event_loop_runtime_sender, mqtt_event_loop_runtime_receiver) = mpsc::channel(32);

    let mqtt_event_loop_task_handle = tokio::spawn(async move {
        async_mqtt_event_loop_task(
            event_channel_sender,
            mqtt_event_loop_runtime_receiver,
            event_loop,
        )
        .await
    });

    AsyncMqttEventLoopTask {
        runtime_event_sender: mqtt_event_loop_runtime_sender,
        task_handle: mqtt_event_loop_task_handle,
    }
}

impl InitializedAsyncAppContext {
    async fn cleanup_finished_module(
        &mut self,
        async_module_runtime: AsyncModuleRuntime,
    ) -> anyhow::Result<()> {
        if let Some(mqtt_connection) = &async_module_runtime.store.data().mqtt_connection {
            if let Err(e) = mqtt_connection.disconnect().await {
                log::error!("Error disconnecting MQTT client: {}", e);
            }
        }

        if let Some(mqtt_event_loop_task_info) =
            async_module_runtime.module_mqtt_event_loop_task_info
        {
            mqtt_event_loop_task_info
                .runtime_event_sender
                .send(RuntimeEvent::RuntimeTaskStop)
                .await?;

            if let Err(e) = mqtt_event_loop_task_info.task_handle.await? {
                log::error!("Error waiting on event loop task to finish: {}", e);
            }
        }

        drop(async_module_runtime.store);
        Ok(())
    }

    async fn start_module(
        engine: &Engine,
        module_name: &str,
        module_template: &InitializedModule<AsyncWasmModuleStore, ModuleRuntimeConfig>,
    ) -> anyhow::Result<tokio::task::JoinHandle<AsyncModuleRuntime>> {
        let mut mqtt_connection = None;
        let mut module_mqtt_event_loop_task_info = None;

        if let Some(mqtt_runtime) =
            initialize_async_mqtt_for_module(&module_template.runtime_config)
        {
            match mqtt_runtime {
                Ok(AsyncMqttRuntime {
                    mqtt,
                    event_loop,
                    event_channel_sender,
                }) => {
                    mqtt_connection = Some(mqtt);

                    module_mqtt_event_loop_task_info = Some(create_async_mqtt_event_loop_task(
                        event_loop,
                        event_channel_sender,
                    ));
                }
                Err(e) => log::error!(
                    "Error starting MQTT runtime for module '{}': {}",
                    module_name,
                    e
                ),
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
            &engine,
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
                module_mqtt_event_loop_task_info,
            }
        }))
    }

    pub async fn start(&mut self) -> anyhow::Result<()> {
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
                InitializedAsyncAppContext::start_module(&self.engine, module_name, &module_data)
                    .await?,
            );
        }

        while let Some(async_module_runtime) = executing_modules.next().await {
            self.cleanup_finished_module(async_module_runtime?).await?;
        }

        Ok(())
    }
}
