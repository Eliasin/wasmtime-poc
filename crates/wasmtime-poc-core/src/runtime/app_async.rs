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

pub const APP_ASYNC_DEBUG_TARGET: &str = "wasmtime_poc_core::runtime::app_async";

use crate::api::{
    debug_async_api, env_async_api, fio_async_api,
    mqtt_async_api::{
        self, AsyncMqttConnection, LockSharedAsyncMqttConnection,
        MessageBusSharedAsyncMqttConnection, MqttClientAction,
    },
    util_async_api,
};

use super::{
    create_async_mqtt_runtime, initialize_fio_for_module, AsyncWasmModuleStore, InitializedModule,
    ModuleRuntimeConfig, MqttFlavor, RuntimeEvent, SharedMqttRuntimeConfig, SharedMqttRuntimeId,
    UninitializedAppContext, UninitializedModule,
};

wasmtime::component::bindgen!({
    path: "../../wit-bindgen/apis.wit",
    async: true,
});

pub struct SharedLockEventLoop {
    pub(crate) mqtt_client: rumqttc::AsyncClient,
    pub(crate) module_event_sender: tokio::sync::mpsc::Sender<SharedMqttModuleEvent>,
    pub(crate) runtime_event_sender: tokio::sync::mpsc::Sender<RuntimeEvent>,
    pub(crate) task_handle: tokio::task::JoinHandle<anyhow::Result<()>>,
}

pub struct SharedMessageBusEventLoop {
    pub(crate) mqtt_client_action_sender: tokio::sync::mpsc::Sender<MqttClientAction>,
    pub(crate) module_event_sender: tokio::sync::mpsc::Sender<SharedMqttModuleEvent>,
    pub(crate) runtime_event_sender: tokio::sync::mpsc::Sender<RuntimeEvent>,
    pub(crate) task_handle: tokio::task::JoinHandle<anyhow::Result<()>>,
}

pub enum SharedMqttEventLoop {
    SharedLock(SharedLockEventLoop),
    SharedMessageBus(SharedMessageBusEventLoop),
}

impl SharedMqttEventLoop {
    pub fn flavor(&self) -> MqttFlavor {
        match self {
            SharedMqttEventLoop::SharedLock(_) => MqttFlavor::SharedLock,
            SharedMqttEventLoop::SharedMessageBus(_) => MqttFlavor::SharedMessageBus,
        }
    }
}

pub struct InitializedAsyncAppContext {
    modules: HashMap<String, InitializedModule<AsyncWasmModuleStore, ModuleRuntimeConfig>>,
    engine: Arc<Engine>,
    shared_mqtt_runtime_configs: Vec<SharedMqttRuntimeConfig>,
    shared_mqtt_event_loops: HashMap<SharedMqttRuntimeId, SharedMqttEventLoop>,
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
            shared_mqtt_event_loops: HashMap::new(),
            shared_mqtt_runtime_configs: self.app_config.shared_mqtt_runtimes,
        })
    }
}

impl InitializedAsyncAppContext {
    async fn cleanup_shared_lock_mqtt(
        module_instance_id: ModuleInstanceId,
        connection: &LockSharedAsyncMqttConnection,
        shared_mqtt_event_loop: &SharedLockEventLoop,
    ) -> anyhow::Result<()> {
        let SharedLockEventLoop {
            mqtt_client: _,
            module_event_sender,
            runtime_event_sender: _,
            task_handle: _,
        } = shared_mqtt_event_loop;

        if let Err(e) = module_event_sender
            .send(SharedMqttModuleEvent::ModuleFinished {
                id: module_instance_id,
            })
            .await
        {
            log::error!(
                "Error sending module finish event to
                                        shared mqtt runtime {} while cleaning up
                                        module instance {}: {}",
                connection.runtime_id(),
                module_instance_id,
                e
            );
        }

        Ok(())
    }

    async fn cleanup_shared_message_bus_mqtt(
        module_instance_id: ModuleInstanceId,
        connection: &MessageBusSharedAsyncMqttConnection,
        shared_mqtt_event_loop: &SharedMessageBusEventLoop,
    ) -> anyhow::Result<()> {
        let SharedMessageBusEventLoop {
            mqtt_client_action_sender: _,
            module_event_sender,
            runtime_event_sender: _,
            task_handle: _,
        } = shared_mqtt_event_loop;

        if let Err(e) = module_event_sender
            .send(SharedMqttModuleEvent::ModuleFinished {
                id: module_instance_id,
            })
            .await
        {
            log::error!(
                "Error sending module finish event to
                                        shared mqtt runtime {} while cleaning up
                                        module instance {}: {}",
                connection.runtime_id(),
                module_instance_id,
                e
            );
        }

        Ok(())
    }

    async fn cleanup_module_mqtt_connection(
        &mut self,
        module_instance_id: ModuleInstanceId,
        mqtt_connection: &AsyncMqttConnection,
    ) -> anyhow::Result<()> {
        let shared_runtime_event_loop = mqtt_connection.runtime_id().and_then(|runtime_id| {
            self.shared_mqtt_event_loops
                .iter_mut()
                .find(|(event_loop_runtime_id, _)| **event_loop_runtime_id == runtime_id)
        });

        match mqtt_connection {
            AsyncMqttConnection::MessageBusShared(connection) => {
                match shared_runtime_event_loop {
                    Some((_, shared_runtime_event_loop)) => {
                        match shared_runtime_event_loop {
                            SharedMqttEventLoop::SharedLock(_) => bail!("Expected runtime {} to be message bus flavor but it is shared lock", connection.runtime_id()),
                            SharedMqttEventLoop::SharedMessageBus(event_loop) => Self::cleanup_shared_message_bus_mqtt(module_instance_id, connection, event_loop).await?,
                        }
                    },
                    None => bail!("Failed to find runtime matching module runtime id {} during cleanup of module instance id {}", connection.runtime_id(), module_instance_id),
                }
            }
            AsyncMqttConnection::LockShared(connection) => {
                match shared_runtime_event_loop {
                    Some((_, shared_runtime_event_loop)) => {
                        match shared_runtime_event_loop {
                            SharedMqttEventLoop::SharedLock(event_loop) => Self::cleanup_shared_lock_mqtt(module_instance_id, connection, event_loop).await?,
                            SharedMqttEventLoop::SharedMessageBus(_) => bail!("Expected runtime {} to be shared lock flavor but it is message bus", connection.runtime_id()),
                        }
                    },
                    None => bail!("Failed to find runtime matching module runtime id {} during cleanup of module instance id {}", connection.runtime_id(), module_instance_id),
                }
            }
            AsyncMqttConnection::Instanced(connection) => {
                if let Err(e) = connection.disconnect().await {
                    log::error!("Error disconnecting MQTT client: {}", e);
                }
            }
        }

        Ok(())
    }

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
            module_mqtt_event_loop_task_info,
            module_instance_id,
            module_name,
        } = async_module_runtime;

        if let Some(mqtt_connection) = &store.data().mqtt_connection {
            log::debug!(
                "Module {module_name} instance {module_instance_id} has mqtt connection that requires cleanup",
            );

            if let Err(e) = self
                .cleanup_module_mqtt_connection(
                    async_module_runtime.module_instance_id,
                    mqtt_connection,
                )
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

        if let Some(AsyncMqttEventLoopTask::Instanced(mqtt_event_loop_task_info)) =
            module_mqtt_event_loop_task_info
        {
            log::debug!(
                "Module {} instance {} has instanced mqtt event loop that must be shut down, sending stop message",
                module_name,
                async_module_runtime.module_instance_id
            );
            mqtt_event_loop_task_info
                .runtime_event_sender
                .send(RuntimeEvent::RuntimeTaskStop)
                .await?;

            if let Err(e) = mqtt_event_loop_task_info.task_handle.await? {
                log::error!("Error waiting on event loop task to finish: {}", e);
            } else {
                log::debug!(
                    "Module {} instance {} instanced mqtt event loop sucessfully shut down",
                    module_name,
                    module_instance_id
                );
            }
        }

        log::debug!(
            "Module {} instance {} finished cleanup, dropping store",
            module_name,
            module_instance_id
        );

        drop(store);

        log::debug!(
            "Module {} instance {} finished cleanup and store sucessfully droppped",
            module_name,
            module_instance_id
        );
        Ok(())
    }

    fn start_shared_lock_event_loop(
        runtime_id: String,
        client_id: String,
        host: String,
        port: u16,
    ) -> anyhow::Result<(SharedMqttRuntimeId, SharedMqttEventLoop)> {
        let mut mqtt_options = rumqttc::MqttOptions::new(client_id, host, port);
        mqtt_options.set_keep_alive(Duration::from_secs(5));

        let (client, event_loop) = rumqttc::AsyncClient::new(mqtt_options, 10);

        let (module_event_sender, module_event_receiver) = mpsc::channel(32);
        let (runtime_event_sender, runtime_event_receiver) = mpsc::channel(32);

        let runtime_id_cloned = runtime_id.clone();
        let mqtt_event_loop_task_handle = tokio::spawn(async move {
            async_shared_lock_mqtt_event_loop_task(
                module_event_receiver,
                runtime_event_receiver,
                event_loop,
                runtime_id_cloned,
            )
            .await
        });

        log::debug!(
            "Done starting shared message bus mqtt event loop id {}",
            runtime_id
        );
        Ok((
            runtime_id,
            SharedMqttEventLoop::SharedLock(SharedLockEventLoop {
                mqtt_client: client,
                module_event_sender,
                runtime_event_sender,
                task_handle: mqtt_event_loop_task_handle,
            }),
        ))
    }

    fn start_shared_message_bus_event_loop(
        runtime_id: String,
        client_id: String,
        host: String,
        port: u16,
    ) -> anyhow::Result<(SharedMqttRuntimeId, SharedMqttEventLoop)> {
        let mut mqtt_options = rumqttc::MqttOptions::new(client_id, host, port);
        mqtt_options.set_keep_alive(Duration::from_secs(5));

        let (client, event_loop) = rumqttc::AsyncClient::new(mqtt_options, 10);

        let (module_event_sender, module_event_receiver) = mpsc::channel(32);
        let (runtime_event_sender, runtime_event_receiver) = mpsc::channel(32);

        let (mqtt_client_action_sender, mqtt_client_action_receiver) = mpsc::channel(32);

        let runtime_id_cloned = runtime_id.clone();
        let mqtt_event_loop_task_handle = tokio::spawn(async move {
            async_shared_message_bus_mqtt_event_loop_task(
                module_event_receiver,
                runtime_event_receiver,
                mqtt_client_action_receiver,
                client,
                event_loop,
                runtime_id_cloned,
            )
            .await
        });

        log::debug!(
            "Done starting shared message bus mqtt event loop id {}",
            runtime_id
        );
        Ok((
            runtime_id,
            SharedMqttEventLoop::SharedMessageBus(SharedMessageBusEventLoop {
                mqtt_client_action_sender,
                module_event_sender,
                runtime_event_sender,
                task_handle: mqtt_event_loop_task_handle,
            }),
        ))
    }

    fn start_shared_mqtt_event_loop(
        config: &SharedMqttRuntimeConfig,
    ) -> anyhow::Result<(SharedMqttRuntimeId, SharedMqttEventLoop)> {
        use super::MqttFlavor::*;

        let SharedMqttRuntimeConfig {
            runtime_id,
            client_id,
            host,
            port,
            flavor,
        } = config;

        log::info!("Starting shared mqtt event loop id {}", runtime_id);
        match flavor {
            SharedMessageBus => Self::start_shared_message_bus_event_loop(
                runtime_id.clone(),
                client_id.clone(),
                host.clone(),
                *port,
            ),
            SharedLock => Self::start_shared_lock_event_loop(
                runtime_id.clone(),
                client_id.clone(),
                host.clone(),
                *port,
            ),
            Instanced => bail!(
                "Shared MQTT runtime config with id {} must be a 'shared' flavor",
                config.runtime_id
            ),
        }
    }

    fn start_shared_mqtt_event_loops(
        shared_mqtt_runtime_configs: &[SharedMqttRuntimeConfig],
    ) -> anyhow::Result<Vec<(SharedMqttRuntimeId, SharedMqttEventLoop)>> {
        shared_mqtt_runtime_configs
            .iter()
            .map(InitializedAsyncAppContext::start_shared_mqtt_event_loop)
            .collect()
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
        shared_mqtt_event_loops: &mut HashMap<SharedMqttRuntimeId, SharedMqttEventLoop>,
    ) -> anyhow::Result<tokio::task::JoinHandle<AsyncModuleRuntime>> {
        let module_instance_id = OsRng.next_u64();
        let module_name_string = module_name.to_string();

        let mut mqtt_connection = None;
        let mut module_mqtt_event_loop_task_info: Option<AsyncMqttEventLoopTask> = None;

        log::debug!(
            "Starting module {} instance id {}",
            module_name,
            module_instance_id
        );

        if let Some(mqtt_config) = &module_template.runtime_config.mqtt {
            match create_async_mqtt_runtime(
                module_instance_id,
                mqtt_config,
                shared_mqtt_event_loops,
            )
            .await
            {
                Ok((connection, event_loop_task_info)) => {
                    log::debug!(
                        "Created mqtt runtime for module {} instance {}",
                        module_name,
                        module_instance_id
                    );
                    mqtt_connection = Some(connection);
                    module_mqtt_event_loop_task_info = Some(event_loop_task_info);
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
                module_mqtt_event_loop_task_info,
                module_instance_id,
                module_name: module_name_string,
            }
        }))
    }

    async fn cleanup_shared_mqtt_event_loops(&mut self) -> anyhow::Result<()> {
        for (runtime_id, event_loop) in &mut self.shared_mqtt_event_loops {
            match event_loop {
                SharedMqttEventLoop::SharedLock(SharedLockEventLoop {
                    mqtt_client,
                    module_event_sender: _,
                    runtime_event_sender,
                    task_handle,
                }) => {
                    if let Err(e) = mqtt_client.disconnect().await {
                        log::error!(
                            "Error disconnecting mqtt event loop while cleaning up shared mqtt event loop with id {}: {}",
                            runtime_id,
                            e
                        )
                    } else {
                        log::debug!(
                            "Successfully disconnected event loop for runtime: {runtime_id}"
                        );
                    }
                    if let Err(e) = runtime_event_sender
                        .send(RuntimeEvent::RuntimeTaskStop)
                        .await
                    {
                        log::error!("Error sending runtime task stop while cleaning up shared mqtt event loop with id {}: {}", runtime_id, e)
                    }

                    if let Err(e) = task_handle.await {
                        log::error!("Error waiting on mqtt task handle while cleaning up shared mqtt event loop with id {}: {}", runtime_id, e)
                    }
                }
                SharedMqttEventLoop::SharedMessageBus(SharedMessageBusEventLoop {
                    mqtt_client_action_sender,
                    module_event_sender: _,
                    runtime_event_sender,
                    task_handle,
                }) => {
                    if let Err(e) = mqtt_client_action_sender
                        .send(MqttClientAction::Disconnect)
                        .await
                    {
                        log::error!(
                            "Error disconnecting mqtt event loop while cleaning up shared mqtt event loop with id {}: {}",
                            runtime_id,
                            e
                        )
                    } else {
                        log::debug!(
                            "Successfully disconnected event loop for runtime: {runtime_id}"
                        );
                    }

                    if let Err(e) = runtime_event_sender
                        .send(RuntimeEvent::RuntimeTaskStop)
                        .await
                    {
                        log::error!("Error sending runtime task stop while cleaning up shared mqtt event loop with id {}: {}", runtime_id, e)
                    }

                    if let Err(e) = task_handle.await {
                        log::error!("Error waiting on mqtt task handle while cleaning up shared mqtt event loop with id {}: {}", runtime_id, e)
                    }
                }
            }
        }

        Ok(())
    }

    pub async fn start(&mut self) -> anyhow::Result<()> {
        let shared_mqtt_event_loops = InitializedAsyncAppContext::start_shared_mqtt_event_loops(
            &self.shared_mqtt_runtime_configs.clone(),
        )?;
        self.shared_mqtt_event_loops.extend(shared_mqtt_event_loops);

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
                    &mut self.shared_mqtt_event_loops,
                )
                .await?,
            );
        }

        while let Some(async_module_runtime) = executing_modules.next().await {
            self.handle_module_join(async_module_runtime).await;
        }

        self.cleanup_shared_mqtt_event_loops().await?;

        Ok(())
    }
}

async fn handle_mqtt_client_action(
    client: &mut rumqttc::AsyncClient,
    client_action: MqttClientAction,
) -> anyhow::Result<()> {
    match client_action {
        MqttClientAction::Publish {
            topic,
            qos,
            retain,
            payload,
        } => client.publish(topic, qos, retain, payload).await?,
        MqttClientAction::Subscribe { topic, qos } => client.subscribe(topic, qos).await?,
        MqttClientAction::Disconnect => client.disconnect().await?,
    }

    Ok(())
}

fn handle_module_event(
    module_event_senders: &mut HashMap<ModuleInstanceId, tokio::sync::mpsc::Sender<rumqttc::Event>>,
    module_event: SharedMqttModuleEvent,
    runtime_id: &SharedMqttRuntimeId,
) {
    match module_event {
        SharedMqttModuleEvent::NewModule {
            id,
            module_mqtt_event_sender,
        } => {
            log::debug!(
                "New module instance id {} added to shared mqtt runtime {}",
                id,
                runtime_id
            );
            module_event_senders.insert(id, module_mqtt_event_sender);
        }
        SharedMqttModuleEvent::ModuleFinished { id } => {
            log::debug!(
                "Module instance id {} finished and is being removed from runtime {}",
                id,
                runtime_id
            );
            module_event_senders.remove(&id);
        }
    }
}

async fn async_shared_message_bus_mqtt_event_loop_task(
    mut module_event_receiver: tokio::sync::mpsc::Receiver<SharedMqttModuleEvent>,
    mut runtime_event_receiver: tokio::sync::mpsc::Receiver<RuntimeEvent>,
    mut mqtt_client_action_receiver: tokio::sync::mpsc::Receiver<MqttClientAction>,
    mut mqtt_client: rumqttc::AsyncClient,
    mut mqtt_event_loop: rumqttc::EventLoop,
    runtime_id: SharedMqttRuntimeId,
) -> anyhow::Result<()> {
    let mut module_event_senders: HashMap<
        ModuleInstanceId,
        tokio::sync::mpsc::Sender<rumqttc::Event>,
    > = HashMap::new();

    loop {
        tokio::select! {
            client_action = mqtt_client_action_receiver.recv() => {
                match client_action {
                    Some(client_action) => {
                        if let Err(e) = handle_mqtt_client_action(&mut mqtt_client, client_action).await {
                            log::error!("Error in shared mqtt runtime {runtime_id}: {e}")
                        }
                    },
                    None => {
                        bail!("Runtime module event channel unexpectedly closed")
                    }
                }
            },
            module_event = module_event_receiver.recv() => {
                match module_event {
                    Some(module_event) => handle_module_event(&mut module_event_senders, module_event, &runtime_id),
                    None => {
                        bail!("Runtime module event channel unexpectedly closed")
                    },
                }
            },
            notification = mqtt_event_loop.poll() => {
                match notification {
                    Ok(notification) => {
                        for module_event_sender in module_event_senders.values_mut() {
                            if module_event_sender.send(notification.clone()).await.is_err() {
                                log::debug!("Error sending mqtt event to module event channel, module is probably awaiting cleanup")
                            }
                        }
                    },
                    Err(e) => {
                        log::warn!("Error in shared mqtt runtime {runtime_id}, runtime may be in middle of cleanup?: {e}");
                    }
                }
            },
            runtime_event = runtime_event_receiver.recv() => {
                match runtime_event {
                    None => {
                        bail!("Runtime event channel unexpectedly closed")
                    },
                    Some(runtime_event) => match runtime_event {
                        RuntimeEvent::RuntimeTaskStop => {
                            break
                        },
                    }
                }
            },
        }
    }

    log::info!(
        "MQTT shared message bus event loop id {} stopping",
        runtime_id
    );
    Ok(())
}

async fn async_shared_lock_mqtt_event_loop_task(
    mut module_event_receiver: tokio::sync::mpsc::Receiver<SharedMqttModuleEvent>,
    mut runtime_event_receiver: tokio::sync::mpsc::Receiver<RuntimeEvent>,
    mut mqtt_event_loop: rumqttc::EventLoop,
    runtime_id: SharedMqttRuntimeId,
) -> anyhow::Result<()> {
    let mut module_event_senders: HashMap<
        ModuleInstanceId,
        tokio::sync::mpsc::Sender<rumqttc::Event>,
    > = HashMap::new();

    loop {
        tokio::select! {
            module_event = module_event_receiver.recv() => {
                match module_event {
                    Some(module_event) => {
                        match module_event {
                            SharedMqttModuleEvent::NewModule { id, module_mqtt_event_sender } => {
                                log::debug!("New module instance id {} added to shared mqtt runtime {}", id, runtime_id);
                                module_event_senders.insert(id, module_mqtt_event_sender);
                            },
                            SharedMqttModuleEvent::ModuleFinished { id } => {
                                log::debug!("Module instance id {} finished and is being removed from runtime {}", id, runtime_id);
                                module_event_senders.remove(&id);
                            },
                        }
                    },
                    None => {
                        bail!("Runtime module event channel unexpectedly closed")
                    },
                }
            },
            notification = mqtt_event_loop.poll() => {
                match notification {
                    Ok(notification) => {
                        for module_event_sender in module_event_senders.values_mut() {
                            if module_event_sender.send(notification.clone()).await.is_err() {
                                log::debug!("Error sending mqtt event to module event channel, module is probably awaiting cleanup")
                            }
                        }
                    },
                    Err(e) => {
                        log::warn!("Error in shared mqtt runtime {runtime_id}, runtime may be in middle of cleanup?: {e}");
                    }
                }
            },
            runtime_event = runtime_event_receiver.recv() => {
                match runtime_event {
                    None => {
                        bail!("Runtime event channel unexpectedly closed")
                    },
                    Some(runtime_event) => match runtime_event {
                        RuntimeEvent::RuntimeTaskStop => {
                            break
                        },
                    }
                }
            },
        }
    }

    log::info!(
        "MQTT shared message bus event loop id {} stopping",
        runtime_id
    );
    Ok(())
}
