use std::{collections::HashMap, time::Duration};

use anyhow::{bail, Context};
use enum_dispatch::enum_dispatch;
use tokio::sync::mpsc;

use crate::{
    api::mqtt_async_api::{
        AsyncMqttConnection, LockSharedAsyncMqttConnection, MessageBusSharedAsyncMqttConnection,
        MqttClientAction,
    },
    runtime::{
        AsyncMqttRuntime, MqttRuntimeConfig, SharedMqttFlavor, SharedMqttRuntimeConfig,
        SharedMqttRuntimeId,
    },
};

use super::{ModuleInstanceId, RuntimeEvent};

pub struct InstancedAsyncMqttEventLoop {
    pub(crate) runtime_event_sender: tokio::sync::mpsc::Sender<RuntimeEvent>,
    pub(crate) task_handle: tokio::task::JoinHandle<anyhow::Result<()>>,
}

impl InstancedAsyncMqttEventLoop {
    async fn event_loop_task(
        event_channel_sender: mpsc::Sender<rumqttc::Event>,
        mut runtime_event_receiver: mpsc::Receiver<RuntimeEvent>,
        mut event_loop: rumqttc::EventLoop,
    ) -> anyhow::Result<()> {
        // TODO: Exponential backoff retry for MQTT
        loop {
            tokio::select! {
                notification = event_loop.poll() => {
                    if let Err(e) = event_channel_sender.send(notification?).await {
                        bail!("Error sending MQTT notification to event channel: {}", e);
                    }
                },
                runtime_event = runtime_event_receiver.recv() => {
                    match runtime_event {
                        None => {
                            bail!("Runtime event channel unexpectedly closed");
                        },
                        Some(runtime_event) => match runtime_event {
                            RuntimeEvent::RuntimeTaskStop => return Ok(()),
                        }
                    }
                },
                else => { return Ok(()); },
            }
        }
    }

    pub fn new(
        event_loop: rumqttc::EventLoop,
        event_channel_sender: mpsc::Sender<rumqttc::Event>,
    ) -> Self {
        let (mqtt_event_loop_runtime_sender, mqtt_event_loop_runtime_receiver) = mpsc::channel(32);

        let mqtt_event_loop_task_handle = tokio::spawn(async move {
            Self::event_loop_task(
                event_channel_sender,
                mqtt_event_loop_runtime_receiver,
                event_loop,
            )
            .await
        });

        InstancedAsyncMqttEventLoop {
            runtime_event_sender: mqtt_event_loop_runtime_sender,
            task_handle: mqtt_event_loop_task_handle,
        }
    }
}

pub enum SharedMqttModuleEvent {
    NewModule {
        id: ModuleInstanceId,
        module_mqtt_event_sender: tokio::sync::mpsc::Sender<rumqttc::Event>,
    },
    ModuleFinished {
        id: ModuleInstanceId,
    },
}

#[enum_dispatch]
pub enum SharedMqttEventLoopEnum {
    SharedLockEventLoop,
    SharedMessageBusEventLoop,
}

#[enum_dispatch(SharedMqttEventLoopEnum)]
pub trait SharedMqttEventLoop
where
    Self: Sized,
{
    async fn cleanup(self, runtime_id: &SharedMqttRuntimeId);
    async fn create_module_runtime(
        &self,
        module_instance_id: ModuleInstanceId,
        runtime_id: String,
        allowed_sub_topics: &[String],
        allowed_pub_topics: &[String],
    ) -> anyhow::Result<AsyncMqttConnection>;
}

pub struct SharedLockEventLoop {
    pub(crate) mqtt_client: rumqttc::AsyncClient,
    pub(crate) module_event_sender: tokio::sync::mpsc::Sender<SharedMqttModuleEvent>,
    pub(crate) runtime_event_sender: tokio::sync::mpsc::Sender<RuntimeEvent>,
    pub(crate) task_handle: tokio::task::JoinHandle<anyhow::Result<()>>,
}

impl SharedLockEventLoop {
    pub fn start(
        runtime_id: String,
        client_id: String,
        host: String,
        port: u16,
    ) -> anyhow::Result<SharedMqttEventLoopEnum> {
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
        Ok(SharedLockEventLoop {
            mqtt_client: client,
            module_event_sender,
            runtime_event_sender,
            task_handle: mqtt_event_loop_task_handle,
        }
        .into())
    }
}

impl SharedMqttEventLoop for SharedLockEventLoop {
    async fn cleanup(self, runtime_id: &SharedMqttRuntimeId) {
        if let Err(e) = self.mqtt_client.disconnect().await {
            log::error!(
                            "Error disconnecting mqtt event loop while cleaning up shared mqtt event loop with id {}: {}",
                            runtime_id,
                            e
                        )
        } else {
            log::debug!("Successfully disconnected event loop for runtime: {runtime_id}");
        }
        if let Err(e) = self
            .runtime_event_sender
            .send(RuntimeEvent::RuntimeTaskStop)
            .await
        {
            log::error!("Error sending runtime task stop while cleaning up shared mqtt event loop with id {}: {}", runtime_id, e)
        }

        if let Err(e) = self.task_handle.await {
            log::error!("Error waiting on mqtt task handle while cleaning up shared mqtt event loop with id {}: {}", runtime_id, e)
        }
    }

    async fn create_module_runtime(
        &self,
        module_instance_id: ModuleInstanceId,
        runtime_id: String,
        allowed_sub_topics: &[String],
        allowed_pub_topics: &[String],
    ) -> anyhow::Result<AsyncMqttConnection> {
        let (mqtt_event_sender, mqtt_event_receiver) = tokio::sync::mpsc::channel(32);

        if let Err(e) = self
            .module_event_sender
            .send(SharedMqttModuleEvent::NewModule {
                id: module_instance_id,
                module_mqtt_event_sender: mqtt_event_sender,
            })
            .await
        {
            bail!(
                "Failed to send new module event to shared message bus shared mqtt runtime: {}",
                e
            )
        }

        Ok(AsyncMqttConnection::LockShared(
            LockSharedAsyncMqttConnection::new(
                self.mqtt_client.clone(),
                mqtt_event_receiver,
                allowed_sub_topics.to_vec(),
                allowed_pub_topics.to_vec(),
                runtime_id,
            ),
        ))
    }
}

pub struct SharedMessageBusEventLoop {
    pub(crate) mqtt_client_action_sender: tokio::sync::mpsc::Sender<MqttClientAction>,
    pub(crate) module_event_sender: tokio::sync::mpsc::Sender<SharedMqttModuleEvent>,
    pub(crate) runtime_event_sender: tokio::sync::mpsc::Sender<RuntimeEvent>,
    pub(crate) task_handle: tokio::task::JoinHandle<anyhow::Result<()>>,
}

impl SharedMessageBusEventLoop {
    pub fn start(
        runtime_id: String,
        client_id: String,
        host: String,
        port: u16,
    ) -> anyhow::Result<SharedMqttEventLoopEnum> {
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
        Ok(SharedMessageBusEventLoop {
            mqtt_client_action_sender,
            module_event_sender,
            runtime_event_sender,
            task_handle: mqtt_event_loop_task_handle,
        }
        .into())
    }
}

impl SharedMqttEventLoop for SharedMessageBusEventLoop {
    async fn create_module_runtime(
        &self,
        module_instance_id: ModuleInstanceId,
        runtime_id: String,
        allowed_sub_topics: &[String],
        allowed_pub_topics: &[String],
    ) -> anyhow::Result<AsyncMqttConnection> {
        let (mqtt_event_sender, mqtt_event_receiver) = tokio::sync::mpsc::channel(32);

        if let Err(e) = self
            .module_event_sender
            .send(SharedMqttModuleEvent::NewModule {
                id: module_instance_id,
                module_mqtt_event_sender: mqtt_event_sender,
            })
            .await
        {
            bail!(
                "Failed to send new module event to shared message bus shared mqtt runtime: {}",
                e
            )
        }

        Ok(AsyncMqttConnection::MessageBusShared(
            MessageBusSharedAsyncMqttConnection::new(
                self.mqtt_client_action_sender.clone(),
                mqtt_event_receiver,
                allowed_sub_topics.to_vec(),
                allowed_pub_topics.to_vec(),
                runtime_id,
            ),
        ))
    }

    async fn cleanup(self, runtime_id: &SharedMqttRuntimeId) {
        if let Err(e) = self
            .mqtt_client_action_sender
            .send(MqttClientAction::Disconnect)
            .await
        {
            log::error!(
                            "Error disconnecting mqtt event loop while cleaning up shared mqtt event loop with id {}: {}",
                            runtime_id,
                            e
                        )
        } else {
            log::debug!("Successfully disconnected event loop for runtime: {runtime_id}");
        }

        if let Err(e) = self
            .runtime_event_sender
            .send(RuntimeEvent::RuntimeTaskStop)
            .await
        {
            log::error!("Error sending runtime task stop while cleaning up shared mqtt event loop with id {}: {}", runtime_id, e)
        }

        if let Err(e) = self.task_handle.await {
            log::error!("Error waiting on mqtt task handle while cleaning up shared mqtt event loop with id {}: {}", runtime_id, e)
        }
    }
}

#[derive(Default)]
pub struct MqttRuntimes {
    shared_runtimes: HashMap<SharedMqttRuntimeId, SharedMqttEventLoopEnum>,
    instanced_runtimes: HashMap<ModuleInstanceId, InstancedAsyncMqttEventLoop>,
}

impl MqttRuntimes {
    fn start_shared_mqtt_event_loop(
        config: &SharedMqttRuntimeConfig,
    ) -> anyhow::Result<(SharedMqttRuntimeId, SharedMqttEventLoopEnum)> {
        use SharedMqttFlavor::*;

        let SharedMqttRuntimeConfig {
            runtime_id,
            client_id,
            host,
            port,
            flavor,
        } = config;

        log::info!("Starting shared mqtt event loop id {}", runtime_id);
        let event_loop = match flavor {
            MessageBus => SharedMessageBusEventLoop::start(
                runtime_id.clone(),
                client_id.clone(),
                host.clone(),
                *port,
            )
            .context("while starting runtime {runtime_id}")?,
            Lock => SharedLockEventLoop::start(
                runtime_id.clone(),
                client_id.clone(),
                host.clone(),
                *port,
            )
            .context("while starting runtime {runtime_id}")?,
        };

        Ok((runtime_id.clone(), event_loop))
    }

    pub async fn start_event_loops_from_configs(&mut self, configs: &[SharedMqttRuntimeConfig]) {
        for result in configs.iter().map(Self::start_shared_mqtt_event_loop) {
            match result {
                Ok((id, event_loop)) => {
                    self.shared_runtimes.insert(id, event_loop);
                }
                Err(e) => log::warn!("Error while starting event loop {e}"),
            }
        }
    }

    pub async fn create_runtime_for_module(
        &mut self,
        module_instance_id: ModuleInstanceId,
        mqtt_config: &MqttRuntimeConfig,
    ) -> anyhow::Result<AsyncMqttConnection> {
        match mqtt_config {
            MqttRuntimeConfig::Shared {
                runtime_id,
                allowed_pub_topics,
                allowed_sub_topics,
            } => {
                let runtime = self
                    .shared_runtimes
                    .iter()
                    .find(|(id, _)| *id == runtime_id);
                match runtime {
                    Some((_, shared_mqtt_runtime)) => {
                        shared_mqtt_runtime
                            .create_module_runtime(
                                module_instance_id,
                                runtime_id.clone(),
                                allowed_sub_topics,
                                allowed_pub_topics,
                            )
                            .await
                    }
                    None => bail!(
                        "Module requests runtime with id {} which is not defined",
                        runtime_id
                    ),
                }
            }
            MqttRuntimeConfig::Instanced {
                config: mqtt_config,
            } => {
                let AsyncMqttRuntime {
                    mqtt,
                    event_channel_sender,
                    event_loop,
                } = AsyncMqttRuntime::create_instanced_mqtt_runtime(mqtt_config)?;

                let mqtt_connection = mqtt;
                self.instanced_runtimes.insert(
                    module_instance_id,
                    InstancedAsyncMqttEventLoop::new(event_loop, event_channel_sender),
                );

                Ok(mqtt_connection)
            }
        }
    }

    pub async fn cleanup(self) {
        for (runtime_id, event_loop) in self.shared_runtimes.into_iter() {
            event_loop.cleanup(&runtime_id).await;
        }

        for (module_instance_id, event_loop) in self.instanced_runtimes.into_iter() {
            log::error!("Manually closing instanced event loop for instance {module_instance_id}");

            if let Err(e) = event_loop
                .runtime_event_sender
                .send(RuntimeEvent::RuntimeTaskStop)
                .await
            {
                log::error!(
                    "Error sending stop to event loop task for instance {module_instance_id}, trying abort: {e}"
                );

                event_loop.task_handle.abort();
            } else if let Err(e) = event_loop.task_handle.await {
                log::error!("Error trying to join instanced event loop for module instance {module_instance_id}: {e}")
            }
        }
    }

    async fn cleanup_shared_lock_mqtt(
        module_instance_id: ModuleInstanceId,
        connection: LockSharedAsyncMqttConnection,
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
        connection: MessageBusSharedAsyncMqttConnection,
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

    async fn cleanup_instanced_module(
        &mut self,
        module_instance_id: ModuleInstanceId,
    ) -> anyhow::Result<()> {
        let Some(instanced_runtime) = self.instanced_runtimes.remove(&module_instance_id) else {
            bail!("No instanced event loop could be found for module instance {module_instance_id}");
        };
        log::debug!(
                "Module instance {} has instanced mqtt event loop that must be shut down, sending stop message",
                module_instance_id
            );
        instanced_runtime
            .runtime_event_sender
            .send(RuntimeEvent::RuntimeTaskStop)
            .await?;

        if let Err(e) = instanced_runtime.task_handle.await? {
            bail!("Error waiting on instanced event loop task for instance {module_instance_id} to finish: {e}");
        } else {
            log::debug!(
                "Module instance {module_instance_id} instanced mqtt event loop sucessfully shut down"
            );
        }

        Ok(())
    }

    pub async fn cleanup_module(
        &mut self,
        mqtt_connection: AsyncMqttConnection,
        module_instance_id: ModuleInstanceId,
    ) -> anyhow::Result<()> {
        let shared_runtime = mqtt_connection.runtime_id().and_then(|runtime_id| {
            self.shared_runtimes
                .iter_mut()
                .find(|(event_loop_runtime_id, _)| **event_loop_runtime_id == runtime_id)
        });

        match mqtt_connection {
            AsyncMqttConnection::MessageBusShared(connection) => match shared_runtime {
                Some((_, shared_runtime)) => match shared_runtime {
                    SharedMqttEventLoopEnum::SharedLockEventLoop(_) => bail!("Expected runtime {} to be message bus flavor but it is shared lock", connection.runtime_id()),
                    SharedMqttEventLoopEnum::SharedMessageBusEventLoop(shared_runtime) => {
                        Self::cleanup_shared_message_bus_mqtt(
                            module_instance_id,
                            connection,
                            shared_runtime,
                        )
                        .await?;
                    }
                },
                None => bail!("Failed to find runtime matching module runtime id {} during cleanup of module instance id {}", connection.runtime_id(), module_instance_id),
            },
            AsyncMqttConnection::LockShared(connection) => match shared_runtime {
                Some((_, shared_runtime)) => match shared_runtime {
                    SharedMqttEventLoopEnum::SharedMessageBusEventLoop(_) => bail!("Expected runtime {} to be shared lock flavor but it is message bus", connection.runtime_id()),
                    SharedMqttEventLoopEnum::SharedLockEventLoop(shared_runtime) => {
                        Self::cleanup_shared_lock_mqtt(
                            module_instance_id,
                            connection,
                            shared_runtime,
                        )
                        .await?;
                    }
                },
                None => bail!("Failed to find runtime matching module runtime id {} during cleanup of module instance id {}", connection.runtime_id(), module_instance_id),

            }
            AsyncMqttConnection::Instanced(connection) => {
                if let Err(e) = connection.disconnect().await {
                    log::error!("Error disconnecting MQTT client: {}", e);
                }

                self.cleanup_instanced_module(module_instance_id).await?;
            }
        };

        Ok(())
    }
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

pub async fn async_shared_message_bus_mqtt_event_loop_task(
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

pub async fn async_shared_lock_mqtt_event_loop_task(
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
