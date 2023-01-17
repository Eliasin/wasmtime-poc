use std::collections::HashMap;

use anyhow::{bail, Context};
use enum_dispatch::enum_dispatch;
use tokio::sync::mpsc;

mod shared_lock_runtime;
mod shared_message_bus_runtime;

pub use shared_lock_runtime::SharedLockRuntime;
pub use shared_message_bus_runtime::SharedMessageBusRuntime;

use crate::{
    api::mqtt_async_api::AsyncMqttConnection,
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
pub enum SharedMqttRuntimeEnum {
    SharedLockRuntime,
    SharedMessageBusRuntime,
}

#[enum_dispatch(SharedMqttRuntimeEnum)]
pub trait SharedMqttRuntime
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

#[derive(Default)]
pub struct MqttRuntimes {
    shared_runtimes: HashMap<SharedMqttRuntimeId, SharedMqttRuntimeEnum>,
    instanced_runtimes: HashMap<ModuleInstanceId, InstancedAsyncMqttEventLoop>,
}

impl MqttRuntimes {
    fn start_shared_mqtt_event_loop(
        config: &SharedMqttRuntimeConfig,
    ) -> anyhow::Result<(SharedMqttRuntimeId, SharedMqttRuntimeEnum)> {
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
            MessageBus => SharedMessageBusRuntime::start(
                runtime_id.clone(),
                client_id.clone(),
                host.clone(),
                *port,
            )
            .context("while starting runtime {runtime_id}")?,
            Lock => {
                SharedLockRuntime::start(runtime_id.clone(), client_id.clone(), host.clone(), *port)
                    .context("while starting runtime {runtime_id}")?
            }
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
                    SharedMqttRuntimeEnum::SharedLockRuntime(_) => bail!("Expected runtime {} to be message bus flavor but it is shared lock", connection.runtime_id()),
                    SharedMqttRuntimeEnum::SharedMessageBusRuntime(shared_runtime) => {
                        shared_runtime.cleanup_module(module_instance_id, connection).await
                    }
                },
                None => bail!("Failed to find runtime matching module runtime id {} during cleanup of module instance id {}", connection.runtime_id(), module_instance_id),
            },
            AsyncMqttConnection::LockShared(connection) => match shared_runtime {
                Some((_, shared_runtime)) => match shared_runtime {
                    SharedMqttRuntimeEnum::SharedMessageBusRuntime(_) => bail!("Expected runtime {} to be shared lock flavor but it is message bus", connection.runtime_id()),
                    SharedMqttRuntimeEnum::SharedLockRuntime(shared_runtime) => {
                        shared_runtime.cleanup_module(module_instance_id, connection).await
                    }
                },
                None => bail!("Failed to find runtime matching module runtime id {} during cleanup of module instance id {}", connection.runtime_id(), module_instance_id),

            }
            AsyncMqttConnection::Instanced(connection) => {
                if let Err(e) = connection.disconnect().await {
                    log::error!("Error disconnecting MQTT client: {}", e);
                }

                self.cleanup_instanced_module(module_instance_id).await
            }
        }
    }
}

pub fn handle_module_event(
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
