use std::collections::HashMap;

use anyhow::{bail, Context};
use async_trait::async_trait;
use enum_dispatch::enum_dispatch;
use tokio::sync::mpsc;

mod instanced_runtime;
mod shared_lock_runtime;
mod shared_message_bus_runtime;

pub use instanced_runtime::InstancedMqttRuntime;
pub use shared_lock_runtime::SharedLockRuntime;
pub use shared_message_bus_runtime::SharedMessageBusRuntime;

use crate::{
    api::mqtt_async_api::MqttConnection,
    runtime::{
        MqttRuntime, MqttRuntimeConfig, SharedMqttFlavor, SharedMqttRuntimeConfig,
        SharedMqttRuntimeId,
    },
};

use super::ModuleInstanceId;

pub enum SharedMqttModuleEvent {
    NewModule {
        id: ModuleInstanceId,
        module_mqtt_event_sender: mpsc::Sender<rumqttc::Event>,
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

#[async_trait]
#[enum_dispatch(SharedMqttRuntimeEnum)]
pub trait SharedMqttRuntime {
    async fn cleanup(self, runtime_id: &SharedMqttRuntimeId);
    async fn create_module_runtime(
        &self,
        module_instance_id: ModuleInstanceId,
        runtime_id: String,
        allowed_sub_topics: &[String],
        allowed_pub_topics: &[String],
    ) -> anyhow::Result<MqttConnection>;
}

#[derive(Default)]
pub struct MqttRuntimes {
    shared_runtimes: HashMap<SharedMqttRuntimeId, SharedMqttRuntimeEnum>,
    instanced_runtimes: HashMap<ModuleInstanceId, InstancedMqttRuntime>,
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
    ) -> anyhow::Result<MqttConnection> {
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
                let MqttRuntime {
                    mqtt,
                    event_channel_sender,
                    event_loop,
                } = MqttRuntime::create_instanced_mqtt_runtime(mqtt_config)?;

                let mqtt_connection = mqtt;
                self.instanced_runtimes.insert(
                    module_instance_id,
                    InstancedMqttRuntime::new(event_loop, event_channel_sender),
                );

                Ok(mqtt_connection)
            }
        }
    }

    pub async fn cleanup(self) {
        for (runtime_id, event_loop) in self.shared_runtimes.into_iter() {
            event_loop.cleanup(&runtime_id).await;
        }

        for (module_instance_id, runtime) in self.instanced_runtimes.into_iter() {
            log::error!("Manually closing instanced runtime for instance {module_instance_id}");
            runtime.cleanup(module_instance_id).await;
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

        instanced_runtime.cleanup(module_instance_id).await;

        Ok(())
    }

    pub async fn cleanup_module(
        &mut self,
        mqtt_connection: MqttConnection,
        module_instance_id: ModuleInstanceId,
    ) -> anyhow::Result<()> {
        let shared_runtime = mqtt_connection.runtime_id().and_then(|runtime_id| {
            self.shared_runtimes
                .iter_mut()
                .find(|(event_loop_runtime_id, _)| **event_loop_runtime_id == runtime_id)
        });

        match mqtt_connection {
            MqttConnection::MessageBusShared(connection) => match shared_runtime {
                Some((_, shared_runtime)) => match shared_runtime {
                    SharedMqttRuntimeEnum::SharedLockRuntime(_) => bail!("Expected runtime {} to be message bus flavor but it is shared lock", connection.runtime_id()),
                    SharedMqttRuntimeEnum::SharedMessageBusRuntime(shared_runtime) => {
                        shared_runtime.cleanup_module(module_instance_id, connection).await
                    }
                },
                None => bail!("Failed to find runtime matching module runtime id {} during cleanup of module instance id {}", connection.runtime_id(), module_instance_id),
            },
            MqttConnection::LockShared(connection) => match shared_runtime {
                Some((_, shared_runtime)) => match shared_runtime {
                    SharedMqttRuntimeEnum::SharedMessageBusRuntime(_) => bail!("Expected runtime {} to be shared lock flavor but it is message bus", connection.runtime_id()),
                    SharedMqttRuntimeEnum::SharedLockRuntime(shared_runtime) => {
                        shared_runtime.cleanup_module(module_instance_id, connection).await
                    }
                },
                None => bail!("Failed to find runtime matching module runtime id {} during cleanup of module instance id {}", connection.runtime_id(), module_instance_id),

            }
            MqttConnection::Instanced(connection) => {
                if let Err(e) = connection.disconnect().await {
                    log::error!("Error disconnecting MQTT client: {}", e);
                }

                self.cleanup_instanced_module(module_instance_id).await
            }
        }
    }
}

pub fn handle_module_event(
    module_event_senders: &mut HashMap<ModuleInstanceId, mpsc::Sender<rumqttc::Event>>,
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
