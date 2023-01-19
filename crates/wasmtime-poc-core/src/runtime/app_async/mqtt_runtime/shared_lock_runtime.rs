use std::{collections::HashMap, time::Duration};

use anyhow::bail;
use async_trait::async_trait;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::{
    api::mqtt_async_api::{LockSharedConnection, MqttConnection},
    runtime::{handle_module_event, ModuleInstanceId, RuntimeEvent, SharedMqttRuntimeId},
};

use super::{SharedMqttModuleEvent, SharedMqttRuntime, SharedMqttRuntimeEnum};

pub struct SharedLockRuntime {
    mqtt_client: rumqttc::AsyncClient,
    module_event_sender: mpsc::Sender<SharedMqttModuleEvent>,
    runtime_event_sender: mpsc::Sender<RuntimeEvent>,
    task_handle: JoinHandle<anyhow::Result<()>>,
}

impl SharedLockRuntime {
    pub fn start(
        runtime_id: String,
        client_id: String,
        host: String,
        port: u16,
    ) -> anyhow::Result<SharedMqttRuntimeEnum> {
        let mut mqtt_options = rumqttc::MqttOptions::new(client_id, host, port);
        mqtt_options.set_keep_alive(Duration::from_secs(5));

        let (client, event_loop) = rumqttc::AsyncClient::new(mqtt_options, 10);

        let (module_event_sender, module_event_receiver) = mpsc::channel(32);
        let (runtime_event_sender, runtime_event_receiver) = mpsc::channel(32);

        let runtime_id_cloned = runtime_id.clone();
        let runtime_task_handle = tokio::spawn(async move {
            async_shared_lock_mqtt_runtime_task(
                module_event_receiver,
                runtime_event_receiver,
                event_loop,
                runtime_id_cloned,
            )
            .await
        });

        log::debug!(
            "Done starting shared message bus mqtt runtime with id {}",
            runtime_id
        );
        Ok(SharedLockRuntime {
            mqtt_client: client,
            module_event_sender,
            runtime_event_sender,
            task_handle: runtime_task_handle,
        }
        .into())
    }

    pub async fn cleanup_module(
        &self,
        module_instance_id: ModuleInstanceId,
        connection: LockSharedConnection,
    ) -> anyhow::Result<()> {
        if let Err(e) = self
            .module_event_sender
            .send(SharedMqttModuleEvent::ModuleFinished {
                id: module_instance_id,
                module_subscriptions: connection
                    .module_subscriptions()
                    .iter()
                    .map(|(topic, _)| topic.clone())
                    .collect(),
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
}

#[async_trait]
impl SharedMqttRuntime for SharedLockRuntime {
    async fn cleanup(self, runtime_id: &SharedMqttRuntimeId) {
        if let Err(e) = self.mqtt_client.disconnect().await {
            log::error!(
                            "Error disconnecting mqtt event loop while cleaning up shared mqtt runtime with id {}: {}",
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
            log::error!("Error sending runtime task stop while cleaning up shared mqtt runtime with id {}: {}", runtime_id, e)
        }

        if let Err(e) = self.task_handle.await {
            log::error!("Error waiting on mqtt task handle while cleaning up shared mqtt runtime with id {}: {}", runtime_id, e)
        }
    }

    async fn create_module_runtime(
        &self,
        module_instance_id: ModuleInstanceId,
        runtime_id: String,
        allowed_sub_topics: &[String],
        allowed_pub_topics: &[String],
    ) -> anyhow::Result<MqttConnection> {
        let (mqtt_event_sender, mqtt_event_receiver) = mpsc::channel(32);

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

        Ok(MqttConnection::LockShared(LockSharedConnection::new(
            self.mqtt_client.clone(),
            mqtt_event_receiver,
            allowed_sub_topics.to_vec(),
            allowed_pub_topics.to_vec(),
            runtime_id,
        )))
    }
}

async fn async_shared_lock_mqtt_runtime_task(
    mut module_event_receiver: mpsc::Receiver<SharedMqttModuleEvent>,
    mut runtime_event_receiver: mpsc::Receiver<RuntimeEvent>,
    mut mqtt_event_loop: rumqttc::EventLoop,
    runtime_id: SharedMqttRuntimeId,
) -> anyhow::Result<()> {
    let mut module_event_senders: HashMap<ModuleInstanceId, mpsc::Sender<rumqttc::Event>> =
        HashMap::new();

    loop {
        tokio::select! {
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
                            if module_event_sender.try_send(notification.clone()).is_err() {
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

    log::info!("MQTT shared message bus runtime id {} stopping", runtime_id);
    Ok(())
}
