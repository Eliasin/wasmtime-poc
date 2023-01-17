use std::{collections::HashMap, time::Duration};

use anyhow::bail;
use async_trait::async_trait;

use crate::{
    api::mqtt_async_api::{MessageBusSharedConnection, MqttClientAction, MqttConnection},
    runtime::{handle_module_event, ModuleInstanceId, RuntimeEvent, SharedMqttRuntimeId},
};

use super::{SharedMqttModuleEvent, SharedMqttRuntime, SharedMqttRuntimeEnum};

pub struct SharedMessageBusRuntime {
    mqtt_client_action_sender: tokio::sync::mpsc::Sender<MqttClientAction>,
    module_event_sender: tokio::sync::mpsc::Sender<SharedMqttModuleEvent>,
    runtime_event_sender: tokio::sync::mpsc::Sender<RuntimeEvent>,
    task_handle: tokio::task::JoinHandle<anyhow::Result<()>>,
}

impl SharedMessageBusRuntime {
    pub fn start(
        runtime_id: String,
        client_id: String,
        host: String,
        port: u16,
    ) -> anyhow::Result<SharedMqttRuntimeEnum> {
        let mut mqtt_options = rumqttc::MqttOptions::new(client_id, host, port);
        mqtt_options.set_keep_alive(Duration::from_secs(5));

        let (client, event_loop) = rumqttc::AsyncClient::new(mqtt_options, 10);

        let (module_event_sender, module_event_receiver) = tokio::sync::mpsc::channel(32);
        let (runtime_event_sender, runtime_event_receiver) = tokio::sync::mpsc::channel(32);

        let (mqtt_client_action_sender, mqtt_client_action_receiver) =
            tokio::sync::mpsc::channel(32);

        let runtime_id_cloned = runtime_id.clone();
        let runtime_task_handle = tokio::spawn(async move {
            async_shared_message_bus_runtime_task(
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
            "Done starting shared message bus mqtt runtime id {}",
            runtime_id
        );
        Ok(SharedMessageBusRuntime {
            mqtt_client_action_sender,
            module_event_sender,
            runtime_event_sender,
            task_handle: runtime_task_handle,
        }
        .into())
    }

    pub async fn cleanup_module(
        &self,
        module_instance_id: ModuleInstanceId,
        connection: MessageBusSharedConnection,
    ) -> anyhow::Result<()> {
        if let Err(e) = self
            .module_event_sender
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
}

#[async_trait]
impl SharedMqttRuntime for SharedMessageBusRuntime {
    async fn create_module_runtime(
        &self,
        module_instance_id: ModuleInstanceId,
        runtime_id: String,
        allowed_sub_topics: &[String],
        allowed_pub_topics: &[String],
    ) -> anyhow::Result<MqttConnection> {
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

        Ok(MqttConnection::MessageBusShared(
            MessageBusSharedConnection::new(
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

async fn async_shared_message_bus_runtime_task(
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

    log::info!("MQTT shared message bus runtime id {} stopping", runtime_id);
    Ok(())
}
