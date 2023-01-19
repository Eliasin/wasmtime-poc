use std::{collections::HashMap, time::Duration};

use anyhow::bail;
use async_trait::async_trait;
use tokio::{
    sync::{broadcast, mpsc},
    task,
};

use crate::{
    api::mqtt_async_api::{MessageBusSharedConnection, MqttClientAction, MqttConnection},
    runtime::{ModuleInstanceId, RuntimeEvent, SharedMqttRuntimeId},
};

use super::{SharedMqttModuleEvent, SharedMqttRuntime, SharedMqttRuntimeEnum};

pub struct SharedMessageBusRuntime {
    mqtt_client_action_sender: mpsc::Sender<MqttClientAction>,
    module_event_sender: mpsc::Sender<SharedMqttModuleEvent>,
    runtime_event_sender: broadcast::Sender<RuntimeEvent>,
    runtime_task_handle: task::JoinHandle<anyhow::Result<()>>,
    client_action_task_handle: task::JoinHandle<anyhow::Result<()>>,
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

        let (mqtt_client, event_loop) = rumqttc::AsyncClient::new(mqtt_options, 10);

        let (module_event_sender, module_event_receiver) = mpsc::channel(32);
        let (runtime_event_sender, runtime_event_receiver) = broadcast::channel::<RuntimeEvent>(32);

        let (mqtt_client_action_sender, mqtt_client_action_receiver) =
            tokio::sync::mpsc::channel(32);

        let (module_subscription_end_sender, module_subscription_end_receiver) = mpsc::channel(256);

        let runtime_id_cloned = runtime_id.clone();
        let runtime_task_handle = tokio::spawn(async move {
            async_shared_message_bus_runtime_task(
                module_event_receiver,
                runtime_event_receiver,
                event_loop,
                module_subscription_end_sender,
                runtime_id_cloned,
            )
            .await
        });

        let runtime_id_cloned = runtime_id.clone();
        let runtime_event_receiver = runtime_event_sender.subscribe();
        let client_action_task_handle = tokio::spawn(async move {
            async_shared_message_bus_client_action_task(
                mqtt_client,
                mqtt_client_action_receiver,
                runtime_event_receiver,
                module_subscription_end_receiver,
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
            runtime_task_handle,
            client_action_task_handle,
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
        {
            log::error!("Error sending runtime task stop while cleaning up shared mqtt runtime with id {}: {}", runtime_id, e)
        }

        if let Err(e) = self.runtime_task_handle.await {
            log::error!("Error waiting on mqtt task handle while cleaning up shared mqtt runtime with id {}: {}", runtime_id, e)
        }

        if let Err(e) = self.client_action_task_handle.await {
            log::error!("Error waiting on mqtt task handle while cleaning up shared mqtt runtime with id {}: {}", runtime_id, e)
        }
    }
}

async fn handle_mqtt_client_action(
    client: &mut rumqttc::AsyncClient,
    client_action: MqttClientAction,
    module_subscriptions: &mut HashMap<String, u32>,
) -> anyhow::Result<()> {
    match client_action {
        MqttClientAction::Publish {
            topic,
            qos,
            retain,
            payload,
        } => client.publish(topic, qos, retain, payload).await?,
        MqttClientAction::Subscribe { topic, qos } => {
            let subscription_count = module_subscriptions.get_mut(&topic);
            match subscription_count {
                Some(subscription_count) => *subscription_count += 1,
                None => {
                    module_subscriptions.insert(topic.clone(), 1);
                }
            }

            client.subscribe(topic, qos).await?
        }
        MqttClientAction::Disconnect => client.disconnect().await?,
    }

    Ok(())
}

async fn handle_module_subscription_end(
    module_subscription_end: &[String],
    mqtt_client: &rumqttc::AsyncClient,
    module_subscriptions: &mut HashMap<String, u32>,
) {
    for topic in module_subscription_end {
        if let Some(subscription_count) = module_subscriptions.get_mut(topic) {
            *subscription_count = subscription_count.saturating_sub(1);

            if *subscription_count == 0 {
                if let Err(e) = mqtt_client.unsubscribe(topic).await {
                    log::error!("Error unsubscribing from topic {topic}: {e}");
                }
            }
        }
    }
}

// This task has to be split up because deadlocks can occur when client actions are handled
// in the same task as the event loop since the client can end up filling a queue that needs
// to be consumed by the event loop, causing a deadlock as the event loop never advances
async fn async_shared_message_bus_client_action_task(
    mut mqtt_client: rumqttc::AsyncClient,
    mut mqtt_client_action_receiver: mpsc::Receiver<MqttClientAction>,
    mut runtime_event_receiver: broadcast::Receiver<RuntimeEvent>,
    mut module_subscription_end_receiver: mpsc::Receiver<Vec<String>>,
    runtime_id: SharedMqttRuntimeId,
) -> anyhow::Result<()> {
    let mut module_subscriptions: HashMap<String, u32> = HashMap::new();

    loop {
        tokio::select! {
            client_action = mqtt_client_action_receiver.recv() => {
                match client_action {
                    Some(client_action) => {
                        if let Err(e) = handle_mqtt_client_action(&mut mqtt_client, client_action, &mut module_subscriptions).await {
                            log::error!("Error in shared mqtt runtime {runtime_id}: {e}")
                        }
                    },
                    None => {
                        bail!("Runtime module event channel unexpectedly closed")
                    }
                }
            },
            runtime_event = runtime_event_receiver.recv() => {
                match runtime_event {
                    Err(e) => {
                        match e {
                            broadcast::error::RecvError::Closed => bail!("Runtime event channel unexpectedly closed"),
                            broadcast::error::RecvError::Lagged(e) => log::warn!("Runtime event channel lagged: {e}"),
                        }
                    },
                    Ok(runtime_event) => match runtime_event {
                        RuntimeEvent::RuntimeTaskStop => {
                            break
                        },
                    }
                }
            },
            module_subscription_end = module_subscription_end_receiver.recv() => {
                match module_subscription_end {
                    Some(module_subscription_end) =>
                        handle_module_subscription_end(
                            &module_subscription_end,
                            &mqtt_client,
                            &mut module_subscriptions
                        )
                        .await,
                    None => {
                        log::trace!("Module subscription sender closed");
                    },
                }
            }
        }
    }

    Ok(())
}

async fn async_shared_message_bus_runtime_task(
    mut module_event_receiver: mpsc::Receiver<SharedMqttModuleEvent>,
    mut runtime_event_receiver: broadcast::Receiver<RuntimeEvent>,
    mut mqtt_event_loop: rumqttc::EventLoop,
    module_subscription_end_sender: mpsc::Sender<Vec<String>>,
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
                    Some(module_event) => match module_event {
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
                        SharedMqttModuleEvent::ModuleFinished {
                            id,
                            module_subscriptions,
                        } => {
                            log::debug!(
                                "Module instance id {} finished and is being removed from runtime {}",
                                id,
                                runtime_id
                            );
                            module_event_senders.remove(&id);

                            if let Err(e) = module_subscription_end_sender.try_send(module_subscriptions) {
                                log::error!("Module subscription leak, topic subscription end failed to send: {e}");
                            }
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
                    Err(e) => {
                        match e {
                            broadcast::error::RecvError::Closed => bail!("Runtime event channel unexpectedly closed"),
                            broadcast::error::RecvError::Lagged(e) => log::warn!("Runtime event channel lagged: {e}"),
                        }
                    },
                    Ok(runtime_event) => match runtime_event {
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
