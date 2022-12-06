use anyhow::anyhow;

wit_bindgen_host_wasmtime_rust::generate!({
    path: "../../wit-bindgen/apis.wit",
    async: true,
});

pub use mqtt::add_to_linker;

fn map_qos(qos: mqtt::QualityOfService) -> rumqttc::QoS {
    use mqtt::QualityOfService::*;
    use rumqttc::QoS;
    match qos {
        AtMostOnce => QoS::AtMostOnce,
        AtLeastOnce => QoS::AtLeastOnce,
        ExactlyOnce => QoS::ExactlyOnce,
    }
}

pub enum AsyncMqttConnection {
    MessageBusShared(MessageBusSharedAsyncMqttConnection),
    LockShared(LockSharedAsyncMqttConnection),
    Instanced(InstancedAsyncMqttConnection),
}

#[wit_bindgen_host_wasmtime_rust::async_trait]
impl mqtt::Mqtt for AsyncMqttConnection {
    async fn publish(
        &mut self,
        topic: String,
        qos: mqtt::QualityOfService,
        retain: bool,
        payload: Vec<u8>,
    ) -> anyhow::Result<Result<(), String>> {
        use AsyncMqttConnection::*;
        match self {
            MessageBusShared(v) => v.publish(topic, qos, retain, payload).await,
            LockShared(v) => v.publish(topic, qos, retain, payload).await,
            Instanced(v) => v.publish(topic, qos, retain, payload).await,
        }
    }

    async fn subscribe(
        &mut self,
        topic: String,
        qos: mqtt::QualityOfService,
    ) -> anyhow::Result<Result<(), String>> {
        use AsyncMqttConnection::*;
        match self {
            MessageBusShared(v) => v.subscribe(topic, qos).await,
            LockShared(v) => v.subscribe(topic, qos).await,
            Instanced(v) => v.subscribe(topic, qos).await,
        }
    }

    async fn poll(&mut self) -> anyhow::Result<Result<mqtt::Event, String>> {
        use AsyncMqttConnection::*;
        match self {
            MessageBusShared(v) => v.poll().await,
            LockShared(v) => v.poll().await,
            Instanced(v) => v.poll().await,
        }
    }
}

pub use shared_connection_message_bus::*;
mod shared_connection_message_bus {
    use crate::runtime::SharedMqttRuntimeId;

    use super::{map_qos, mqtt};
    use anyhow::anyhow;
    use rumqttc::Incoming;
    use tokio::sync::mpsc;

    #[derive(Debug)]
    pub enum MqttClientAction {
        Publish {
            topic: String,
            qos: rumqttc::QoS,
            retain: bool,
            payload: Vec<u8>,
        },
        Subscribe {
            topic: String,
            qos: rumqttc::QoS,
        },
        Disconnect,
    }

    pub struct MessageBusSharedAsyncMqttConnection {
        mqtt_action_sender: mpsc::Sender<MqttClientAction>,
        mqtt_event_receiver: mpsc::Receiver<rumqttc::Event>,
        subbed_topics: Vec<(String, mqtt::QualityOfService)>,
        allowed_sub_topics: Vec<String>,
        allowed_pub_topics: Vec<String>,
        runtime_id: SharedMqttRuntimeId,
    }

    impl MessageBusSharedAsyncMqttConnection {
        pub fn new(
            mqtt_action_sender: mpsc::Sender<MqttClientAction>,
            mqtt_event_receiver: mpsc::Receiver<rumqttc::Event>,
            allowed_sub_topics: Vec<String>,
            allowed_pub_topics: Vec<String>,
            runtime_id: SharedMqttRuntimeId,
        ) -> MessageBusSharedAsyncMqttConnection {
            MessageBusSharedAsyncMqttConnection {
                mqtt_action_sender,
                mqtt_event_receiver,
                subbed_topics: vec![],
                allowed_sub_topics,
                allowed_pub_topics,
                runtime_id,
            }
        }

        pub fn runtime_id(&self) -> &SharedMqttRuntimeId {
            &self.runtime_id
        }
    }

    #[wit_bindgen_host_wasmtime_rust::async_trait]
    impl mqtt::Mqtt for MessageBusSharedAsyncMqttConnection {
        async fn publish(
            &mut self,
            topic: String,
            qos: mqtt::QualityOfService,
            retain: bool,
            payload: Vec<u8>,
        ) -> anyhow::Result<Result<(), String>> {
            if self.allowed_pub_topics.contains(&topic.to_string()) {
                Ok(Ok(self
                    .mqtt_action_sender
                    .send(MqttClientAction::Publish {
                        topic,
                        qos: map_qos(qos),
                        retain,
                        payload,
                    })
                    .await?))
            } else {
                Err(anyhow!(
                    "publish to topic '{}' not allowed by config policy",
                    topic
                ))
            }
        }

        async fn subscribe(
            &mut self,
            topic: String,
            qos: mqtt::QualityOfService,
        ) -> anyhow::Result<Result<(), String>> {
            if self.allowed_sub_topics.contains(&topic.to_string()) {
                if let Some((_, subbed_qos)) = self
                    .subbed_topics
                    .iter()
                    .find(|(subbed_topic, _)| *subbed_topic == topic)
                {
                    if map_qos(qos) != map_qos(*subbed_qos) {
                        let result = self
                            .mqtt_action_sender
                            .send(MqttClientAction::Subscribe {
                                topic: topic.clone(),
                                qos: map_qos(qos),
                            })
                            .await
                            .map_err(|e| format!("MQTT Error: {e}"));

                        if result.is_ok() {
                            self.subbed_topics.push((topic, qos));
                        }
                        Ok(result)
                    } else {
                        Ok(Ok(()))
                    }
                } else {
                    let result = self
                        .mqtt_action_sender
                        .send(MqttClientAction::Subscribe {
                            topic: topic.clone(),
                            qos: map_qos(qos),
                        })
                        .await
                        .map_err(|e| format!("MQTT Error: {e}"));

                    if result.is_ok() {
                        self.subbed_topics.push((topic, qos));
                    }
                    Ok(result)
                }
            } else {
                Err(anyhow!(
                    "publish to topic '{}' not allowed by config policy",
                    topic
                ))
            }
        }

        async fn poll(&mut self) -> anyhow::Result<Result<mqtt::Event, String>> {
            loop {
                match self.mqtt_event_receiver.recv().await {
                    Some(notification) => {
                        use rumqttc::Event;
                        match notification {
                            Event::Incoming(incoming) => match incoming {
                                Incoming::Publish(publish) => {
                                    if self
                                        .subbed_topics
                                        .iter()
                                        .map(|(topic, _)| topic)
                                        .any(|topic| **topic == publish.topic)
                                    {
                                        return Ok(Ok(mqtt::Event::Incoming(
                                            mqtt::IncomingEvent::Publish(mqtt::PublishEvent {
                                                topic: publish.topic,
                                                payload: publish.payload.to_vec(),
                                            }),
                                        )));
                                    } else {
                                        continue;
                                    }
                                }
                                _ => continue,
                            },
                            Event::Outgoing(_) => continue,
                        };
                    }
                    None => {
                        return Ok(Err(
                            "MQTT event channel unexpectedly disconnected".to_string()
                        ))
                    }
                }
            }
        }
    }
}

pub use shared_connection_lock::*;
mod shared_connection_lock {
    use std::sync::Arc;

    use super::{map_qos, mqtt};
    use anyhow::anyhow;
    use rumqttc::Incoming;
    use tokio::sync::{mpsc, Mutex};

    pub struct LockSharedAsyncMqttConnection {
        mqtt_client: Arc<Mutex<rumqttc::AsyncClient>>,
        mqtt_event_receiver: mpsc::Receiver<rumqttc::Event>,
        subbed_topics: Vec<(String, mqtt::QualityOfService)>,
        allowed_sub_topics: Vec<String>,
        allowed_pub_topics: Vec<String>,
    }

    impl LockSharedAsyncMqttConnection {
        pub async fn disconnect(&self) -> anyhow::Result<()> {
            let mqtt_client = self.mqtt_client.lock().await;

            Ok(mqtt_client.disconnect().await?)
        }
    }

    #[wit_bindgen_host_wasmtime_rust::async_trait]
    impl mqtt::Mqtt for LockSharedAsyncMqttConnection {
        async fn publish(
            &mut self,
            topic: String,
            qos: mqtt::QualityOfService,
            retain: bool,
            payload: Vec<u8>,
        ) -> anyhow::Result<Result<(), String>> {
            if self.allowed_pub_topics.contains(&topic.to_string()) {
                let mqtt_client = self.mqtt_client.lock().await;
                Ok(mqtt_client
                    .publish(topic, map_qos(qos), retain, payload)
                    .await
                    .map_err(|e| format!("MQTT Error: {e}")))
            } else {
                Err(anyhow!(
                    "publish to topic '{}' not allowed by config policy",
                    topic
                ))
            }
        }

        async fn subscribe(
            &mut self,
            topic: String,
            qos: mqtt::QualityOfService,
        ) -> anyhow::Result<Result<(), String>> {
            if self.allowed_sub_topics.contains(&topic.to_string()) {
                if let Some((_, subbed_qos)) = self
                    .subbed_topics
                    .iter()
                    .find(|(subbed_topic, _)| *subbed_topic == topic)
                {
                    if map_qos(qos) != map_qos(*subbed_qos) {
                        let mqtt_client = self.mqtt_client.lock().await;
                        let result = mqtt_client
                            .subscribe(&topic, map_qos(qos))
                            .await
                            .map_err(|e| format!("MQTT Error: {e}"));

                        if result.is_ok() {
                            self.subbed_topics.push((topic, qos));
                        }
                        Ok(result)
                    } else {
                        Ok(Ok(()))
                    }
                } else {
                    let mqtt_client = self.mqtt_client.lock().await;
                    let result = mqtt_client
                        .subscribe(&topic, map_qos(qos))
                        .await
                        .map_err(|e| format!("MQTT Error: {e}"));

                    if result.is_ok() {
                        self.subbed_topics.push((topic, qos));
                    }
                    Ok(result)
                }
            } else {
                Err(anyhow!(
                    "subscribe to topic '{}' not allowed by config policy",
                    topic
                ))
            }
        }

        async fn poll(&mut self) -> anyhow::Result<Result<mqtt::Event, String>> {
            loop {
                match self.mqtt_event_receiver.recv().await {
                    Some(notification) => {
                        use rumqttc::Event;
                        match notification {
                            Event::Incoming(incoming) => match incoming {
                                Incoming::Publish(publish) => {
                                    if self
                                        .subbed_topics
                                        .iter()
                                        .map(|(topic, _)| topic)
                                        .any(|topic| **topic == publish.topic)
                                    {
                                        return Ok(Ok(mqtt::Event::Incoming(
                                            mqtt::IncomingEvent::Publish(mqtt::PublishEvent {
                                                topic: publish.topic,
                                                payload: publish.payload.to_vec(),
                                            }),
                                        )));
                                    } else {
                                        continue;
                                    }
                                }
                                _ => continue,
                            },
                            Event::Outgoing(_) => continue,
                        };
                    }
                    None => {
                        return Ok(Err(
                            "MQTT event channel unexpectedly disconnected".to_string()
                        ))
                    }
                }
            }
        }
    }
}

pub use instanced_connection::*;
mod instanced_connection {
    use super::{map_qos, mqtt};
    use anyhow::anyhow;
    use rumqttc::Incoming;
    use tokio::sync::mpsc;

    pub struct InstancedAsyncMqttConnection {
        mqtt_client: rumqttc::AsyncClient,
        mqtt_event_receiver: mpsc::Receiver<rumqttc::Event>,
        allowed_sub_topics: Vec<String>,
        allowed_pub_topics: Vec<String>,
    }

    impl InstancedAsyncMqttConnection {
        pub fn new(
            mqtt_client: rumqttc::AsyncClient,
            mqtt_event_receiver: mpsc::Receiver<rumqttc::Event>,
            allowed_sub_topics: Vec<String>,
            allowed_pub_topics: Vec<String>,
        ) -> InstancedAsyncMqttConnection {
            InstancedAsyncMqttConnection {
                mqtt_client,
                mqtt_event_receiver,
                allowed_sub_topics,
                allowed_pub_topics,
            }
        }
    }

    impl InstancedAsyncMqttConnection {
        pub async fn disconnect(&self) -> anyhow::Result<()> {
            Ok(self.mqtt_client.disconnect().await?)
        }
    }

    #[wit_bindgen_host_wasmtime_rust::async_trait]
    impl mqtt::Mqtt for InstancedAsyncMqttConnection {
        async fn publish(
            &mut self,
            topic: String,
            qos: mqtt::QualityOfService,
            retain: bool,
            payload: Vec<u8>,
        ) -> anyhow::Result<Result<(), String>> {
            if self.allowed_pub_topics.contains(&topic.to_string()) {
                Ok(self
                    .mqtt_client
                    .publish(topic, map_qos(qos), retain, payload)
                    .await
                    .map_err(|e| format!("MQTT Error: {e}")))
            } else {
                Err(anyhow!(
                    "publish to topic '{}' not allowed by config policy",
                    topic
                ))
            }
        }

        async fn subscribe(
            &mut self,
            topic: String,
            qos: mqtt::QualityOfService,
        ) -> anyhow::Result<Result<(), String>> {
            if self.allowed_sub_topics.contains(&topic.to_string()) {
                Ok(self
                    .mqtt_client
                    .subscribe(topic, map_qos(qos))
                    .await
                    .map_err(|e| format!("MQTT Error: {e}")))
            } else {
                Err(anyhow!(
                    "subscribe to topic '{}' not allowed by config policy",
                    topic
                ))
            }
        }

        async fn poll(&mut self) -> anyhow::Result<Result<mqtt::Event, String>> {
            loop {
                match self.mqtt_event_receiver.recv().await {
                    Some(notification) => {
                        use rumqttc::Event;
                        match notification {
                            Event::Incoming(incoming) => match incoming {
                                Incoming::Publish(publish) => {
                                    return Ok(Ok(mqtt::Event::Incoming(
                                        mqtt::IncomingEvent::Publish(mqtt::PublishEvent {
                                            topic: publish.topic,
                                            payload: publish.payload.to_vec(),
                                        }),
                                    )));
                                }
                                _ => continue,
                            },
                            Event::Outgoing(_) => continue,
                        }
                    }
                    None => {
                        return Ok(Err(
                            "MQTT event channel unexpectedly disconnected".to_string()
                        ))
                    }
                }
            }
        }
    }
}

#[wit_bindgen_host_wasmtime_rust::async_trait]
impl<M: mqtt::Mqtt + Send + Sync> mqtt::Mqtt for Option<M> {
    async fn publish(
        &mut self,
        topic: String,
        qos: mqtt::QualityOfService,
        retain: bool,
        payload: Vec<u8>,
    ) -> anyhow::Result<Result<(), String>> {
        if let Some(connection) = self {
            connection.publish(topic, qos, retain, payload).await
        } else {
            Err(anyhow!("Module does not have configured mqtt runtime"))
        }
    }

    async fn subscribe(
        &mut self,
        topic: String,
        qos: mqtt::QualityOfService,
    ) -> anyhow::Result<Result<(), String>> {
        if let Some(connection) = self {
            connection.subscribe(topic, qos).await
        } else {
            Err(anyhow!("Module does not have configured mqtt runtime"))
        }
    }

    async fn poll(&mut self) -> anyhow::Result<Result<mqtt::Event, String>> {
        if let Some(connection) = self {
            connection.poll().await
        } else {
            Err(anyhow!("Module does not have configured mqtt runtime"))
        }
    }
}
