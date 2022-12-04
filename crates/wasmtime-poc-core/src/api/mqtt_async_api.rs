use anyhow::anyhow;
use rumqttc::Incoming;
use tokio::sync::mpsc;

wit_bindgen_host_wasmtime_rust::generate!({
    path: "../../wit-bindgen/apis.wit",
    async: true,
});

pub use mqtt::add_to_linker;

pub struct AsyncMqttConnection {
    mqtt_client: rumqttc::AsyncClient,
    mqtt_event_receiver: mpsc::Receiver<rumqttc::Event>,
    allowed_sub_topics: Vec<String>,
    allowed_pub_topics: Vec<String>,
}

impl AsyncMqttConnection {
    pub fn new(
        mqtt_client: rumqttc::AsyncClient,
        mqtt_event_receiver: mpsc::Receiver<rumqttc::Event>,
        allowed_sub_topics: Vec<String>,
        allowed_pub_topics: Vec<String>,
    ) -> AsyncMqttConnection {
        AsyncMqttConnection {
            mqtt_client,
            mqtt_event_receiver,
            allowed_sub_topics,
            allowed_pub_topics,
        }
    }
}

fn map_qos(qos: mqtt::QualityOfService) -> rumqttc::QoS {
    use mqtt::QualityOfService::*;
    use rumqttc::QoS;
    match qos {
        AtMostOnce => QoS::AtMostOnce,
        AtLeastOnce => QoS::AtLeastOnce,
        ExactlyOnce => QoS::ExactlyOnce,
    }
}

impl AsyncMqttConnection {
    pub async fn disconnect(&self) -> anyhow::Result<()> {
        Ok(self.mqtt_client.disconnect().await?)
    }
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
        if self.allowed_pub_topics.contains(&topic.to_string()) {
            Ok(self
                .mqtt_client
                .publish(topic, map_qos(qos), retain, payload)
                .await
                .map_err(|e| format!("MQTT Error: {}", e)))
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
                .map_err(|e| format!("MQTT Error: {}", e)))
        } else {
            Err(anyhow!(
                "subscribe to topic '{}' not allowed by config policy",
                topic
            ))
        }
    }

    async fn poll(&mut self) -> anyhow::Result<Result<mqtt::Event, String>> {
        match self.mqtt_event_receiver.recv().await {
            Some(notification) => {
                use rumqttc::Event;
                Ok(match notification {
                    Event::Incoming(incoming) => match incoming {
                        Incoming::Publish(publish) => Ok(mqtt::Event::Incoming(
                            mqtt::IncomingEvent::Publish(mqtt::PublishEvent {
                                topic: publish.topic,
                                payload: publish.payload.to_vec(),
                            }),
                        )),
                        _ => Err("unsupported event".to_string()),
                    },
                    Event::Outgoing(_) => Err("ignored outgoing event".to_string()),
                })
            }
            None => Err(anyhow!(
                "Tokio MQTT event channel unexpectedly disconnected"
            )),
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
