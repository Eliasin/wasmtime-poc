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

#[async_trait::async_trait]
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
