use super::{map_qos, mqtt};
use anyhow::anyhow;
use rumqttc::Incoming;
use tokio::sync::mpsc;

pub struct InstancedConnection {
    mqtt_client: rumqttc::AsyncClient,
    mqtt_event_receiver: mpsc::Receiver<rumqttc::Event>,
    allowed_sub_topics: Vec<String>,
    allowed_pub_topics: Vec<String>,
}

impl InstancedConnection {
    pub fn new(
        mqtt_client: rumqttc::AsyncClient,
        mqtt_event_receiver: mpsc::Receiver<rumqttc::Event>,
        allowed_sub_topics: Vec<String>,
        allowed_pub_topics: Vec<String>,
    ) -> InstancedConnection {
        InstancedConnection {
            mqtt_client,
            mqtt_event_receiver,
            allowed_sub_topics,
            allowed_pub_topics,
        }
    }
}

impl InstancedConnection {
    pub async fn disconnect(&self) -> anyhow::Result<()> {
        Ok(self.mqtt_client.disconnect().await?)
    }
}

#[async_trait::async_trait]
impl mqtt::Mqtt for InstancedConnection {
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
