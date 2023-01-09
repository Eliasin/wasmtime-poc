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

#[async_trait::async_trait]
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
