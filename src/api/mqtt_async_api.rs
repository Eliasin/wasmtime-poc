use anyhow::anyhow;
use rumqttc::Incoming;
use tokio::sync::mpsc;

wit_bindgen_host_wasmtime_rust::generate!({
    path: "./wit-bindgen/apis.wit",
    async: true,
});

pub use mqtt::add_to_linker;

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
}

pub struct MqttConnection {
    mqtt_client_action_sender: mpsc::Sender<MqttClientAction>,
    mqtt_event_receiver: mpsc::Receiver<rumqttc::Event>,
    allowed_sub_topics: Vec<String>,
    allowed_pub_topics: Vec<String>,
}

impl MqttConnection {
    pub fn new(
        mqtt_client_action_sender: mpsc::Sender<MqttClientAction>,
        mqtt_event_receiver: mpsc::Receiver<rumqttc::Event>,
        allowed_sub_topics: Vec<String>,
        allowed_pub_topics: Vec<String>,
    ) -> MqttConnection {
        MqttConnection {
            mqtt_client_action_sender,
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

#[wit_bindgen_host_wasmtime_rust::async_trait]
impl mqtt::Mqtt for MqttConnection {
    async fn publish(
        &mut self,
        topic: String,
        qos: mqtt::QualityOfService,
        retain: bool,
        payload: Vec<u8>,
    ) -> anyhow::Result<()> {
        if self.allowed_pub_topics.contains(&topic.to_string()) {
            self.mqtt_client_action_sender
                .send(MqttClientAction::Publish {
                    topic: topic.to_string(),
                    qos: map_qos(qos),
                    retain,
                    payload: payload.to_owned(),
                })
                .await
                .map_err(|e| anyhow!("error sending event to mqtt runtime: {}", e))?;

            Ok(())
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
    ) -> anyhow::Result<()> {
        if self.allowed_sub_topics.contains(&topic.to_string()) {
            self.mqtt_client_action_sender
                .send(MqttClientAction::Subscribe {
                    topic: topic.to_string(),
                    qos: map_qos(qos),
                })
                .await
                .map_err(|e| anyhow!("error sending event to mqtt runtime: {}", e))?;

            Ok(())
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
impl mqtt::Mqtt for Option<MqttConnection> {
    async fn publish(
        &mut self,
        topic: String,
        qos: mqtt::QualityOfService,
        retain: bool,
        payload: Vec<u8>,
    ) -> anyhow::Result<()> {
        if let Some(connection) = self {
            connection.publish(topic, qos, retain, payload).await?;
            Ok(())
        } else {
            Err(anyhow!("Module does not have configured mqtt runtime"))
        }
    }

    async fn subscribe(
        &mut self,
        topic: String,
        qos: mqtt::QualityOfService,
    ) -> anyhow::Result<()> {
        if let Some(connection) = self {
            connection.subscribe(topic, qos).await?;
            Ok(())
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
