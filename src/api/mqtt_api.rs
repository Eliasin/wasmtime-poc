use rumqttc::Incoming;
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::TryRecvError;
use wit_bindgen_host_wasmtime_rust::export;
export!("./wit-bindgen/mqtt.wit");

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

impl mqtt::Mqtt for MqttConnection {
    fn publish_sync(
        &mut self,
        topic: &str,
        qos: mqtt::QualityOfService,
        retain: bool,
        payload: &[u8],
    ) -> Result<(), String> {
        if self.allowed_pub_topics.contains(&topic.to_string()) {
            self.mqtt_client_action_sender
                .blocking_send(MqttClientAction::Publish {
                    topic: topic.to_string(),
                    qos: map_qos(qos),
                    retain,
                    payload: payload.to_owned(),
                })
                .map_err(|e| format!("error sending event to mqtt runtime: {}", e))?;

            Ok(())
        } else {
            Err(format!(
                "publish to topic '{}' not allowed by config policy",
                topic
            ))
        }
    }

    fn subscribe_sync(&mut self, topic: &str, qos: mqtt::QualityOfService) -> Result<(), String> {
        if self.allowed_sub_topics.contains(&topic.to_string()) {
            self.mqtt_client_action_sender
                .blocking_send(MqttClientAction::Subscribe {
                    topic: topic.to_string(),
                    qos: map_qos(qos),
                })
                .map_err(|e| format!("error sending event to mqtt runtime: {}", e))?;

            Ok(())
        } else {
            Err(format!(
                "subscribe to topic '{}' not allowed by config policy",
                topic
            ))
        }
    }

    fn poll_sync(&mut self) -> Result<Vec<Result<mqtt::Event, String>>, String> {
        let mut events = vec![];

        loop {
            match self.mqtt_event_receiver.try_recv() {
                Ok(notification) => {
                    use rumqttc::Event;
                    events.push(match notification {
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
                Err(err) => match err {
                    TryRecvError::Empty => break,
                    TryRecvError::Disconnected => {
                        return Err("Tokio MQTT event channel unexpectedly disconnected".to_string())
                    }
                },
            }
        }

        Ok(events)
    }
}

impl mqtt::Mqtt for Option<MqttConnection> {
    fn publish_sync(
        &mut self,
        topic: &str,
        qos: mqtt::QualityOfService,
        retain: bool,
        payload: &[u8],
    ) -> Result<(), String> {
        if let Some(connection) = self {
            connection.publish_sync(topic, qos, retain, payload)
        } else {
            Err("Module does not have configured mqtt runtime".to_string())
        }
    }

    fn subscribe_sync(&mut self, topic: &str, qos: mqtt::QualityOfService) -> Result<(), String> {
        if let Some(connection) = self {
            connection.subscribe_sync(topic, qos)
        } else {
            Err("Module does not have configured mqtt runtime".to_string())
        }
    }

    fn poll_sync(&mut self) -> Result<Vec<Result<mqtt::Event, String>>, String> {
        if let Some(connection) = self {
            connection.poll_sync()
        } else {
            Err("Module does not have configured mqtt runtime".to_string())
        }
    }
}
