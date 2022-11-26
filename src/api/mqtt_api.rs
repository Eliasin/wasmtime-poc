use std::sync::Arc;
use std::sync::Mutex;

use rumqttc::Incoming;
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::TryRecvError;
use wit_bindgen_host_wasmtime_rust::export;
export!("./wit-bindgen/mqtt.wit");

pub use mqtt::add_to_linker;

pub struct MqttConnection {
    client: Arc<Mutex<rumqttc::AsyncClient>>,
    events: mpsc::Receiver<rumqttc::Event>,
    allowed_sub_topics: Vec<String>,
    allowed_pub_topics: Vec<String>,
    rt: tokio::runtime::Runtime,
}

impl MqttConnection {
    pub fn new(
        client: rumqttc::AsyncClient,
        events: mpsc::Receiver<rumqttc::Event>,
        allowed_sub_topics: Vec<String>,
        allowed_pub_topics: Vec<String>,
        rt: tokio::runtime::Runtime,
    ) -> MqttConnection {
        let client = Arc::new(Mutex::new(client));

        MqttConnection {
            client,
            events,
            allowed_sub_topics,
            allowed_pub_topics,
            rt,
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
        let client = &mut self.client.lock().unwrap();

        if self.allowed_pub_topics.contains(&topic.to_string()) {
            self.rt
                .block_on(client.publish(topic, map_qos(qos), retain, payload))
                .map_err(|e| format!("rumqttc error: '{}'", e))?;

            Ok(())
        } else {
            Err(format!(
                "publish to topic '{}' not allowed by config policy",
                topic
            ))
        }
    }

    fn subscribe_sync(&mut self, topic: &str, qos: mqtt::QualityOfService) -> Result<(), String> {
        let client = &mut self.client.lock().unwrap();

        if self.allowed_sub_topics.contains(&topic.to_string()) {
            self.rt
                .block_on(client.subscribe(topic, map_qos(qos)))
                .map_err(|e| format!("rumqttc error: '{}'", e))?;
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
            match self.events.try_recv() {
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
