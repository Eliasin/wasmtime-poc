use std::sync::Arc;
use std::sync::Mutex;

use rumqttc::Incoming;
use wit_bindgen_host_wasmtime_rust::export;
export!("./wit-bindgen/mqtt.wit");

pub use mqtt::add_to_linker;

#[derive(Clone)]
pub struct MqttConnection {
    session: Arc<Mutex<(rumqttc::Client, rumqttc::Connection)>>,
    allowed_sub_topics: Vec<String>,
    allowed_pub_topics: Vec<String>,
}

impl MqttConnection {
    pub fn new(
        client: rumqttc::Client,
        connection: rumqttc::Connection,
        allowed_sub_topics: Vec<String>,
        allowed_pub_topics: Vec<String>,
    ) -> MqttConnection {
        let session = Arc::new(Mutex::new((client, connection)));

        MqttConnection {
            session,
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
        let client = &mut self.session.lock().unwrap().0;

        if self.allowed_pub_topics.contains(&topic.to_string()) {
            client
                .publish(topic, map_qos(qos), retain, payload)
                .map_err(|e| format!("rumqttc error: '{}'", e))?;
        }

        Ok(())
    }

    fn subscribe_sync(&mut self, topic: &str, qos: mqtt::QualityOfService) -> Result<(), String> {
        let client = &mut self.session.lock().unwrap().0;

        if self.allowed_sub_topics.contains(&topic.to_string()) {
            client
                .subscribe(topic, map_qos(qos))
                .map_err(|e| format!("rumqttc error: '{}'", e))?;
        }

        Ok(())
    }

    fn poll_sync(&mut self) -> Vec<Result<mqtt::Event, String>> {
        let connection = &mut self.session.lock().unwrap().1;

        connection
            .iter()
            .map(|notification| {
                use rumqttc::Event;
                match notification {
                    Ok(notification) => match notification {
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
                    },
                    Err(e) => Err(format!("rumqttc error: '{}'", e)),
                }
            })
            .collect()
    }
}
