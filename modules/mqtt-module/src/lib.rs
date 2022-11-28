wit_bindgen_guest_rust::generate!("../../wit-bindgen/apis.wit");

struct Start;

export_apis!(Start);

use mqtt::QualityOfService as QoS;

fn instance_a() -> Result<(), String> {
    mqtt::subscribe("hello/mqttA", mqtt::QualityOfService::ExactlyOnce);

    for i in 0..10 {
        mqtt::publish(
            "hello/mqttB",
            QoS::ExactlyOnce,
            true,
            format!("{}", i).as_bytes(),
        );

        loop {
            match mqtt::poll() {
                Ok(event) => {
                    if let mqtt::Event::Incoming(event) = event {
                        if let mqtt::IncomingEvent::Publish(event) = event {
                            debug::warn(
                                format!(
                                    "Instance A received {}, i = {}",
                                    String::from_utf8_lossy(&event.payload),
                                    i
                                )
                                .as_str(),
                            );
                            break;
                        }
                    }
                }
                Err(_) => {}
            }
        }
    }

    debug::info("Instance A finished execution");

    Ok(())
}

fn instance_b() -> Result<(), String> {
    mqtt::subscribe("hello/mqttB", mqtt::QualityOfService::ExactlyOnce);

    for i in 0..10 {
        loop {
            match mqtt::poll() {
                Ok(event) => {
                    if let mqtt::Event::Incoming(event) = event {
                        if let mqtt::IncomingEvent::Publish(event) = event {
                            debug::warn(
                                format!(
                                    "Instance B received {}, i = {}",
                                    String::from_utf8_lossy(&event.payload),
                                    i
                                )
                                .as_str(),
                            );

                            mqtt::publish(
                                "hello/mqttA",
                                QoS::ExactlyOnce,
                                true,
                                format!("Received {}", String::from_utf8_lossy(&event.payload))
                                    .as_bytes(),
                            );
                            break;
                        }
                    }
                }
                Err(_) => {}
            }
        }
    }

    debug::warn("Instance B finished execution");

    Ok(())
}

impl apis::Apis for Start {
    fn start() -> Result<(), String> {
        let instance_name = env::get_val("instance_name").unwrap();

        match instance_name.as_str() {
            "a" => instance_a(),
            "b" => instance_b(),
            _ => Err("Unrecognized instance name".to_string()),
        }
    }
}
