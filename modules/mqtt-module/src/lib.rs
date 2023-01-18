wit_bindgen_guest_rust::generate!("../../wit-bindgen/apis.wit");

struct Start;

export_apis!(Start);

impl apis::Apis for Start {
    fn start(_arg: Option<Vec<u8>>) -> Result<(), String> {
        mqtt::subscribe("hash/request", mqtt::QualityOfService::AtMostOnce)?;
        mqtt::subscribe("hash/stop", mqtt::QualityOfService::ExactlyOnce)?;

        mqtt::publish(
            "hash/start",
            mqtt::QualityOfService::ExactlyOnce,
            false,
            &[],
        )?;

        loop {
            if let Ok(mqtt::Event::Incoming(mqtt::IncomingEvent::Publish(event))) = mqtt::poll() {
                match event.topic.as_str() {
                    "hash/request" => {
                        if let Err(e) = spawn::spawn("hash_worker", Some(&event.payload)) {
                            debug::error(format!("Error while spawning hash worker: {e}").as_str());
                        }
                    }
                    "hash/stop" => {
                        break;
                    }
                    _ => {}
                }
            }
        }

        Ok(())
    }
}
