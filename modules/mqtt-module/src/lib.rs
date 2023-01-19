wit_bindgen_guest_rust::generate!("../../wit-bindgen/apis.wit");

struct Start;

export_apis!(Start);

impl apis::Apis for Start {
    fn start(_arg: Option<Vec<u8>>) -> Result<(), String> {
        mqtt::subscribe("hash/request", mqtt::QualityOfService::AtMostOnce)?;

        debug::info("Sending start signal");

        mqtt::publish(
            "hash/start",
            mqtt::QualityOfService::ExactlyOnce,
            false,
            &[],
        )?;

        let mut n = 0;
        loop {
            if let Ok(mqtt::Event::Incoming(mqtt::IncomingEvent::Publish(event))) = mqtt::poll() {
                match event.topic.as_str() {
                    "hash/request" => {
                        n += 1;
                        if let Err(e) = spawn::spawn("hash_worker", Some(&event.payload)) {
                            debug::error(format!("Error while spawning hash worker: {e}").as_str());
                        }

                        if n >= 512 {
                            break;
                        }
                    }
                    _ => {}
                }
            }
        }

        Ok(())
    }
}
