use serde::{Deserialize, Serialize};

wit_bindgen_guest_rust::generate!("../../wit-bindgen/apis.wit");

#[derive(Debug, Deserialize, Serialize)]
struct ComputationRequest {
    square: Vec<u8>,
    double: Vec<u8>,
}

#[derive(Debug, Deserialize, Serialize)]
struct ComputationResponse {
    square: Vec<u8>,
    double: Vec<u8>,
}

impl ComputationResponse {
    pub fn from_request(request: &ComputationRequest) -> ComputationResponse {
        ComputationResponse {
            square: request.square.iter().map(|n| n * n).collect(),
            double: request.double.iter().map(|n| n + n).collect(),
        }
    }
}

struct Start;

export_apis!(Start);

use mqtt::QualityOfService as QoS;

fn instance_a() -> Result<(), String> {
    mqtt::subscribe("hello/mqttA", mqtt::QualityOfService::ExactlyOnce)?;
    util::sleep(250);

    for i in 0..10 {
        let computation_request = ComputationRequest {
            square: (0..(i + 1)).collect(),
            double: (1..(i + 2)).collect(),
        };
        let payload = serde_json::to_string(&computation_request).map_err(|e| format!("{}", e))?;

        debug::info(format!("Instance A sending {:?}", computation_request).as_str());
        util::sleep(250);
        mqtt::publish("hello/mqttB", QoS::ExactlyOnce, false, payload.as_bytes())?;

        loop {
            match mqtt::poll() {
                Ok(event) => {
                    if let mqtt::Event::Incoming(event) = event {
                        if let mqtt::IncomingEvent::Publish(event) = event {
                            if let Ok(response) =
                                serde_json::from_slice::<ComputationResponse>(&event.payload)
                            {
                                debug::info(
                                    format!("Instance A received {:?}, i = {}", &response, i)
                                        .as_str(),
                                );
                                break;
                            }
                        }
                    }
                }
                Err(e) => debug::error(format!("Instance A received error: {}", e).as_str()),
            }
        }
    }

    debug::info("****** Instance A finished execution ******");

    Ok(())
}

fn instance_b() -> Result<(), String> {
    mqtt::subscribe("hello/mqttB", mqtt::QualityOfService::ExactlyOnce)?;
    util::sleep(250);

    for i in 0..10 {
        loop {
            match mqtt::poll() {
                Ok(event) => {
                    if let mqtt::Event::Incoming(event) = event {
                        if let mqtt::IncomingEvent::Publish(event) = event {
                            if let Ok(request) =
                                serde_json::from_slice::<ComputationRequest>(&event.payload)
                            {
                                debug::info(
                                    format!("Instance B received {:?}, i = {}", &request, i)
                                        .as_str(),
                                );

                                let computation_response =
                                    ComputationResponse::from_request(&request);

                                let payload_string: String =
                                    serde_json::to_string(&computation_response)
                                        .map_err(|e| format!("{}", e))?;
                                let payload = payload_string.as_bytes();

                                debug::info(
                                    format!(
                                        "Instance B sending {:?}, i = {}",
                                        computation_response, i
                                    )
                                    .as_str(),
                                );

                                util::sleep(250);
                                mqtt::publish("hello/mqttA", QoS::ExactlyOnce, false, payload)?;
                                break;
                            }
                        }
                    }
                }
                Err(_) => {}
            }
        }
    }

    debug::info("****** Instance B finished execution *****");

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
