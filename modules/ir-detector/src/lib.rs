wit_bindgen::generate!("apis");

struct Start;

export_apis!(Start);

impl Apis for Start {
    fn start(_arg: Option<Vec<u8>>) -> Result<(), String> {
        let Some(socket_path) = env::get_val("lirc-socket-path") else {
            debug::error("could not get lirc-socket-path from env");
            return Ok(());
        };

        if let Err(e) = unix_stream_socket::connect(&socket_path) {
            debug::error(&format!("error while connecting to unix socket: {e}"));
            return Ok(());
        };

        loop {
            let Some(socket_event) = unix_stream_socket::poll_sockets() else {
                return Ok(());
            };

            if let Err(e) = mqtt::publish(
                "ir/event",
                mqtt::QualityOfService::ExactlyOnce,
                false,
                &socket_event.data,
            ) {
                debug::error(&format!("error when sending ir mqtt event: {e}"))
            };
        }
    }
}
