wit_bindgen_guest_rust::generate!("../../wit-bindgen/apis.wit");

struct Start;

export_apis!(Start);

impl apis::Apis for Start {
    fn start() -> Result<(), String> {
        let instance_name = env::get_val("instance_name");

        debug::warn(format!("INSTANCE NAME {:?}", instance_name).as_str());

        Ok(())
    }
}
