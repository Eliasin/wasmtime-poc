use wit_bindgen_guest_rust::import;

import!("../../wit-bindgen/env.wit");
import!("../../wit-bindgen/debug.wit");

#[no_mangle]
pub extern "C" fn start() {
    debug::warn("STARTED");

    let instance_name = env::get_val("instance_name");

    // debug::warn(format!("INSTANCE NAME {:?}", instance_name).as_str());
}
