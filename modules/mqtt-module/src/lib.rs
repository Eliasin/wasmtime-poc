use wit_bindgen_guest_rust::import;

import!("../../wit-bindgen/mqtt.wit");
import!("../../wit-bindgen/debug.wit");

#[no_mangle]
pub extern "C" fn start() {
    debug::warn("STARTED");
}
