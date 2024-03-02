use log::info;
use metrics::{describe_counter, register_counter};

pub fn init() {
    info!("Initializing metrics...");
    register_counter!("gral_mycounter_total");
    describe_counter!("gral_mycounter_total", "My first test counter");
}
