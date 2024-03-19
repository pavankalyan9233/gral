use log::info;
use metrics::{describe_counter, describe_gauge, register_counter, register_gauge};

pub fn init() {
    info!("Initializing metrics...");
    register_counter!("gral_mycounter_total");
    describe_counter!("gral_mycounter_total", "My first test counter");
    register_gauge!("number_of_graphs");
    describe_gauge!("number_of_graphs", "Current number of graphs in memory");
    register_gauge!("number_of_computations");
    describe_gauge!(
        "number_of_computations",
        "Current number of computations in memory"
    );
}
