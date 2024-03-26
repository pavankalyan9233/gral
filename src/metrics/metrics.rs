pub mod metrics {
    use log::info;
    use metrics::{describe_counter, describe_gauge, register_counter, register_gauge};

    pub fn init() {
        info!("Initializing metrics...");
        register_counter!("gral_mycounter_total");
        describe_counter!("gral_mycounter_total", "My first test counter");
        register_gauge!("number_of_graphs");
        describe_gauge!("number_of_graphs", "Current number of graphs in memory");
        register_gauge!("number_of_jobs");
        describe_gauge!("number_of_jobs", "Current number of jobs in memory");
    }
}
