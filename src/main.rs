use log::info;
use gral::server;

fn main() {
    info!("Hello, this is gral!");
    server::run();
}
