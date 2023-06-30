use byteorder::{BigEndian, WriteBytesExt};
use log::info;
use std::net::IpAddr;
use std::sync::{Arc, Mutex};
use tokio::sync::oneshot;
use warp::{http::Response, Filter};

mod api;
mod args;
mod computations;
mod conncomp;
mod graphs;

use crate::api::{api_filter, handle_errors};
use crate::args::parse_args;
use crate::computations::Computations;
use crate::graphs::Graphs;

const VERSION: u32 = 0x00100;

#[tokio::main]
async fn main() {
    env_logger::builder()
        .format_timestamp(Some(env_logger::fmt::TimestampPrecision::Micros))
        .init();
    info!("Hello, this is gral!");

    let args = match parse_args() {
        Ok(v) => v,
        Err(e) => {
            eprintln!("Error: {}.", e);
            std::process::exit(1);
        }
    };

    info!("{:#?}", args);

    // Setup version handler directly here:
    let version = warp::path!("v1" / "version").and(warp::get()).map(|| {
        let mut v = Vec::new();
        v.write_u32::<BigEndian>(VERSION as u32).unwrap();
        v.write_u32::<BigEndian>(1 as u32).unwrap();
        v.write_u32::<BigEndian>(1 as u32).unwrap();

        Response::builder()
            .header("Content-Type", "x-application-gral")
            .body(v)
    });
    let (tx_shutdown, rx_shutdown) = oneshot::channel::<()>();
    let tx_arc = Arc::new(Mutex::new(Some(tx_shutdown)));
    let tx_clone = tx_arc.clone();
    let shutdown = warp::path!("v1" / "shutdown")
        .and(warp::delete())
        .map(move || {
            let mut tx = tx_clone.lock().unwrap();
            if tx.is_some() {
                let tx = tx.take();
                tx.unwrap()
                    .send(())
                    .expect("Expected to be able to send signal!");
            }
            let mut v = Vec::new();
            v.write_u32::<BigEndian>(VERSION as u32).unwrap();

            Response::builder()
                .header("Content-Type", "x-application-gral")
                .body(v)
        });
    let the_graphs = Arc::new(Mutex::new(Graphs { list: vec![] }));
    let the_computations = Arc::new(Mutex::new(Computations::new()));

    let apifilters = version
        .or(shutdown)
        .or(api_filter(the_graphs.clone(), the_computations.clone()))
        .recover(handle_errors);
    let ip_addr: IpAddr = args
        .bind_addr
        .parse()
        .expect(format!("Could not parse bind address: {}", args.bind_addr).as_str());

    if args.use_tls {
        let (_addr, server) = warp::serve(apifilters)
            .tls()
            .cert_path("tls/cert.pem")
            .key_path("tls/key.pem")
            .client_auth_required_path("tls/authca.pem")
            .bind_with_graceful_shutdown((ip_addr, args.port), async move {
                /*
                tokio::signal::ctrl_c()
                    .await
                    .expect("failed to listen to shutdown signal");
                    */
                rx_shutdown.await.unwrap();
                info!("Received shutdown...");
            });
        let j = tokio::task::spawn(server);
        j.await.expect("Join did not work!");
    } else {
        let (_addr, server) =
            warp::serve(apifilters).bind_with_graceful_shutdown((ip_addr, args.port), async move {
                /* tokio::signal::ctrl_c()
                .await
                .expect("failed to listen to shutdown signal"); */
                rx_shutdown.await.unwrap();
                info!("Received shutdown...");
            });
        let j = tokio::task::spawn(server);
        j.await.expect("Join did not work!");
    }
}
