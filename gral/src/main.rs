use byteorder::{BigEndian, WriteBytesExt};
use log::{info, LevelFilter};
use metrics_exporter_prometheus::PrometheusBuilder;
use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::{Arc, Mutex};
use tokio::sync::oneshot;
use warp::{http::Response, http::StatusCode, Filter};

mod api;
mod api_bin;
mod arangodb;
mod args;
mod computations;
mod conncomp;
mod graphs;
mod metrics;

use crate::api::api_filter;
use crate::api_bin::{api_bin_filter, handle_errors};
use crate::args::parse_args;
use crate::computations::Computations;
use crate::graphs::Graphs;

pub const VERSION: u32 = 0x00100;

#[tokio::main]
async fn main() {
    env_logger::Builder::new()
        .format_timestamp(Some(env_logger::fmt::TimestampPrecision::Micros))
        .filter_level(LevelFilter::Info)
        .parse_env("RUST_LOG")
        .init();
    info!("Hello, this is gral!");
    let prom_builder = PrometheusBuilder::new();
    let metrics_handle = prom_builder
        .install_recorder()
        .expect("failed to install Prometheus recorder");
    metrics::init();

    let args = match parse_args() {
        Ok(v) => v,
        Err(e) => {
            eprintln!("Error: {}.", e);
            std::process::exit(1);
        }
    };

    info!("{:#?}", args);

    let log_incoming = warp::log::custom(|info| {
        info!(
            "{} {} {} {:?}",
            info.method(),
            info.path(),
            info.status(),
            info.elapsed(),
        );
    });

    let (tx_shutdown, rx_shutdown) = oneshot::channel::<()>();
    let tx_arc = Arc::new(Mutex::new(Some(tx_shutdown)));
    let tx_clone = tx_arc.clone();
    let shutdown = warp::path!("api" / "graphanalytics" / "v1" / "engines" / String / "shutdown")
        .and(warp::delete())
        .map(move |_engine_id: String| {
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
    let the_graphs = Arc::new(Mutex::new(Graphs {
        list: HashMap::new(),
    }));
    let the_computations = Arc::new(Mutex::new(Computations::new()));
    let the_args = Arc::new(Mutex::new(args.clone()));

    let api_metrics = warp::path!("v2" / "metrics").and(warp::get()).map(move || {
        let out = metrics_handle.render();
        warp::reply::with_status(out, StatusCode::OK)
    });

    let apifilters = shutdown
        .with(log_incoming)
        .or(api_filter(
            the_graphs.clone(),
            the_computations.clone(),
            the_args.clone(),
        ))
        .or(api_bin_filter(
            the_graphs.clone(),
            the_computations.clone(),
            the_args.clone(),
        ))
        .or(api_metrics)
        .recover(handle_errors);
    let ip_addr: IpAddr = args
        .bind_addr
        .parse()
        .expect(format!("Could not parse bind address: {}", args.bind_addr).as_str());

    if args.use_tls {
        if args.use_auth {
            let (_addr, server) = warp::serve(apifilters)
                .tls()
                .cert_path(&args.cert)
                .key_path(&args.key)
                .client_auth_required_path("tls/authca.pem")
                .bind_with_graceful_shutdown((ip_addr, args.port), async move {
                    rx_shutdown.await.unwrap();
                    info!("Received shutdown...");
                });
            let j = tokio::task::spawn(server);
            j.await.expect("Join did not work!");
        } else {
            let (_addr, server) = warp::serve(apifilters)
                .tls()
                .cert_path(&args.cert)
                .key_path(&args.key)
                .bind_with_graceful_shutdown((ip_addr, args.port), async move {
                    rx_shutdown.await.unwrap();
                    info!("Received shutdown...");
                });
            let j = tokio::task::spawn(server);
            j.await.expect("Join did not work!");
        }
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
