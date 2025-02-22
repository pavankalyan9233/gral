use http::Error;
use log::{debug, info, warn};
use metrics_exporter_prometheus::PrometheusBuilder;
use std::net::IpAddr;
use std::sync::{Arc, Mutex};
use tokio::sync::oneshot;
use warp::{http::StatusCode, reply::WithStatus, Filter};

use crate::api::graphanalyticsengine::GraphAnalyticsEngineShutdownResponse;
use crate::api::{api_filter, handle_errors};
use crate::args::parse_args;
use crate::auth::with_auth;
use crate::computations::Computations;
use crate::graph_store::graphs::Graphs;
use crate::logging::{api_logs, initialize_logging};
use crate::metrics;

#[tokio::main]
pub async fn run() {
    let memlog = initialize_logging();
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
    debug!("{:#?}", args);
    let the_args = Arc::new(Mutex::new(args.clone()));

    if args.warp_trace {
        let ts = tracing_subscriber::fmt()
            .with_max_level(tracing::level_filters::LevelFilter::TRACE)
            .finish();
        let tse = tracing::subscriber::set_global_default(ts)
            .map_err(|_err| eprintln!("Unable to set global default subscriber"));
        if let Err(e) = tse {
            warn!("Could not set up tracing: {e:?}");
        }
    }

    let log_incoming = warp::log::custom(|info| {
        info!("{} {} {:?}", info.method(), info.path(), info.elapsed(),);
    });

    let (tx_shutdown, rx_shutdown) = oneshot::channel::<()>();
    let tx_arc = Arc::new(Mutex::new(Some(tx_shutdown)));
    let tx_clone = tx_arc.clone();
    let shutdown = warp::path!("v1" / "shutdown")
        .and(warp::delete())
        .and(with_auth(the_args.clone()))
        .map(move |_user| -> Result<WithStatus<Vec<u8>>, Error> {
            let mut tx = tx_clone.lock().unwrap();
            if tx.is_some() {
                let tx = tx.take();
                tx.unwrap()
                    .send(())
                    .expect("Expected to be able to send signal!");
            }
            let response = GraphAnalyticsEngineShutdownResponse {
                error: false,
                error_code: 0,
                error_message: "".to_string(),
            };
            Ok(warp::reply::with_status(
                serde_json::to_vec(&response).expect("Should be serializable"),
                StatusCode::OK,
            ))
        });
    let the_graphs = Arc::new(Mutex::new(Graphs::new()));
    let the_computations = Arc::new(Mutex::new(Computations::new()));

    let api_metrics = warp::path!("v1" / "statistics")
        .and(warp::get())
        .map(move || {
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
        .or(api_metrics)
        .or(api_logs(memlog))
        .recover(handle_errors);
    let ip_addr: IpAddr = args
        .bind_addr
        .parse()
        .unwrap_or_else(|_| panic!("Could not parse bind address: {}", args.bind_addr));

    if args.use_tls {
        if args.use_auth {
            let (_addr, server) = warp::serve(apifilters)
                .tls()
                .cert(&args.cert)
                .key(&args.key)
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
                .cert(&args.cert)
                .key(&args.key)
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
