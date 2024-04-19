use log::{LevelFilter, Log};
use std::collections::VecDeque;
use std::convert::Infallible;
use std::sync::{Arc, Mutex};
use warp::{http::StatusCode, Filter, Rejection, Reply};

pub struct MemoryLogger {
    limit: usize,
    logs: Arc<Mutex<VecDeque<String>>>,
}

impl Log for MemoryLogger {
    fn enabled(&self, _metadata: &log::Metadata<'_>) -> bool {
        true
    }

    fn log(&self, record: &log::Record<'_>) {
        let mut guard = self.logs.lock().unwrap();
        guard.push_back(format!(
            "Log message: {} - {}",
            record.level(),
            record.args()
        ));
        while guard.len() > self.limit {
            guard.pop_front();
        }
    }

    fn flush(&self) {}
}

impl MemoryLogger {
    pub fn new(limit: usize) -> MemoryLogger {
        MemoryLogger {
            limit,
            logs: Arc::new(Mutex::new(VecDeque::with_capacity(limit))),
        }
    }

    pub fn get_memlog(&self) -> Arc<Mutex<VecDeque<String>>> {
        self.logs.clone()
    }
}

struct CombineLogger<L1, L2>(pub L1, pub L2);

impl<L1: Log, L2: Log> Log for CombineLogger<L1, L2> {
    fn enabled(&self, metadata: &log::Metadata<'_>) -> bool {
        self.0.enabled(metadata) || self.1.enabled(metadata)
    }

    fn log(&self, record: &log::Record<'_>) {
        self.0.log(record);
        self.1.log(record);
    }

    fn flush(&self) {
        self.0.flush();
        self.1.flush();
    }
}

fn set_two_loggers(a: env_logger::Logger, b: MemoryLogger) {
    log::set_boxed_logger(Box::new(CombineLogger(a, b))).expect("logging already initialized");
}

pub fn initialize_logging() -> Arc<Mutex<VecDeque<String>>> {
    let e_logger = env_logger::Builder::new()
        .format_timestamp(Some(env_logger::fmt::TimestampPrecision::Micros))
        .filter_level(LevelFilter::Info)
        .parse_env("RUST_LOG")
        .build();
    let m_logger = MemoryLogger::new(1000);
    let memlog = m_logger.get_memlog();
    set_two_loggers(e_logger, m_logger);
    log::set_max_level(LevelFilter::Info);
    memlog
}

pub fn with_memlog(
    memlog: Arc<Mutex<VecDeque<String>>>,
) -> impl Filter<Extract = (Arc<Mutex<VecDeque<String>>>,), Error = Infallible> + Clone {
    warp::any().map(move || memlog.clone())
}

pub fn api_logs(
    memlog: Arc<Mutex<VecDeque<String>>>,
) -> impl Filter<Extract = (impl Reply,), Error = Rejection> + Clone {
    warp::path!("v1" / "logs")
        .and(warp::get())
        .and(with_memlog(memlog))
        .map(move |memlog: Arc<Mutex<VecDeque<String>>>| {
            let mut s: String = String::with_capacity(100000);
            let guard = memlog.lock().unwrap();
            for l in guard.iter() {
                s.push_str(l);
                s.push('\n');
            }
            warp::reply::with_status(s, StatusCode::OK)
        })
}
