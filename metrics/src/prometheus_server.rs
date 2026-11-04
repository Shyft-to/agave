//! A tiny HTTP server that exposes [`crate::prometheus_metrics::REGISTRY`] on a
//! `GET /metrics` endpoint, following the same "dedicated thread + dedicated
//! Tokio runtime" pattern already used by the validator's other background
//! services (e.g. the admin RPC service).

use {
    hyper::{
        Body, Request, Response, Server, StatusCode,
        service::{make_service_fn, service_fn},
    },
    log::{error, info},
    std::{
        convert::Infallible,
        net::SocketAddr,
        sync::{Arc, Mutex},
        thread::{self, JoinHandle},
    },
    tokio::sync::oneshot,
};

/// A cheaply clonable handle that can request the server to stop. Safe to call
/// `close()` from multiple owners (e.g. both a `validator_exit` shutdown
/// callback and an explicit `join()`) — only the first call has an effect.
#[derive(Clone)]
pub struct MetricsServerCloseHandle {
    shutdown_tx: Arc<Mutex<Option<oneshot::Sender<()>>>>,
}

impl MetricsServerCloseHandle {
    pub fn close(&self) {
        if let Some(tx) = self.shutdown_tx.lock().unwrap().take() {
            let _ = tx.send(());
        }
    }
}

pub struct MetricsServer {
    thread_hdl: JoinHandle<()>,
    close_handle: MetricsServerCloseHandle,
}

impl MetricsServer {
    pub fn new(listen_addr: SocketAddr) -> Self {
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let close_handle = MetricsServerCloseHandle {
            shutdown_tx: Arc::new(Mutex::new(Some(shutdown_tx))),
        };

        let thread_hdl = thread::Builder::new()
            .name("solPromMetrics".to_string())
            .spawn(move || {
                let runtime = tokio::runtime::Builder::new_multi_thread()
                    .worker_threads(1)
                    .enable_all()
                    .build()
                    .expect("failed to build the prometheus metrics server tokio runtime");
                runtime.block_on(Self::run(listen_addr, shutdown_rx));
            })
            .expect("failed to spawn solPromMetrics thread");

        Self {
            thread_hdl,
            close_handle,
        }
    }

    /// A clonable handle to request shutdown, independent of `self` — for
    /// registering with `validator_exit` while `self` is stored elsewhere.
    pub fn close_handle(&self) -> MetricsServerCloseHandle {
        self.close_handle.clone()
    }

    async fn run(listen_addr: SocketAddr, shutdown_rx: oneshot::Receiver<()>) {
        let make_svc = make_service_fn(|_conn| async {
            Ok::<_, Infallible>(service_fn(handle_request))
        });

        let server = match Server::try_bind(&listen_addr) {
            Ok(builder) => builder.serve(make_svc),
            Err(err) => {
                error!("prometheus metrics server failed to bind {listen_addr}: {err}");
                return;
            }
        };

        info!("prometheus metrics server listening on {listen_addr}");
        let graceful = server.with_graceful_shutdown(async {
            let _ = shutdown_rx.await;
        });

        if let Err(err) = graceful.await {
            error!("prometheus metrics server error: {err}");
        }
    }

    /// Signal the server to stop accepting new work. Idempotent.
    pub fn close(&self) {
        self.close_handle.close();
    }

    /// Signal shutdown (if not already done) and block until the server thread exits.
    pub fn join(self) {
        self.close();
        let _ = self.thread_hdl.join();
    }
}

async fn handle_request(req: Request<Body>) -> Result<Response<Body>, Infallible> {
    let response = if req.uri().path() == "/metrics" {
        Response::builder()
            .status(StatusCode::OK)
            .header("Content-Type", "text/plain; version=0.0.4")
            .body(Body::from(crate::prometheus_metrics::gather_text()))
            .unwrap()
    } else {
        Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body(Body::empty())
            .unwrap()
    };
    Ok(response)
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_start_and_close() {
        let server = MetricsServer::new("127.0.0.1:0".parse().unwrap());
        server.join();
    }

    #[tokio::test]
    async fn test_serves_metrics() {
        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        // Bind on an ephemeral port ourselves first so we know which port to hit,
        // then hand that same address to a real server on that fixed port.
        let listener = std::net::TcpListener::bind(addr).unwrap();
        let bound_addr = listener.local_addr().unwrap();
        drop(listener);

        let server = MetricsServer::new(bound_addr);
        // Give the server thread a moment to bind and start serving.
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        let url = format!("http://{bound_addr}/metrics");
        let response = reqwest::get(&url).await.unwrap();
        assert!(response.status().is_success());

        let not_found = reqwest::get(format!("http://{bound_addr}/nope"))
            .await
            .unwrap();
        assert_eq!(not_found.status(), reqwest::StatusCode::NOT_FOUND);

        server.close();
        server.join();
    }
}
