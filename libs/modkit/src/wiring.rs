//! Framework-level `wire_and_watch` utility for reconnection-safe
//! out-of-process gRPC wiring.
//!
//! Bridges `InstanceEventSource`, `ClientHub`, and `DirectoryClient` so that
//! gRPC clients are automatically re-wired when an `OoP` module reconnects on
//! a new endpoint.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use anyhow::Result;
use tokio::sync::{broadcast, watch};
use tokio_util::sync::CancellationToken;

use crate::DirectoryClient;
use crate::client_hub::ClientHub;
use crate::runtime::{InstanceEventKind, InstanceEventSource};

/// The watcher background task has exited (cancelled or event source closed).
#[derive(Debug, thiserror::Error)]
#[error("wire_and_watch background task has exited")]
pub struct WatcherGoneError;

/// Handle returned by [`wire_and_watch`] for monitoring re-wire activity.
pub struct GrpcWatcher {
    rewire_rx: watch::Receiver<u64>,
}

impl GrpcWatcher {
    /// Wait for the next re-wire to complete. Returns the total re-wire count.
    ///
    /// Useful for deterministic test synchronization and monitoring.
    ///
    /// # Errors
    ///
    /// Returns [`WatcherGoneError`] if the background watcher task has exited
    /// (e.g. cancelled or event source closed).
    pub async fn rewired(&mut self) -> std::result::Result<u64, WatcherGoneError> {
        self.rewire_rx
            .changed()
            .await
            .map_err(|_| WatcherGoneError)?;
        Ok(*self.rewire_rx.borrow())
    }
}

/// Wire a gRPC client into `ClientHub` and watch for instance events
/// to automatically re-wire when endpoints change.
///
/// # Arguments
///
/// * `hub` — the `ClientHub` where the client will be registered
/// * `directory` — resolves service endpoints (e.g. `LocalDirectoryClient`)
/// * `events` — source of instance lifecycle events (e.g. `ModuleManager`)
/// * `service_name` — the gRPC service name to watch
///   (e.g. `"calculator.v1.CalculatorService"`)
/// * `connect` — factory that creates a new `Arc<T>` client from an endpoint URI
/// * `cancel` — token to stop the background watcher task
///
/// # Errors
///
/// Returns an error if the initial endpoint resolution or connection fails.
pub async fn wire_and_watch<T: ?Sized + Send + Sync + 'static>(
    hub: Arc<ClientHub>,
    directory: Arc<dyn DirectoryClient>,
    events: Arc<dyn InstanceEventSource>,
    service_name: impl Into<String>,
    connect: impl Fn(String) -> Pin<Box<dyn Future<Output = Result<Arc<T>>> + Send>>
    + Send
    + Sync
    + 'static,
    cancel: CancellationToken,
) -> Result<GrpcWatcher> {
    let service_name = service_name.into();

    // Subscribe before initial wiring so events that fire during
    // resolve/connect are queued and processed by the watcher task.
    let (rewire_tx, rewire_rx) = watch::channel(0u64);
    let mut events_rx = events.subscribe();

    // Initial wiring: resolve → connect → register
    let endpoint = directory.resolve_grpc_service(&service_name).await?;
    let client = connect(endpoint.uri).await?;
    hub.register::<T>(client);

    let svc = service_name.clone();
    tokio::spawn(async move {
        let mut rewire_count: u64 = 0;

        loop {
            tokio::select! {
                () = cancel.cancelled() => break,
                event = events_rx.recv() => {
                    // In multi-instance topologies this re-resolves on *any*
                    // event for the watched service, even if the event doesn't
                    // affect the current client's endpoint. This is intentional:
                    // the re-wire is cheap (resolve + connect), last-writer-wins
                    // is safe for single-instance OoP, and comparing URIs would
                    // introduce false negatives (same-port reconnect, round-robin
                    // rotation). The Lagged path already unconditionally re-resolves.
                    // Determine whether to rewire, and whether failure should
                    // remove the existing client. Additive events (Registered,
                    // BecameHealthy) mean "not ready yet" on failure — keeping
                    // the existing client is strictly better than removing it.
                    let (should_rewire, remove_on_failure) = match &event {
                        Ok(ev) => {
                            let dominated = ev.services.contains(&svc);
                            let destructive = matches!(
                                ev.kind,
                                InstanceEventKind::Deregistered | InstanceEventKind::Evicted
                            );
                            (dominated, destructive)
                        }
                        Err(broadcast::error::RecvError::Lagged(_)) => (true, true),
                        Err(broadcast::error::RecvError::Closed) => break,
                    };

                    if !should_rewire {
                        continue;
                    }

                    match directory.resolve_grpc_service(&svc).await {
                        Ok(ep) => match connect(ep.uri).await {
                            Ok(new_client) => {
                                hub.register::<T>(new_client);
                                rewire_count += 1;
                                rewire_tx.send(rewire_count).ok();
                            }
                            Err(e) => {
                                if remove_on_failure {
                                    hub.remove::<T>();
                                }
                                tracing::warn!(
                                    service = %svc,
                                    error = %e,
                                    "wire_and_watch: connect failed{}",
                                    if remove_on_failure { ", removed stale client" } else { ", kept existing client" }
                                );
                            }
                        },
                        Err(e) => {
                            if remove_on_failure {
                                hub.remove::<T>();
                            }
                            tracing::warn!(
                                service = %svc,
                                error = %e,
                                "wire_and_watch: resolve failed{}",
                                if remove_on_failure { ", removed stale client" } else { ", kept existing client" }
                            );
                        }
                    }
                }
            }
        }
    });

    Ok(GrpcWatcher { rewire_rx })
}
