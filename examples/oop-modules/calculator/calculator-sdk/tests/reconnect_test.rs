//! Integration test reproducing OoP reconnect bug (cyberfabric-core#671).
//!
//! **Bug**: When an OoP follower disconnects and reconnects on a new ephemeral
//! port, the master's gateway still sends gRPC requests to the old dead socket
//! because `wire_client()` is guarded by `OnceCell` and the cached tonic
//! `Channel` in `ClientHub` is never refreshed.
//!
//! **Fix**: `wire_and_watch_client` subscribes to instance events and
//! automatically re-wires the client when instances change.

use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::TcpListenerStream;
use tokio_util::sync::CancellationToken;
use tonic::{Request, Response, Status, transport::Server};
use uuid::Uuid;

use calculator_sdk::{
    AddRequest, AddResponse, CalculatorClientV1, CalculatorService, CalculatorServiceServer,
    SERVICE_NAME, wire_and_watch_client,
};
use modkit::runtime::InstanceEventSource;
use modkit::{
    ClientHub, DirectoryClient, Endpoint, LocalDirectoryClient, ModuleInstance, ModuleManager,
};
use modkit_security::SecurityContext;

// ---------------------------------------------------------------------------
// Test server: minimal CalculatorService that returns a + b
// ---------------------------------------------------------------------------

struct TestCalculatorService;

#[tonic::async_trait]
impl CalculatorService for TestCalculatorService {
    async fn add(&self, request: Request<AddRequest>) -> Result<Response<AddResponse>, Status> {
        let req = request.into_inner();
        Ok(Response::new(AddResponse { sum: req.a + req.b }))
    }
}

// ---------------------------------------------------------------------------
// Test server lifecycle helpers
// ---------------------------------------------------------------------------

struct TestServer {
    port: u16,
    cancel: CancellationToken,
    handle: JoinHandle<()>,
}

/// Spawn a tonic gRPC server on an ephemeral port (`127.0.0.1:0`).
///
/// The `TcpListener` is bound *before* the server task is spawned, so the
/// port is ready for connections as soon as this function returns — no sleeps
/// needed.
async fn spawn_test_server() -> TestServer {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let cancel = CancellationToken::new();
    let cancel_clone = cancel.clone();

    let incoming = TcpListenerStream::new(listener);
    let handle = tokio::spawn(async move {
        Server::builder()
            .add_service(CalculatorServiceServer::new(TestCalculatorService))
            .serve_with_incoming_shutdown(incoming, async move {
                cancel_clone.cancelled().await;
            })
            .await
            .unwrap();
    });

    TestServer {
        port,
        cancel,
        handle,
    }
}

impl TestServer {
    /// Signal shutdown and wait for the server task to fully exit.
    ///
    /// After this returns the TCP listener is dropped and the port is free.
    /// Any existing HTTP/2 connections are torn down.
    async fn shutdown(self) {
        self.cancel.cancel();
        self.handle.await.unwrap();
    }
}

// ---------------------------------------------------------------------------
// Helper: register a healthy instance in ModuleManager
// ---------------------------------------------------------------------------

/// Creates a `ModuleInstance` for "calculator" with the given port and registers
/// it in `ModuleManager`, but does NOT send a heartbeat. The instance stays in
/// `Registered` state (not yet discoverable by `pick_service_round_robin`).
fn register_instance_only(mgr: &ModuleManager, port: u16) -> Uuid {
    let instance_id = Uuid::new_v4();
    let instance = Arc::new(
        ModuleInstance::new("calculator", instance_id)
            .with_grpc_service(SERVICE_NAME, Endpoint::http("127.0.0.1", port)),
    );
    mgr.register_instance(instance);
    instance_id
}

/// Creates a `ModuleInstance` for "calculator" with the given port, registers
/// it in `ModuleManager`, and sends a heartbeat to transition it to `Healthy`
/// so that `pick_service_round_robin` can discover it.
fn register_healthy_instance(mgr: &ModuleManager, port: u16) -> Uuid {
    let instance_id = Uuid::new_v4();
    let instance = Arc::new(
        ModuleInstance::new("calculator", instance_id)
            .with_grpc_service(SERVICE_NAME, Endpoint::http("127.0.0.1", port)),
    );
    mgr.register_instance(instance);
    mgr.update_heartbeat("calculator", instance_id, Instant::now());
    instance_id
}

// ---------------------------------------------------------------------------
// The tests
// ---------------------------------------------------------------------------

/// Tests that `wire_and_watch_client` re-wires the gRPC client when an OoP
/// module reconnects on a different port.
///
/// After server1 shuts down and server2 starts on a new port, the watcher
/// receives the `InstanceEvent::Registered` event, re-resolves the endpoint,
/// and atomically replaces the client in `ClientHub`.
#[tokio::test]
async fn test_client_reaches_new_server_after_oop_reconnect() {
    // === Phase 1: Start server1, wire the client, verify it works =========

    let server1 = spawn_test_server().await;
    let port_a = server1.port;

    let mgr = Arc::new(ModuleManager::new());
    let directory: Arc<dyn DirectoryClient> = Arc::new(LocalDirectoryClient::new(mgr.clone()));
    let events: Arc<dyn InstanceEventSource> = mgr.clone();
    let hub = Arc::new(ClientHub::new());
    let cancel = CancellationToken::new();

    let instance_id_1 = register_healthy_instance(&mgr, port_a);

    // wire_and_watch_client resolves the endpoint, creates a gRPC client,
    // registers it in the hub, and starts a background watcher.
    let _watcher = wire_and_watch_client(&hub, &directory, &events, cancel.clone())
        .await
        .unwrap();

    let client = hub.get::<dyn CalculatorClientV1>().unwrap();
    let ctx = SecurityContext::anonymous();

    let result = client.add(&ctx, 2, 3).await;
    assert_eq!(result.unwrap(), 5, "Phase 1: add(2, 3) should return 5");

    // === Phase 2: Simulate OoP disconnect + reconnect =====================

    // Shutdown server1 and wait for the task to fully exit.
    server1.shutdown().await;
    mgr.deregister("calculator", instance_id_1);

    // Start server2 on a *different* ephemeral port.
    let server2 = spawn_test_server().await;
    let port_b = server2.port;
    assert_ne!(port_a, port_b, "server2 must bind to a different port");

    // Register server2 as a new healthy instance in the directory.
    let _instance_id_2 = register_healthy_instance(&mgr, port_b);

    // === Phase 3: Verify the watcher re-wired the client ==================
    //
    // The watcher received the Registered event (from register_healthy_instance)
    // and re-resolved + reconnected. Use a retry loop to allow the async
    // re-wire to complete.

    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        // Re-fetch client from hub each iteration (don't cache the Arc)
        let Ok(client) = hub.get::<dyn CalculatorClientV1>() else {
            if tokio::time::Instant::now() < deadline {
                tokio::time::sleep(Duration::from_millis(50)).await;
                continue;
            }
            panic!("client still absent from hub after reconnect");
        };
        match client.add(&ctx, 10, 20).await {
            Ok(sum) => {
                assert_eq!(sum, 30, "add(10,20) should return 30");
                break;
            }
            Err(_) if tokio::time::Instant::now() < deadline => {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            Err(e) => panic!("add(10,20) still failing after reconnect: {e}"),
        }
    }

    // Cleanup
    cancel.cancel();
    server2.shutdown().await;
}

/// Deterministic test using `GrpcWatcher::rewired()` to synchronize on
/// the re-wire event instead of a retry loop.
#[tokio::test]
async fn test_grpc_watcher_signals_rewire() {
    let server1 = spawn_test_server().await;
    let port_a = server1.port;

    let mgr = Arc::new(ModuleManager::new());
    let directory: Arc<dyn DirectoryClient> = Arc::new(LocalDirectoryClient::new(mgr.clone()));
    let events: Arc<dyn InstanceEventSource> = mgr.clone();
    let hub = Arc::new(ClientHub::new());
    let cancel = CancellationToken::new();

    let instance_id_1 = register_healthy_instance(&mgr, port_a);

    let mut watcher = wire_and_watch_client(&hub, &directory, &events, cancel.clone())
        .await
        .unwrap();

    // Verify initial wiring works
    let ctx = SecurityContext::anonymous();
    let client = hub.get::<dyn CalculatorClientV1>().unwrap();
    assert_eq!(client.add(&ctx, 1, 1).await.unwrap(), 2);

    // Simulate reconnect
    server1.shutdown().await;
    mgr.deregister("calculator", instance_id_1);

    let server2 = spawn_test_server().await;
    let port_b = server2.port;
    assert_ne!(port_a, port_b);

    let _instance_id_2 = register_healthy_instance(&mgr, port_b);

    // Wait for the watcher to signal a re-wire (deterministic, no retry loop)
    let count = watcher.rewired().await.unwrap();
    assert!(count >= 1, "expected at least 1 re-wire, got {count}");

    // Verify new client reaches server2
    let client = hub.get::<dyn CalculatorClientV1>().unwrap();
    assert_eq!(
        client.add(&ctx, 100, 200).await.unwrap(),
        300,
        "after re-wire, add(100, 200) should return 300"
    );

    // Cleanup
    cancel.cancel();
    server2.shutdown().await;
}

/// Reproduces the race between registration and first heartbeat.
///
/// When an OoP module registers but hasn't sent its first heartbeat yet,
/// the instance is in `Registered` state. The watcher receives the
/// `Registered` event but `pick_service_round_robin` can't find the
/// instance (it filters on `Healthy | Ready`). Without the fix, this
/// would remove the existing client from `ClientHub` permanently.
///
/// The fix has two parts:
/// 1. `update_heartbeat` fires a `BecameHealthy` event on Registered→Healthy
/// 2. The watcher doesn't remove the client on resolve failure for additive events
#[tokio::test]
async fn test_watcher_survives_registration_before_healthy() {
    // === Phase 1: Start server1, wire the client, verify it works =========

    let server1 = spawn_test_server().await;
    let port_a = server1.port;

    let mgr = Arc::new(ModuleManager::new());
    let directory: Arc<dyn DirectoryClient> = Arc::new(LocalDirectoryClient::new(mgr.clone()));
    let events: Arc<dyn InstanceEventSource> = mgr.clone();
    let hub = Arc::new(ClientHub::new());
    let cancel = CancellationToken::new();

    let instance_id_1 = register_healthy_instance(&mgr, port_a);

    let mut watcher = wire_and_watch_client(&hub, &directory, &events, cancel.clone())
        .await
        .unwrap();

    let ctx = SecurityContext::anonymous();
    let client = hub.get::<dyn CalculatorClientV1>().unwrap();
    assert_eq!(client.add(&ctx, 2, 3).await.unwrap(), 5);

    // === Phase 2: Simulate disconnect =====================================

    server1.shutdown().await;
    mgr.deregister("calculator", instance_id_1);

    // === Phase 3: Register server2 WITHOUT heartbeat (stays Registered) ===

    let server2 = spawn_test_server().await;
    let port_b = server2.port;
    assert_ne!(port_a, port_b, "server2 must bind to a different port");

    let instance_id_2 = register_instance_only(&mgr, port_b);

    // Yield to let the watcher process the Registered event. The resolve
    // will fail (instance is Registered, not Healthy), but the fix ensures
    // the watcher does NOT remove the existing client.
    tokio::task::yield_now().await;
    tokio::time::sleep(Duration::from_millis(50)).await;

    // === Phase 4: Send heartbeat → BecameHealthy event fires ==============

    mgr.update_heartbeat("calculator", instance_id_2, Instant::now());

    // Wait for the watcher to re-wire via the BecameHealthy event
    let count = tokio::time::timeout(Duration::from_secs(5), watcher.rewired())
        .await
        .expect("timed out waiting for rewire")
        .expect("watcher gone");
    assert!(count >= 1, "expected at least 1 re-wire, got {count}");

    // Verify the new client reaches server2
    let client = hub.get::<dyn CalculatorClientV1>().unwrap();
    assert_eq!(
        client.add(&ctx, 100, 200).await.unwrap(),
        300,
        "after BecameHealthy re-wire, add(100, 200) should return 300"
    );

    // Cleanup
    cancel.cancel();
    server2.shutdown().await;
}
