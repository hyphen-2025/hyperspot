//! Wiring for Calculator SDK
//!
//! Provides `wire_and_watch_client` (reconnection-safe) to register the
//! gRPC client into ClientHub.

use std::sync::Arc;

use anyhow::Result;
use cf_system_sdks::directory::DirectoryClient;
use modkit::client_hub::ClientHub;
use modkit::runtime::InstanceEventSource;
use tokio_util::sync::CancellationToken;

use crate::SERVICE_NAME;
use crate::api::CalculatorClientV1;
use crate::client::CalculatorGrpcClient;

/// Wire the Calculator gRPC client into the ClientHub and watch for reconnections.
///
/// This function:
/// 1. Resolves the CalculatorService endpoint via `DirectoryClient`
/// 2. Creates a gRPC client and registers it in the ClientHub
/// 3. Spawns a background task that re-wires the client when instances change
///
/// Returns a [`modkit::GrpcWatcher`] for monitoring re-wire events.
pub async fn wire_and_watch_client(
    hub: &Arc<ClientHub>,
    directory: &Arc<dyn DirectoryClient>,
    events: &Arc<dyn InstanceEventSource>,
    cancel: CancellationToken,
) -> Result<modkit::GrpcWatcher> {
    modkit::wire_and_watch::<dyn CalculatorClientV1>(
        Arc::clone(hub),
        Arc::clone(directory),
        Arc::clone(events),
        SERVICE_NAME,
        |uri| {
            Box::pin(async move {
                let client = CalculatorGrpcClient::connect(&uri).await?;
                Ok(Arc::new(client) as Arc<dyn CalculatorClientV1>)
            })
        },
        cancel,
    )
    .await
}
