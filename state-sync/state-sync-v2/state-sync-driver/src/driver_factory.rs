// Copyright (c) Aptos
// SPDX-License-Identifier: Apache-2.0

use crate::notification_handlers::ClientNotificationHandler;
use crate::{
    driver::{DriverConfiguration, StateSyncDriver},
    driver_client::{ClientNotification, ClientNotificationListener, DriverClient},
    metadata_storage::MetadataStorageInterface,
    notification_handlers::{
        CommitNotificationListener, ConsensusNotificationHandler, ErrorNotificationListener,
        MempoolNotificationHandler,
    },
    storage_synchronizer::StorageSynchronizer,
};
use aptos_config::config::NodeConfig;
use aptos_data_client::aptosnet::AptosNetDataClient;
use aptos_infallible::Mutex;
use aptos_types::move_resource::MoveStorage;
use aptos_types::waypoint::Waypoint;
use consensus_notifications::ConsensusNotificationListener;
use data_streaming_service::streaming_client::StreamingServiceClient;
use event_notifications::{EventNotificationSender, EventSubscriptionService};
use executor_types::ChunkExecutorTrait;
use futures::channel::mpsc;
use futures::executor::block_on;
use mempool_notifications::MempoolNotificationSender;
use std::sync::Arc;
use storage_interface::DbReaderWriter;
use tokio::runtime::{Builder, Runtime};

/// Creates a new state sync driver and client
pub struct DriverFactory<MetadataStorage> {
    client_notification_sender: mpsc::UnboundedSender<ClientNotification>,
    metadata_storage: MetadataStorage,
    _driver_runtime: Option<Runtime>,
}

impl<MetadataStorage: MetadataStorageInterface + Clone + Send + Sync + 'static>
    DriverFactory<MetadataStorage>
{
    /// Creates and spawns a new state sync driver
    pub fn create_and_spawn_driver<
        ChunkExecutor: ChunkExecutorTrait + 'static,
        MempoolNotifier: MempoolNotificationSender + 'static,
    >(
        create_runtime: bool,
        node_config: &NodeConfig,
        waypoint: Waypoint,
        storage: DbReaderWriter,
        chunk_executor: Arc<ChunkExecutor>,
        mempool_notification_sender: MempoolNotifier,
        metadata_storage: MetadataStorage,
        consensus_listener: ConsensusNotificationListener,
        mut event_subscription_service: EventSubscriptionService,
        aptos_data_client: AptosNetDataClient,
        streaming_service_client: StreamingServiceClient,
    ) -> Self {
        // Notify subscribers of the initial on-chain config values
        match (&*storage.reader).fetch_latest_state_checkpoint_version() {
            Ok(synced_version) => {
                if let Err(error) =
                    event_subscription_service.notify_initial_configs(synced_version)
                {
                    panic!(
                        "Failed to notify subscribers of initial on-chain configs: {:?}",
                        error
                    )
                }
            }
            Err(error) => panic!("Failed to fetch the initial synced version: {:?}", error),
        }

        // Create the client notification listener and handler
        let (client_notification_sender, client_notification_receiver) = mpsc::unbounded();
        let client_notification_listener =
            ClientNotificationListener::new(client_notification_receiver);
        let client_notification_handler =
            ClientNotificationHandler::new(client_notification_listener);

        // Create various notification listeners and handlers
        let (commit_notification_sender, commit_notification_listener) =
            CommitNotificationListener::new();
        let consensus_notification_handler = ConsensusNotificationHandler::new(consensus_listener);
        let (error_notification_sender, error_notification_listener) =
            ErrorNotificationListener::new();
        let mempool_notification_handler =
            MempoolNotificationHandler::new(mempool_notification_sender);

        // Create a new runtime (if required)
        let driver_runtime = if create_runtime {
            Some(
                Builder::new_multi_thread()
                    .thread_name("state-sync-driver")
                    .enable_all()
                    .build()
                    .expect("Failed to create state sync v2 driver runtime!"),
            )
        } else {
            None
        };

        // Create the storage synchronizer
        let event_subscription_service = Arc::new(Mutex::new(event_subscription_service));
        let (storage_synchronizer, _, _) = StorageSynchronizer::new(
            node_config.state_sync.state_sync_driver,
            chunk_executor,
            commit_notification_sender,
            error_notification_sender,
            event_subscription_service.clone(),
            mempool_notification_handler.clone(),
            metadata_storage.clone(),
            storage.clone(),
            driver_runtime.as_ref(),
        );

        // Create the driver configuration
        let driver_configuration = DriverConfiguration::new(
            node_config.state_sync.state_sync_driver,
            node_config.base.role,
            waypoint,
        );

        // Create the state sync driver
        let state_sync_driver = StateSyncDriver::new(
            client_notification_handler,
            commit_notification_listener,
            consensus_notification_handler,
            driver_configuration,
            error_notification_listener,
            event_subscription_service,
            mempool_notification_handler,
            metadata_storage.clone(),
            storage_synchronizer,
            aptos_data_client,
            streaming_service_client,
            storage.reader,
        );

        // Spawn the driver
        if let Some(driver_runtime) = &driver_runtime {
            driver_runtime.spawn(state_sync_driver.start_driver());
        } else {
            tokio::spawn(state_sync_driver.start_driver());
        }

        Self {
            client_notification_sender,
            _driver_runtime: driver_runtime,
            metadata_storage,
        }
    }

    /// Returns a new client that can be used to communicate with the driver
    pub fn create_driver_client(&self) -> DriverClient<MetadataStorage> {
        DriverClient::new(
            self.metadata_storage.clone(),
            self.client_notification_sender.clone(),
        )
    }
}

/// A struct for holding the various runtimes required by state sync v2.
/// Note: it's useful to maintain separate runtimes because the logger
/// can prepend all logs with the runtime thread name.
pub struct StateSyncRuntimes<MetadataStorage> {
    _aptos_data_client: Runtime,
    state_sync: DriverFactory<MetadataStorage>,
    _storage_service: Runtime,
    _streaming_service: Runtime,
}

impl<MetadataStorage: MetadataStorageInterface + Clone + Send + Sync + 'static>
    StateSyncRuntimes<MetadataStorage>
{
    pub fn new(
        aptos_data_client: Runtime,
        state_sync: DriverFactory<MetadataStorage>,
        storage_service: Runtime,
        streaming_service: Runtime,
    ) -> Self {
        Self {
            _aptos_data_client: aptos_data_client,
            state_sync,
            _storage_service: storage_service,
            _streaming_service: streaming_service,
        }
    }

    pub fn block_until_completed(&self) {
        let state_sync_client = self.state_sync.create_driver_client();
        block_on(async move { state_sync_client.notify_once_completed().await })
            .expect("State sync v2 initialization failure");
    }
}
