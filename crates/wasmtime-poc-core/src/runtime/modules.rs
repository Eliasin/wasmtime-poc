use anyhow::{bail, Context};
use serde_derive::Deserialize;
use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    time::Duration,
};
use tokio::sync::mpsc;

use crate::{
    api::fio_async_api::AsyncFileIOState,
    api::mqtt_async_api::{
        AsyncMqttConnection, InstancedAsyncMqttConnection, LockSharedAsyncMqttConnection,
        MessageBusSharedAsyncMqttConnection,
    },
};

use super::{
    AsyncMqttEventLoopTask, InstancedAsyncMqttEventLoopTask, ModuleInstanceId, RuntimeEvent,
    SharedLockEventLoop, SharedMessageBusEventLoop, SharedMqttEventLoop, SharedMqttModuleEvent,
};

#[derive(Deserialize, Clone)]
pub struct InstancedMqttRuntimeConfig {
    id: String,
    host: String,
    port: u16,
    allowed_sub_topics: Vec<String>,
    allowed_pub_topics: Vec<String>,
    event_channel_bound: Option<u32>,
}

#[derive(Deserialize, Clone)]
#[serde(untagged)]
pub enum MqttRuntimeConfig {
    Instanced {
        #[serde(flatten)]
        config: InstancedMqttRuntimeConfig,
    },
    Shared {
        runtime_id: String,
        allowed_sub_topics: Vec<String>,
        allowed_pub_topics: Vec<String>,
    },
}

#[derive(Deserialize, Clone, Copy)]
pub enum MqttFlavor {
    #[serde(rename = "instanced")]
    Instanced,
    #[serde(rename = "shared_lock")]
    SharedLock,
    #[serde(rename = "shared_message_bus")]
    SharedMessageBus,
}

pub type SharedMqttRuntimeId = String;
#[derive(Deserialize, Clone)]
pub struct SharedMqttRuntimeConfig {
    pub runtime_id: SharedMqttRuntimeId,
    pub client_id: String,
    pub host: String,
    pub port: u16,

    /// This is checked at runtime to be one of the "shared" variants
    pub flavor: MqttFlavor,
}

#[derive(Deserialize, Clone)]
pub struct FileIORuntimeConfig {
    allowed_write_files: Vec<String>,
    allowed_write_folders: Vec<String>,
    allowed_read_files: Vec<String>,
    allowed_read_folders: Vec<String>,
}

#[derive(Deserialize, Clone)]
pub struct ModuleRuntimeConfig {
    pub on_startup: bool,
    pub mqtt: Option<MqttRuntimeConfig>,
    pub fio: Option<FileIORuntimeConfig>,
    pub env: HashMap<String, String>,
}

#[derive(Deserialize, Clone)]
#[serde(untagged)]
pub enum ModuleLocator {
    TestingModule {
        module_id: String,
        test_module_repository_id: String,
    },
    ModuleFile {
        path: Box<Path>,
    },
}

#[derive(Deserialize, Clone)]
pub struct ModuleConfig {
    pub name: String,
    pub runtime: ModuleRuntimeConfig,
    pub module_locator: ModuleLocator,
}

#[derive(Deserialize, Clone)]
pub struct AppConfig {
    pub modules: Vec<ModuleConfig>,
    #[serde(default)]
    pub shared_mqtt_runtimes: Vec<SharedMqttRuntimeConfig>,
}

impl AppConfig {
    pub fn from_app_config_file(path: impl AsRef<Path>) -> anyhow::Result<AppConfig> {
        let config_file_contents = std::fs::read_to_string(&path).with_context(|| {
            format!(
                "Could not find app config file at path: {}",
                path.as_ref().display()
            )
        })?;

        toml::from_str(&config_file_contents).context("Malformed app config")
    }
}

pub struct AsyncMqttRuntime {
    pub mqtt: AsyncMqttConnection,
    pub event_channel_sender: mpsc::Sender<rumqttc::Event>,
    pub event_loop: rumqttc::EventLoop,
}

pub struct AsyncFileIORuntime {
    pub fio: AsyncFileIOState,
}

pub struct AsyncWasmModuleStore {
    pub mqtt_connection: Option<AsyncMqttConnection>,
    pub fio: Option<AsyncFileIOState>,
    pub env: HashMap<String, String>,
}

fn create_instanced_mqtt_runtime(
    mqtt_config: &InstancedMqttRuntimeConfig,
) -> anyhow::Result<AsyncMqttRuntime> {
    let mut mqtt_options = rumqttc::MqttOptions::new(
        mqtt_config.id.clone(),
        mqtt_config.host.clone(),
        mqtt_config.port,
    );
    mqtt_options.set_keep_alive(Duration::from_secs(5));

    let (client, event_loop) = rumqttc::AsyncClient::new(mqtt_options, 10);

    let event_channel_bound: usize = mqtt_config.event_channel_bound.unwrap_or(256).try_into()?;

    let (mqtt_event_sender, mqtt_event_receiver) = mpsc::channel(event_channel_bound);

    Ok(AsyncMqttRuntime {
        mqtt: AsyncMqttConnection::Instanced(InstancedAsyncMqttConnection::new(
            client,
            mqtt_event_receiver,
            mqtt_config.allowed_sub_topics.clone(),
            mqtt_config.allowed_pub_topics.clone(),
        )),
        event_channel_sender: mqtt_event_sender,
        event_loop,
    })
}

fn create_fio_runtime(fio_config: &FileIORuntimeConfig) -> anyhow::Result<AsyncFileIORuntime> {
    Ok(AsyncFileIORuntime {
        fio: AsyncFileIOState::new(
            fio_config
                .allowed_write_files
                .iter()
                .map(PathBuf::from)
                .collect(),
            fio_config
                .allowed_write_folders
                .iter()
                .map(PathBuf::from)
                .collect(),
            fio_config
                .allowed_read_files
                .iter()
                .map(PathBuf::from)
                .collect(),
            fio_config
                .allowed_read_folders
                .iter()
                .map(PathBuf::from)
                .collect(),
        ),
    })
}

pub async fn async_instanced_mqtt_event_loop_task(
    event_channel_sender: mpsc::Sender<rumqttc::Event>,
    mut runtime_event_receiver: mpsc::Receiver<RuntimeEvent>,
    mut event_loop: rumqttc::EventLoop,
) -> anyhow::Result<()> {
    // TODO: Exponential backoff retry for MQTT
    loop {
        tokio::select! {
            notification = event_loop.poll() => {
                if let Err(e) = event_channel_sender.send(notification?).await {
                    bail!("Error sending MQTT notification to event channel: {}", e);
                }
            },
            runtime_event = runtime_event_receiver.recv() => {
                match runtime_event {
                    None => {
                        bail!("Runtime event channel unexpectedly closed");
                    },
                    Some(runtime_event) => match runtime_event {
                        RuntimeEvent::RuntimeTaskStop => return Ok(()),
                    }
                }
            },
            else => { return Ok(()); },
        }
    }
}

fn create_instanced_async_mqtt_event_loop_task(
    event_loop: rumqttc::EventLoop,
    event_channel_sender: mpsc::Sender<rumqttc::Event>,
) -> InstancedAsyncMqttEventLoopTask {
    let (mqtt_event_loop_runtime_sender, mqtt_event_loop_runtime_receiver) = mpsc::channel(32);

    let mqtt_event_loop_task_handle = tokio::spawn(async move {
        async_instanced_mqtt_event_loop_task(
            event_channel_sender,
            mqtt_event_loop_runtime_receiver,
            event_loop,
        )
        .await
    });

    InstancedAsyncMqttEventLoopTask {
        runtime_event_sender: mqtt_event_loop_runtime_sender,
        task_handle: mqtt_event_loop_task_handle,
    }
}

async fn create_shared_lock_runtime(
    module_instance_id: ModuleInstanceId,
    runtime_id: String,
    allowed_sub_topics: &[String],
    allowed_pub_topics: &[String],
    event_loop: &SharedMqttEventLoop,
) -> anyhow::Result<(AsyncMqttConnection, AsyncMqttEventLoopTask)> {
    match event_loop {
        SharedMqttEventLoop::SharedLock(SharedLockEventLoop {
            mqtt_client,
            module_event_sender,
            runtime_event_sender: _,
            task_handle: _,
        }) => {
            let (mqtt_event_sender, mqtt_event_receiver) = tokio::sync::mpsc::channel(32);

            if let Err(e) = module_event_sender
                .send(SharedMqttModuleEvent::NewModule {
                    id: module_instance_id,
                    module_mqtt_event_sender: mqtt_event_sender,
                })
                .await
            {
                bail!(
                    "Failed to send new module event to shared message bus shared mqtt runtime: {}",
                    e
                )
            }

            let mqtt_connection =
                AsyncMqttConnection::LockShared(LockSharedAsyncMqttConnection::new(
                    mqtt_client.clone(),
                    mqtt_event_receiver,
                    allowed_sub_topics.to_vec(),
                    allowed_pub_topics.to_vec(),
                    runtime_id,
                ));

            Ok((mqtt_connection, AsyncMqttEventLoopTask::LockShared))
        }
        SharedMqttEventLoop::SharedMessageBus(SharedMessageBusEventLoop {
            mqtt_client_action_sender: _,
            module_event_sender: _,
            runtime_event_sender: _,
            task_handle: _,
        }) => {
            bail!(
                "Inconsistency detected in shared mqtt
                            runtimes, runtime event loop for message bus 
                            flavor matches shared lock module runtime_id"
            );
        }
    }
}

async fn create_shared_message_bus_runtime(
    module_instance_id: ModuleInstanceId,
    runtime_id: String,
    allowed_sub_topics: &[String],
    allowed_pub_topics: &[String],
    event_loop: &SharedMqttEventLoop,
) -> anyhow::Result<(AsyncMqttConnection, AsyncMqttEventLoopTask)> {
    match event_loop {
        SharedMqttEventLoop::SharedLock(SharedLockEventLoop {
            mqtt_client: _,
            module_event_sender: _,
            runtime_event_sender: _,
            task_handle: _,
        }) => {
            bail!(
                "Inconsistency detected in shared mqtt
                            runtimes, runtime event loop for shared lock
                            flavor matches message bus module runtime_id"
            );
        }
        SharedMqttEventLoop::SharedMessageBus(SharedMessageBusEventLoop {
            mqtt_client_action_sender,
            module_event_sender,
            runtime_event_sender: _,
            task_handle: _,
        }) => {
            let (mqtt_event_sender, mqtt_event_receiver) = tokio::sync::mpsc::channel(32);

            if let Err(e) = module_event_sender
                .send(SharedMqttModuleEvent::NewModule {
                    id: module_instance_id,
                    module_mqtt_event_sender: mqtt_event_sender,
                })
                .await
            {
                bail!(
                    "Failed to send new module event to shared message bus shared mqtt runtime: {}",
                    e
                )
            }

            let mqtt_connection =
                AsyncMqttConnection::MessageBusShared(MessageBusSharedAsyncMqttConnection::new(
                    mqtt_client_action_sender.clone(),
                    mqtt_event_receiver,
                    allowed_sub_topics.to_vec(),
                    allowed_pub_topics.to_vec(),
                    runtime_id,
                ));

            Ok((mqtt_connection, AsyncMqttEventLoopTask::MessageBusShared))
        }
    }
}

pub async fn create_async_mqtt_runtime(
    module_instance_id: ModuleInstanceId,
    mqtt_config: &MqttRuntimeConfig,
    shared_mqtt_event_loops: &mut HashMap<SharedMqttRuntimeId, SharedMqttEventLoop>,
) -> anyhow::Result<(AsyncMqttConnection, AsyncMqttEventLoopTask)> {
    match mqtt_config {
        MqttRuntimeConfig::Shared {
            runtime_id,
            allowed_pub_topics,
            allowed_sub_topics,
        } => {
            let flavor = shared_mqtt_event_loops
                .iter()
                .find(|(id, _)| *id == runtime_id);
            match flavor {
                Some((_, shared_mqtt_runtime)) => match shared_mqtt_runtime.flavor() {
                    MqttFlavor::Instanced => bail!("Configured flavor must be shared variant"),
                    MqttFlavor::SharedLock => {
                        create_shared_lock_runtime(
                            module_instance_id,
                            runtime_id.clone(),
                            allowed_sub_topics,
                            allowed_pub_topics,
                            shared_mqtt_runtime,
                        )
                        .await
                    }
                    MqttFlavor::SharedMessageBus => {
                        create_shared_message_bus_runtime(
                            module_instance_id,
                            runtime_id.clone(),
                            allowed_sub_topics,
                            allowed_pub_topics,
                            shared_mqtt_runtime,
                        )
                        .await
                    }
                },
                None => bail!(
                    "Module requests runtime with id {} which is not defined",
                    runtime_id
                ),
            }
        }
        MqttRuntimeConfig::Instanced {
            config: mqtt_config,
        } => {
            let AsyncMqttRuntime {
                mqtt,
                event_channel_sender,
                event_loop,
            } = create_instanced_mqtt_runtime(mqtt_config)?;

            let mqtt_connection = mqtt;
            let module_mqtt_event_loop_task_info = AsyncMqttEventLoopTask::Instanced(
                create_instanced_async_mqtt_event_loop_task(event_loop, event_channel_sender),
            );

            Ok((mqtt_connection, module_mqtt_event_loop_task_info))
        }
    }
}

pub fn initialize_fio_for_module(
    module_runtime_config: &ModuleRuntimeConfig,
) -> Option<anyhow::Result<AsyncFileIORuntime>> {
    module_runtime_config.fio.as_ref().map(create_fio_runtime)
}
