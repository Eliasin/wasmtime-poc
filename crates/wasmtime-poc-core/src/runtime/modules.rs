use anyhow::{anyhow, Context};
use serde_derive::Deserialize;
use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    time::Duration,
};
use tokio::sync::mpsc;

use super::{AsyncMqttEventLoopTask, InstancedAsyncMqttEventLoopTask, RuntimeEvent};
use crate::{
    api::fio_async_api::AsyncFileIOState,
    api::mqtt_async_api::{AsyncMqttConnection, InstancedAsyncMqttConnection},
};

#[derive(Deserialize, Clone, Copy)]
#[serde(untagged)]
pub enum MqttFlavor {
    #[serde(rename(deserialize = "instanced"))]
    Instanced,
    #[serde(rename(deserialize = "shared_lock"))]
    SharedLock,
    #[serde(rename(deserialize = "shared_message_bus"))]
    SharedMessageBus,
}

#[derive(Deserialize, Clone)]
pub struct MqttRuntimeConfig {
    id: String,
    host: String,
    port: u16,
    allowed_sub_topics: Vec<String>,
    allowed_pub_topics: Vec<String>,
    event_channel_bound: Option<u32>,
    flavor: Option<MqttFlavor>,
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

#[derive(Deserialize)]
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

#[derive(Deserialize)]
pub struct ModuleConfig {
    pub name: String,
    pub runtime: ModuleRuntimeConfig,
    pub module_locator: ModuleLocator,
}

#[derive(Deserialize)]
pub struct AppConfig {
    pub modules: Vec<ModuleConfig>,
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
    mqtt_config: &MqttRuntimeConfig,
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
                    return Err(anyhow!("Error sending MQTT notification to event channel: {}", e));
                }
            },
            runtime_event = runtime_event_receiver.recv() => {
                match runtime_event {
                    None => {
                        return Err(anyhow!("Runtime event channel unexpectedly closed"));
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

pub fn create_async_mqtt_runtime(
    mqtt_config: &MqttRuntimeConfig,
) -> anyhow::Result<(AsyncMqttConnection, AsyncMqttEventLoopTask)> {
    match mqtt_config.flavor.unwrap_or(MqttFlavor::Instanced) {
        MqttFlavor::SharedLock => todo!(),
        MqttFlavor::SharedMessageBus => todo!(),
        MqttFlavor::Instanced => {
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
