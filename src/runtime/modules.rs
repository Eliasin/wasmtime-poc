use anyhow::{anyhow, Context};
use serde_derive::Deserialize;
use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    time::Duration,
};
use tokio::sync::mpsc;

use super::RuntimeEvent;
use crate::{api::fio_async_api::FileIOState, api::mqtt_async_api::MqttConnection};

#[derive(Deserialize, Clone)]
pub struct MqttRuntimeConfig {
    id: String,
    host: String,
    port: u16,
    allowed_sub_topics: Vec<String>,
    allowed_pub_topics: Vec<String>,
    event_channel_bound: Option<u32>,
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
pub struct ModuleConfig {
    pub name: String,
    pub runtime: ModuleRuntimeConfig,
    pub wasm_module_path: Box<Path>,
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

        Ok(toml::from_str(&config_file_contents).context("Malformed app config")?)
    }
}

pub struct AsyncMqttRuntime {
    pub mqtt: MqttConnection,
    pub event_channel_sender: mpsc::Sender<rumqttc::Event>,
    pub event_loop: rumqttc::EventLoop,
}

pub struct FileIORuntime {
    pub fio: FileIOState,
}

pub struct WasmModuleStore {
    pub mqtt_connection: Option<MqttConnection>,
    pub fio: Option<FileIOState>,
    pub env: HashMap<String, String>,
}

fn create_mqtt_runtime(mqtt_config: &MqttRuntimeConfig) -> anyhow::Result<AsyncMqttRuntime> {
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
        mqtt: MqttConnection::new(
            client,
            mqtt_event_receiver,
            mqtt_config.allowed_sub_topics.clone(),
            mqtt_config.allowed_pub_topics.clone(),
        ),
        event_channel_sender: mqtt_event_sender,
        event_loop,
    })
}

fn create_fio_runtime(fio_config: &FileIORuntimeConfig) -> anyhow::Result<FileIORuntime> {
    Ok(FileIORuntime {
        fio: FileIOState::new(
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

pub async fn async_mqtt_event_loop_task(
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

pub fn initialize_async_mqtt_for_module(
    module_runtime_config: &ModuleRuntimeConfig,
) -> Option<anyhow::Result<AsyncMqttRuntime>> {
    module_runtime_config.mqtt.as_ref().map(create_mqtt_runtime)
}

pub fn initialize_fio_for_module(
    module_runtime_config: &ModuleRuntimeConfig,
) -> Option<anyhow::Result<FileIORuntime>> {
    module_runtime_config.fio.as_ref().map(create_fio_runtime)
}
