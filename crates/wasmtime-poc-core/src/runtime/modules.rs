use anyhow::Context;
use serde_derive::Deserialize;
use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    time::Duration,
};
use tokio::sync::mpsc;

use crate::{
    api::fio_async_api::FileIOState,
    api::mqtt_async_api::{InstancedConnection, MqttConnection},
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
pub enum SharedMqttFlavor {
    #[serde(rename = "shared_lock")]
    Lock,
    #[serde(rename = "shared_message_bus")]
    MessageBus,
}

pub type SharedMqttRuntimeId = String;
#[derive(Deserialize, Clone)]
pub struct SharedMqttRuntimeConfig {
    pub runtime_id: SharedMqttRuntimeId,
    pub client_id: String,
    pub host: String,
    pub port: u16,

    pub flavor: SharedMqttFlavor,
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

pub struct MqttRuntime {
    pub mqtt: MqttConnection,
    pub event_channel_sender: mpsc::Sender<rumqttc::Event>,
    pub event_loop: rumqttc::EventLoop,
}

impl MqttRuntime {
    pub fn create_instanced_mqtt_runtime(
        mqtt_config: &InstancedMqttRuntimeConfig,
    ) -> anyhow::Result<MqttRuntime> {
        let mut mqtt_options = rumqttc::MqttOptions::new(
            mqtt_config.id.clone(),
            mqtt_config.host.clone(),
            mqtt_config.port,
        );
        mqtt_options.set_keep_alive(Duration::from_secs(5));

        let (client, event_loop) = rumqttc::AsyncClient::new(mqtt_options, 10);

        let event_channel_bound: usize =
            mqtt_config.event_channel_bound.unwrap_or(256).try_into()?;

        let (mqtt_event_sender, mqtt_event_receiver) = mpsc::channel(event_channel_bound);

        Ok(MqttRuntime {
            mqtt: MqttConnection::Instanced(InstancedConnection::new(
                client,
                mqtt_event_receiver,
                mqtt_config.allowed_sub_topics.clone(),
                mqtt_config.allowed_pub_topics.clone(),
            )),
            event_channel_sender: mqtt_event_sender,
            event_loop,
        })
    }
}

pub struct AsyncFileIORuntime {
    pub fio: FileIOState,
}

pub struct AsyncWasmModuleStore {
    pub mqtt_connection: Option<MqttConnection>,
    pub fio: Option<FileIOState>,
    pub env: HashMap<String, String>,
}

fn create_fio_runtime(fio_config: &FileIORuntimeConfig) -> anyhow::Result<AsyncFileIORuntime> {
    Ok(AsyncFileIORuntime {
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

pub fn initialize_fio_for_module(
    module_runtime_config: &ModuleRuntimeConfig,
) -> Option<anyhow::Result<AsyncFileIORuntime>> {
    module_runtime_config.fio.as_ref().map(create_fio_runtime)
}
