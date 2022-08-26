use serde_derive::Deserialize;
use std::{path::Path, time::Duration};

use crate::mqtt_api::MqttConnection;

#[derive(Deserialize, Clone)]
pub struct MqttRuntimeConfig {
    id: String,
    host: String,
    port: u16,
    allowed_sub_topics: Vec<String>,
    allowed_pub_topics: Vec<String>,
}

#[derive(Deserialize, Clone)]
pub struct ModuleRuntimeConfig {
    pub mqtt: MqttRuntimeConfig,
}

#[derive(Deserialize)]
pub struct ModuleConfig {
    pub runtime: ModuleRuntimeConfig,
    pub wasm_module_path: Box<Path>,
}

#[derive(Clone)]
pub struct WasmModuleStore {
    pub mqtt: MqttConnection,
}

impl WasmModuleStore {
    pub fn new(config: &ModuleRuntimeConfig) -> anyhow::Result<WasmModuleStore> {
        let mut mqttoptions = rumqttc::MqttOptions::new(
            config.mqtt.id.clone(),
            config.mqtt.host.clone(),
            config.mqtt.port,
        );
        mqttoptions.set_keep_alive(Duration::from_secs(5));

        let (client, connection) = rumqttc::Client::new(mqttoptions, 10);

        let mqtt = MqttConnection::new(
            client,
            connection,
            config.mqtt.allowed_sub_topics.clone(),
            config.mqtt.allowed_pub_topics.clone(),
        );

        Ok(WasmModuleStore { mqtt })
    }
}
