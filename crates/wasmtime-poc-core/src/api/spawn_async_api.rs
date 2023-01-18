wasmtime::component::bindgen!({
    path: "../../wit-bindgen/apis.wit",
    async: true,
});

pub use spawn::add_to_linker;
use tokio::sync::mpsc;

use crate::runtime::{SpawnRequest, SpawnRuntimeConfig};

pub struct SpawnState {
    spawn_request_sender: mpsc::Sender<SpawnRequest>,
    allowed_modules: Vec<String>,
}

impl SpawnState {
    pub fn from_config(
        spawn_runtim_config: &SpawnRuntimeConfig,
        spawn_request_sender: mpsc::Sender<SpawnRequest>,
    ) -> Self {
        let SpawnRuntimeConfig { allowed_modules } = spawn_runtim_config.clone();

        Self {
            allowed_modules,
            spawn_request_sender,
        }
    }
}

#[async_trait::async_trait]
impl spawn::Spawn for SpawnState {
    async fn spawn(
        &mut self,
        module_name: String,
        arg: Option<Vec<u8>>,
    ) -> anyhow::Result<Result<(), String>> {
        Ok(if self.allowed_modules.contains(&module_name) {
            self.spawn_request_sender
                .send(SpawnRequest { module_name, arg })
                .await
                .map_err(|_| "Could not send module spawn request".to_string())
        } else {
            Err(format!(
                "Module spawn request for {module_name} is not permitted"
            ))
        })
    }
}
