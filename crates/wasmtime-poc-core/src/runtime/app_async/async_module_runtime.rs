use wasmtime::Store;

use super::{AsyncWasmModuleStore, RuntimeEvent};

pub struct InstancedAsyncMqttEventLoopTask {
    pub(crate) runtime_event_sender: tokio::sync::mpsc::Sender<RuntimeEvent>,
    pub(crate) task_handle: tokio::task::JoinHandle<anyhow::Result<()>>,
}

pub enum AsyncMqttEventLoopTask {
    Instanced(InstancedAsyncMqttEventLoopTask),
    LockShared,
    MessageBusShared,
}

pub struct AsyncModuleRuntime {
    pub(super) store: Store<AsyncWasmModuleStore>,
    pub(super) module_mqtt_event_loop_task_info: Option<AsyncMqttEventLoopTask>,
    pub(super) module_instance_id: ModuleInstanceId,
    pub(super) module_name: String,
}

pub type ModuleInstanceId = u64;

pub enum MessageBusSharedMqttModuleEvent {
    NewModule {
        id: ModuleInstanceId,
        module_mqtt_event_sender: tokio::sync::mpsc::Sender<rumqttc::Event>,
    },
    ModuleFinished {
        id: ModuleInstanceId,
    },
}

pub enum LockSharedMqttModuleEvent {
    NewModule {
        id: ModuleInstanceId,
        module_mqtt_event_sender: tokio::sync::mpsc::Sender<rumqttc::Event>,
    },
    ModuleFinished {
        id: ModuleInstanceId,
    },
}
