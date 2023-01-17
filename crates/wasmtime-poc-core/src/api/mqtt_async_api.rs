use anyhow::anyhow;

pub use instanced::*;
pub use lock_shared::*;
pub use message_bus_shared::*;

mod instanced;
mod lock_shared;
mod message_bus_shared;

wasmtime::component::bindgen!({
    path: "../../wit-bindgen/apis.wit",
    async: true,
});

pub use mqtt::add_to_linker;

use crate::runtime::SharedMqttRuntimeId;

fn map_qos(qos: mqtt::QualityOfService) -> rumqttc::QoS {
    use mqtt::QualityOfService::*;
    use rumqttc::QoS;
    match qos {
        AtMostOnce => QoS::AtMostOnce,
        AtLeastOnce => QoS::AtLeastOnce,
        ExactlyOnce => QoS::ExactlyOnce,
    }
}

pub enum MqttConnection {
    MessageBusShared(MessageBusSharedConnection),
    LockShared(LockSharedConnection),
    Instanced(InstancedConnection),
}

impl MqttConnection {
    pub fn runtime_id(&self) -> Option<SharedMqttRuntimeId> {
        match self {
            MqttConnection::MessageBusShared(connection) => Some(connection.runtime_id().clone()),
            MqttConnection::LockShared(connection) => Some(connection.runtime_id().clone()),
            MqttConnection::Instanced(_) => None,
        }
    }
}

#[async_trait::async_trait]
impl mqtt::Mqtt for MqttConnection {
    async fn publish(
        &mut self,
        topic: String,
        qos: mqtt::QualityOfService,
        retain: bool,
        payload: Vec<u8>,
    ) -> anyhow::Result<Result<(), String>> {
        use MqttConnection::*;
        match self {
            MessageBusShared(v) => v.publish(topic, qos, retain, payload).await,
            LockShared(v) => v.publish(topic, qos, retain, payload).await,
            Instanced(v) => v.publish(topic, qos, retain, payload).await,
        }
    }

    async fn subscribe(
        &mut self,
        topic: String,
        qos: mqtt::QualityOfService,
    ) -> anyhow::Result<Result<(), String>> {
        use MqttConnection::*;
        match self {
            MessageBusShared(v) => v.subscribe(topic, qos).await,
            LockShared(v) => v.subscribe(topic, qos).await,
            Instanced(v) => v.subscribe(topic, qos).await,
        }
    }

    async fn poll(&mut self) -> anyhow::Result<Result<mqtt::Event, String>> {
        use MqttConnection::*;
        match self {
            MessageBusShared(v) => v.poll().await,
            LockShared(v) => v.poll().await,
            Instanced(v) => v.poll().await,
        }
    }
}

#[async_trait::async_trait]
impl<M: mqtt::Mqtt + Send + Sync> mqtt::Mqtt for Option<M> {
    async fn publish(
        &mut self,
        topic: String,
        qos: mqtt::QualityOfService,
        retain: bool,
        payload: Vec<u8>,
    ) -> anyhow::Result<Result<(), String>> {
        if let Some(connection) = self {
            connection.publish(topic, qos, retain, payload).await
        } else {
            Err(anyhow!("Module does not have configured mqtt runtime"))
        }
    }

    async fn subscribe(
        &mut self,
        topic: String,
        qos: mqtt::QualityOfService,
    ) -> anyhow::Result<Result<(), String>> {
        if let Some(connection) = self {
            connection.subscribe(topic, qos).await
        } else {
            Err(anyhow!("Module does not have configured mqtt runtime"))
        }
    }

    async fn poll(&mut self) -> anyhow::Result<Result<mqtt::Event, String>> {
        if let Some(connection) = self {
            connection.poll().await
        } else {
            Err(anyhow!("Module does not have configured mqtt runtime"))
        }
    }
}
