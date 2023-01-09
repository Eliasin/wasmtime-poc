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

fn map_qos(qos: mqtt::QualityOfService) -> rumqttc::QoS {
    use mqtt::QualityOfService::*;
    use rumqttc::QoS;
    match qos {
        AtMostOnce => QoS::AtMostOnce,
        AtLeastOnce => QoS::AtLeastOnce,
        ExactlyOnce => QoS::ExactlyOnce,
    }
}

pub enum AsyncMqttConnection {
    MessageBusShared(MessageBusSharedAsyncMqttConnection),
    LockShared(LockSharedAsyncMqttConnection),
    Instanced(InstancedAsyncMqttConnection),
}

#[async_trait::async_trait]
impl mqtt::Mqtt for AsyncMqttConnection {
    async fn publish(
        &mut self,
        topic: String,
        qos: mqtt::QualityOfService,
        retain: bool,
        payload: Vec<u8>,
    ) -> anyhow::Result<Result<(), String>> {
        use AsyncMqttConnection::*;
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
        use AsyncMqttConnection::*;
        match self {
            MessageBusShared(v) => v.subscribe(topic, qos).await,
            LockShared(v) => v.subscribe(topic, qos).await,
            Instanced(v) => v.subscribe(topic, qos).await,
        }
    }

    async fn poll(&mut self) -> anyhow::Result<Result<mqtt::Event, String>> {
        use AsyncMqttConnection::*;
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
