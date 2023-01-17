use anyhow::bail;
use tokio::{sync::mpsc, task::JoinHandle};

use crate::runtime::{ModuleInstanceId, RuntimeEvent};

pub struct InstancedMqttRuntime {
    runtime_event_sender: mpsc::Sender<RuntimeEvent>,
    task_handle: JoinHandle<anyhow::Result<()>>,
}

impl InstancedMqttRuntime {
    async fn event_loop_task(
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

    pub fn new(
        event_loop: rumqttc::EventLoop,
        event_channel_sender: mpsc::Sender<rumqttc::Event>,
    ) -> Self {
        let (mqtt_event_loop_runtime_sender, mqtt_event_loop_runtime_receiver) = mpsc::channel(32);

        let mqtt_event_loop_task_handle = tokio::spawn(async move {
            Self::event_loop_task(
                event_channel_sender,
                mqtt_event_loop_runtime_receiver,
                event_loop,
            )
            .await
        });

        InstancedMqttRuntime {
            runtime_event_sender: mqtt_event_loop_runtime_sender,
            task_handle: mqtt_event_loop_task_handle,
        }
    }

    pub async fn cleanup(self, module_instance_id: ModuleInstanceId) {
        if let Err(e) = self
            .runtime_event_sender
            .send(RuntimeEvent::RuntimeTaskStop)
            .await
        {
            log::error!("Error sending runtime task stop to instanced mqtt runtime for module instance {module_instance_id}, aborting: {e}");
            self.task_handle.abort();
        } else if let Err(e) = self.task_handle.await {
            log::error!("Error waiting on instanced event loop task for instance {module_instance_id} to finish: {e}");
        } else {
            log::debug!(
                "Module instance {module_instance_id} instanced mqtt event loop sucessfully shut down"
            );
        }
    }
}
