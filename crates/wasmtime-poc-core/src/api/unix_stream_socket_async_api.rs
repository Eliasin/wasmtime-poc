use anyhow::bail;
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use std::collections::HashMap;
use tokio::io::AsyncReadExt;
use tokio::net::UnixStream;

wasmtime::component::bindgen!({
    path: "../../wit-bindgen/apis.wit",
    async: true,
});

pub use unix_stream_socket::add_to_linker;
pub use unix_stream_socket::SocketMessage;

use crate::runtime::UnixStreamSocketConfig;

pub struct UnixStreamSocketAsyncState {
    allowed_socket_paths: Vec<String>,
    sockets: HashMap<String, UnixStream>,
}

impl UnixStreamSocketAsyncState {
    pub fn from_config(config: &UnixStreamSocketConfig) -> Self {
        Self {
            allowed_socket_paths: config.allowed_socket_paths.clone(),
            sockets: HashMap::new(),
        }
    }
}

#[async_trait::async_trait]
impl unix_stream_socket::UnixStreamSocket for UnixStreamSocketAsyncState {
    async fn connect(&mut self, socket_path: String) -> anyhow::Result<Result<(), String>> {
        if !self.allowed_socket_paths.contains(&socket_path) {
            return Ok(Err(format!(
                "socket path '{socket_path}' is not in this module's allow list"
            )));
        } else {
            let socket = match UnixStream::connect(&socket_path).await {
                Ok(socket) => socket,
                Err(e) => return Ok(Err(format!("error opening unix stream socket: {e}"))),
            };

            self.sockets.insert(socket_path, socket);

            Ok(Ok(()))
        }
    }

    async fn poll_sockets(&mut self) -> anyhow::Result<Option<SocketMessage>> {
        let mut readable_futures = FuturesUnordered::new();

        for (socket_path, socket) in self.sockets.iter_mut() {
            readable_futures.push(async move {
                if let Err(e) = socket.readable().await {
                    log::error!("error while checking socket readability {e}");
                    None
                } else {
                    Some((socket_path, socket))
                }
            });
        }

        while let Some(readable) = readable_futures.next().await {
            if let Some((readable_socket_path, readable_socket)) = readable {
                let mut buf = [0; 256];

                match readable_socket.read(&mut buf).await {
                    Ok(bytes_read) => {
                        return Ok(Some(SocketMessage {
                            socket_path: readable_socket_path.clone(),
                            data: buf[0..bytes_read].to_vec(),
                        }));
                    }
                    Err(e) => log::error!("socket read error: {e}"),
                }
            }
        }

        Ok(None)
    }
}

#[async_trait::async_trait]
impl<U: unix_stream_socket::UnixStreamSocket + Send + Sync> unix_stream_socket::UnixStreamSocket
    for Option<U>
{
    async fn connect(&mut self, socket_path: String) -> anyhow::Result<Result<(), String>> {
        match self {
            Some(socket_state) => socket_state.connect(socket_path).await,
            None => bail!("no unix stream sockets are configured for this module"),
        }
    }

    async fn poll_sockets(&mut self) -> anyhow::Result<Option<SocketMessage>> {
        match self {
            Some(socket_state) => socket_state.poll_sockets().await,
            None => bail!("no unix stream sockets are configured for this module"),
        }
    }
}
