use anyhow::Context;
use std::collections::HashMap;
use std::io::SeekFrom;
use std::path::{Path, PathBuf};
use tokio::fs::{self, File};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

wasmtime::component::bindgen!({
    path: "../../wit-bindgen/apis.wit",
    async: true,
});

pub use fio::add_to_linker;

use crate::runtime::FileIORuntimeConfig;

pub struct FileIOState {
    allowed_write_files: Vec<PathBuf>,
    allowed_write_directories: Vec<PathBuf>,
    allowed_read_files: Vec<PathBuf>,
    allowed_read_directories: Vec<PathBuf>,
    open_file_handles: HashMap<PathBuf, File>,
}

impl FileIOState {
    pub fn from_config(fio_config: &FileIORuntimeConfig) -> Self {
        let FileIORuntimeConfig {
            allowed_write_files,
            allowed_write_directories,
            allowed_read_files,
            allowed_read_directories,
        } = fio_config;

        let allowed_write_files = allowed_write_files.iter().map(PathBuf::from).collect();
        let allowed_write_directories = allowed_write_directories
            .iter()
            .map(PathBuf::from)
            .collect();
        let allowed_read_files = allowed_read_files.iter().map(PathBuf::from).collect();
        let allowed_read_directories = allowed_read_directories.iter().map(PathBuf::from).collect();

        Self {
            allowed_write_files,
            allowed_write_directories,
            allowed_read_files,
            allowed_read_directories,
            open_file_handles: HashMap::new(),
        }
    }
}

fn is_file_operation_allowed(
    file_path: impl AsRef<Path>,
    allowed_directories: &[PathBuf],
    allowed_files: &[PathBuf],
) -> bool {
    allowed_files
        .iter()
        .any(|p| p.as_path() == file_path.as_ref())
        || allowed_directories
            .iter()
            .any(|f| file_path.as_ref().starts_with(f))
}

#[async_trait::async_trait]
impl fio::Fio for FileIOState {
    async fn read_bytes(
        &mut self,
        file_path: String,
        num_bytes: u64,
    ) -> anyhow::Result<Result<Vec<u8>, String>> {
        let path = match fs::canonicalize(&file_path).await {
            Ok(path) => path,
            Err(_) => return Ok(Err("Missing permissions".to_string())),
        };
        let num_bytes: usize = match num_bytes.try_into().with_context(|| {
            format!(
                "requested read size {num_bytes} exceeds host usize max {}",
                usize::MAX
            )
        }) {
            Ok(num_bytes) => num_bytes,
            Err(e) => return Ok(Err(e.to_string())),
        };

        let mut buff = vec![0; num_bytes];
        if is_file_operation_allowed(
            &path,
            &self.allowed_read_directories,
            &self.allowed_write_files,
        ) {
            let file = match self.open_file_handles.get_mut(&path) {
                Some(f) => f,
                None => {
                    if !path.exists() {
                        return Ok(Err(format!(
                            "requested read to {file_path} references path that does not exist"
                        )));
                    } else if !path.is_file() {
                        return Ok(Err(format!(
                            "requested read to {file_path} references path that is not a file"
                        )));
                    }

                    let f = File::open(path.clone()).await.with_context(|| {
                        format!("Error in fio runtime reading {}", path.display())
                    })?;
                    self.open_file_handles
                        .entry(path.clone())
                        .insert_entry(f)
                        .into_mut()
                }
            };

            if let Err(e) = file
                .read_exact(&mut buff[0..num_bytes])
                .await
                .with_context(|| format!("requested read to {file_path} failed"))
            {
                return Ok(Err(e.to_string()));
            }
        } else {
            return Ok(Err("Missing permissions".to_string()));
        }
        Ok(Ok(buff))
    }

    async fn seek_bytes(
        &mut self,
        file_path: String,
        seek_motion: fio::SeekMotion,
    ) -> anyhow::Result<Result<u64, String>> {
        let path = match fs::canonicalize(&file_path).await {
            Ok(path) => path,
            Err(_) => return Ok(Err("Missing permissions".to_string())),
        };
        if is_file_operation_allowed(
            &path,
            &self.allowed_read_directories,
            &self.allowed_read_files,
        ) {
            let file = match self.open_file_handles.get_mut(&path) {
                Some(f) => f,
                None => {
                    if !path.exists() {
                        return Ok(Err(format!(
                            "requested seek on {file_path} references path that does not exist"
                        )));
                    } else if !path.is_file() {
                        return Ok(Err(format!(
                            "requested seek on {file_path} references path that is not a file"
                        )));
                    }

                    let f = File::open(path.clone()).await?;
                    self.open_file_handles
                        .entry(path.clone())
                        .insert_entry(f)
                        .into_mut()
                }
            };

            let new_position = {
                (match seek_motion {
                    fio::SeekMotion::FromStart(bytes) => file.seek(SeekFrom::Start(bytes)).await,
                    fio::SeekMotion::FromEnd(bytes) => file.seek(SeekFrom::End(bytes)).await,
                    fio::SeekMotion::Forwards(bytes) => file.seek(SeekFrom::Current(bytes)).await,
                    fio::SeekMotion::Backwards(bytes) => file.seek(SeekFrom::Current(bytes)).await,
                })
                .with_context(|| format!("requested seek on {file_path} failed"))
                .map_err(|e| e.to_string())
            };

            Ok(new_position)
        } else {
            return Ok(Err("Missing permissions".to_string()));
        }
    }

    async fn write_bytes(
        &mut self,
        file_path: String,
        buffer: Vec<u8>,
    ) -> anyhow::Result<Result<(), String>> {
        let path = match fs::canonicalize(&file_path).await {
            Ok(path) => path,
            Err(_) => return Ok(Err("Missing permissions".to_string())),
        };

        if is_file_operation_allowed(
            &path,
            &self.allowed_write_directories,
            &self.allowed_write_files,
        ) {
            let file = match self.open_file_handles.get_mut(&path) {
                Some(f) => f,
                None => {
                    if !path.exists() {
                        return Ok(Err(format!(
                            "requested write to {file_path} references path that does not exist"
                        )));
                    } else if !path.is_file() {
                        return Ok(Err(format!(
                            "requested write to {file_path} references path that is not a file",
                        )));
                    }

                    let f = File::open(path.clone()).await.with_context(|| {
                        format!("Error in fio runtime writing {}", path.display())
                    })?;
                    self.open_file_handles
                        .entry(path)
                        .insert_entry(f)
                        .into_mut()
                }
            };
            file.write_all(&buffer).await?;
            Ok(Ok(()))
        } else {
            return Ok(Err("Missing permissions".to_string()));
        }
    }

    async fn append_bytes(
        &mut self,
        file_path: String,
        buffer: Vec<u8>,
    ) -> anyhow::Result<Result<(), String>> {
        let path = match fs::canonicalize(&file_path).await {
            Ok(path) => path,
            Err(_) => return Ok(Err("Missing permissions".to_string())),
        };
        if is_file_operation_allowed(
            &path,
            &self.allowed_write_directories,
            &self.allowed_write_files,
        ) {
            let file = match self.open_file_handles.get_mut(&path) {
                Some(f) => f,
                None => {
                    if !path.exists() {
                        return Ok(Err(format!(
                            "requested read to {file_path} references path that does not exist",
                        )));
                    } else if !path.is_file() {
                        return Ok(Err(format!(
                            "requested read to {file_path} references path that is not a file",
                        )));
                    }

                    let f = File::open(path.clone()).await.with_context(|| {
                        format!("Error in fio runtime appending to {}", path.display())
                    })?;
                    self.open_file_handles
                        .entry(path)
                        .insert_entry(f)
                        .into_mut()
                }
            };
            file.seek(SeekFrom::End(0)).await?;
            file.write_all(&buffer).await?;

            Ok(Ok(()))
        } else {
            return Ok(Err("Missing permissions".to_string()));
        }
    }
}

#[async_trait::async_trait]
impl<F: fio::Fio + Send + Sync> fio::Fio for Option<F> {
    async fn read_bytes(
        &mut self,
        file_path: String,
        num_bytes: u64,
    ) -> anyhow::Result<Result<Vec<u8>, String>> {
        match self {
            Some(v) => v.read_bytes(file_path, num_bytes).await,
            None => Ok(Err("module is missing fio runtime".to_string())),
        }
    }

    async fn seek_bytes(
        &mut self,
        file_path: String,
        seek_motion: fio::SeekMotion,
    ) -> anyhow::Result<Result<u64, String>> {
        match self {
            Some(v) => v.seek_bytes(file_path, seek_motion).await,
            None => Ok(Err("module is missing fio runtime".to_string())),
        }
    }

    async fn write_bytes(
        &mut self,
        file_path: String,
        buffer: Vec<u8>,
    ) -> anyhow::Result<Result<(), String>> {
        match self {
            Some(v) => v.write_bytes(file_path, buffer).await,
            None => Ok(Err("module is missing fio runtime".to_string())),
        }
    }

    async fn append_bytes(
        &mut self,
        file_path: String,
        buffer: Vec<u8>,
    ) -> anyhow::Result<Result<(), String>> {
        match self {
            Some(v) => v.append_bytes(file_path, buffer).await,
            None => Ok(Err("module is missing fio runtime".to_string())),
        }
    }
}
