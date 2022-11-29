use anyhow::{anyhow, Context};
use std::collections::HashMap;
use std::fs;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

wit_bindgen_host_wasmtime_rust::generate!({
    path: "./wit-bindgen/apis.wit",
    async: true,
});

pub use fio::add_to_linker;

pub struct AsyncFileIOState {
    allowed_write_files: Vec<PathBuf>,
    allowed_write_directories: Vec<PathBuf>,
    allowed_read_files: Vec<PathBuf>,
    allowed_read_directories: Vec<PathBuf>,
    open_file_handles: HashMap<PathBuf, File>,
}

impl AsyncFileIOState {
    pub fn new(
        allowed_write_files: Vec<PathBuf>,
        allowed_write_folders: Vec<PathBuf>,
        allowed_read_files: Vec<PathBuf>,
        allowed_read_folders: Vec<PathBuf>,
    ) -> AsyncFileIOState {
        AsyncFileIOState {
            allowed_write_files,
            allowed_write_directories: allowed_write_folders,
            allowed_read_files,
            allowed_read_directories: allowed_read_folders,
            open_file_handles: HashMap::new(),
        }
    }
}

fn is_file_operation_allowed(
    file_path: impl AsRef<Path>,
    allowed_directories: &Vec<PathBuf>,
    allowed_files: &Vec<PathBuf>,
) -> bool {
    allowed_files
        .iter()
        .any(|p| p.as_path() == file_path.as_ref())
        || allowed_directories
            .iter()
            .any(|f| file_path.as_ref().starts_with(f))
}

#[wit_bindgen_host_wasmtime_rust::async_trait]
impl fio::Fio for AsyncFileIOState {
    async fn read_bytes(&mut self, file_path: String, num_bytes: u64) -> anyhow::Result<Vec<u8>> {
        let path = fs::canonicalize(file_path).map_err(|_| anyhow!("Missing permissions"))?;
        let num_bytes: usize = num_bytes.try_into().with_context(|| {
            format!(
                "num_bytes requested '{}' exceeds host usize max {}",
                num_bytes,
                usize::MAX
            )
        })?;

        let mut buff = vec![0; num_bytes];
        if self.allowed_read_files.contains(&path)
            || self
                .allowed_read_directories
                .iter()
                .any(|f| path.starts_with(f))
        {
            let mut file = match self.open_file_handles.get(&path) {
                Some(f) => f,
                None => {
                    let f = File::open(path.clone())?;
                    self.open_file_handles.insert(path.clone(), f);
                    &self
                        .open_file_handles
                        .get(&path)
                        .expect("Value was just inserted")
                }
            };
            file.read(&mut buff[0..num_bytes])?;
        } else {
            return Err(anyhow!("Missing permissions"));
        }
        Ok(buff)
    }

    async fn seek_bytes(
        &mut self,
        file_path: String,
        seek_motion: fio::SeekMotion,
    ) -> anyhow::Result<u64> {
        let path = fs::canonicalize(file_path).map_err(|_| anyhow!("Missing permissions"))?;
        if is_file_operation_allowed(
            &path,
            &self.allowed_read_directories,
            &self.allowed_read_files,
        ) {
            let mut file = match self.open_file_handles.get(&path) {
                Some(f) => f,
                None => {
                    let f = File::open(path.clone())?;
                    self.open_file_handles.insert(path.clone(), f);
                    &self
                        .open_file_handles
                        .get(&path)
                        .expect("Value was just inserted")
                }
            };
            Ok(match seek_motion {
                fio::SeekMotion::FromStart(bytes) => file.seek(SeekFrom::Start(bytes))?,
                fio::SeekMotion::FromEnd(bytes) => file.seek(SeekFrom::End(bytes))?,
                fio::SeekMotion::Forwards(bytes) => file.seek(SeekFrom::Current(bytes))?,
                fio::SeekMotion::Backwards(bytes) => file.seek(SeekFrom::Current(bytes))?,
            })
        } else {
            return Err(anyhow!("Missing permissions"));
        }
    }

    async fn write_bytes(&mut self, file_path: String, buffer: Vec<u8>) -> anyhow::Result<()> {
        let path = fs::canonicalize(file_path).map_err(|_| anyhow!("Missing permissions"))?;
        if is_file_operation_allowed(
            &path,
            &self.allowed_write_directories,
            &self.allowed_write_files,
        ) {
            let mut file = match self.open_file_handles.get(&path) {
                Some(f) => f,
                None => {
                    let f = File::open(path.clone())?;
                    self.open_file_handles.insert(path.clone(), f);
                    &self
                        .open_file_handles
                        .get(&path)
                        .expect("Value was just inserted")
                }
            };
            file.write_all(&buffer)?;
            Ok(())
        } else {
            return Err(anyhow!("Missing permissions"));
        }
    }

    async fn append_bytes(&mut self, file_path: String, buffer: Vec<u8>) -> anyhow::Result<()> {
        let path = fs::canonicalize(file_path).map_err(|_| anyhow!("Missing permissions"))?;
        if is_file_operation_allowed(
            &path,
            &self.allowed_write_directories,
            &self.allowed_write_files,
        ) {
            let mut file = match self.open_file_handles.get(&path) {
                Some(f) => f,
                None => {
                    let f = File::open(path.clone())?;
                    self.open_file_handles.insert(path.clone(), f);
                    &self
                        .open_file_handles
                        .get(&path)
                        .expect("Value was just inserted")
                }
            };
            file.seek(SeekFrom::End(0))?;
            file.write_all(&buffer)?;

            Ok(())
        } else {
            return Err(anyhow!("Missing permissions"));
        }
    }
}
