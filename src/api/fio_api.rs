use std::collections::HashMap;
use std::fs;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use wit_bindgen_host_wasmtime_rust::export;

export!("./wit-bindgen/fio.wit");

pub struct FileIOState {
    allowed_write_files: Vec<PathBuf>,
    allowed_write_directories: Vec<PathBuf>,
    allowed_read_files: Vec<PathBuf>,
    allowed_read_directories: Vec<PathBuf>,
    open_file_handles: HashMap<PathBuf, File>,
}

impl FileIOState {
    pub fn new(
        allowed_write_files: Vec<PathBuf>,
        allowed_write_folders: Vec<PathBuf>,
        allowed_read_files: Vec<PathBuf>,
        allowed_read_folders: Vec<PathBuf>,
    ) -> FileIOState {
        FileIOState {
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

impl fio::Fio for FileIOState {
    fn read_bytes(&mut self, file_path: &str, num_bytes: u64) -> Result<Vec<u8>, String> {
        let path = fs::canonicalize(file_path).map_err(|_| format!("IO Error"))?;
        let num_bytes: usize = num_bytes.try_into().map_err(|e| {
            format!(
                "Error while converting requested num_bytes into usize: {}",
                e
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
                    let f = File::open(path.clone()).map_err(|e| e.to_string())?;
                    self.open_file_handles.insert(path.clone(), f);
                    &self
                        .open_file_handles
                        .get(&path)
                        .expect("Value was just inserted")
                }
            };
            file.read(&mut buff[0..num_bytes])
                .map_err(|e| e.to_string())?;
        } else {
            return Err("IO Error".to_string());
        }
        Ok(buff)
    }

    fn seek_bytes(&mut self, file_path: &str, seek_motion: fio::SeekMotion) -> Result<u64, String> {
        let path = fs::canonicalize(file_path).map_err(|_| "IO Error")?;
        if is_file_operation_allowed(
            &path,
            &self.allowed_read_directories,
            &self.allowed_read_files,
        ) {
            let mut file = match self.open_file_handles.get(&path) {
                Some(f) => f,
                None => {
                    let f = File::open(path.clone()).map_err(|e| e.to_string())?;
                    self.open_file_handles.insert(path.clone(), f);
                    &self
                        .open_file_handles
                        .get(&path)
                        .expect("Value was just inserted")
                }
            };
            Ok(match seek_motion {
                fio::SeekMotion::FromStart(amt) => {
                    file.seek(SeekFrom::Start(amt)).map_err(|e| e.to_string())?
                }
                fio::SeekMotion::FromEnd(amt) => {
                    file.seek(SeekFrom::End(amt)).map_err(|e| e.to_string())?
                }
                fio::SeekMotion::Forwards(amt) => file
                    .seek(SeekFrom::Current(amt))
                    .map_err(|e| e.to_string())?,
                fio::SeekMotion::Backwards(amt) => file
                    .seek(SeekFrom::Current(amt))
                    .map_err(|e| e.to_string())?,
            })
        } else {
            return Err("File not allowed to be read".to_string());
        }
    }

    fn write_bytes(&mut self, file_path: &str, buffer: &[u8]) -> Result<(), String> {
        let path = fs::canonicalize(file_path).map_err(|_| "IO Error")?;
        if is_file_operation_allowed(
            &path,
            &self.allowed_write_directories,
            &self.allowed_write_files,
        ) {
            let mut file = match self.open_file_handles.get(&path) {
                Some(f) => f,
                None => {
                    let f = File::open(path.clone()).map_err(|e| e.to_string())?;
                    self.open_file_handles.insert(path.clone(), f);
                    &self
                        .open_file_handles
                        .get(&path)
                        .expect("Value was just inserted")
                }
            };
            file.write_all(buffer).map_err(|e| e.to_string())
        } else {
            return Err("IO Error".to_string());
        }
    }

    fn append_bytes(&mut self, file_path: &str, buffer: &[u8]) -> Result<(), String> {
        let path = fs::canonicalize(file_path).map_err(|_| "IO Error")?;
        if is_file_operation_allowed(
            &path,
            &self.allowed_write_directories,
            &self.allowed_write_files,
        ) {
            let mut file = match self.open_file_handles.get(&path) {
                Some(f) => f,
                None => {
                    let f = File::open(path.clone()).map_err(|e| e.to_string())?;
                    self.open_file_handles.insert(path.clone(), f);
                    &self
                        .open_file_handles
                        .get(&path)
                        .expect("Value was just inserted")
                }
            };
            file.seek(SeekFrom::End(0)).map_err(|e| e.to_string())?;
            file.write_all(buffer).map_err(|e| e.to_string())
        } else {
            return Err("File not allowed to be written to".to_string());
        }
    }
}
