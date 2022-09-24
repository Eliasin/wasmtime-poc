use std::collections::HashMap;
use std::fs::{canonicalize, File};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::PathBuf;

use wit_bindgen_host_wasmtime_rust::export;

export!("./wit-bindgen/fio.wit");

pub struct FileIOState {
    allowed_write_files: Vec<PathBuf>,
    allowed_write_folders: Vec<PathBuf>,
    allowed_read_files: Vec<PathBuf>,
    allowed_read_folders: Vec<PathBuf>,
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
            allowed_write_folders,
            allowed_read_files,
            allowed_read_folders,
            open_file_handles: HashMap::new(),
        }
    }
}

impl fio::Fio for FileIOState {
    fn read_bytes(&mut self, file_path: &str) -> Result<Vec<u8>, String> {
        let path = canonicalize(file_path).unwrap();
        let buff: &mut [u8] = &mut [];
        if self.allowed_read_files.contains(&path)
            || self
                .allowed_read_folders
                .iter()
                .any(|f| path.starts_with(f))
        {
            let mut file = match self.open_file_handles.get(&path) {
                Some(f) => f,
                None => {
                    let f = File::open(path.clone()).map_err(|e| e.to_string())?;
                    self.open_file_handles.insert(path.clone(), f);
                    &self.open_file_handles.get(&path).unwrap()
                }
            };
            file.read(buff).map_err(|e| e.to_string())?;
        } else {
            return Err("File not allowed to be read".to_string());
        }
        Ok(buff.to_vec())
    }

    fn seek_bytes(&mut self, file_path: &str, seek_motion: fio::SeekMotion) -> Result<u64, String> {
        let path = canonicalize(file_path).unwrap();
        if self.allowed_read_files.contains(&path)
            || self
                .allowed_read_folders
                .iter()
                .any(|f| path.starts_with(f))
        {
            let mut file = match self.open_file_handles.get(&path) {
                Some(f) => f,
                None => {
                    let f = File::open(path.clone()).map_err(|e| e.to_string())?;
                    self.open_file_handles.insert(path.clone(), f);
                    &self.open_file_handles.get(&path).unwrap()
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
        let path = canonicalize(file_path).unwrap();
        if self.allowed_write_files.contains(&path)
            || self
                .allowed_write_folders
                .iter()
                .any(|f| path.starts_with(f))
        {
            let mut file = match self.open_file_handles.get(&path) {
                Some(f) => f,
                None => {
                    let f = File::open(path.clone()).map_err(|e| e.to_string())?;
                    self.open_file_handles.insert(path.clone(), f);
                    &self.open_file_handles.get(&path).unwrap()
                }
            };
            file.write_all(buffer).map_err(|e| e.to_string())
        } else {
            return Err("File not allowed to be written to".to_string());
        }
    }

    fn append_bytes(&mut self, file_path: &str, buffer: &[u8]) -> Result<(), String> {
        let path = canonicalize(file_path).unwrap();
        if self.allowed_write_files.contains(&path)
            || self
                .allowed_write_folders
                .iter()
                .any(|f| path.starts_with(f))
        {
            let mut file = match self.open_file_handles.get(&path) {
                Some(f) => f,
                None => {
                    let f = File::open(path.clone()).map_err(|e| e.to_string())?;
                    self.open_file_handles.insert(path.clone(), f);
                    &self.open_file_handles.get(&path).unwrap()
                }
            };
            file.seek(SeekFrom::End(0)).map_err(|e| e.to_string())?;
            file.write_all(buffer).map_err(|e| e.to_string())
        } else {
            return Err("File not allowed to be written to".to_string());
        }
    }
}
