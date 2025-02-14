#![deny(missing_docs)]

//! A Key-Value store, using an on-disk serialized log for persistence.

pub mod append_log;

use append_log::{AppendLog, LogCommand};
use failure::{Error, Fail};
use std::fs::{self, OpenOptions};
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

/// The result type used for KvStore.
pub type Result<T> = std::result::Result<T, Error>;

#[derive(Fail, Debug)]
#[fail(display = "Key not found: {}", key)]
/// Error returned for get and remove when the key is not found.
pub struct KeyNotFoundError {
    key: String,
}

#[derive(Fail, Debug)]
#[fail(display = "Internal Key-Value Error")]
/// Error returned for get and remove when the key is not found.
pub struct InternalKvError;

#[derive(Fail, Debug)]
#[fail(display = "Path is not a directory: {:?}", dir)]
/// Error returned for get and remove when the key is not found.
pub struct InvalidPathError {
    dir: PathBuf,
}

const KV_FILE_PREFIX: &str = "kv_store.log";

/// A persistant Sting based Key-Value store.
pub struct KvStore {
    /// Log representation of the on-disk file.
    log: Arc<RwLock<AppendLog>>,
    log_file: PathBuf,
}

impl KvStore {
    /// Finds all files in the dir that have the prefix of KV_FILE_PREFIX, and returns the path to the one with the largest suffix.
    fn locate_kv_file(dir: &Path) -> Result<Option<PathBuf>> {
        let mut candidates = Vec::new();
        for dent in dir.read_dir()? {
            let p = dent?.path();
            if let Some(s) = p.file_name() {
                if let Some(s) = s.to_str() {
                    if s.starts_with(KV_FILE_PREFIX) {
                        candidates.push(p);
                    }
                }
            };
        }

        let mut p = None;
        let mut max = 0;

        for c in candidates {
            let c_name = c.to_string_lossy();
            let s: Vec<&str> = c_name.rsplit('.').collect();
            if s.len() > 1 {
                if let Ok(idx) = s[0].parse() {
                    if idx > max {
                        max = idx;
                        let mut pb = dir.to_path_buf();
                        pb.push(c);
                        p = Some(pb);
                    }
                }
            }
        }

        Ok(p)
    }

    /// Open a KvStore for a given path. If the path is a directory then a file will be created in this directory.
    /// If the path does not exist then a file will be created and initialized at that location.
    pub fn open(path: &Path) -> Result<KvStore> {
        // TODO - this should just take a directory and we will create multiple files in there for the log.
        if !path.exists() || !path.is_dir() {
            return Err(Error::from(InvalidPathError {
                dir: path.to_owned(),
            }));
        }

        let log_file = match KvStore::locate_kv_file(&path)? {
            Some(f) => f,
            None => {
                let mut pb = path.to_owned();
                let mut filename = String::from(KV_FILE_PREFIX);
                filename.push_str(".0");
                pb.push(filename);
                eprintln!("No files found, starting new one: {:?}", pb);
                pb
            }
        };

        eprintln!("Using KV Log File: {:?}", log_file);
        if !log_file.exists() {
            OpenOptions::new()
                .create(true)
                .append(true)
                .open(&log_file)?;
        }

        let log = AppendLog::load(&log_file)?;

        let store = KvStore {
            log: Arc::new(RwLock::new(log)),
            log_file,
        };
        // store.compact_log()?;
        Ok(store)
    }

    /// Get the value associated with the provided key, or None otherwise.
    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        match self.log.read().unwrap().fetch_by_key(key.as_bytes())? {
            Some(bytes) => Ok(Some(String::from_utf8(bytes.to_vec())?)),
            None => Ok(None),
        }
    }

    /// Set a value for a given key, overriding a previously set value if it exists.
    pub fn set(&mut self, key: String, val: String) -> Result<()> {
        self.log
            .write()
            .unwrap()
            .append(LogCommand::Set, key.as_bytes(), Some(val.as_bytes()))?;
        self.try_compact()
    }

    /// Remove a key and value from the store.
    pub fn remove(&mut self, key: String) -> Result<()> {
        let k = key.as_bytes();

        {
            let mut l = self.log.write().unwrap();

            if !l.contains(k) {
                return Err(Error::from(KeyNotFoundError { key }));
            }

            l.append(LogCommand::Remove, k, None)?;
        }
        self.try_compact()
    }

    fn try_compact(&mut self) -> Result<()> {
        // Compact when the log is more than 10x the index entries.
        {
            let l = self.log.read().unwrap();
            if l.len() < 10 * l.index_len() {
                return Ok(());
            }
        }
        self.compact_log()
    }

    /// Compacts the log to a new file.
    pub fn compact_log(&mut self) -> Result<()> {
        let name = self.log_file.file_name().unwrap().to_string_lossy();
        let s: Vec<&str> = name.rsplit('.').collect();
        let mut idx: u64 = s[0].parse()?;
        idx += 1;
        let i = idx.to_string();
        let mut new_name = String::from(KV_FILE_PREFIX);
        new_name.push_str(".");
        new_name.push_str(i.as_str());
        eprintln!("New Log Name: {}", new_name);

        let mut new_log = PathBuf::from(&self.log_file);
        new_log.set_file_name(new_name);
        self.log.write().unwrap().compact(&new_log)?;

        fs::remove_file(self.log_file.to_owned())?;
        self.log_file = new_log;

        Ok(())
    }
}

impl Clone for KvStore {
    fn clone(&self) -> Self {
        KvStore {
            log: self.log.clone(),
            log_file: self.log_file.clone(),
        }
    }
}

impl Drop for KvStore {
    fn drop(&mut self) {
        self.try_compact().unwrap();
    }
}
