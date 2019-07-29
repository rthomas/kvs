#![deny(missing_docs)]

//! A Key-Value store, using an on-disk serialized log for persistence.

use failure::{Error, Fail};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Seek, SeekFrom};
use std::path::{Path, PathBuf};

mod log;

/// The result type used for KvStore.
pub type Result<T> = std::result::Result<T, Error>;

#[derive(Clone, Debug, Serialize, Deserialize)]
enum LogCommand {
    Set,
    Remove,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct LogEntry {
    cmd: LogCommand,
    key: String,
    val: Option<String>,
}

impl LogEntry {
    fn new(cmd: LogCommand, key: String, val: Option<String>) -> LogEntry {
        LogEntry { cmd, key, val }
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct Log {
    entries: Vec<LogEntry>,
}

impl Log {
    fn new() -> Log {
        Log {
            entries: Vec::new(),
        }
    }

    fn append(&mut self, e: LogEntry) {
        self.entries.push(e);
    }
}

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

const DEFAULT_FILE_PREFIX: &str = "kv_store.json";
const COMPACTION_THRESHOLD: usize = 4;

/// A persistant Sting based Key-Value store.
pub struct KvStore {
    log_file: File,
    /// Log representation of the on-disk file.
    log: Log,
    /// The length of the log entries that were read off of disk.
    log_len: usize,
    /// An in-memory reference from a key to an index in the log for the Set command of the key.
    store: HashMap<String, usize>,
}

impl KvStore {
    /// Open a KvStore.
    ///
    /// The path provided should be a directory, otherwise an error will be returned.
    /// If the path does not exist it will be created.
    ///
    /// Files in this path (if it exists) will be loaded into the in-memory index.
    pub fn open(path: &Path) -> Result<KvStore> {
        if !path.is_dir() {
            return Err(Error::from(InvalidPathError {
                dir: path.to_owned(),
            }));
        }
        unimplemented!()
    }
    /// Open a KvStore for a given path. If the path is a directory then a file will be created in this directory.
    /// If the path does not exist then a file will be created and initialized at that location.
    pub fn old_open(path: &Path) -> Result<KvStore> {
        // TODO - this should just take a directory and we will create multiple files in there for the log.
        let mut buf = path.to_path_buf();
        buf.push(DEFAULT_FILE_PREFIX);

        let mut file_path = path;
        if path.is_dir() {
            file_path = buf.as_path();
        }
        if !file_path.exists() {
            // Lets seed this with an empty store if the file doesn't exist.
            let f = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open(&file_path)?;
            let writer = BufWriter::new(&f);
            serde_json::to_writer(writer, &Log::new())?;
        }
        let f = OpenOptions::new()
            .read(true)
            .write(true)
            .create(false)
            .open(&file_path)?;
        let reader = BufReader::new(&f);
        let log: Log = serde_json::from_reader(reader)?;
        let index = KvStore::build_index(&log);

        let store = KvStore {
            log_file: f,
            log_len: log.entries.len(),
            log: log,
            store: index,
        };
        // store.compact_log()?;
        Ok(store)
    }

    /// Get the value associated with the provided key, or None otherwise.
    pub fn get(&self, key: String) -> Result<Option<String>> {
        match self.store.get(&key) {
            Some(idx) => match self.log.entries.get(*idx) {
                Some(log_entry) => Ok(log_entry.val.to_owned()),
                None => Err(Error::from(InternalKvError {})),
            },
            None => Ok(None),
        }
    }

    /// Set a value for a given key, overriding a previously set value if it exists.
    pub fn set(&mut self, key: String, val: String) -> Result<()> {
        let entry = LogEntry::new(LogCommand::Set, key.clone(), Some(val));
        self.log.entries.push(entry);
        self.store.insert(key, self.log.entries.len() - 1);
        self.try_compaction()?;
        Ok(())
    }

    /// Remnove a key and value from the store.
    pub fn remove(&mut self, key: String) -> Result<()> {
        if !self.store.contains_key(&key) {
            return Err(Error::from(KeyNotFoundError { key }));
        }
        self.store.remove(&key);
        let entry = LogEntry::new(LogCommand::Remove, key, None);
        self.log.entries.push(entry);
        self.try_compaction()?;
        Ok(())
    }

    fn build_index(log: &Log) -> HashMap<String, usize> {
        let mut index = HashMap::new();
        for (idx, entry) in log.entries.iter().enumerate() {
            match &entry.cmd {
                LogCommand::Set => {
                    index.insert(entry.key.to_string(), idx);
                }
                LogCommand::Remove => {
                    index.remove(&entry.key);
                }
            }
        }
        index
    }

    /// Attempts to compact the log if there have been more than COMPACTION_THRESHOLD mutations since it was loaded.
    fn try_compaction(&mut self) -> Result<()> {
        if self.log.entries.len() - self.log_len < COMPACTION_THRESHOLD {
            // Nothing to do here...
            return Ok(());
        }
        // TODO - need to compact to another file and restructure this. Compacting to the existing files doesnt work without clearing it first.
        self.compact_log()?;
        self.flush_to_disk()?;
        Ok(())
    }

    /// Compaction will play all of the Set commands from the memory index into a new log, and then from that log generate a new in-memory index for it.
    fn compact_log(&mut self) -> Result<()> {
        let mut new_log = Log::new();
        for (_, idx) in &self.store {
            match self.log.entries.get(*idx) {
                Some(log_entry) => {
                    new_log.append(log_entry.clone());
                }
                None => {
                    return Err(Error::from(InternalKvError {}));
                }
            }
        }
        let old_len = self.log.entries.len();
        let new_index = KvStore::build_index(&new_log);
        self.log_len = new_log.entries.len();
        self.log = new_log;
        self.store = new_index;

        let new_len = self.log.entries.len();

        println!("Log Compacted from {} --> {}", old_len, new_len);
        Ok(())
    }

    /// Appends the unwritten logs to disk.
    fn flush_to_disk(&mut self) -> Result<()> {
        if self.log.entries.len() == 0 {
            // If there's nothing here or we haven't written anything then skip it.
            return Ok(());
        }
        self.log_file.seek(SeekFrom::Start(0))?;
        // TODO - actually append here, rather than just writing out the entire file...
        let writer = BufWriter::new(&self.log_file);
        serde_json::to_writer(writer, &self.log)?;
        self.log_len = self.log.entries.len();
        Ok(())
    }
}

impl Drop for KvStore {
    fn drop(&mut self) {
        match self.flush_to_disk() {
            Ok(_) => {}
            Err(e) => {
                eprintln!(
                    "Could not flush to disk on drop - nothing we can do here... {}",
                    e
                );
            }
        }
    }
}
